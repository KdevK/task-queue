package worker

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"task-queue/internal/broker"
	"task-queue/internal/domain"
	"task-queue/internal/storage"
)

type Pool struct {
	broker      broker.Broker
	repo        storage.Repository
	registry    *Registry
	numWorkers  int
	taskTimeout time.Duration
	logger      *slog.Logger
}

func NewPool(
	b broker.Broker,
	repo storage.Repository,
	registry *Registry,
	numWorkers int,
	taskTimeout time.Duration,
	logger *slog.Logger,
) *Pool {
	return &Pool{
		broker:      b,
		repo:        repo,
		registry:    registry,
		numWorkers:  numWorkers,
		taskTimeout: taskTimeout,
		logger:      logger,
	}
}

func (p *Pool) Run(ctx context.Context) error {
	out, err := p.broker.Subscribe(ctx)
	if err != nil {
		return fmt.Errorf("worker: failed to subscribe: %w", err)
	}

	var wg sync.WaitGroup
	for i := 0; i < p.numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			p.runWorker(ctx, out)
		}()
	}

	wg.Wait()
	return nil
}

func (p *Pool) runWorker(ctx context.Context, out <-chan broker.Delivery) {
	for {
		select {
		case <-ctx.Done():
			return
		case d, ok := <-out:
			if !ok {
				return
			}
			p.processTask(d)
		}
	}
}

func (p *Pool) processTask(d broker.Delivery) {
	taskCtx, cancel := context.WithTimeout(context.Background(), p.taskTimeout)
	defer cancel()

	handler, found := p.registry.Get(d.Task.Type)
	if !found {
		p.logger.Error("no handler registered for type", "taskType", d.Task.Type)
		if updErr := p.repo.UpdateStatus(taskCtx, d.Task.ID, domain.StatusFailed); updErr != nil {
			p.logger.Error("failed to update status", "error", updErr, "taskID", d.Task.ID)
		}
		if ackErr := p.broker.Ack(taskCtx, d); ackErr != nil {
			p.logger.Error("failed to ack task", "error", ackErr, "taskID", d.Task.ID)
		}
		return
	}

	handlerErr := handler(taskCtx, d.Task.Payload)

	if handlerErr != nil {
		failed, nackErr := p.broker.Nack(taskCtx, d)
		// handler error, nack error (task not found or full queue)
		if nackErr != nil {
			p.logger.Error("failed to nack task", "error", nackErr, "taskID", d.Task.ID)
			return
		}
		// handler error, nack failed (max retries)
		if failed {
			if updErr := p.repo.UpdateStatus(taskCtx, d.Task.ID, domain.StatusFailed); updErr != nil {
				p.logger.Error("failed to update status", "error", updErr, "taskID", d.Task.ID)
			}
			return
		}
		// handler error, nack success - retry scheduled
		return
	}
	updErr := p.repo.UpdateStatus(taskCtx, d.Task.ID, domain.StatusDone)
	if updErr != nil {
		p.logger.Error("failed to update status", "error", updErr, "taskID", d.Task.ID)
	}
	ackErr := p.broker.Ack(taskCtx, d)
	if ackErr != nil {
		p.logger.Error("failed to ack task", "error", ackErr, "taskID", d.Task.ID)
	}
}
