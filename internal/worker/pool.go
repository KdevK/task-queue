package worker

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"task-queue/internal/broker"
	"task-queue/internal/domain"
	"task-queue/internal/storage"

	"golang.org/x/sync/errgroup"
)

type Pool struct {
	broker      broker.Broker
	repo        storage.Repository
	registry    Registry
	numWorkers  int
	taskTimeout time.Duration
	logger      slog.Logger
}

func NewPool(
	b broker.Broker,
	repo storage.Repository,
	registry Registry,
	numWorkers int,
	taskTimeout time.Duration,
	logger slog.Logger,
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
		return err
	}

	g, gCtx := errgroup.WithContext(ctx)
	for i := 0; i < p.numWorkers; i++ {
		g.Go(func() error {
			p.runWorker(gCtx, out)
			return nil
		})
	}

	return g.Wait()
}

func (p *Pool) runWorker(ctx context.Context, out <-chan *domain.Task) {
	for {
		select {
		case <-ctx.Done():
			return
		case task, ok := <-out:
			if !ok {
				return
			}
			p.processTask(ctx, task)
		}
	}
}

func (p *Pool) processTask(ctx context.Context, task *domain.Task) {
	taskCtx, cancel := context.WithTimeout(context.Background(), p.taskTimeout)
	defer cancel()

	handler, found := p.registry.Get(task.Type)
	if !found {
		updErr := p.repo.UpdateStatus(taskCtx, task.ID, domain.StatusFailed)
		if updErr != nil {
			return
		}
		ackErr := p.broker.Ack(taskCtx, task.ID)
		if ackErr != nil {
			return
		}
		errStr := fmt.Sprintf("failed to Get handler for type %v", task.Type)
		p.logger.Error(errStr)
		return
	}

	handlerErr := handler(taskCtx, task.Payload)

	if handlerErr != nil {
		failed, nackErr := p.broker.Nack(taskCtx, task.ID)
		if nackErr != nil {
			errStr := fmt.Sprintf("failed to Nack task: %v", nackErr)
			p.logger.Error(errStr)
			return
		}
		if failed {
			updErr := p.repo.UpdateStatus(taskCtx, task.ID, domain.StatusFailed)
			if updErr != nil {
				errStr := fmt.Sprintf("failed to Nack task: %v", nackErr)
				p.logger.Error(errStr)
			}
			errStr := fmt.Sprintf("failed to Nack task: %v", nackErr)
			p.logger.Error(errStr)
			return
		}
		return
	}
	updErr := p.repo.UpdateStatus(taskCtx, task.ID, domain.StatusDone)
	if updErr != nil {
		errStr := fmt.Sprintf("failed to Ack task: %v", updErr)
		p.logger.Error(errStr)
	}
	ackErr := p.broker.Ack(taskCtx, task.ID)
	if ackErr != nil {
		errStr := fmt.Sprintf("failed to Ack task: %v", ackErr)
		p.logger.Error(errStr)
	}
}
