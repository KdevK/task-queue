package broker

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"task-queue/internal/domain"
)

var ErrQueueFull = errors.New("broker: queue is full")
var ErrTaskNotFound = errors.New("broker: task not found in pending")

type InMemoryBroker struct {
	tasks   chan *domain.Task
	pending map[string]*domain.Task
	mu      sync.Mutex
	logger  *slog.Logger
}

func NewInMemoryBroker(bufferSize int, logger *slog.Logger) *InMemoryBroker {
	return &InMemoryBroker{
		tasks:   make(chan *domain.Task, bufferSize),
		pending: make(map[string]*domain.Task),
		logger:  logger,
	}
}

func (b *InMemoryBroker) Publish(ctx context.Context, task *domain.Task) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	select {
	case b.tasks <- task:
		return nil
	default:
		return ErrQueueFull
	}
}

func (b *InMemoryBroker) Subscribe(ctx context.Context) (<-chan *domain.Task, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	out := make(chan *domain.Task)
	go func() {
		for {
			select {
			case task := <-b.tasks:
				b.mu.Lock()
				b.pending[task.ID] = task
				b.mu.Unlock()

				select {
				case out <- task:
				case <-ctx.Done():
					close(out)
					return
				}
			case <-ctx.Done():
				close(out)
				return
			}
		}

	}()
	return out, nil
}

func (b *InMemoryBroker) Ack(ctx context.Context, taskID string) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.pending[taskID]; !ok {
		return ErrTaskNotFound
	}
	delete(b.pending, taskID)
	return nil
}

func (b *InMemoryBroker) Nack(ctx context.Context, taskID string) (failed bool, err error) {
	if ctx.Err() != nil {
		return true, ctx.Err()
	}
	b.mu.Lock()
	defer b.mu.Unlock()

	task, ok := b.pending[taskID]
	if !ok {
		return true, ErrTaskNotFound
	}

	if task.RetryCount < task.MaxRetries {
		select {
		case b.tasks <- task:
			task.RetryCount++
			delete(b.pending, taskID)
			return false, nil
		default:
			return true, ErrQueueFull
		}
	}
	delete(b.pending, taskID)
	b.logger.Warn("task exhausted retries, marking as failed", "taskID", task.ID, "retryCount", task.RetryCount)
	return true, nil
}
