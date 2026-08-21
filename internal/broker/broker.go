package broker

import (
	"context"
	"task-queue/internal/domain"
)

type Broker interface {
	Publish(ctx context.Context, task *domain.Task) error
	Subscribe(ctx context.Context) (<-chan *domain.Task, error)
	Ack(ctx context.Context, taskID string) error
	Nack(ctx context.Context, taskID string) error
}
