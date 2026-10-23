package broker

import (
	"context"
	"task-queue/internal/domain"
)

type Delivery struct {
	Task  *domain.Task
	Token any
}

type Broker interface {
	Publish(ctx context.Context, task *domain.Task) error
	Subscribe(ctx context.Context) (<-chan Delivery, error)
	Ack(ctx context.Context, d Delivery) error
	Nack(ctx context.Context, d Delivery) (bool, error)
}
