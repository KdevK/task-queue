package storage

import (
	"context"
	"task-queue/internal/domain"
)

type Repository interface {
	Create(ctx context.Context, task *domain.Task) error
	GetByID(ctx context.Context, id string) (*domain.Task, error)
	UpdateStatus(ctx context.Context, id string, status domain.Status) error
}
