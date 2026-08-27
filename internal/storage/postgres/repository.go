package postgres

import (
	"context"
	"errors"
	"fmt"
	"task-queue/internal/domain"
	"task-queue/internal/storage"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Repository struct {
	pool *pgxpool.Pool
}

func NewPostgresRepository(pool *pgxpool.Pool) *Repository {
	return &Repository{pool: pool}
}

func (r *Repository) Create(ctx context.Context, task *domain.Task) error {
	query := `
		INSERT INTO tasks (id, type, payload, status, retry_count, max_retries, created_at, scheduled_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
	`
	_, err := r.pool.Exec(
		ctx,
		query,
		task.ID,
		task.Type,
		task.Payload,
		task.Status,
		task.RetryCount,
		task.MaxRetries,
		task.CreatedAt,
		task.ScheduledAt,
	)
	if err != nil {
		return fmt.Errorf("storage: failed to create task: %w", err)
	}
	return nil
}

func (r *Repository) GetByID(ctx context.Context, id string) (*domain.Task, error) {
	query := `
		SELECT id, type, payload, status, retry_count, max_retries, created_at, scheduled_at
		FROM tasks WHERE id = $1
	`
	var task domain.Task
	err := r.pool.QueryRow(ctx, query, id).Scan(
		&task.ID,
		&task.Type,
		&task.Payload,
		&task.Status,
		&task.RetryCount,
		&task.MaxRetries,
		&task.CreatedAt,
		&task.ScheduledAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, storage.ErrNotFound
		}
		return nil, fmt.Errorf("storage: failed to get task: %w", err)
	}
	return &task, nil
}

func (r *Repository) UpdateStatus(ctx context.Context, id string, status domain.Status) error {
	query := `
		UPDATE tasks SET status = $1 WHERE id = $2
	`
	tag, err := r.pool.Exec(ctx, query, status, id)
	if err != nil {
		return fmt.Errorf("storage: failed to update task: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return storage.ErrNotFound
	}
	return nil
}
