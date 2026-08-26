package postgres

import (
	"context"
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
		SELECT * FROM tasks WHERE id = $1
	`
	rows, err := r.pool.Query(ctx, query, id)
	if err != nil {
		return nil, storage.ErrNotFound
	}
	defer rows.Close()

	user, userErr := pgx.CollectOneRow(rows, pgx.RowToStructByName[domain.Task])
	if userErr != nil {
		return nil, fmt.Errorf("storage: failed to map data to task: %w", err)
	}

	return &user, nil
}
