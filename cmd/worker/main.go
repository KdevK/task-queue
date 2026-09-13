package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"task-queue/internal/broker"
	"task-queue/internal/domain"
	"task-queue/internal/storage/postgres"
	"task-queue/internal/worker"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	connString  = "postgres://taskqueue:taskqueue@localhost:5433/taskqueue?sslmode=disable"
	bufferSize  = 100
	numWorkers  = 5
	taskTimeout = 30 * time.Second
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGINT)
	defer cancel()

	pool, err := pgxpool.New(ctx, connString)
	if err != nil {
		logger.Error("failed to create pgx pool", "error", err)
	}
	defer pool.Close()

	repo := postgres.NewPostgresRepository(pool)

	b := broker.NewInMemoryBroker(bufferSize, logger)

	reg := worker.NewRegistry()
	reg.Register("demo", func(ctx context.Context, payload json.RawMessage) error {
		logger.Info("processing demo task", "payload", string(payload))
		return nil
	})

	p := worker.NewPool(b, repo, reg, numWorkers, taskTimeout, logger)

	// TODO: remove once cmd/api exists
	demoTask := &domain.Task{
		ID:          uuid.NewString(),
		Type:        "demo",
		Payload:     json.RawMessage(`{"hello":"world"}`),
		Status:      domain.StatusPending,
		RetryCount:  0,
		MaxRetries:  3,
		CreatedAt:   time.Now().UTC(),
		ScheduledAt: time.Now().UTC(),
	}
	if crErr := repo.Create(ctx, demoTask); crErr != nil {
		logger.Error("failed to create demo task", "error", crErr)
	}
	if pubErr := b.Publish(ctx, demoTask); pubErr != nil {
		logger.Error("failed to publish demo task", "error", pubErr)
	}

	logger.Info("starting worker pool", "numWorkers", numWorkers)
	if runErr := p.Run(ctx); runErr != nil {
		logger.Error("worker pool exited with error", "error", runErr)
		os.Exit(1)
	}
	logger.Info("worker pool stopped gracefully")
}
