package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"task-queue/internal/config"
	"time"

	"task-queue/internal/broker"
	"task-queue/internal/domain"
	"task-queue/internal/storage/postgres"
	"task-queue/internal/worker"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	cfg := config.Load()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGINT)
	defer cancel()

	pgxPool, err := pgxpool.New(ctx, cfg.Postgres.ConnString)
	if err != nil {
		logger.Error("failed to create pgx pool", "error", err)
	}
	defer pgxPool.Close()

	repo := postgres.NewPostgresRepository(pgxPool)

	b := broker.NewInMemoryBroker(cfg.Broker.BufferSize, logger)

	reg := worker.NewRegistry()
	reg.Register("demo", func(ctx context.Context, payload json.RawMessage) error {
		logger.Info("processing demo task", "payload", string(payload))
		return nil
	})

	p := worker.NewPool(b, repo, reg, cfg.Worker.NumWorkers, cfg.Worker.TaskTimeout, logger)

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

	logger.Info("starting worker pool", "numWorkers", cfg.Worker.NumWorkers)
	if runErr := p.Run(ctx); runErr != nil {
		logger.Error("worker pool exited with error", "error", runErr)
		os.Exit(1)
	}
	logger.Info("worker pool stopped gracefully")
}
