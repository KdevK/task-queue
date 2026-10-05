package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	server "task-queue/internal/api/http"
	"task-queue/internal/broker"
	"task-queue/internal/config"
	"task-queue/internal/storage/postgres"
	"task-queue/internal/worker"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const shutdownTimeout = 15 * time.Second

func main() {
	if err := run(); err != nil {
		os.Exit(1)
	}
}

func run() error {
	cfg := config.Load()
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	srvCtx, srvCancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer srvCancel()

	poolCtx, poolCancel := context.WithCancel(srvCtx)
	defer poolCancel()

	dbCtx, dbCancel := context.WithCancel(poolCtx)
	defer dbCancel()

	pool, poolErr := pgxpool.New(dbCtx, cfg.Postgres.ConnString)
	if poolErr != nil {
		return fmt.Errorf("failed to start pgx pool: %v", poolErr)
	}
	defer pool.Close()

	if err := pool.Ping(dbCtx); err != nil {
		return fmt.Errorf("failed to ping DB")
	}

	repo := postgres.NewPostgresRepository(pool)
	b := broker.NewInMemoryBroker(cfg.Broker.BufferSize, logger)

	handler := &server.Handler{
		Repo:   repo,
		Broker: b,
		Logger: logger,
	}

	router := server.NewRouter(handler, logger)
	srv := &http.Server{
		Addr:              cfg.API.Addr,
		Handler:           router,
		ReadTimeout:       cfg.API.ReadTimeout,
		ReadHeaderTimeout: cfg.API.ReadHeaderTimeout,
		WriteTimeout:      cfg.API.WriteTimeout,
		IdleTimeout:       cfg.API.IdleTimeout,
	}

	// TEMPORARY: all-in-one until Kafka
	reg := worker.NewRegistry()
	reg.Register("demo", func(poolCtx context.Context, payload json.RawMessage) error {
		select {
		case <-time.After(2 * time.Second):
			logger.LogAttrs(poolCtx, slog.LevelInfo, "task done", slog.Any("payload", payload))
			return nil
		case <-poolCtx.Done():
			return poolCtx.Err()
		}
	})

	workerPool := worker.NewPool(b, repo, reg, cfg.Worker.NumWorkers, cfg.Worker.TaskTimeout, logger)
	if err := workerPool.Run(poolCtx); err != nil {
		return fmt.Errorf("failed to run worker pool")
	}

	errCh := make(chan error, 1)
	go func() {
		if err := srv.ListenAndServe(); err != nil {
			if !errors.Is(err, http.ErrServerClosed) {
				errCh <- err
				return
			}
		}
	}()

	var serveErr error

	select {
	case <-srvCtx.Done(): // planned stop by the signal
	case serveErr = <-errCh:
	}

	srvCancel()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), shutdownTimeout)

	shutdownErr := srv.Shutdown(shutdownCtx)
	shutdownCancel()

	if shutdownErr != nil {
		logger.Error("http shutdown failed", "error", shutdownErr)
	} else {
		logger.Info("http stopped")
	}

	// TEMPORARY: stop worker pool

	if serveErr != nil {
		logger.Error("http server failed", "error", serveErr)
		return fmt.Errorf("http server: %w", serveErr)
	}
	logger.Info("shutdown signal received")
	return nil
}
