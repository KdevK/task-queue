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

const dbConnectTimeout = 10 * time.Second

func main() {
	if err := run(); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, "Error:", err)
		os.Exit(1)
	}
}

func run() error {
	cfg := config.Load()
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	srvCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	// database initialization
	dbPool, dbPoolErr := pgxpool.New(context.Background(), cfg.Postgres.ConnString)
	if dbPoolErr != nil {
		return fmt.Errorf("failed to start pgx pool: %w", dbPoolErr)
	}
	defer func() {
		dbPool.Close()
		logger.Info("database connection closed")
	}()

	pingCtx, pingCancel := context.WithTimeout(context.Background(), dbConnectTimeout)
	pingErr := dbPool.Ping(pingCtx)
	pingCancel()
	if pingErr != nil {
		return fmt.Errorf("failed to ping database: %w", pingErr)
	}

	repo := postgres.NewPostgresRepository(dbPool)
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
	reg.Register("demo", func(ctx context.Context, payload json.RawMessage) error {
		select {
		case <-time.After(3 * time.Second):
			logger.Info("task done", "type", "demo")
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})

	workerPoolCtx, workerPoolCancel := context.WithCancel(context.Background())
	defer workerPoolCancel()

	workerPool := worker.NewPool(b, repo, reg, cfg.Worker.NumWorkers, cfg.Worker.TaskTimeout, logger)

	workerPoolDone := make(chan error, 1)
	go func() {
		workerPoolDone <- workerPool.Run(workerPoolCtx)
	}()

	errCh := make(chan error, 1)
	go func() {
		if err := srv.ListenAndServe(); err != nil {
			if !errors.Is(err, http.ErrServerClosed) {
				errCh <- err
				return
			}
		}
	}()

	// graceful shutdown starts from here
	var serveErr error

	// stop server
	select {
	case <-srvCtx.Done(): // planned stop by the signal
	case serveErr = <-errCh:
	}

	if serveErr != nil {
		logger.Error("http server failed", "error", serveErr)
	} else {
		logger.Info("shutdown signal received")
	}

	stop() // interrupt signals after this step will kill the process immediately

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), cfg.API.ShutdownTimeout)
	shutdownErr := srv.Shutdown(shutdownCtx)
	shutdownCancel()

	if shutdownErr != nil {
		logger.Error("http shutdown failed", "error", shutdownErr)
	} else {
		logger.Info("http stopped")
	}

	// TEMPORARY: stop worker pool
	workerPoolCancel()

	workerPoolStopTimeout := cfg.Worker.TaskTimeout + time.Second
	select {
	case err := <-workerPoolDone:
		if err != nil {
			logger.Error("worker pool stop error", "error", err)
		} else {
			logger.Info("worker pool stopped")
		}
	case <-time.After(workerPoolStopTimeout):
		logger.Error("worker pool timed out", "timeout", workerPoolStopTimeout)
	}

	if serveErr != nil {
		return fmt.Errorf("http server: %w", serveErr)
	}
	return nil
}
