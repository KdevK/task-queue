//go:build integration

package postgres_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"task-queue/internal/domain"
	"task-queue/internal/storage/postgres"
	"testing"
	"time"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	postgresmodule "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

var testPool *pgxpool.Pool

func TestMain(m *testing.M) {
	os.Exit(runTests(m))
}

func runTests(m *testing.M) int {
	ctx := context.Background()

	pgContainer, err := postgresmodule.Run(ctx,
		"postgres:16-alpine",
		postgresmodule.WithDatabase("taskqueue"),
		postgresmodule.WithUsername("taskqueue"),
		postgresmodule.WithPassword("taskqueue"),
		testcontainers.WithWaitStrategy(
			wait.ForListeningPort("5432/tcp"),
		),
	)
	if err != nil {
		log.Printf("failed to start postgres container: %v", err)
		return 1
	}
	defer func() {
		if termErr := pgContainer.Terminate(ctx); termErr != nil {
			log.Printf("failed to terminate container: %v", termErr)
		}
	}()

	connString, connErr := pgContainer.ConnectionString(ctx, "sslmode=disable")
	if connErr != nil {
		log.Printf("failed to construct connection string: %v", connErr)
		return 1
	}

	if migErr := runMigrations(connString); migErr != nil {
		log.Printf("failed to run migrations: %v", migErr)
		return 1
	}

	pool, pErr := pgxpool.New(ctx, connString)
	if pErr != nil {
		log.Printf("failed to create pgx pool: %v", pErr)
		return 1
	}
	defer pool.Close()

	testPool = pool

	return m.Run()
}

func runMigrations(connString string) error {
	m, err := migrate.New("file://../../../migrations", connString)
	if err != nil {
		return fmt.Errorf("failed to init migrate: %w", err)
	}
	if upErr := m.Up(); upErr != nil && !errors.Is(upErr, migrate.ErrNoChange) {
		return fmt.Errorf("failed to apply migrations: %w", upErr)
	}
	return nil
}

func makeTestTask(id string) *domain.Task {
	return &domain.Task{
		ID:          id,
		Type:        "test-task",
		Payload:     json.RawMessage(`{"key":"value"}`),
		Status:      domain.StatusPending,
		RetryCount:  0,
		MaxRetries:  5,
		CreatedAt:   time.Now().UTC(),
		ScheduledAt: time.Now().UTC(),
	}
}

func TestRepository_Create_GetByID_RoundTrip(t *testing.T) {
	repository := postgres.NewPostgresRepository(testPool)
	taskID := uuid.NewString()
	task := makeTestTask(taskID)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	crErr := repository.Create(ctx, task)
	if crErr != nil {
		t.Fatalf("failed to create task: %w", crErr)
	}

	getTask, getErr := repository.GetByID(ctx, taskID)
	if getErr != nil {
		t.Fatalf("failed to get task: %w", getErr)
	}

	assertions := map[string]bool{
		"ID":          task.ID == getTask.ID,
		"Type":        task.Type == getTask.Type,
		"Status":      task.Status == getTask.Status,
		"RetryCount":  task.RetryCount == getTask.RetryCount,
		"MaxRetries":  task.MaxRetries == getTask.MaxRetries,
		"Payload":     bytes.Equal(task.Payload, getTask.Payload),
		"CreatedAt":   task.CreatedAt.Equal(getTask.CreatedAt),
		"ScheduledAt": task.ScheduledAt.Equal(getTask.ScheduledAt),
	}

	for key, value := range assertions {
		if !value {
			t.Errorf("field %v is not equal", key)
		}
	}
}
