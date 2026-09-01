//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
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
	if upErr := m.Up(); upErr != nil && !errors.Is(err, migrate.ErrNoChange) {
		return fmt.Errorf("failed to apply migrations: %w", upErr)
	}
	return nil
}
