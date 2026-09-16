package config

import (
	"os"
	"strconv"
	"testing"
	"time"
)

func TestGetEnv_Fallback(t *testing.T) {
	const envKey = "CONFIG_TEST"
	unsetFunc := func() {
		unsetErr := os.Unsetenv(envKey)
		if unsetErr != nil {
			t.Fatalf("failed to unset env value for test key: %v", unsetErr)
		}
	}
	defer unsetFunc()
	unsetFunc()

	fallbackVal := 5
	envVal := getEnv(envKey, fallbackVal, strconv.Atoi)

	if envVal != fallbackVal {
		t.Fatalf("expected env value %v, got %v", fallbackVal, envVal)
	}
}

func TestGetEnv_ParsePanic(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("expected panic, got nil")
		}
	}()

	const envKey = "CONFIG_TEST"
	const envValue = "INCONVERTIBLE_VALUE"

	t.Setenv(envKey, envValue)

	fallbackVal := 5
	// unsuccessfully converts string to int and expects panic
	envVal := getEnv(envKey, fallbackVal, strconv.Atoi)

	if envVal != fallbackVal {
		t.Fatalf("expected env value %v, got %v", fallbackVal, envVal)
	}
}

func TestGetEnvRequired_NoValuePanic(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("expected panic, got nil")
		}
	}()

	const envKey = "CONFIG_TEST"

	unsetErr := os.Unsetenv(envKey)
	if unsetErr != nil {
		t.Fatalf("failed to unset env value for test key: %v", unsetErr)
	}

	getEnvRequired(envKey, strconv.Atoi)
}

func TestLoad_NoPostgresConfigPanic(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("expected panic, got nil")
		}
	}()

	t.Setenv("BROKER_BUFFER_SIZE", "50")
	t.Setenv("POOL_NUM_WORKERS", "5")
	t.Setenv("WORKER_TASK_TIMEOUT", "30s")

	Load()
}

func TestLoad_NegativeNumWorkersPanic(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("expected panic, got nil")
		}
	}()

	t.Setenv("POSTGRES_CONN_STRING", "postgres://test:test@localhost:5432/taskqueue?sslmode=disable")
	t.Setenv("BROKER_BUFFER_SIZE", "50")
	t.Setenv("WORKER_TASK_TIMEOUT", "30s")

	// negative workers amount
	t.Setenv("POOL_NUM_WORKERS", "-1")

	Load()
}

func TestLoad_NegativeTaskTimeoutPanic(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("expected panic, got nil")
		}
	}()

	t.Setenv("POSTGRES_CONN_STRING", "postgres://test:test@localhost:5432/taskqueue?sslmode=disable")
	t.Setenv("BROKER_BUFFER_SIZE", "50")
	t.Setenv("POOL_NUM_WORKERS", "5")

	// negative task timeout
	t.Setenv("WORKER_TASK_TIMEOUT", "-5s")

	Load()
}

func TestLoad_StructFields(t *testing.T) {
	const (
		bufferSize  = 50
		numWorkers  = 7
		taskTimeout = 30 * time.Second
	)

	t.Setenv("POSTGRES_CONN_STRING", "postgres://test:test@localhost:5432/taskqueue?sslmode=disable")
	t.Setenv("BROKER_BUFFER_SIZE", strconv.Itoa(bufferSize))
	t.Setenv("POOL_NUM_WORKERS", strconv.Itoa(numWorkers))
	t.Setenv("WORKER_TASK_TIMEOUT", taskTimeout.String())

	cfg := Load()

	if cfg.Broker.BufferSize != bufferSize {
		t.Errorf("expected BufferSize %v, got %v", bufferSize, cfg.Broker.BufferSize)
	}
	if cfg.Worker.NumWorkers != numWorkers {
		t.Errorf("expected NumWorkers %v, got %v", numWorkers, cfg.Worker.NumWorkers)
	}
	if cfg.Worker.TaskTimeout != taskTimeout {
		t.Errorf("expected TaskTimeout %v, got %v", taskTimeout, cfg.Worker.TaskTimeout)
	}
}
