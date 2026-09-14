package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

type Config struct {
	Postgres PostgresConfig
	Worker   WorkerConfig
}

type PostgresConfig struct {
	ConnString string
}

type WorkerConfig struct {
	BufferSize  int
	NumWorkers  int
	TaskTimeout time.Duration
}

func Load() *Config {
	err := loadDotenv(".env")
	if err != nil {
		panic(fmt.Sprintf("config: error loading .env file: %v", err))
	}

	return &Config{
		Postgres: PostgresConfig{
			ConnString: getEnvRequired("POSTGRES_CONN_STRING", func(s string) (string, error) {
				return s, nil
			}),
		},
		Worker: WorkerConfig{
			BufferSize:  getEnv("BROKER_BUFFER_SIZE", 100, strconv.Atoi),
			NumWorkers:  getEnv("POOL_NUM_WORKERS", 5, strconv.Atoi),
			TaskTimeout: getEnv("WORKER_TASK_TIMEOUT", 30*time.Second, time.ParseDuration),
		},
	}
}

func getEnv[T any](key string, fallback T, parse func(string) (T, error)) T {
	val := os.Getenv(key)
	if val == "" {
		return fallback
	}
	parsed, err := parse(val)
	if err != nil {
		panic(fmt.Sprintf("config: invalid value for %s: %v", key, err))
	}
	return parsed
}

func getEnvRequired[T any](key string, parse func(string) (T, error)) T {
	val := os.Getenv(key)
	if val == "" {
		panic(fmt.Sprintf("config: %s is required", key))
	}
	parsed, err := parse(val)
	if err != nil {
		panic(fmt.Sprintf("config: invalid value for %s: %v", key, val))
	}
	return parsed
}
