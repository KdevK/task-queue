package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

type Config struct {
	Postgres PostgresConfig
	Broker   BrokerConfig
	Worker   WorkerConfig
}

type PostgresConfig struct {
	ConnString string
}

type BrokerConfig struct {
	BufferSize int
}

type WorkerConfig struct {
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
		Broker: BrokerConfig{
			BufferSize: getEnv("BROKER_BUFFER_SIZE", 100, parsePositiveInt),
		},
		Worker: WorkerConfig{
			NumWorkers:  getEnv("POOL_NUM_WORKERS", 5, parsePositiveInt),
			TaskTimeout: getEnv("WORKER_TASK_TIMEOUT", 30*time.Second, parsePositiveDuration),
		},
	}
}

// panics on invalid value
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

// panics on invalid value
func getEnvRequired[T any](key string, parse func(string) (T, error)) T {
	val := os.Getenv(key)
	if val == "" {
		panic(fmt.Sprintf("config: %s is required", key))
	}
	parsed, err := parse(val)
	if err != nil {
		panic(fmt.Sprintf("config: invalid value for %s: %v", key, err))
	}
	return parsed
}

func parsePositiveInt(s string) (int, error) {
	val, err := strconv.Atoi(s)
	if err != nil {
		return 0, err
	}
	if val < 1 {
		return val, fmt.Errorf("value must be positive")
	}
	return val, nil
}

func parsePositiveDuration(s string) (time.Duration, error) {
	val, err := time.ParseDuration(s)
	if err != nil {
		return 0, err
	}
	if val < 1 {
		return val, fmt.Errorf("time duration must be positive")
	}
	return val, nil
}
