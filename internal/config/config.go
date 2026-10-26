package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	Postgres PostgresConfig
	Broker   BrokerConfig
	Worker   WorkerConfig
	API      APIConfig
	Kafka    KafkaConfig
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

type APIConfig struct {
	Addr              string
	ReadHeaderTimeout time.Duration
	ReadTimeout       time.Duration
	WriteTimeout      time.Duration
	IdleTimeout       time.Duration
	ShutdownTimeout   time.Duration
}

type KafkaConfig struct {
	KafkaBrokers []string
	KafkaTopic   string
}

func Load() *Config {
	err := loadDotenv(".env")
	if err != nil {
		panic(fmt.Sprintf("config: error loading .env file: %v", err))
	}

	return &Config{
		Postgres: PostgresConfig{
			ConnString: getEnvRequired("POSTGRES_CONN_STRING", parseSimpleString),
		},
		Broker: BrokerConfig{
			BufferSize: getEnv("BROKER_BUFFER_SIZE", 100, parsePositiveInt),
		},
		Worker: WorkerConfig{
			NumWorkers:  getEnv("POOL_NUM_WORKERS", 5, parsePositiveInt),
			TaskTimeout: getEnv("WORKER_TASK_TIMEOUT", 30*time.Second, parsePositiveDuration),
		},
		API: APIConfig{
			Addr:              getEnv("API_ADDR", ":8080", parseSimpleString),
			ReadHeaderTimeout: getEnv("API_READ_HEADER_TIMEOUT", 5*time.Second, parsePositiveDuration),
			ReadTimeout:       getEnv("API_READ_TIMEOUT", 10*time.Second, parsePositiveDuration),
			WriteTimeout:      getEnv("API_WRITE_TIMEOUT", 10*time.Second, parsePositiveDuration),
			IdleTimeout:       getEnv("API_IDLE_TIMEOUT", 60*time.Second, parsePositiveDuration),
			ShutdownTimeout:   getEnv("API_SHUTDOWN_TIMEOUT", 15*time.Second, parsePositiveDuration),
		},
		Kafka: KafkaConfig{
			KafkaBrokers: getEnv("KAFKA_BROKERS", []string{"localhost:29092"}, parseStringList),
			KafkaTopic:   getEnv("KAFKA_TOPIC", "tasks", parseSimpleString),
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

func parseSimpleString(s string) (string, error) {
	return s, nil
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

func parseStringList(s string) ([]string, error) {
	vals := strings.Split(s, ",")
	var result []string

	for _, val := range vals {
		cleaned := strings.TrimSpace(val)
		if cleaned != "" {
			result = append(result, cleaned)
		}
	}

	if len(result) == 0 {
		return nil, fmt.Errorf("string must contain multiple values divided by comma")
	}
	return result, nil
}
