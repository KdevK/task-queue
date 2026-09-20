package server

import (
	"encoding/json"
	"fmt"
	"task-queue/internal/domain"
	"time"
)

type CreateTaskRequest struct {
	Type        string          `json:"type"`
	Payload     json.RawMessage `json:"payload"`
	MaxRetries  *int            `json:"max_retries,omitempty"`
	ScheduledAt *time.Time      `json:"scheduled_at,omitempty"`
}

func (r *CreateTaskRequest) Validate() error {
	if r.Type == "" {
		return fmt.Errorf("type must not be empty")
	}
	if *r.MaxRetries < 1 {
		return fmt.Errorf("max retries must be at least 1")
	}
	return nil
}

func (r *CreateTaskRequest) ApplyDefaults() {
	if r.MaxRetries == nil {
		maxRetries := domain.DefaultMaxRetries
		r.MaxRetries = &maxRetries
	}
	if r.ScheduledAt == nil {
		scheduledAt := time.Now().UTC()
		r.ScheduledAt = &scheduledAt
	}
}

type CreateTaskResponse struct {
	ID     string        `json:"id"`
	Status domain.Status `json:"status"`
}

type TaskResponse struct {
	ID          string        `json:"id"`
	Type        string        `json:"type"`
	Status      domain.Status `json:"status"`
	RetryCount  int           `json:"retry_count"`
	MaxRetries  int           `json:"max_retries"`
	CreatedAt   time.Time     `json:"created_at"`
	ScheduledAt time.Time     `json:"scheduled_at"`
}

type ErrorResponse struct {
	Error string `json:"error"`
}
