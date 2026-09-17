package http

import (
	"encoding/json"
	"time"
)

type CreateTaskRequest struct {
	Type        string          `json:"type"`
	Payload     json.RawMessage `json:"payload"`
	MaxRetries  *int            `json:"max_retries,omitempty"`
	ScheduledAt *time.Time      `json:"scheduled_at,omitempty"`
}

type CreateTaskResponse struct {
	ID     string `json:"id"`
	Status string `json:"status"`
}

type TaskResponse struct {
	ID          string    `json:"id"`
	Type        string    `json:"type"`
	Status      string    `json:"status"`
	RetryCount  int       `json:"retry_count"`
	MaxRetries  int       `json:"max_retries"`
	CreatedAt   time.Time `json:"created_at"`
	ScheduledAt time.Time `json:"scheduled_at"`
}

type ErrorResponse struct {
	Error string `json:"error"`
}
