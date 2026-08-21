package domain

import (
	"encoding/json"
	"time"
)

type Status string

const (
	StatusPending Status = "pending"
	StatusRunning Status = "running"
	StatusDone    Status = "done"
	StatusFailed  Status = "failed"
)

type Task struct {
	ID          string
	Type        string
	Payload     json.RawMessage
	Status      Status
	RetryCount  int
	MaxRetries  int
	CreatedAt   time.Time
	ScheduledAt time.Time
}
