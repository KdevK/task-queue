package broker

import (
	"task-queue/internal/domain"
	"testing"
	"time"
)

const maxRetries = 5

func makeTask(t *testing.T, taskID string) domain.Task {
	t.Helper()
	return domain.Task{
		ID:          taskID,
		Type:        "Task",
		Payload:     nil,
		Status:      "Pending",
		RetryCount:  0,
		MaxRetries:  maxRetries,
		CreatedAt:   time.Now(),
		ScheduledAt: time.Now(),
	}
}

func waitForPending(t *testing.T, broker *InMemoryBroker, taskID string, timeout time.Duration) bool {
	t.Helper()
	deadline := time.After(timeout)
	for {
		broker.mu.Lock()
		_, ok := broker.pending[taskID]
		broker.mu.Unlock()

		if ok {
			return true
		}

		select {
		case <-deadline:
			return false
		case <-time.After(time.Millisecond):
		}
	}
}
