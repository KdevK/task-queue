package broker

import (
	"context"
	"errors"
	"fmt"
	"task-queue/internal/domain"
	"testing"
	"time"
)

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

func TestInMemoryBroker_Publish_Success(t *testing.T) {
	broker := NewInMemoryBroker(5)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	task := domain.Task{
		ID:          "testID",
		Type:        "Task",
		Payload:     nil,
		Status:      "Pending",
		RetryCount:  0,
		MaxRetries:  5,
		CreatedAt:   time.Now(),
		ScheduledAt: time.Now(),
	}

	err := broker.Publish(ctx, &task)
	if err != nil {
		t.Errorf("publish failed: %v", err)
	}

}

func TestInMemoryBroker_Publish_FullChannel(t *testing.T) {
	const bufferSize = 5

	broker := NewInMemoryBroker(bufferSize)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for i := 0; i < bufferSize; i++ {
		localTask := domain.Task{
			ID:          fmt.Sprintf("TaskID%v", i),
			Type:        "Task",
			Payload:     nil,
			Status:      "Pending",
			RetryCount:  0,
			MaxRetries:  5,
			CreatedAt:   time.Now(),
			ScheduledAt: time.Now(),
		}
		broker.tasks <- &localTask
	}

	task := domain.Task{
		ID:          "TestID",
		Type:        "Task",
		Payload:     nil,
		Status:      "Pending",
		RetryCount:  0,
		MaxRetries:  5,
		CreatedAt:   time.Now(),
		ScheduledAt: time.Now(),
	}

	err := broker.Publish(ctx, &task)
	if !errors.Is(err, ErrQueueFull) {
		t.Errorf("expected %v, got: %v", ErrQueueFull, err)
	}
}

func TestInMemoryBroker_Publish_CancelledCtx(t *testing.T) {
	broker := NewInMemoryBroker(5)
	task := domain.Task{
		ID:          "TaskID",
		Type:        "Task",
		Payload:     nil,
		Status:      "Pending",
		RetryCount:  0,
		MaxRetries:  5,
		CreatedAt:   time.Now(),
		ScheduledAt: time.Now(),
	}

	ctx, cancel := context.WithCancel(context.Background())
	// context is cancelled immediately
	cancel()

	err := broker.Publish(ctx, &task)
	if !errors.Is(err, ctx.Err()) {
		t.Errorf("expected %v, got: %v", ctx.Err(), err)
	}
}

func TestInMemoryBroker_Subscribe_Success(t *testing.T) {
	broker := NewInMemoryBroker(5)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	taskID := "taskID"
	task := domain.Task{
		ID:          taskID,
		Type:        "Task",
		Payload:     nil,
		Status:      "Pending",
		RetryCount:  0,
		MaxRetries:  5,
		CreatedAt:   time.Now(),
		ScheduledAt: time.Now(),
	}

	err := broker.Publish(ctx, &task)
	if err != nil {
		t.Fatalf("publish failed: %v", err)
	}

	out, subErr := broker.Subscribe(ctx)
	if subErr != nil {
		t.Fatalf("subscribe failed: %v", err)
	}

	receivedTask := <-out
	if receivedTask == nil {
		t.Fatalf("received nil from channel")
	}

	if receivedTask.ID != taskID {
		t.Fatalf("expected task.ID %v, got: %v", taskID, receivedTask.ID)
	}

	pendingTask, ok := broker.pending[taskID]
	if !ok {
		t.Fatalf("failed to get a task from pending")
	}

	if pendingTask.ID != taskID {
		t.Fatalf("expected task.ID %v, got: %v", taskID, pendingTask.ID)
	}
}

func TestInMemoryBroker_Subscribe_CtxCancelled_WhileBlockedOnSend(t *testing.T) {
	broker := NewInMemoryBroker(5)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	taskID := "TaskID"
	task := domain.Task{
		ID:          "TaskID",
		Type:        "Task",
		Payload:     nil,
		Status:      "Pending",
		RetryCount:  0,
		MaxRetries:  5,
		CreatedAt:   time.Now(),
		ScheduledAt: time.Now(),
	}

	pubErr := broker.Publish(ctx, &task)
	if pubErr != nil {
		t.Fatalf("publish error: %v", pubErr)
	}

	out, subErr := broker.Subscribe(ctx)
	if subErr != nil {
		t.Fatalf("subscribe error: %v", subErr)
	}

	flag := waitForPending(t, broker, taskID, time.Second)
	if !flag {
		t.Fatalf("task never appeared in pending")
	}
	cancel()

	localCtx, localCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer localCancel()

	chOpen := true
	for {
		select {
		case <-localCtx.Done():
		case _, chOpen = <-out:
			if chOpen {
				continue
			}
		}
		break
	}

	if chOpen {
		t.Fatalf("channel isn't closed")
	}
}

func TestInMemoryBroker_Subscribe_CtxCancelled_WhileIdle(t *testing.T) {
	broker := NewInMemoryBroker(5)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	out, subErr := broker.Subscribe(ctx)
	if subErr != nil {
		t.Fatalf("subscribe error: %v", subErr)
	}
	cancel()

	localCtx, localCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer localCancel()

	chOpen := true
	for {
		select {
		case <-localCtx.Done():
		case _, chOpen = <-out:
			if chOpen {
				continue
			}
		}
		break
	}

	if chOpen {
		t.Fatalf("channel isn't closed")
	}
}
