package broker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"task-queue/internal/domain"
	"testing"
	"time"
)

func TestInMemoryBroker_Publish_Success(t *testing.T) {
	broker := NewInMemoryBroker(5)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	task := makeTask(t, "testID")

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
		localTask := makeTask(t, fmt.Sprintf("taskID%v", i))
		broker.tasks <- &localTask
	}

	task := makeTask(t, "taskID")

	err := broker.Publish(ctx, &task)
	if !errors.Is(err, ErrQueueFull) {
		t.Errorf("expected %v, got: %v", ErrQueueFull, err)
	}
}

func TestInMemoryBroker_Publish_CancelledCtx(t *testing.T) {
	broker := NewInMemoryBroker(5)
	task := makeTask(t, "taskID")

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
	task := makeTask(t, taskID)

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

	taskID := "testID"
	task := makeTask(t, taskID)

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

func TestInMemoryBroker_Ack_Success(t *testing.T) {
	broker := NewInMemoryBroker(5)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	taskID := "testID"
	task := makeTask(t, taskID)
	broker.pending[taskID] = &task

	err := broker.Ack(ctx, taskID)
	if err != nil {
		t.Fatalf("error acknowledging the task: %v", err)
	}

	_, gotTask := broker.pending[taskID]
	if gotTask {
		t.Fatalf("task wasn't deleted from pending")
	}
}

func TestInMemoryBroker_Ack_TaskNotFound(t *testing.T) {
	broker := NewInMemoryBroker(5)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	nonExistentTaskID := "testID"
	err := broker.Ack(ctx, nonExistentTaskID)
	if !errors.Is(err, ErrTaskNotFound) {
		t.Fatalf("expected %v, got: %v", ErrTaskNotFound, err)
	}
}

func TestInMemoryBroker_Ack_CtxCancelled(t *testing.T) {
	broker := NewInMemoryBroker(5)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	taskID := "testID"
	task := makeTask(t, taskID)
	broker.pending[taskID] = &task

	cancel()

	err := broker.Ack(ctx, taskID)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected %v, got: %v", context.Canceled, err)
	}
}

func TestInMemoryBroker_Nack_RetrySucceeds(t *testing.T) {
	broker := NewInMemoryBroker(5)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	taskID := "testID"
	task := makeTask(t, taskID)
	broker.pending[taskID] = &task

	failed, err := broker.Nack(ctx, taskID)
	if err != nil {
		t.Fatalf("error acknowledging the task: %v", err)
	}
	if failed {
		t.Fatalf("task was marked as failed")
	}

	var receivedTask *domain.Task
	select {
	case receivedTask = <-broker.tasks:
		if receivedTask.ID != taskID {
			t.Fatalf("expected task.ID %v, got: %v", taskID, receivedTask.ID)
		}
	default:
		t.Fatalf("task wasn't found in broker tasks")
	}

	if receivedTask.RetryCount != 1 {
		t.Fatalf("expected RetryCount 1, got: %v", receivedTask.RetryCount)
	}

	_, gotTask := broker.pending[taskID]
	if gotTask {
		t.Fatalf("task wasn't deleted from pending")
	}
}

func TestInMemoryBroker_Nack_RetryFails(t *testing.T) {
	bufferSize := 3
	broker := NewInMemoryBroker(bufferSize)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	taskID := "testID"
	task := makeTask(t, taskID)
	broker.pending[taskID] = &task

	for i := 0; i < bufferSize; i++ {
		localTask := makeTask(t, fmt.Sprintf("testID%v", i))
		broker.tasks <- &localTask
	}

	failed, err := broker.Nack(ctx, taskID)
	if !errors.Is(err, ErrQueueFull) {
		t.Fatalf("expected error %v, got: %v", ErrQueueFull, err)
	}
	if !failed {
		t.Fatalf("task was expected to be marked as failed")
	}

	retrievedTask, gotTask := broker.pending[taskID]
	if !gotTask {
		t.Fatalf("could get the task from broker pending")
	}

	if retrievedTask.RetryCount != 0 {
		t.Fatalf("expected Retry Count 0, got: %v", retrievedTask.RetryCount)
	}
}

func TestInMemoryBroker_Nack_RetryCountMaxed(t *testing.T) {
	broker := NewInMemoryBroker(5)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	taskID := "testID"
	task := makeTask(t, taskID)
	task.RetryCount = maxRetries
	broker.pending[taskID] = &task

	failed, err := broker.Nack(ctx, taskID)
	if err != nil {
		t.Fatalf("error acknowledging the task: %v", err)
	}
	if !failed {
		t.Fatalf("task was expected to be marked as failed")
	}

	_, gotTask := broker.pending[taskID]
	if gotTask {
		t.Fatalf("task wasn't deleted from pending")
	}
}

func TestInMemoryBroker_Nack_TaskNotFound(t *testing.T) {
	broker := NewInMemoryBroker(5)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	nonExistentTaskID := "testID"
	failed, err := broker.Nack(ctx, nonExistentTaskID)

	if !errors.Is(err, ErrTaskNotFound) {
		t.Fatalf("expected error %v, got: %v", ErrTaskNotFound, err)
	}
	if !failed {
		t.Fatalf("task was expected to be marked as failed")
	}
}

func TestInMemoryBroker_Nack_CtxCancelled(t *testing.T) {
	broker := NewInMemoryBroker(5)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	taskID := "testID"
	task := makeTask(t, taskID)
	broker.pending[taskID] = &task

	cancel()

	failed, err := broker.Nack(ctx, taskID)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected %v, got: %v", context.Canceled, err)
	}
	if !failed {
		t.Fatalf("task was expected to be marked as failed")
	}
}

func TestInMemoryBroker_Subscribe_ConcurrentScenario(t *testing.T) {
	broker := NewInMemoryBroker(5)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	const workerNum = 20
	for i := 0; i < workerNum; i++ {
		wg.Add(1)
		task := makeTask(t, fmt.Sprintf("testID%v", i))
		go func() {
			defer wg.Done()
			err := broker.Publish(ctx, &task)
			if errors.Is(err, ErrQueueFull) {
				deadline := time.After(3 * time.Second)
				for {
					select {
					case <-deadline:
						t.Errorf("couldn't publish all tasks")
						return
					case <-time.After(time.Millisecond):
						retryErr := broker.Publish(ctx, &task)
						if retryErr == nil {
							return
						}
					}
				}
			}
		}()
	}

	out, subErr := broker.Subscribe(ctx)
	if subErr != nil {
		t.Fatalf("subscribe error: %v", subErr)
	}

	result := make(map[string]*domain.Task, workerNum)

	deadline := time.After(5 * time.Second)

loop:
	for len(result) < workerNum {
		select {
		case val := <-out:
			if _, ok := result[val.ID]; ok {
				t.Fatalf("duplicate taskID: %v", val.ID)
			}
			result[val.ID] = val
		case <-deadline:
			break loop
		}
	}

	if len(result) != workerNum {
		t.Fatalf("expected %d tasks, got: %d", workerNum, len(result))
	}

	wg.Wait()
}
