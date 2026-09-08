package worker

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"task-queue/internal/broker"
	"task-queue/internal/domain"
	"task-queue/internal/storage"
	"testing"
	"time"
)

type fakeBroker struct {
	ackFunc  func(ctx context.Context, taskID string) error
	nackFunc func(ctx context.Context, taskID string) (failed bool, err error)
}

func (b *fakeBroker) Publish(ctx context.Context, task *domain.Task) error {
	return nil
}

func (b *fakeBroker) Subscribe(ctx context.Context) (<-chan *domain.Task, error) {
	return nil, nil
}

func (b *fakeBroker) Ack(ctx context.Context, taskID string) error {
	return b.ackFunc(ctx, taskID)
}

func (b *fakeBroker) Nack(ctx context.Context, taskID string) (bool, error) {
	return b.nackFunc(ctx, taskID)
}

type fakeRepository struct {
	createFunc func(ctx context.Context, task *domain.Task) error
	getFunc    func(ctx context.Context, id string) (*domain.Task, error)
	updateFunc func(ctx context.Context, id string, status domain.Status) error
}

func (r *fakeRepository) Create(ctx context.Context, task *domain.Task) error {
	return r.createFunc(ctx, task)
}

func (r *fakeRepository) GetByID(ctx context.Context, id string) (*domain.Task, error) {
	return r.getFunc(ctx, id)
}

func (r *fakeRepository) UpdateStatus(ctx context.Context, id string, status domain.Status) error {
	return r.updateFunc(ctx, id, status)
}

func makeTask(taskID string) domain.Task {
	return domain.Task{
		ID:          taskID,
		Type:        "Task",
		Payload:     nil,
		Status:      "Pending",
		RetryCount:  0,
		MaxRetries:  5,
		CreatedAt:   time.Now(),
		ScheduledAt: time.Now(),
	}
}

func newTestPool(b broker.Broker, repo storage.Repository, reg *Registry) *Pool {
	return NewPool(b, repo, reg, 3, 5*time.Second, slog.New(slog.NewJSONHandler(io.Discard, nil)))
}

func TestPool_ProcessTask_NoHandler(t *testing.T) {
	ackCalled, nackCalled := false, false
	var newStatus domain.Status
	var idToUpdate string

	b := fakeBroker{
		ackFunc: func(ctx context.Context, taskID string) error {
			ackCalled = true
			return nil
		},
		nackFunc: func(ctx context.Context, taskID string) (failed bool, err error) {
			nackCalled = true
			return false, nil
		},
	}

	repo := fakeRepository{updateFunc: func(ctx context.Context, id string, status domain.Status) error {
		newStatus = status
		idToUpdate = id
		return nil
	}}

	pool := newTestPool(&b, &repo, NewRegistry())
	task := makeTask("testID")
	pool.processTask(&task)

	if newStatus != domain.StatusFailed {
		t.Errorf("expected status %v, got %v", domain.StatusFailed, newStatus)
	}
	if idToUpdate != task.ID {
		t.Errorf("expected id %v, got %v", task.ID, idToUpdate)
	}
	if !ackCalled {
		t.Errorf("task was not acknowledged")
	}
	if nackCalled {
		t.Errorf("task was nacknowledged")
	}
}

func TestPool_ProcessTask_Success(t *testing.T) {
	ackCalled, nackCalled := false, false
	var newStatus domain.Status
	var idToUpdate string

	b := fakeBroker{
		ackFunc: func(ctx context.Context, taskID string) error {
			ackCalled = true
			return nil
		},
		nackFunc: func(ctx context.Context, taskID string) (failed bool, err error) {
			nackCalled = true
			return false, nil
		},
	}
	repo := fakeRepository{
		updateFunc: func(ctx context.Context, id string, status domain.Status) error {
			idToUpdate = id
			newStatus = status
			return nil
		},
	}
	reg := NewRegistry()

	pool := newTestPool(&b, &repo, reg)
	task := makeTask("testID")

	reg.Register(task.Type, func(ctx context.Context, payload json.RawMessage) error {
		return nil
	})

	pool.processTask(&task)

	if newStatus != domain.StatusDone {
		t.Errorf("expected status %v, got %v", domain.StatusDone, newStatus)
	}
	if idToUpdate != task.ID {
		t.Errorf("expected id %v, got %v", task.ID, idToUpdate)
	}
	if !ackCalled {
		t.Errorf("task was not acknowledged")
	}
	if nackCalled {
		t.Errorf("task was nacknowledged")
	}
}
