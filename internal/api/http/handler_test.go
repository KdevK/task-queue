package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"task-queue/internal/domain"
	"task-queue/internal/storage"
	"testing"
	"time"
)

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

type fakeBroker struct {
	publishFunc   func(ctx context.Context, task *domain.Task) error
	subscribeFunc func(ctx context.Context) (<-chan *domain.Task, error)
	ackFunc       func(ctx context.Context, taskID string) error
	nackFunc      func(ctx context.Context, taskID string) (bool, error)
}

func (b *fakeBroker) Publish(ctx context.Context, task *domain.Task) error {
	return b.publishFunc(ctx, task)
}

func (b *fakeBroker) Subscribe(ctx context.Context) (<-chan *domain.Task, error) {
	return b.subscribeFunc(ctx)
}

func (b *fakeBroker) Ack(ctx context.Context, taskID string) error {
	return b.ackFunc(ctx, taskID)
}

func (b *fakeBroker) Nack(ctx context.Context, taskID string) (bool, error) {
	return b.nackFunc(ctx, taskID)
}

func makeTaskRequest() CreateTaskRequest {
	maxRetries := 5
	scheduledAt := time.Now().UTC()
	return CreateTaskRequest{
		Type:        "Task",
		Payload:     nil,
		MaxRetries:  &maxRetries,
		ScheduledAt: &scheduledAt,
	}
}

func structToBody(t *testing.T, v any) io.Reader {
	t.Helper()

	rawJson, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("failed to marshal object into JSON: %v", err)
	}

	return bytes.NewReader(rawJson)
}

func TestHandler_CreateTask_BadRequest(t *testing.T) {
	tests := []struct {
		name       string
		jsonString string
	}{
		{
			name:       "incorrect json format",
			jsonString: `{"invalid_json": "comma after value",}`,
		},
		{
			name:       "unknown field",
			jsonString: `{"type": "taskType", "unknownField": "value"}`,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			createCalled, publishCalled := false, false
			repo := fakeRepository{
				createFunc: func(ctx context.Context, task *domain.Task) error {
					createCalled = true
					return nil
				},
			}
			b := fakeBroker{
				publishFunc: func(ctx context.Context, task *domain.Task) error {
					publishCalled = true
					return nil
				},
			}

			handler := Handler{
				Repo:   &repo,
				Broker: &b,
				Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
			}

			body := strings.NewReader(tc.jsonString)

			r := httptest.NewRequest(http.MethodPost, "/tasks", body)
			w := httptest.NewRecorder()

			handler.CreateTask(w, r)

			if w.Code != http.StatusBadRequest {
				t.Fatalf("expected status code %v, got %v", http.StatusBadRequest, w.Code)
			}
			if createCalled {
				t.Fatalf("Create must not be called")
			}
			if publishCalled {
				t.Fatalf("Publish must not be called")
			}

			var response ErrorResponse
			if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
				t.Fatalf("failed to decode response body %v", err)
			}
			if response.Error == "" {
				t.Fatalf("error description must not be empty")
			}
		})
	}
}

func TestHandler_CreateTask_ValidationError(t *testing.T) {
	tests := []struct {
		name           string
		taskType       string
		taskMaxRetries int
	}{
		{
			name:           "empty type",
			taskType:       "",
			taskMaxRetries: 5,
		},
		{
			name:           "negative max retries",
			taskType:       "testType",
			taskMaxRetries: -1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			createCalled, publishCalled := false, false
			repo := fakeRepository{
				createFunc: func(ctx context.Context, task *domain.Task) error {
					createCalled = true
					return nil
				},
			}
			b := fakeBroker{
				publishFunc: func(ctx context.Context, task *domain.Task) error {
					publishCalled = true
					return nil
				},
			}

			handler := Handler{
				Repo:   &repo,
				Broker: &b,
				Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
			}

			taskPayload := CreateTaskRequest{
				Type:       tc.taskType,
				MaxRetries: &tc.taskMaxRetries,
			}
			body := structToBody(t, taskPayload)

			r := httptest.NewRequest(http.MethodPost, "/tasks", body)
			w := httptest.NewRecorder()

			handler.CreateTask(w, r)

			if w.Code != http.StatusBadRequest {
				t.Fatalf("expected status code %v, got %v", http.StatusBadRequest, w.Code)
			}
			if createCalled {
				t.Fatalf("Create must not be called")
			}
			if publishCalled {
				t.Fatalf("Publish must not be called")
			}

			var response ErrorResponse
			if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
				t.Fatalf("failed to decode response body %v", err)
			}
			if response.Error == "" {
				t.Fatalf("error description must not be empty")
			}
		})
	}
}

func TestHandler_CreateTask_CreateError(t *testing.T) {
	createCalled, publishCalled := false, false

	repo := fakeRepository{
		createFunc: func(ctx context.Context, task *domain.Task) error {
			createCalled = true
			return fmt.Errorf("failed to create task in repository")
		},
	}
	b := fakeBroker{
		publishFunc: func(ctx context.Context, task *domain.Task) error {
			publishCalled = true
			return nil
		},
	}

	handler := Handler{
		Repo:   &repo,
		Broker: &b,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	taskPayload := makeTaskRequest()
	body := structToBody(t, taskPayload)

	rq := httptest.NewRequest(http.MethodPost, "/tasks", body)
	w := httptest.NewRecorder()

	handler.CreateTask(w, rq)

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("expected status %v, got %v", http.StatusInternalServerError, w.Code)
	}
	if !createCalled {
		t.Fatalf("Create must be called")
	}
	if publishCalled {
		t.Fatalf("Publish must not be called")
	}

	var response ErrorResponse
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatalf("failed to decode response body: %v", err)
	}
	if response.Error == "" {
		t.Fatalf("error description must not be empty")
	}
}

func TestHandler_CreateTask_PublishErrorUpdateError(t *testing.T) {
	createCalled, updateCalled, publishCalled := false, false, false
	var idToCreate, idToUpdate string
	var statusToUpdate domain.Status

	repo := fakeRepository{
		createFunc: func(ctx context.Context, task *domain.Task) error {
			createCalled = true
			idToCreate = task.ID
			return nil
		},
		updateFunc: func(ctx context.Context, id string, status domain.Status) error {
			updateCalled = true
			idToUpdate = id
			statusToUpdate = status
			return fmt.Errorf("failed to update task in repository")
		},
	}
	b := fakeBroker{
		publishFunc: func(ctx context.Context, task *domain.Task) error {
			publishCalled = true
			return fmt.Errorf("failed to publish task in broker")
		},
	}

	handler := Handler{
		Repo:   &repo,
		Broker: &b,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	taskPayload := makeTaskRequest()
	body := structToBody(t, taskPayload)

	rq := httptest.NewRequest(http.MethodPost, "/tasks", body)
	w := httptest.NewRecorder()

	handler.CreateTask(w, rq)

	if w.Code != http.StatusAccepted {
		t.Fatalf("expected status %v, got %v", http.StatusAccepted, w.Code)
	}
	if !createCalled {
		t.Fatalf("Create must be called")
	}
	if !publishCalled {
		t.Fatalf("Publish must be called")
	}
	if !updateCalled {
		t.Fatalf("Update must be called")
	}
	if idToUpdate != idToCreate {
		t.Fatalf("ID to update %v must match ID to create %v", idToUpdate, idToCreate)
	}
	if statusToUpdate != domain.StatusQueueFailed {
		t.Fatalf("expected status %v, got %v", domain.StatusQueueFailed, statusToUpdate)
	}

	var response CreateTaskResponse
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatalf("failed to decode response body: %v", err)
	}
	if response.Status != domain.StatusQueueFailed {
		t.Fatalf("error description must not be empty")
	}
	if response.ID != idToCreate {
		t.Fatalf("response ID %v does not match created task ID %v", response.ID, idToCreate)
	}
}

func TestHandler_CreateTask_PublishErrorUpdateSuccess(t *testing.T) {
	createCalled, updateCalled, publishCalled := false, false, false
	var idToCreate, idToUpdate string
	var statusToUpdate domain.Status

	repo := fakeRepository{
		createFunc: func(ctx context.Context, task *domain.Task) error {
			createCalled = true
			idToCreate = task.ID
			return nil
		},
		updateFunc: func(ctx context.Context, id string, status domain.Status) error {
			updateCalled = true
			idToUpdate = id
			statusToUpdate = status
			return fmt.Errorf("failed to update task in repository")
		},
	}
	b := fakeBroker{
		publishFunc: func(ctx context.Context, task *domain.Task) error {
			publishCalled = true
			return fmt.Errorf("failed to publish task in broker")
		},
	}

	handler := Handler{
		Repo:   &repo,
		Broker: &b,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	taskPayload := makeTaskRequest()
	body := structToBody(t, taskPayload)

	rq := httptest.NewRequest(http.MethodPost, "/tasks", body)
	w := httptest.NewRecorder()

	handler.CreateTask(w, rq)

	if w.Code != http.StatusAccepted {
		t.Fatalf("expected status %v, got %v", http.StatusAccepted, w.Code)
	}
	if !createCalled {
		t.Fatalf("Create must be called")
	}
	if !publishCalled {
		t.Fatalf("Publish must be called")
	}
	if !updateCalled {
		t.Fatalf("Update must be called")
	}
	if idToUpdate != idToCreate {
		t.Fatalf("ID to update %v must match ID to create %v", idToUpdate, idToCreate)
	}
	if statusToUpdate != domain.StatusQueueFailed {
		t.Fatalf("expected status %v, got %v", domain.StatusQueueFailed, statusToUpdate)
	}

	var response CreateTaskResponse
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatalf("failed to decode response body: %v", err)
	}
	if response.Status != domain.StatusQueueFailed {
		t.Fatalf("error description must not be empty")
	}
	if response.ID != idToCreate {
		t.Fatalf("response ID %v does not match created task ID %v", response.ID, idToCreate)
	}
}

func TestHandler_CreateTask_Success(t *testing.T) {
	createCalled, publishCalled := false, false
	var idToCreate string
	var idToPublish string

	repo := fakeRepository{
		createFunc: func(ctx context.Context, task *domain.Task) error {
			createCalled = true
			idToCreate = task.ID
			return nil
		},
	}
	b := fakeBroker{
		publishFunc: func(ctx context.Context, task *domain.Task) error {
			publishCalled = true
			idToPublish = task.ID
			return nil
		},
	}

	handler := Handler{
		Repo:   &repo,
		Broker: &b,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	taskPayload := makeTaskRequest()
	body := structToBody(t, taskPayload)

	rq := httptest.NewRequest(http.MethodPost, "/tasks", body)
	w := httptest.NewRecorder()

	handler.CreateTask(w, rq)

	if w.Code != http.StatusAccepted {
		t.Fatalf("expected code %v, got %v", http.StatusAccepted, w.Code)
	}
	if !createCalled {
		t.Fatalf("task was not created in db")
	}
	if !publishCalled {
		t.Fatalf("task was not published to broker")
	}
	if idToPublish != idToCreate {
		t.Fatalf("Create and Publish received different task IDs: %v vs %v", idToPublish, idToCreate)
	}

	var response CreateTaskResponse
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatalf("failed to decode response body %v", err)
	}
	if response.Status != domain.StatusPending {
		t.Fatalf("expected status %v, got %v", domain.StatusPending, response.Status)
	}
	if response.ID != idToCreate {
		t.Fatalf("response ID %v does not match created task ID %v", response.ID, idToCreate)
	}
}

func TestHandler_CreateTask_EmptyOptionalFieldsFilled(t *testing.T) {
	createCalled, publishCalled := false, false
	var idToCreate string
	var idToPublish string

	var maxRetriesToCreate int
	var scheduledAtToCreate time.Time

	repo := fakeRepository{
		createFunc: func(ctx context.Context, task *domain.Task) error {
			createCalled = true
			idToCreate = task.ID
			maxRetriesToCreate = task.MaxRetries
			scheduledAtToCreate = task.ScheduledAt
			return nil
		},
	}
	b := fakeBroker{
		publishFunc: func(ctx context.Context, task *domain.Task) error {
			publishCalled = true
			idToPublish = task.ID
			return nil
		},
	}

	handler := Handler{
		Repo:   &repo,
		Broker: &b,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	taskPayload := CreateTaskRequest{
		Type:        "taskType",
		Payload:     nil,
		MaxRetries:  nil,
		ScheduledAt: nil,
	}
	body := structToBody(t, taskPayload)

	rq := httptest.NewRequest(http.MethodPost, "/tasks", body)
	w := httptest.NewRecorder()

	handler.CreateTask(w, rq)

	if w.Code != http.StatusAccepted {
		t.Fatalf("expected code %v, got %v", http.StatusAccepted, w.Code)
	}
	if !createCalled {
		t.Fatalf("task was not created in db")
	}
	if !publishCalled {
		t.Fatalf("task was not published to broker")
	}
	if idToPublish != idToCreate {
		t.Fatalf("Create and Publish received different task IDs: %v vs %v", idToPublish, idToCreate)
	}

	var response CreateTaskResponse
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatalf("failed to decode response body %v", err)
	}
	if response.ID != idToCreate {
		t.Fatalf("response ID %v does not match created task ID %v", response.ID, idToCreate)
	}

	if response.Status != domain.StatusPending {
		t.Fatalf("expected status %v, got %v", domain.StatusPending, response.Status)
	}

	if maxRetriesToCreate != domain.DefaultMaxRetries {
		t.Fatalf("expected MaxRetries %v, got %v", domain.DefaultMaxRetries, maxRetriesToCreate)
	}
	if scheduledAtToCreate.IsZero() {
		t.Fatalf("expected ScheduledAt to be filled with current date")
	}
}

func TestHandler_GetTask_EmptyID(t *testing.T) {
	getCalled := false

	repo := fakeRepository{
		getFunc: func(ctx context.Context, id string) (*domain.Task, error) {
			getCalled = true
			task := domain.Task{
				ID:          "testId",
				Type:        "test",
				Payload:     nil,
				Status:      domain.StatusDone,
				RetryCount:  0,
				MaxRetries:  5,
				CreatedAt:   time.Now().UTC(),
				ScheduledAt: time.Now().UTC(),
			}
			return &task, nil
		},
	}
	b := fakeBroker{}

	handler := Handler{
		Repo:   &repo,
		Broker: &b,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	r := httptest.NewRequest(http.MethodGet, "/tasks/", nil)
	pathValue := ""
	r.SetPathValue("id", pathValue)
	w := httptest.NewRecorder()

	handler.GetTask(w, r)

	var response ErrorResponse
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatalf("failed to decode response body: %v", err)
	}

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected status code %v, got %v", http.StatusBadRequest, w.Code)
	}
	if response.Error == "" {
		t.Fatalf("error description must not be empty")
	}
	if getCalled {
		t.Fatalf("GetTask must not be called")
	}
}

func TestHandler_GetTask_TaskNotFound(t *testing.T) {
	var idToGet string
	repo := fakeRepository{
		getFunc: func(ctx context.Context, id string) (*domain.Task, error) {
			idToGet = id
			return nil, storage.ErrNotFound
		},
	}
	b := fakeBroker{}

	handler := Handler{
		Repo:   &repo,
		Broker: &b,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	r := httptest.NewRequest(http.MethodGet, "/tasks/testUuid", nil)
	pathValue := "testUuid"
	r.SetPathValue("id", pathValue)
	w := httptest.NewRecorder()

	handler.GetTask(w, r)

	var response ErrorResponse
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatalf("failed to decode response body: %v", err)
	}

	if pathValue != idToGet {
		t.Fatalf("id to get must match id from request")
	}
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected status code %v, got %v", http.StatusNotFound, w.Code)
	}
	if response.Error == "" {
		t.Fatalf("error description must not be empty")
	}
}

func TestHandler_GetTask_DatabaseError(t *testing.T) {
	var idToGet string
	repo := fakeRepository{
		getFunc: func(ctx context.Context, id string) (*domain.Task, error) {
			idToGet = id
			return nil, fmt.Errorf("failed to get task from db")
		},
	}
	b := fakeBroker{}

	handler := Handler{
		Repo:   &repo,
		Broker: &b,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	r := httptest.NewRequest(http.MethodGet, "/tasks/testUuid", nil)
	pathValue := "testUuid"
	r.SetPathValue("id", pathValue)
	w := httptest.NewRecorder()

	handler.GetTask(w, r)

	var response ErrorResponse
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatalf("failed to decode response body: %v", err)
	}

	if pathValue != idToGet {
		t.Fatalf("id to get must match id from request")
	}
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("expected status code %v, got %v", http.StatusInternalServerError, w.Code)
	}
	if response.Error == "" {
		t.Fatalf("error description must not be empty")
	}
}

func TestHandler_GetTask_Success(t *testing.T) {
	var idToGet string
	task := domain.Task{
		ID:          "testId",
		Type:        "test",
		Payload:     nil,
		Status:      domain.StatusDone,
		RetryCount:  0,
		MaxRetries:  5,
		CreatedAt:   time.Now().UTC(),
		ScheduledAt: time.Now().UTC(),
	}

	repo := fakeRepository{
		getFunc: func(ctx context.Context, id string) (*domain.Task, error) {
			idToGet = id
			return &task, nil
		},
	}
	b := fakeBroker{}

	handler := Handler{
		Repo:   &repo,
		Broker: &b,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	r := httptest.NewRequest(http.MethodGet, "/tasks/testUuid", nil)
	pathValue := "testUuid"
	r.SetPathValue("id", pathValue)
	w := httptest.NewRecorder()

	handler.GetTask(w, r)

	if pathValue != idToGet {
		t.Fatalf("id to get must match id from request")
	}
	if w.Code != http.StatusOK {
		t.Fatalf("expected status code %v, got %v", http.StatusOK, w.Code)
	}

	var response TaskResponse
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatalf("failed to decode response body: %v", err)
	}
	if response.ID != task.ID {
		t.Fatalf("expected ID %v, got %v", task.ID, response.ID)
	}
	if response.Status != task.Status {
		t.Fatalf("expected status %v, got %v", task.Status, response.Status)
	}
	if response.MaxRetries != task.MaxRetries {
		t.Fatalf("expected MaxRetries %v, got %v", task.MaxRetries, response.MaxRetries)
	}
}
