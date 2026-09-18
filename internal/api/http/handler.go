package server

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"task-queue/internal/broker"
	"task-queue/internal/domain"
	"task-queue/internal/storage"
	"time"

	"github.com/google/uuid"
)

const defaultTimeout = 30 * time.Second

type Handler struct {
	Repo   storage.Repository
	Broker broker.Broker
	Logger *slog.Logger
}

func respondJSON(w http.ResponseWriter, logger *slog.Logger, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(payload); err != nil {
		logger.Error("failed to encode response", "error", err)
	}
}

func respondError(w http.ResponseWriter, logger *slog.Logger, status int, message string) {
	respondJSON(w, logger, status, ErrorResponse{Error: message})
}

func (h *Handler) CreateTask(w http.ResponseWriter, r *http.Request) {
	defer func() {
		_ = r.Body.Close()
	}()

	var request CreateTaskRequest

	decodeErr := json.NewDecoder(r.Body).Decode(&request)
	if decodeErr != nil {
		respondError(w, h.Logger, http.StatusBadRequest, decodeErr.Error())
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	valErr := request.Validate()
	if valErr != nil {
		respondError(w, h.Logger, http.StatusBadRequest, valErr.Error())
		return
	}

	task := domain.Task{
		ID:          uuid.NewString(),
		Type:        request.Type,
		Payload:     request.Payload,
		Status:      domain.StatusPending,
		RetryCount:  0,
		MaxRetries:  *request.MaxRetries,
		CreatedAt:   time.Now().UTC(),
		ScheduledAt: *request.ScheduledAt,
	}

	crErr := h.Repo.Create(ctx, &task)
	if crErr != nil {
		respondError(w, h.Logger, http.StatusInternalServerError, "failed to create task in repository")
		return
	}

	pubErr := h.Broker.Publish(ctx, &task)
	if pubErr != nil {
		status := domain.StatusQueueFailed
		updErr := h.Repo.UpdateStatus(ctx, task.ID, domain.StatusQueueFailed)
		if updErr != nil {
			h.Logger.Error("failed to update status of task", "error", updErr)
			status = task.Status
		}
		response := CreateTaskResponse{
			ID:     task.ID,
			Status: status,
		}
		respondJSON(w, h.Logger, http.StatusAccepted, response)
		return
	}

	response := CreateTaskResponse{
		ID:     task.ID,
		Status: task.Status,
	}
	respondJSON(w, h.Logger, http.StatusAccepted, response)
}
