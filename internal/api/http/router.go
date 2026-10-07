package server

import (
	"log/slog"
	"net/http"
	"time"
)

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (rec *statusRecorder) WriteHeader(status int) {
	rec.status = status
	rec.ResponseWriter.WriteHeader(status)
}

func NewRouter(handler *Handler, logger *slog.Logger) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /tasks", handler.CreateTask)
	mux.HandleFunc("GET /tasks/{id}", handler.GetTask)

	var h http.Handler = mux
	h = recoverMiddleware(h, logger)
	h = loggingMiddleware(h, logger)
	return h
}

func recoverMiddleware(next http.Handler, logger *slog.Logger) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if err := recover(); err != nil {
				logger.Error("panic recovered", "method", r.Method, "path", r.URL.Path, "error", err)
				respondError(w, logger, http.StatusInternalServerError, "internal error")
			}
		}()
		next.ServeHTTP(w, r)
	})
}

func loggingMiddleware(next http.Handler, logger *slog.Logger) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}

		start := time.Now()
		next.ServeHTTP(rec, r)
		duration := time.Since(start)

		method := r.Method
		path := r.URL.Path
		status := rec.status

		switch {
		case status >= 500:
			logger.Error("request handled", "method", method, "path", path, "status", status, "duration", duration.String())
		case status >= 400:
			logger.Warn("request handled", "method", method, "path", path, "status", status, "duration", duration.String())
		default:
			logger.Info("request handled", "method", method, "path", path, "status", status, "duration", duration.String())
		}
	})
}
