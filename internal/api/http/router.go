package server

import (
	"log/slog"
	"net/http"
)

func NewRouter(handler *Handler, logger *slog.Logger) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /tasks", handler.CreateTask)
	mux.HandleFunc("GET /tasks/{id}", handler.GetTask)

	var h http.Handler = mux
	h = recoverMiddleware(h, logger)
	return h
}

func recoverMiddleware(next http.Handler, logger *slog.Logger) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if err := recover(); err != nil {
				logger.Error("panic recovered")
				respondError(w, logger, http.StatusInternalServerError, "internal error")
			}
		}()
		next.ServeHTTP(w, r)
	})
}
