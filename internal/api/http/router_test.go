package server

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func Test_recoverMiddleware_PanicRecovered(t *testing.T) {
	panicking := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		panic("test")
	})

	wrapped := recoverMiddleware(panicking, slog.New(slog.NewTextHandler(io.Discard, nil)))

	r := httptest.NewRequest(http.MethodGet, "/test", nil)
	w := httptest.NewRecorder()

	wrapped.ServeHTTP(w, r)

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("expected status %v, got %v", http.StatusInternalServerError, w.Code)
	}
}

func Test_loggingMiddleware_LogSuccessful(t *testing.T) {
	tests := []struct {
		name       string
		wantStatus int
		wantLevel  string
	}{
		{
			name:       "log level: info",
			wantStatus: http.StatusOK,
			wantLevel:  "INFO",
		},
		{
			name:       "log level: warn",
			wantStatus: http.StatusBadRequest,
			wantLevel:  "WARN",
		},
		{
			name:       "log level: error",
			wantStatus: http.StatusInternalServerError,
			wantLevel:  "ERROR",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var logBuffer bytes.Buffer
			logHandler := slog.NewJSONHandler(&logBuffer, nil)
			logger := slog.New(logHandler)

			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tt.wantStatus)
			})
			wrapped := loggingMiddleware(handler, logger)

			wantMethod := http.MethodGet
			wantPath := "/test"

			r := httptest.NewRequest(wantMethod, wantPath, nil)
			w := httptest.NewRecorder()

			wrapped.ServeHTTP(w, r)

			var logEntry struct {
				Level  string `json:"level"`
				Method string `json:"method"`
				Path   string `json:"path"`
				Status int    `json:"status"`
			}

			if err := json.Unmarshal(logBuffer.Bytes(), &logEntry); err != nil {
				t.Fatalf("failed to decode log record: %v", err)
			}
			if logEntry.Level != tt.wantLevel {
				t.Fatalf("expected logged method %v, got %v", tt.wantLevel, logEntry.Level)
			}
			if logEntry.Method != wantMethod {
				t.Fatalf("expected logged method %v, got %v", wantMethod, logEntry.Method)
			}
			if logEntry.Path != wantPath {
				t.Fatalf("expected logged path %v, got %v", wantPath, logEntry.Path)
			}
			if logEntry.Status != tt.wantStatus {
				t.Fatalf("expected status %v, got %v", tt.wantStatus, logEntry.Status)
			}
		})
	}
}

func Test_loggingMiddleware_recoverMiddleware(t *testing.T) {
	tests := []struct {
		name          string
		wantStatus    int
		wantLevel     string
		handlerPanics bool
	}{
		{
			name:       "log level: info",
			wantStatus: http.StatusOK,
			wantLevel:  "INFO",
		},
		{
			name:       "log level: warn",
			wantStatus: http.StatusBadRequest,
			wantLevel:  "WARN",
		},
		{
			name:       "no panic, log level: error",
			wantStatus: http.StatusInternalServerError,
			wantLevel:  "ERROR",
		},
		{
			name:          "has panic, log level: error",
			wantStatus:    http.StatusInternalServerError,
			wantLevel:     "ERROR",
			handlerPanics: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var logBuffer bytes.Buffer
			logHandler := slog.NewJSONHandler(&logBuffer, nil)
			logger := slog.New(logHandler)

			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if tt.handlerPanics {
					panic("test")
				}
				w.WriteHeader(tt.wantStatus)
			})
			wrapped := recoverMiddleware(handler, logger)
			doubleWrapped := loggingMiddleware(wrapped, logger)

			wantMethod := http.MethodGet
			wantPath := "/test"

			r := httptest.NewRequest(wantMethod, wantPath, nil)
			w := httptest.NewRecorder()

			doubleWrapped.ServeHTTP(w, r)

			var logEntry struct {
				Level   string `json:"level"`
				Message string `json:"msg"`
				Method  string `json:"method"`
				Path    string `json:"path"`
				Status  int    `json:"status"`
			}

			output := strings.TrimSpace(logBuffer.String())
			if tt.handlerPanics {
				var panicLog struct {
					Level   string `json:"level"`
					Message string `json:"msg"`
					Method  string `json:"method"`
					Path    string `json:"path"`
					Error   string `json:"error"`
				}
				lines := strings.Split(output, "\n")
				if len(lines) != 2 {
					t.Fatalf("expected 2 log records, got %v", len(lines))
				}
				if err := json.Unmarshal([]byte(lines[0]), &panicLog); err != nil {
					t.Fatalf("failed to decode log record: %v", err)
				}

				output = lines[1]

				if panicLog.Level != tt.wantLevel {
					t.Fatalf("expected logged method %v, got %v", tt.wantLevel, panicLog.Level)
				}
				if panicLog.Method != wantMethod {
					t.Fatalf("expected logged method %v, got %v", wantMethod, panicLog.Method)
				}
				if panicLog.Path != wantPath {
					t.Fatalf("expected logged path %v, got %v", wantPath, panicLog.Path)
				}
				if panicLog.Error == "" {
					t.Fatalf("expected error %v, got empty field", tt.wantStatus)
				}
			}

			if err := json.Unmarshal([]byte(output), &logEntry); err != nil {
				t.Fatalf("failed to decode log record: %v", err)
			}
			if logEntry.Level != tt.wantLevel {
				t.Fatalf("expected logged method %v, got %v", tt.wantLevel, logEntry.Level)
			}
			if logEntry.Method != wantMethod {
				t.Fatalf("expected logged method %v, got %v", wantMethod, logEntry.Method)
			}
			if logEntry.Path != wantPath {
				t.Fatalf("expected logged path %v, got %v", wantPath, logEntry.Path)
			}
			if logEntry.Status != tt.wantStatus {
				t.Fatalf("expected status %v, got %v", tt.wantStatus, logEntry.Status)
			}
		})
	}
}
