package worker

import (
	"context"
	"encoding/json"
)

type Handler func(ctx context.Context, payload json.RawMessage) error

type Registry struct {
	handlers map[string]Handler
}

func NewRegistry() *Registry {
	return &Registry{handlers: make(map[string]Handler)}
}

func (r *Registry) Register(taskType string, h Handler) {
	_, ok := r.handlers[taskType]
	if ok {
		panic("handler for this task type already exists")
	}
	r.handlers[taskType] = h
}

func (r *Registry) Get(taskType string) (Handler, bool) {
	h, ok := r.handlers[taskType]
	return h, ok
}
