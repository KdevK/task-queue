package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
)

func handlerFunc(ctx context.Context, payload json.RawMessage) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	fmt.Println(payload)
	return nil
}

func TestRegistry_Register_Success(t *testing.T) {
	reg := NewRegistry()
	taskType := "testType"
	reg.Register(taskType, handlerFunc)

	if _, ok := reg.handlers[taskType]; !ok {
		t.Fatalf("failed to get handler from registry")
	}
}

func TestRegistry_Register_Panic(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("expected panic, got nil")
		}
	}()

	reg := NewRegistry()
	taskType := "testType"

	reg.Register(taskType, handlerFunc)
	reg.Register(taskType, handlerFunc)
}

func TestRegistry_Get_Success(t *testing.T) {
	reg := NewRegistry()
	taskType := "testType"
	reg.handlers[taskType] = handlerFunc

	_, ok := reg.Get(taskType)
	if !ok {
		t.Fatalf("failed to get handler")
	}
}

func TestRegistry_Get_NoValue(t *testing.T) {
	reg := NewRegistry()
	nonRegisteredType := "testType"

	_, ok := reg.Get(nonRegisteredType)
	if ok {
		t.Fatalf("no value should be extracted")
	}
}
