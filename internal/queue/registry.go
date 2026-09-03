package queue

import (
	"context"
	"fmt"
	"sync"
)

// Handler processes a single job's payload. Returning an error marks the job
// for retry (or permanent failure once attempts are exhausted).
type Handler func(ctx context.Context, payload []byte) error

// Registry maps job type strings to their handlers. It is safe for concurrent
// use: Register is expected at startup, Get from every worker goroutine.
type Registry struct {
	mu       sync.RWMutex
	handlers map[string]Handler
}

// NewRegistry returns an empty Registry.
func NewRegistry() *Registry {
	return &Registry{handlers: make(map[string]Handler)}
}

// Register associates a handler with a job type. It panics if the type is empty
// or already registered — both are programmer errors at wiring time.
func (r *Registry) Register(jobType string, h Handler) {
	if jobType == "" {
		panic("queue: cannot register handler for empty job type")
	}
	if h == nil {
		panic(fmt.Sprintf("queue: nil handler for job type %q", jobType))
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.handlers[jobType]; exists {
		panic(fmt.Sprintf("queue: handler already registered for job type %q", jobType))
	}
	r.handlers[jobType] = h
}

// Get looks up the handler for a job type.
func (r *Registry) Get(jobType string) (Handler, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	h, ok := r.handlers[jobType]
	return h, ok
}
