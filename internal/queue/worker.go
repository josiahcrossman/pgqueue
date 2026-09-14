package queue

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/josiahcrossman/pgqueue/internal/config"
)

// Worker runs a pool of goroutines that claim jobs, dispatch them to registered
// handlers by job type, and record the outcome.
type Worker struct {
	q        *Queue
	registry *Registry
	cfg      config.Config
	log      *slog.Logger
}

// NewWorker wires a Worker. The registry should already have its handlers
// registered before Run is called.
func NewWorker(q *Queue, r *Registry, cfg config.Config, log *slog.Logger) *Worker {
	return &Worker{q: q, registry: r, cfg: cfg, log: log}
}

// Run starts cfg.WorkerCount goroutines and blocks until ctx is cancelled. On
// cancellation it stops claiming new work and waits for every goroutine to
// finish the batch it is currently processing before returning. In-flight
// handlers are given a context that is NOT cancelled by shutdown, so a running
// job always runs to completion.
func (w *Worker) Run(ctx context.Context) error {
	w.log.Info("worker pool starting",
		"workers", w.cfg.WorkerCount,
		"batch_size", w.cfg.BatchSize,
		"poll_interval", w.cfg.PollInterval,
	)

	var wg sync.WaitGroup
	for i := 0; i < w.cfg.WorkerCount + 1; i++ {
		if i == w.cfg.WorkerCount {
			wg.Add(1)
			go func() {
				defer wg.Done()
				w.reclaimStaleLoop(ctx)
			}()
		}else {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				w.runLoop(ctx, id)
			}(i)
		}
	}

	wg.Wait()
	w.log.Info("worker pool stopped")
	return nil
}
func (w *Worker) reclaimStaleLoop(ctx context.Context) {
	log := w.log.With("worker", "reclaimer")
	log.Debug("reclaim stale loop started")

	for {
		if ctx.Err() != nil {
			log.Debug("reclaim stale loop exiting")
			return
		}

		count, err := w.q.reclaimStale(ctx, w.cfg.StaleLockTimeout)
		if err != nil {
			log.Error("reclaim stale failed", "error", err)
			if w.sleep(ctx, w.cfg.PollInterval) {
				return
			}
		}

		log.Info("reclaimed stale jobs", "count", count)
		if w.sleep(ctx, w.cfg.PollInterval) {
			return
		}
	}
}
// runLoop is one worker goroutine. It repeatedly claims a batch and processes
// it. When there is no work it sleeps for the poll interval. It exits once ctx
// is cancelled — after finishing any batch already in hand.
func (w *Worker) runLoop(ctx context.Context, id int) {
	log := w.log.With("worker", id)
	log.Debug("worker loop started")

	for {
		// Stop claiming new work once shutdown has been requested.
		if ctx.Err() != nil {
			log.Debug("worker loop exiting")
			return
		}

		jobs, err := w.q.claimBatch(ctx, w.cfg.BatchSize)
		if err != nil {
			if ctx.Err() != nil {
				// Claim failed because we're shutting down; that's expected.
				log.Debug("worker loop exiting")
				return
			}
			log.Error("claim batch failed", "error", err)
			if w.sleep(ctx, w.cfg.PollInterval) {
				return
			}
			continue
		}

		if len(jobs) == 0 {
			if w.sleep(ctx, w.cfg.PollInterval) {
				return
			}
			continue
		}

		log.Debug("claimed batch", "count", len(jobs))
		// Finish the whole claimed batch even if shutdown arrives mid-batch, so
		// no claimed job is stranded. Handlers and outcome writes use a detached
		// context that shutdown does not cancel.
		jobCtx := context.WithoutCancel(ctx)
		for _, job := range jobs {
			w.process(jobCtx, log, job)
		}
	}
}

// process dispatches one job to its handler and records the outcome.
func (w *Worker) process(ctx context.Context, log *slog.Logger, job Job) {
	log = log.With("job_id", job.ID, "job_type", job.Type, "attempt", job.Attempts)

	handler, ok := w.registry.Get(job.Type)
	if !ok {
		// No handler for this type: treat as a failed attempt so it retries or,
		// eventually, lands in failed rather than looping forever as running.
		err := errors.New("no handler registered for job type")
		log.Error("dispatch failed", "error", err)
		w.recordFailure(ctx, log, job, err)
		return
	}

	start := time.Now()
	err := w.safeInvoke(ctx, handler, job.Payload)
	dur := time.Since(start)

	if err != nil {
		log.Warn("job failed", "error", err, "duration", dur)
		w.recordFailure(ctx, log, job, err)
		return
	}

	if err := w.q.markDone(ctx, job.ID); err != nil {
		log.Error("mark done failed", "error", err)
		return
	}
	log.Info("job done", "duration", dur)
}

// recordFailure bumps the attempt count and lets markFailed decide retry vs.
// permanent failure. attempts is the count *after* this run.
func (w *Worker) recordFailure(ctx context.Context, log *slog.Logger, job Job, cause error) {
	attempts := job.Attempts + 1
	if err := w.q.markFailed(ctx, job.ID, attempts, cause); err != nil {
		log.Error("mark failed failed", "error", err)
		return
	}
	if attempts >= w.cfg.MaxAttempts {
		log.Error("job permanently failed", "attempts", attempts, "max_attempts", w.cfg.MaxAttempts)
	} else {
		log.Info("job scheduled for retry", "attempts", attempts, "backoff", w.q.backoff(attempts))
	}
}

// safeInvoke runs a handler, converting a panic into an error so one bad
// handler cannot take down a worker goroutine.
func (w *Worker) safeInvoke(ctx context.Context, h Handler, payload []byte) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = &HandlerPanicError{Value: r}
		}
	}()
	return h(ctx, payload)
}

// sleep waits for d or until ctx is cancelled. It reports whether ctx was
// cancelled (true means the caller should stop).
func (w *Worker) sleep(ctx context.Context, d time.Duration) (cancelled bool) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return true
	case <-t.C:
		return false
	}
}

// HandlerPanicError wraps a value recovered from a panicking handler.
type HandlerPanicError struct{ Value any }

func (e *HandlerPanicError) Error() string {
	return fmt.Sprintf("handler panicked: %v", e.Value)
}
