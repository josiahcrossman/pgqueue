//go:build integration

// Package integration exercises the queue against a real Postgres from
// docker-compose. Run with: make up && make migrate && make test-integration
//
// Until the SQL stubs (Enqueue, claimBatch, markDone, markFailed) are filled
// in, these tests fail with the TODO panics. Once implemented correctly they
// pass with no changes here.
package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/josiahcrossman/pgqueue/internal/config"
	"github.com/josiahcrossman/pgqueue/internal/queue"
)

const defaultTestDBURL = "postgres://pgqueue:pgqueue@localhost:5432/pgqueue?sslmode=disable"

// testConfig builds a config from DATABASE_URL (falling back to the compose
// default) and lets each test override the tuning knobs.
func testConfig(t *testing.T) config.Config {
	t.Helper()
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		url = defaultTestDBURL
	}
	return config.Config{
		DatabaseURL:  url,
		WorkerCount:  2,
		PollInterval: 20 * time.Millisecond,
		BatchSize:    5,
		MaxAttempts:  3,
		BaseBackoff:  50 * time.Millisecond,
	}
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// setup connects, truncates the tables, and returns a ready pool. It skips the
// test if Postgres is unreachable so the suite degrades gracefully.
func setup(t *testing.T, cfg config.Config) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()
	pool, err := queue.Connect(ctx, cfg)
	if err != nil {
		t.Skipf("Postgres not reachable (run `make up && make migrate`): %v", err)
	}
	t.Cleanup(pool.Close)

	if _, err := pool.Exec(ctx, `TRUNCATE jobs, job_runs`); err != nil {
		t.Fatalf("truncate failed (did you run `make migrate`?): %v", err)
	}
	return pool
}

// jobStatus reads a job's status and attempts.
func jobStatus(t *testing.T, pool *pgxpool.Pool, id int64) (string, int) {
	t.Helper()
	var status string
	var attempts int
	err := pool.QueryRow(context.Background(),
		`SELECT status, attempts FROM jobs WHERE id = $1`, id).Scan(&status, &attempts)
	require.NoError(t, err)
	return status, attempts
}

// waitFor polls cond until it returns true or the timeout elapses.
func waitFor(t *testing.T, timeout time.Duration, cond func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return cond()
}

// runWorkers starts a worker pool in the background and returns a stop func that
// cancels it and waits for a clean drain.
func runWorkers(cfg config.Config, q *queue.Queue, reg *queue.Registry) (stop func()) {
	ctx, cancel := context.WithCancel(context.Background())
	w := queue.NewWorker(q, reg, cfg, testLogger())
	done := make(chan struct{})
	go func() {
		_ = w.Run(ctx)
		close(done)
	}()
	return func() {
		cancel()
		<-done
	}
}

// TestJobRunsAndIsMarkedDone: a successfully handled job ends up StatusDone and
// its handler ran exactly once.
func TestJobRunsAndIsMarkedDone(t *testing.T) {
	cfg := testConfig(t)
	pool := setup(t, cfg)
	q := queue.New(pool, cfg, testLogger())

	var runs atomic.Int64
	reg := queue.NewRegistry()
	reg.Register("ok", func(ctx context.Context, payload []byte) error {
		runs.Add(1)
		return nil
	})

	id, err := q.Enqueue(context.Background(), "ok", []byte(`{}`))
	require.NoError(t, err)

	stop := runWorkers(cfg, q, reg)
	defer stop()

	ok := waitFor(t, 5*time.Second, func() bool {
		status, _ := jobStatus(t, pool, id)
		return status == string(queue.StatusDone)
	})
	require.True(t, ok, "job never reached status=done")
	require.Equal(t, int64(1), runs.Load(), "handler should run exactly once")
}

// TestFailingJobRetriesWithBackoffThenFails: a job whose handler always errors
// is retried up to MaxAttempts (with backoff between attempts) and then marked
// StatusFailed.
func TestFailingJobRetriesWithBackoffThenFails(t *testing.T) {
	cfg := testConfig(t)
	cfg.MaxAttempts = 3
	cfg.BaseBackoff = 100 * time.Millisecond
	pool := setup(t, cfg)
	q := queue.New(pool, cfg, testLogger())

	var attempts atomic.Int64
	reg := queue.NewRegistry()
	reg.Register("boom", func(ctx context.Context, payload []byte) error {
		attempts.Add(1)
		return fmt.Errorf("intentional failure")
	})

	id, err := q.Enqueue(context.Background(), "boom", []byte(`{}`))
	require.NoError(t, err)

	start := time.Now()
	stop := runWorkers(cfg, q, reg)
	defer stop()

	ok := waitFor(t, 10*time.Second, func() bool {
		status, _ := jobStatus(t, pool, id)
		return status == string(queue.StatusFailed)
	})
	require.True(t, ok, "job never reached status=failed")
	elapsed := time.Since(start)

	status, dbAttempts := jobStatus(t, pool, id)
	require.Equal(t, string(queue.StatusFailed), status)
	require.Equal(t, cfg.MaxAttempts, dbAttempts, "attempts column should equal MaxAttempts")
	require.Equal(t, int64(cfg.MaxAttempts), attempts.Load(), "handler should run once per attempt")

	// Backoff between attempts 1->2 (100ms) and 2->3 (200ms) means the run
	// cannot have completed instantly. Lower bound is deliberately loose to
	// avoid flakiness.
	require.GreaterOrEqual(t, elapsed, 250*time.Millisecond, "retries should be spaced out by backoff")
}

// TestConcurrentWorkersProcessEachJobOnce: with two independent worker pools
// hammering the same queue, every job runs exactly once (no double-claim).
func TestConcurrentWorkersProcessEachJobOnce(t *testing.T) {
	cfg := testConfig(t)
	cfg.WorkerCount = 4
	cfg.BatchSize = 10
	pool := setup(t, cfg)
	q := queue.New(pool, cfg, testLogger())

	const n = 300

	reg := queue.NewRegistry()
	reg.Register("count", func(ctx context.Context, payload []byte) error {
		var p struct {
			Seq int `json:"seq"`
		}
		if err := json.Unmarshal(payload, &p); err != nil {
			return err
		}
		_, err := pool.Exec(ctx, `
			INSERT INTO job_runs (job_id, run_count) VALUES ($1, 1)
			ON CONFLICT (job_id) DO UPDATE SET run_count = job_runs.run_count + 1`, p.Seq)
		return err
	})

	for i := 0; i < n; i++ {
		payload, _ := json.Marshal(map[string]int{"seq": i})
		_, err := q.Enqueue(context.Background(), "count", payload)
		require.NoError(t, err)
	}

	// Two separate pools => two independent sets of goroutines competing.
	var wg sync.WaitGroup
	stops := make([]func(), 2)
	for i := range stops {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			stops[i] = runWorkers(cfg, q, reg)
		}(i)
	}
	wg.Wait()
	defer func() {
		for _, s := range stops {
			s()
		}
	}()

	ok := waitFor(t, 20*time.Second, func() bool {
		var remaining int
		err := pool.QueryRow(context.Background(),
			`SELECT count(*) FROM jobs WHERE status IN ('queued', 'running')`).Scan(&remaining)
		require.NoError(t, err)
		return remaining == 0
	})
	require.True(t, ok, "queue never drained")

	// Exactly-once: n distinct rows, none with run_count != 1.
	var distinct, notOnce int
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT count(*) FROM job_runs`).Scan(&distinct))
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT count(*) FROM job_runs WHERE run_count <> 1`).Scan(&notOnce))

	require.Equal(t, n, distinct, "every job should have run")
	require.Equal(t, 0, notOnce, "no job should run more than once")
}
