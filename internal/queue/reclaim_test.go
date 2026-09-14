//go:build integration

// This test lives in package queue (not the external test/ package) because
// reclaimStale is unexported, same as claimBatch, markDone, and markFailed.
// Run with: make up && make migrate && go test -tags=integration ./internal/queue/...
package queue

import (
	"context"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/josiahcrossman/pgqueue/internal/config"
)

const reclaimTestDBURL = "postgres://pgqueue:pgqueue@localhost:5432/pgqueue?sslmode=disable"

func reclaimTestConfig() config.Config {
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		url = reclaimTestDBURL
	}
	return config.Config{
		DatabaseURL:  url,
		WorkerCount:  1,
		PollInterval: 20 * time.Millisecond,
		BatchSize:    5,
		MaxAttempts:  3,
		BaseBackoff:  50 * time.Millisecond,
	}
}

// setupReclaimQueue connects, truncates, and returns a ready Queue + pool. It
// skips the test if Postgres is unreachable, same as test/integration_test.go.
func setupReclaimQueue(t *testing.T) (*Queue, *pgxpool.Pool, config.Config) {
	t.Helper()
	cfg := reclaimTestConfig()
	ctx := context.Background()

	pool, err := Connect(ctx, cfg)
	if err != nil {
		t.Skipf("Postgres not reachable (run `make up && make migrate`): %v", err)
	}
	t.Cleanup(pool.Close)

	if _, err := pool.Exec(ctx, `TRUNCATE jobs, job_runs`); err != nil {
		t.Fatalf("truncate failed (did you run `make migrate`?): %v", err)
	}

	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	return New(pool, cfg, log), pool, cfg
}

// simulateOrphan inserts a job already in the state a crashed worker would
// leave behind: status=running, attempts=startAttempts, and locked_at set
// lockAge in the past — bypassing claimBatch entirely, since the point is to
// reproduce the aftermath of a claim whose worker never came back.
func simulateOrphan(t *testing.T, pool *pgxpool.Pool, startAttempts int, lockAge time.Duration) int64 {
	t.Helper()
	var id int64
	err := pool.QueryRow(context.Background(), `
		INSERT INTO jobs (type, payload, status, attempts, run_after, locked_at, created_at, updated_at)
		VALUES ('orphan', '{}', 'running', $1, now(), now() - make_interval(secs => $2), now(), now())
		RETURNING id`,
		startAttempts, lockAge.Seconds()).Scan(&id)
	require.NoError(t, err)
	return id
}

func TestReclaimStaleRequeuesOrphanedJob(t *testing.T) {
	q, pool, _ := setupReclaimQueue(t)
	ctx := context.Background()

	id := simulateOrphan(t, pool, 0, time.Hour)

	n, err := q.reclaimStale(ctx, 5*time.Minute)
	require.NoError(t, err)
	require.Equal(t, 1, n)

	var status string
	var attempts int
	var lockedAt *time.Time
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT status, attempts, locked_at FROM jobs WHERE id = $1`, id).
		Scan(&status, &attempts, &lockedAt))

	require.Equal(t, string(StatusQueued), status, "orphaned job with attempts remaining should be requeued")
	require.Equal(t, 1, attempts)
	require.Nil(t, lockedAt, "requeued job should have its lock cleared")
}

func TestReclaimStaleFailsJobAtMaxAttempts(t *testing.T) {
	q, pool, cfg := setupReclaimQueue(t)
	ctx := context.Background()

	id := simulateOrphan(t, pool, cfg.MaxAttempts-1, time.Hour)

	n, err := q.reclaimStale(ctx, 5*time.Minute)
	require.NoError(t, err)
	require.Equal(t, 1, n)

	var status string
	var attempts int
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT status, attempts FROM jobs WHERE id = $1`, id).
		Scan(&status, &attempts))

	require.Equal(t, string(StatusFailed), status, "orphaned job at max attempts should be permanently failed")
	require.Equal(t, cfg.MaxAttempts, attempts)
}

func TestReclaimStaleIgnoresFreshLocks(t *testing.T) {
	q, pool, _ := setupReclaimQueue(t)
	ctx := context.Background()

	// Locked one second ago -- nowhere near stale. A live worker could still
	// legitimately be processing this job.
	id := simulateOrphan(t, pool, 0, 1*time.Second)

	n, err := q.reclaimStale(ctx, 5*time.Minute)
	require.NoError(t, err)
	require.Equal(t, 0, n, "a job with a fresh lock must not be reclaimed")

	var status string
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT status FROM jobs WHERE id = $1`, id).Scan(&status))
	require.Equal(t, string(StatusRunning), status)
}
