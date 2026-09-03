// Command loadtest seeds N no-op jobs, drains them with the worker pool, and
// reports throughput. Critically, it asserts exactly-once execution: the no-op
// handler increments a counter row per seeded job, and after draining every
// job must have run exactly once.
//
//	go run ./cmd/loadtest -n 10000
//
// Note: this truncates the jobs and job_runs tables at the start so the
// measurement is clean. Do not point it at a database you care about.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/josiahcrossman/pgqueue/internal/config"
	"github.com/josiahcrossman/pgqueue/internal/queue"
)

// loadtestJobType is the no-op job type seeded by this command.
const loadtestJobType = "loadtest_noop"

// seqPayload is the payload shape: a unique sequence number that doubles as the
// key in job_runs so the handler can record "this logical job ran".
type seqPayload struct {
	Seq int `json:"seq"`
}

func main() {
	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	var n int
	var drainTimeout time.Duration
	flag.IntVar(&n, "n", 5000, "number of jobs to seed")
	flag.DurationVar(&drainTimeout, "timeout", 2*time.Minute, "max time to wait for the queue to drain")
	flag.Parse()

	if err := run(log, n, drainTimeout); err != nil {
		log.Error("loadtest failed", "error", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger, n int, drainTimeout time.Duration) error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}

	ctx := context.Background()
	pool, err := queue.Connect(ctx, cfg)
	if err != nil {
		return err
	}
	defer pool.Close()

	if err := reset(ctx, pool); err != nil {
		return fmt.Errorf("reset tables: %w", err)
	}

	q := queue.New(pool, cfg, log)

	// The no-op handler records that a job ran by upserting its counter row.
	registry := queue.NewRegistry()
	registry.Register(loadtestJobType, func(ctx context.Context, payload []byte) error {
		var p seqPayload
		if err := json.Unmarshal(payload, &p); err != nil {
			return err
		}
		_, err := pool.Exec(ctx, `
			INSERT INTO job_runs (job_id, run_count) VALUES ($1, 1)
			ON CONFLICT (job_id) DO UPDATE SET run_count = job_runs.run_count + 1`,
			p.Seq)
		return err
	})

	// Seed.
	log.Info("seeding jobs", "count", n)
	seedStart := time.Now()
	for i := 0; i < n; i++ {
		payload, _ := json.Marshal(seqPayload{Seq: i})
		if _, err := q.Enqueue(ctx, loadtestJobType, payload); err != nil {
			return fmt.Errorf("enqueue job %d: %w", i, err)
		}
	}
	log.Info("seeded", "count", n, "seed_duration", time.Since(seedStart))

	// Drain: run the worker pool and time how long until the queue empties.
	workerCtx, stopWorkers := context.WithCancel(ctx)
	defer stopWorkers()

	worker := queue.NewWorker(q, registry, cfg, log)
	done := make(chan error, 1)
	go func() { done <- worker.Run(workerCtx) }()

	processStart := time.Now()
	if err := waitDrained(ctx, pool, drainTimeout); err != nil {
		stopWorkers()
		<-done
		return err
	}
	elapsed := time.Since(processStart)

	// Stop the workers and wait for them to exit cleanly.
	stopWorkers()
	if err := <-done; err != nil {
		return err
	}

	// Report throughput.
	jobsPerSec := float64(n) / elapsed.Seconds()
	fmt.Println("----------------------------------------")
	fmt.Printf("jobs:            %d\n", n)
	fmt.Printf("wall time:       %s\n", elapsed.Round(time.Millisecond))
	fmt.Printf("throughput:      %.0f jobs/sec\n", jobsPerSec)
	fmt.Println("----------------------------------------")

	// Assert exactly-once.
	return assertExactlyOnce(ctx, pool, n)
}

// reset truncates the queue and counter tables for a clean run.
func reset(ctx context.Context, pool *pgxpool.Pool) error {
	_, err := pool.Exec(ctx, `TRUNCATE jobs, job_runs`)
	return err
}

// waitDrained polls until no job remains queued or running, or the timeout
// elapses.
func waitDrained(ctx context.Context, pool *pgxpool.Pool, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	for {
		var remaining int
		err := pool.QueryRow(ctx,
			`SELECT count(*) FROM jobs WHERE status IN ('queued', 'running')`).Scan(&remaining)
		if err != nil {
			return fmt.Errorf("count remaining jobs: %w", err)
		}
		if remaining == 0 {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("queue did not drain within %s: %d jobs remaining", timeout, remaining)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// assertExactlyOnce fails if any seeded job did not run exactly once.
func assertExactlyOnce(ctx context.Context, pool *pgxpool.Pool, n int) error {
	var distinct, notOnce, maxCount int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM job_runs`).Scan(&distinct); err != nil {
		return err
	}
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM job_runs WHERE run_count <> 1`).Scan(&notOnce); err != nil {
		return err
	}
	if err := pool.QueryRow(ctx,
		`SELECT coalesce(max(run_count), 0) FROM job_runs`).Scan(&maxCount); err != nil {
		return err
	}

	if distinct != n {
		return fmt.Errorf("exactly-once FAILED: %d distinct jobs ran, expected %d", distinct, n)
	}
	if notOnce != 0 {
		return fmt.Errorf("exactly-once FAILED: %d jobs ran more than once (max run_count=%d)", notOnce, maxCount)
	}

	fmt.Printf("exactly-once OK: all %d jobs ran exactly once (max run_count=%d)\n", n, maxCount)
	return nil
}
