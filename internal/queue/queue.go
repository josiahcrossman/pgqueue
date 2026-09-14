// Package queue is a Postgres-backed job queue.
//
// The SQL statements and transaction boundaries for claiming and completing
// jobs are intentionally left unimplemented (they panic with a TODO). Fill them
// in; the surrounding worker loop, registry, backoff, and shutdown are complete
// and require no other changes once the stubs are done.
package queue

import (
	"context"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/josiahcrossman/pgqueue/internal/config"
)

// Status is a job's lifecycle state, stored in the jobs.status column.
type Status string

const (
	StatusQueued  Status = "queued"  // eligible to be claimed once run_after has passed
	StatusRunning Status = "running" // claimed by a worker, in flight
	StatusDone    Status = "done"    // completed successfully
	StatusFailed  Status = "failed"  // permanently failed after max attempts
)

// Job mirrors a row in the jobs table. The column names assumed when scanning
// are documented in migrations/0001_init.up.sql.
type Job struct {
	ID        int64
	Type      string
	Payload   []byte // jsonb
	Status    Status
	Attempts  int
	RunAfter  time.Time
	LockedAt  *time.Time // NULL when unclaimed
	CreatedAt time.Time
	UpdatedAt time.Time
}

// Queue provides enqueue and claim/complete operations against a jobs table.
type Queue struct {
	pool *pgxpool.Pool
	cfg  config.Config
	log  *slog.Logger
}

// New constructs a Queue over the given pool.
func New(pool *pgxpool.Pool, cfg config.Config, log *slog.Logger) *Queue {
	return &Queue{pool: pool, cfg: cfg, log: log}
}

// Enqueue inserts a new job that becomes eligible immediately (run_after = now,
// status = queued, attempts = 0) and returns its generated id.
//
// TODO: write the INSERT. It should set type and payload from the arguments,
// initialise the lifecycle columns, and return the new id.
func (q *Queue) Enqueue(ctx context.Context, jobType string, payload []byte) (int64, error) {
	query := `
		INSERT INTO jobs (type, payload, status, attempts, run_after, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		RETURNING id;
	`
	time := time.Now()
	row := q.pool.QueryRow(ctx, query, jobType, payload, StatusQueued, 0, time, time, time)
	var id int64
	err := row.Scan(&id)
	if err != nil {
		return 0, err
	}
	return id, nil
}

// claimBatch atomically claims up to batchSize jobs that are eligible to run
// (queued and run_after <= now) and returns them.
//
// Guarantees the caller relies on:
//   - it returns at most batchSize jobs;
//   - no two concurrent workers ever receive the same job from overlapping
//     calls;
//   - a returned job is marked as claimed/running so it is not handed out again
//     until it is completed or its lock is considered stale.
//
// How you achieve those guarantees — the locking strategy and where the
// transaction boundary sits — is the exercise. Decide it yourself.
//
// TODO: write claim query and decide the transaction boundary.
func (q *Queue) claimBatch(ctx context.Context, batchSize int) ([]Job, error) {
	
	query := `
		WITH candidates AS (
			SELECT id
			FROM jobs
			WHERE status = 'queued'
			AND run_after <= now()
			ORDER BY run_after
			LIMIT $1
			FOR UPDATE SKIP LOCKED
		)
		UPDATE jobs
		SET status = 'running', locked_at = now(), updated_at = now()
		FROM candidates
		WHERE jobs.id = candidates.id
		RETURNING ` + jobColumns + `;
	`
	rows, err := q.pool.Query(ctx, query, batchSize)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	jobs := []Job{}
	for rows.Next() {
		job, err := rowToJob(rows)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, job)
	}
	return jobs, nil

}

// markDone marks a claimed job as successfully completed.
//
// TODO: write the UPDATE that transitions the job to StatusDone and clears its
// lock.
func (q *Queue) markDone(ctx context.Context, jobID int64) error {
	query := `
		UPDATE jobs
		SET status = 'done', locked_at = NULL, updated_at = now()
		WHERE id = $1;
	`
	_, err := q.pool.Exec(ctx, query, jobID)
	if err != nil {
		return err
	}
	return nil
}

// markFailed records a failed attempt. attempts is the job's attempt count
// after this run (i.e. how many times it has now been tried); runErr is why it
// failed. If attempts < cfg.MaxAttempts the job should be rescheduled to run
// after backoff(attempts) has elapsed and returned to a claimable state;
// otherwise it should be marked StatusFailed permanently. Use q.backoff to
// compute the delay.
//
// TODO: write the UPDATE(s) implementing that retry-or-fail decision.
func (q *Queue) markFailed(ctx context.Context, jobID int64, attempts int, runErr error) error {
	if attempts < q.cfg.MaxAttempts {
		query := `
			UPDATE jobs
			SET status = 'queued', locked_at = NULL, updated_at = now(), attempts = $3, run_after = now() + $2
			WHERE id = $1;
		`
		_, err := q.pool.Exec(ctx, query, jobID, q.backoff(attempts), attempts)
		if err != nil {
			return err
		}
	}else {	
		query := `
			UPDATE jobs
			SET status = 'failed', locked_at = NULL, updated_at = now(), attempts = $2
			WHERE id = $1;
		`
		_, err := q.pool.Exec(ctx, query, jobID, attempts)
		if err != nil {
			return err
		}
	}
	return nil
}

// backoff returns the delay before a job's next attempt using exponential
// backoff: base * 2^(attempts-1), capped to keep the shift in range. This is
// fully implemented; markFailed should call it.
func (q *Queue) backoff(attempts int) time.Duration {
	if attempts < 1 {
		attempts = 1
	}
	// Cap the exponent so the shift can't overflow or produce absurd delays.
	const maxShift = 16
	shift := attempts - 1
	if shift > maxShift {
		shift = maxShift
	}
	return q.cfg.BaseBackoff * time.Duration(int64(1)<<uint(shift))
}

// rowToJob scans a single job row. The column order here is the order the claim
// query's RETURNING/SELECT must produce. Provided so your claim SQL has a
// scanning target ready to use.
func rowToJob(row pgx.Row) (Job, error) {
	var j Job
	err := row.Scan(
		&j.ID,
		&j.Type,
		&j.Payload,
		&j.Status,
		&j.Attempts,
		&j.RunAfter,
		&j.LockedAt,
		&j.CreatedAt,
		&j.UpdatedAt,
	)
	return j, err
}

// jobColumns lists the columns, in scan order, that rowToJob expects. Handy for
// building the SELECT/RETURNING list in the claim query you write.
const jobColumns = "jobs.id, jobs.type, jobs.payload, jobs.status, jobs.attempts, jobs.run_after, jobs.locked_at, jobs.created_at, jobs.updated_at"
