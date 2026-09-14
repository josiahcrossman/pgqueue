package queue

import (
	"context"
	"errors"
	"time"
)

// reclaimStale finds jobs stuck at StatusRunning because the worker that
// claimed them died — crashed, was killed, lost its connection — before
// calling markDone or markFailed. locked_at records the instant each job was
// claimed, so a running job whose locked_at is older than staleAfter is
// treated as orphaned rather than actively in progress.
//
// Recovering an orphaned job means treating it exactly like a failed attempt:
// retry it with backoff if attempts remain, or mark it permanently failed
// once attempts are exhausted (see markFailed).
//
// Guarantees the caller relies on:
//   - a job still being legitimately processed by a live worker (locked_at
//     within staleAfter of now) must never be touched;
//   - if reclaimStale is called concurrently — e.g. on an overlapping
//     schedule, or from more than one process — the same orphaned job must
//     never be reclaimed twice.
//
// Returns the number of jobs reclaimed (both outcomes — requeued and
// permanently failed — count). How you achieve those guarantees, the query
// and its transaction boundary, is yours to decide, same as claimBatch.
//
// TODO: write reclaim query and decide the transaction boundary.
func (q *Queue) reclaimStale(ctx context.Context, staleAfter time.Duration) (int, error) {
	query := `	
		WITH candidates AS (
			SELECT id
			FROM jobs
			WHERE status = 'running'
			AND locked_at < now() - make_interval(secs => $1)
			ORDER BY locked_at
			LIMIT 100
			FOR UPDATE SKIP LOCKED
		)
		UPDATE jobs
		SET locked_at = now()
		FROM candidates
		WHERE jobs.id = candidates.id
		RETURNING ` + jobColumns + `;
	`
	rows, err := q.pool.Query(ctx, query, staleAfter.Seconds())
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		job, err := rowToJob(rows)
		if err != nil {
			return 0, err
		}
		err = q.markFailed(ctx, job.ID, job.Attempts + 1, errors.New("job timed out"))
		if err != nil {
			return 0, err
		}
		count++
	}
	return count, nil
}
