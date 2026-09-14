# pgqueue

A small Postgres-backed job queue library in Go, built as a learning project.
The scaffolding — worker pool, handler registry, config, migrations, graceful
shutdown, loadtest harness, and integration tests — is complete. The SQL and
transaction boundaries are left as stubs for you to implement.

## What you implement

Four functions in `internal/queue/queue.go` panic with a `TODO`:

| Function      | What to write |
|---------------|---------------|
| `Enqueue`     | The `INSERT` that adds a job. |
| `claimBatch`  | The claim query **and** the transaction boundary. Must return ≤ batchSize jobs and guarantee no two workers ever see the same job. The *how* (locking strategy) is the exercise. |
| `markDone`    | The `UPDATE` that completes a job. |
| `markFailed`  | The `UPDATE`(s) that either reschedule with backoff or mark permanently failed. |

Plus the schema itself: `migrations/0001_init.up.sql` is an empty stub with a
comment listing the columns. You write the `CREATE TABLE` and decide the
indexes.

`rowToJob` and the `jobColumns` constant in `queue.go` give you a ready scan
target and column list for the claim query.

## Requirements

- Go 1.22+
- Docker + Docker Compose (Postgres 16)

## Setup

```bash
# 0. Resolve dependencies and generate go.sum (first checkout only)
make tidy   # == go mod tidy

# 1. Start Postgres 16 (waits for the healthcheck)
make up

# 2. Fill in migrations/0001_init.up.sql, then apply migrations
make migrate

# 3. Implement the four stubs in internal/queue/queue.go

# 4. Run the worker
go run ./cmd/worker
```

Configuration is read from the environment (see `.env.example`):

| Variable        | Default                              | Meaning |
|-----------------|--------------------------------------|---------|
| `DATABASE_URL`  | *(required)*                         | pgx connection string |
| `WORKER_COUNT`  | `4`                                  | worker goroutines |
| `POLL_INTERVAL` | `500ms`                              | sleep when no work is found |
| `BATCH_SIZE`    | `10`                                 | jobs claimed per poll |
| `MAX_ATTEMPTS`  | `5`                                  | attempts before permanent failure |
| `BASE_BACKOFF`  | `1s`                                 | retry delay base: `base * 2^(attempts-1)` |

The `Makefile` exports a `DATABASE_URL` default matching `docker-compose.yml`.

## Make targets

| Target                  | Description |
|-------------------------|-------------|
| `make up`               | start Postgres and wait for healthy |
| `make down`             | stop Postgres and drop the volume |
| `make migrate`          | apply migrations |
| `make migrate-down`     | roll back the latest migration |
| `make test`             | unit tests (no DB) |
| `make test-integration` | integration tests against Postgres |
| `make loadtest N=10000` | seed, drain, report throughput, assert exactly-once |

## Integration tests

```bash
make up
make migrate          # after you've written the schema
make test-integration # after you've implemented the stubs
```

They cover: a job runs and is marked done; a failing job retries with backoff
and stops at max attempts; two concurrent worker pools never process the same
job twice. Until the stubs are implemented they fail with the `TODO` panics.

## Loadtest

```bash
make loadtest N=20000
```

Seeds N no-op jobs (each carries a unique sequence number), drains them, and
prints wall time and jobs/sec. The no-op handler upserts a counter row keyed by
that sequence number; the run then asserts every job produced exactly one row
with `run_count = 1`. It **truncates** `jobs` and `job_runs` first.

## Layout

```
cmd/worker     worker process: pool, SIGINT/SIGTERM graceful drain
cmd/migrate    embedded migration runner (up/down)
cmd/loadtest   throughput + exactly-once harness
internal/config    env-var config loading
internal/queue     queue, worker pool, registry, migrations, pool setup
internal/example   sample handler
migrations         SQL migrations (0001 is a stub)
test               integration tests (build tag: integration)
```

## Findings

**Schema:** `run_after`, `locked_at`, `created_at`, `updated_at` are all
`TIMESTAMPTZ`, not `TIMESTAMP`. An earlier draft used bare `TIMESTAMP`, which
would have compared against `now()` (a `timestamptz`) using the session's
timezone setting for the implicit cast — a silent source of drift if the app
and DB ever disagreed on timezone.

**Index:** a partial index, `CREATE INDEX ... ON jobs (run_after) WHERE status
= 'queued'`. A plain composite index on `(status, run_after)` would also work,
but it indexes every row regardless of status, so it grows forever as jobs
accumulate in `done`/`failed`. The partial index only ever contains rows the
claim query actually cares about, so it stays small for the life of the table.

**Locking:** `claimBatch` is a single statement — a `WITH candidates AS
(SELECT ... FOR UPDATE SKIP LOCKED) UPDATE jobs ... FROM candidates`. Row-level
locking (`FOR UPDATE`) means the moment a row is selected, it's already
claimed; `SKIP LOCKED` means a worker that encounters a row another worker
currently holds doesn't block waiting for it — it just moves on to the next
eligible row. Combining the select and the update into one statement, with
`LIMIT` inside the CTE, means the claim is a single short transaction that
commits as soon as it completes; job execution and `markDone`/`markFailed`
happen in separate, later transactions, so a slow or hanging handler never
holds a claim-time lock open.

**Bugs found while building this (in the order they surfaced):**
1. `markFailed` was missing a final `return nil` — compile error.
2. `claimBatch` had a `return jobs, nil` before the `rows.Err()` check, making
   the error check dead code — iteration errors were silently swallowed.
3. `markFailed`'s retry branch didn't persist `attempts` or apply
   `q.backoff()` at all — every failure went straight to permanently failed on
   the first attempt.
4. Fixed to persist `attempts` in the retry branch, but the permanent-failure
   branch still didn't — so a job that exhausted its attempts ended at
   `status = failed` with `attempts` one short of `MaxAttempts`.
5. `RETURNING id, ...` in `claimBatch` failed with `column reference "id" is
   ambiguous` — the `candidates` CTE also projects a column named `id`, so
   once it's joined into the `UPDATE ... FROM candidates`, plain `id` could
   mean either table. Fixed by qualifying every column in `jobColumns` with
   `jobs.`. This one didn't show up as a wrong answer — the query errored on
   every call, so nothing was ever claimed at all, and jobs just piled up as
   `queued` while every integration test time out.

**Loadtest:** 20,000 jobs, `WORKER_COUNT=4`, `BATCH_SIZE=10`: **4,756
jobs/sec**, 4.2s wall time, exactly-once holds (every job has exactly one
`job_runs` row with `run_count = 1`).

**Integration tests:** all three pass against real Postgres 16 (no mocks).

## Backlog / known limitations

- **Stale lock reclaim**: if a worker dies mid-job (crash, OOM-kill, deploy),
  the job it claimed stays at `status = running` forever — `locked_at` exists
  to support detecting this (a `running` row whose `locked_at` is older than
  some timeout is almost certainly orphaned) but the reclaim logic itself is
  not built. Not covered by the current integration tests, which only exercise
  healthy workers. Deferred deliberately, not an oversight.
