-- 0002_job_runs.up.sql
--
-- NOT part of the exercise — this table is fully written.
-- The loadtest handler increments the row for its job id every time it runs.
-- Exactly-once holds iff, after draining, every seeded job has exactly one row
-- here with run_count = 1.
CREATE TABLE IF NOT EXISTS job_runs (
    job_id    bigint PRIMARY KEY,
    run_count integer NOT NULL DEFAULT 0
);
