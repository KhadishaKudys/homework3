-- Homework 3 - Part 2.2: bottleneck analysis on the Postgres side
-- Run with psql:  psql -h localhost -U postgres -d postgres -f src/sql/analysis.sql

-- 1) Relevant server settings
SELECT name, setting, unit
FROM pg_settings
WHERE name IN ('max_connections', 'shared_buffers', 'synchronous_commit', 'fsync',
               'deadlock_timeout', 'password_encryption', 'work_mem',
               'log_connections', 'log_min_duration_statement', 'log_lock_waits');

-- 2) Table size (grows during the benchmark because of the INSERT transactions)
SELECT count(*) AS tuples, pg_size_pretty(pg_total_relation_size('blocking_data')) AS size
FROM blocking_data;

SELECT n_live_tup, n_dead_tup, last_autovacuum
FROM pg_stat_user_tables
WHERE relname = 'blocking_data';

-- 3) Execution plans of the four queries of the workload
EXPLAIN (ANALYZE, BUFFERS) SELECT data_value FROM blocking_data WHERE data_id = 12345;
EXPLAIN (ANALYZE, BUFFERS) SELECT sum(data_value) FROM blocking_data;

BEGIN;
EXPLAIN (ANALYZE, BUFFERS) UPDATE blocking_data SET data_value = 1.0, modified_by = 1 WHERE data_id = 12345;
EXPLAIN (ANALYZE, BUFFERS) INSERT INTO blocking_data VALUES (999999, 1.0, 1);
ROLLBACK;

-- 4) Postgres logging (optional, needs superuser, takes effect after reload)
--    log every new connection and every statement slower than 10 ms, and lock waits
-- ALTER SYSTEM SET log_connections = 'on';
-- ALTER SYSTEM SET log_min_duration_statement = 10;
-- ALTER SYSTEM SET log_lock_waits = 'on';
-- SELECT pg_reload_conf();
--
-- undo:
-- ALTER SYSTEM RESET log_connections;
-- ALTER SYSTEM RESET log_min_duration_statement;
-- ALTER SYSTEM RESET log_lock_waits;
-- SELECT pg_reload_conf();

-- 5) While the benchmark is running: how many connections are open?
SELECT state, count(*)
FROM pg_stat_activity
WHERE datname = 'postgres'
GROUP BY state;
