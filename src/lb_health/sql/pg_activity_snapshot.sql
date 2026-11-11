-- Fallback for idle-in-transaction counts when ASH does not sample them.
-- Point-in-time view: in daily mode it only catches transactions open at run time.
SELECT pid, datname, usename, application_name, state, wait_event_type, wait_event,
       xact_start, (extract(epoch FROM now() - xact_start) * 1000)::bigint AS xact_age_ms,
       state_change, pg_blocking_pids(pid) AS blocking_pids
FROM pg_stat_activity
WHERE backend_type = 'client backend' AND pid <> pg_backend_pid()
  AND state IN ('idle in transaction', 'idle in transaction (aborted)')
  AND xact_start IS NOT NULL;
