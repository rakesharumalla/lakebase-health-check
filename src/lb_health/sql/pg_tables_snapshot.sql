-- Per-table statistics. Cumulative counters; deltas are computed by the job.
SELECT n.nspname AS schemaname, c.relname AS tblname, c.oid,
       s.n_live_tup, s.n_dead_tup, s.seq_scan, s.seq_tup_read, s.idx_scan,
       s.last_vacuum, s.last_autovacuum, s.last_analyze, s.last_autoanalyze
FROM pg_stat_user_tables s
JOIN pg_class c ON c.oid = s.relid
JOIN pg_namespace n ON n.oid = c.relnamespace;
