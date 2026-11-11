-- Hourly rollup of system.lakebase telemetry (connections, idle-in-transaction, CPU, cache), one row per
-- (endpoint, hour, metric) in lb_health_metrics shape. Placeholders (__X__) are substituted by
-- 02_system_tables_rollup.py. lbh_scope maps telemetry uids to the API ids stored in the table.
--
-- Telemetry columns used (system.lakebase is Beta — every reference lives in this file):
--   database_gauges   : ts, compute_id, project_id, endpoint_id, numbackends
--   compute_gauges    : ts, compute_id, project_id, endpoint_id, max_connections, host_logical_cpus,
--                       lfc_working_set_size_60m (bytes), lfc_size_limit (bytes)
--   compute_counters  : ts, previous_ts, compute_id, project_id, endpoint_id, host_cpu_seconds,
--                       host_busiest_cpu_seconds, lfc_hits, lfc_misses
--   database_counters : ts, previous_ts, project_id, endpoint_id, idle_in_transaction_time_ms, session_time_ms,
--                       shared_blks_hit, shared_blks_read (Postgres shared buffers, the first cache tier)
--   active_session_history : ts, compute_id, project_id, endpoint_id, pid, backend_state, xact_age_ms,
--                       wait_event_type, wait_event, blocking_pid, appname_hash, datid
WITH
scope AS (SELECT * FROM lbh_scope),

-- 1a. connections: sum numbackends over databases per sample, average per compute-minute
dg_ts AS (
  SELECT g.endpoint_id AS e_uid, g.compute_id, g.ts, sum(g.numbackends) AS conns
  FROM system.lakebase.database_gauges g
  WHERE g.ts >= __START__ AND g.ts < __END__
    AND g.project_id IN (__PROJECT_UIDS__)
    AND g.endpoint_id IN (SELECT endpoint_uid FROM scope)
  GROUP BY ALL
),
dg_min AS (
  SELECT e_uid, compute_id, date_trunc('minute', ts) AS minute, avg(conns) AS conns
  FROM dg_ts GROUP BY ALL
),
cg_min AS (
  SELECT c.endpoint_id AS e_uid, c.compute_id, date_trunc('minute', c.ts) AS minute,
    min(c.max_connections) AS max_connections,
    max(c.host_logical_cpus) AS cpus,
    max(c.lfc_working_set_size_60m) AS ws60,
    min(c.lfc_size_limit) AS lfc_limit,
    max(c.ts) AS max_ts
  FROM system.lakebase.compute_gauges c
  WHERE c.ts >= __CF_START__ AND c.ts < __END__          -- extra hour so values can carry forward
    AND c.project_id IN (__PROJECT_UIDS__)
    AND c.endpoint_id IN (SELECT endpoint_uid FROM scope)
  GROUP BY ALL
),
-- gauge alignment: join on (compute_id, minute); carry compute_gauges forward per compute
gm AS (
  SELECT coalesce(d.e_uid, c.e_uid) AS e_uid, coalesce(d.compute_id, c.compute_id) AS compute_id,
    coalesce(d.minute, c.minute) AS minute, d.conns, c.max_connections, c.cpus, c.ws60, c.lfc_limit
  FROM dg_min d
  FULL OUTER JOIN cg_min c ON d.e_uid = c.e_uid AND d.compute_id = c.compute_id AND d.minute = c.minute
),
gf AS (
  SELECT e_uid, compute_id, minute, conns, ws60, lfc_limit,
    last_value(max_connections, true) OVER w AS max_connections,
    last_value(cpus, true) OVER w AS cpus
  FROM gm
  WINDOW w AS (PARTITION BY e_uid, compute_id ORDER BY minute ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
conn_ep_min AS (  -- endpoint-level connections per minute (summed across computes)
  SELECT e_uid, minute, sum(conns) AS conns FROM gf
  WHERE minute >= __START__ AND conns IS NOT NULL GROUP BY ALL
),
conn_h AS (
  SELECT e_uid, date_trunc('hour', minute) AS hour,
    percentile(conns / nullif(max_connections, 0), 0.95) FILTER (WHERE conns IS NOT NULL) AS conn_util_p95,
    max(conns / nullif(max_connections, 0)) AS conn_util_max,
    min(max_connections) AS max_connections_min,
    max(ws60) AS lfc_working_set_60m_max,
    min(lfc_limit) AS lfc_size_limit_min,
    max(cpus) AS host_logical_cpus_max
  FROM gf WHERE minute >= __START__
  GROUP BY ALL
),
conn_avg_h AS (
  SELECT e_uid, date_trunc('hour', minute) AS hour, avg(conns) AS conn_count_avg
  FROM conn_ep_min GROUP BY ALL
),

-- 2/3. CPU and cache from compute_counters
cpu_fallback AS (SELECT e_uid, compute_id, max(cpus) AS cpus FROM gf GROUP BY ALL),
cc AS (
  SELECT c.endpoint_id AS e_uid, c.compute_id, c.ts, date_trunc('minute', c.ts) AS minute,
    (unix_millis(c.ts) - unix_millis(c.previous_ts)) / 1000.0 AS interval_s,
    c.host_cpu_seconds, c.host_busiest_cpu_seconds, c.lfc_hits, c.lfc_misses
  FROM system.lakebase.compute_counters c
  WHERE c.ts >= __START__ AND c.ts < __END__
    AND c.project_id IN (__PROJECT_UIDS__)
    AND c.endpoint_id IN (SELECT endpoint_uid FROM scope)
),
cpu_rows AS (
  SELECT cc.*, coalesce(g.cpus, f.cpus) AS cpus,
    cc.host_cpu_seconds / (cc.interval_s * coalesce(g.cpus, f.cpus)) AS raw_util,
    cc.host_busiest_cpu_seconds / cc.interval_s AS raw_busy
  FROM cc
  LEFT JOIN gf g ON g.e_uid = cc.e_uid AND g.compute_id = cc.compute_id AND g.minute = cc.minute
  LEFT JOIN cpu_fallback f ON f.e_uid = cc.e_uid AND f.compute_id = cc.compute_id
  WHERE cc.interval_s > 0 AND cc.interval_s <= __MAX_INTERVAL_S__
),
cpu_h AS (
  SELECT e_uid, date_trunc('hour', ts) AS hour,
    sum(host_cpu_seconds) FILTER (WHERE cpus > 0) / nullif(sum(interval_s * cpus) FILTER (WHERE cpus > 0), 0) AS cpu_util_avg,
    percentile(least(greatest(raw_util, 0), __CLAMP__), 0.95) AS cpu_util_p95,
    max(least(greatest(raw_util, 0), __CLAMP__)) AS cpu_util_max,
    max(least(greatest(raw_busy, 0), __CLAMP__)) AS cpu_busiest_core_max,
    count_if(raw_util > __CLAMP__ OR raw_busy > __CLAMP__ OR raw_util < 0 OR raw_busy < 0) AS clamped_samples,
    sum(lfc_hits) AS lfc_hits,
    sum(lfc_misses) AS lfc_misses
  FROM cpu_rows GROUP BY ALL
),

-- 1b source A. idle-in-transaction share (summable interval counters, all databases)
dc_h AS (
  SELECT d.endpoint_id AS e_uid, date_trunc('hour', d.ts) AS hour,
    sum(d.idle_in_transaction_time_ms) AS idle_in_txn_time_ms,
    sum(d.session_time_ms) AS session_time_ms,
    sum(d.shared_blks_hit) AS shared_blks_hit,
    sum(d.shared_blks_read) AS shared_blks_read
  FROM system.lakebase.database_counters d
  WHERE d.ts >= __START__ AND d.ts < __END__
    AND d.project_id IN (__PROJECT_UIDS__)
    AND d.endpoint_id IN (SELECT endpoint_uid FROM scope)
    AND (unix_millis(d.ts) - unix_millis(d.previous_ts)) / 1000.0 > 0
    AND (unix_millis(d.ts) - unix_millis(d.previous_ts)) / 1000.0 <= __MAX_INTERVAL_S__
  GROUP BY ALL
),

-- 1b source B. idle-in-transaction sessions from ASH. A transaction = (compute, pid, xact start
-- rounded to the second); it is counted once, in the hour it was first seen above the threshold.
ash AS (
  SELECT a.endpoint_id AS e_uid, a.compute_id, a.pid, a.ts, a.xact_age_ms, a.wait_event_type,
    a.wait_event, a.blocking_pid, a.appname_hash, a.datid,
    round((unix_millis(a.ts) - a.xact_age_ms) / 1000.0) AS xact_start_s
  FROM system.lakebase.active_session_history a
  WHERE a.ts >= __START__ AND a.ts < __END__
    AND a.project_id IN (__PROJECT_UIDS__)
    AND a.endpoint_id IN (SELECT endpoint_uid FROM scope)
    AND a.backend_state IN (__IIT_STATES__)
    AND a.xact_age_ms IS NOT NULL
),
ash_max_h AS (
  SELECT e_uid, date_trunc('hour', ts) AS hour, max(xact_age_ms) AS idle_in_txn_max_age_ms
  FROM ash GROUP BY ALL
),
ash_txn AS (
  SELECT e_uid, compute_id, pid, xact_start_s,
    min(ts) FILTER (WHERE xact_age_ms > __LONG_MS__) AS first_long,
    min(ts) FILTER (WHERE xact_age_ms > __VERY_LONG_MS__) AS first_very_long,
    max_by(named_struct('xact_age_ms', xact_age_ms, 'pid', pid, 'wait_event_type', wait_event_type,
                        'wait_event', wait_event, 'blocking_pid', blocking_pid,
                        'appname_hash', appname_hash, 'datid', datid), xact_age_ms) AS top
  FROM ash GROUP BY ALL
),
long_h AS (
  SELECT e_uid, date_trunc('hour', first_long) AS hour, count(*) AS n,
    slice(sort_array(collect_list(top), false), 1, 5) AS top5
  FROM ash_txn WHERE first_long IS NOT NULL GROUP BY ALL
),
very_long_h AS (
  SELECT e_uid, date_trunc('hour', first_very_long) AS hour, count(*) AS n,
    slice(sort_array(collect_list(top), false), 1, 5) AS top5
  FROM ash_txn WHERE first_very_long IS NOT NULL GROUP BY ALL
),

-- wide hourly row per endpoint
keys AS (
  SELECT e_uid, hour FROM conn_h UNION SELECT e_uid, hour FROM cpu_h
  UNION SELECT e_uid, hour FROM dc_h UNION SELECT e_uid, hour FROM ash_max_h
),
wide AS (
  SELECT k.e_uid, k.hour,
    ca.conn_count_avg, c.conn_util_p95, c.conn_util_max, c.max_connections_min,
    dc.idle_in_txn_time_ms, dc.session_time_ms,
    dc.idle_in_txn_time_ms / nullif(dc.session_time_ms, 0) AS idle_in_txn_share,
    coalesce(am.idle_in_txn_max_age_ms, 0) AS idle_in_txn_max_age_ms,
    coalesce(l.n, 0) AS idle_in_txn_over_60s_count, l.top5 AS top5_long,
    coalesce(vl.n, 0) AS idle_in_txn_over_300s_count, vl.top5 AS top5_very_long,
    cp.cpu_util_avg, cp.cpu_util_p95, cp.cpu_util_max, cp.cpu_busiest_core_max, cp.clamped_samples,
    c.host_logical_cpus_max,
    cp.cpu_util_avg / nullif(ca.conn_count_avg, 0) AS cpu_per_connection,
    cp.lfc_hits, cp.lfc_misses,
    cp.lfc_hits / nullif(cp.lfc_hits + cp.lfc_misses, 0) AS lfc_hit_rate,
    dc.shared_blks_hit, dc.shared_blks_read,
    -- overall cache hit rate: reads served by shared buffers, plus shared-buffer misses served by the LFC
    (dc.shared_blks_hit + least(coalesce(cp.lfc_hits, 0), dc.shared_blks_read))
      / nullif(dc.shared_blks_hit + dc.shared_blks_read, 0) AS cache_hit_rate,
    c.lfc_working_set_60m_max, c.lfc_size_limit_min,
    c.lfc_working_set_60m_max / nullif(c.lfc_size_limit_min, 0) AS lfc_working_set_fit
  FROM keys k
  LEFT JOIN conn_h c ON c.e_uid = k.e_uid AND c.hour = k.hour
  LEFT JOIN conn_avg_h ca ON ca.e_uid = k.e_uid AND ca.hour = k.hour
  LEFT JOIN cpu_h cp ON cp.e_uid = k.e_uid AND cp.hour = k.hour
  LEFT JOIN dc_h dc ON dc.e_uid = k.e_uid AND dc.hour = k.hour
  LEFT JOIN ash_max_h am ON am.e_uid = k.e_uid AND am.hour = k.hour
  LEFT JOIN long_h l ON l.e_uid = k.e_uid AND l.hour = k.hour
  LEFT JOIN very_long_h vl ON vl.e_uid = k.e_uid AND vl.hour = k.hour
  WHERE k.hour >= __START__ AND k.hour < __END__
),
narrow AS (
  SELECT w.e_uid, w.hour, m.metric, m.value, m.detail
  FROM wide w
  LATERAL VIEW inline(array(
    named_struct('metric', 'conn_count_avg', 'value', CAST(w.conn_count_avg AS DOUBLE), 'detail', CAST(NULL AS STRING)),
    named_struct('metric', 'conn_util_p95', 'value', CAST(w.conn_util_p95 AS DOUBLE), 'detail', CAST(NULL AS STRING)),
    named_struct('metric', 'conn_util_max', 'value', CAST(w.conn_util_max AS DOUBLE), 'detail', CAST(NULL AS STRING)),
    named_struct('metric', 'max_connections_min', 'value', CAST(w.max_connections_min AS DOUBLE), 'detail', CAST(NULL AS STRING)),
    named_struct('metric', 'idle_in_txn_time_ms', 'value', CAST(w.idle_in_txn_time_ms AS DOUBLE), 'detail', CAST(NULL AS STRING)),
    named_struct('metric', 'session_time_ms', 'value', CAST(w.session_time_ms AS DOUBLE), 'detail', CAST(NULL AS STRING)),
    named_struct('metric', 'idle_in_txn_share', 'value', CAST(w.idle_in_txn_share AS DOUBLE), 'detail', CAST(NULL AS STRING)),
    named_struct('metric', 'idle_in_txn_max_age_ms', 'value', CAST(w.idle_in_txn_max_age_ms AS DOUBLE), 'detail', CAST(NULL AS STRING)),
    named_struct('metric', 'idle_in_txn_over_60s_count', 'value', CAST(w.idle_in_txn_over_60s_count AS DOUBLE),
                 'detail', CASE WHEN w.top5_long IS NOT NULL THEN to_json(named_struct('top', w.top5_long)) END),
    named_struct('metric', 'idle_in_txn_over_300s_count', 'value', CAST(w.idle_in_txn_over_300s_count AS DOUBLE),
                 'detail', CASE WHEN w.top5_very_long IS NOT NULL THEN to_json(named_struct('top', w.top5_very_long)) END),
    named_struct('metric', 'cpu_util_avg', 'value', CAST(w.cpu_util_avg AS DOUBLE), 'detail', CAST(NULL AS STRING)),
    named_struct('metric', 'cpu_util_p95', 'value', CAST(w.cpu_util_p95 AS DOUBLE), 'detail', CAST(NULL AS STRING)),
    named_struct('metric', 'cpu_util_max', 'value', CAST(w.cpu_util_max AS DOUBLE),
                 'detail', CASE WHEN w.clamped_samples > 0 THEN to_json(named_struct('clamped_samples', w.clamped_samples)) END),
    named_struct('metric', 'cpu_busiest_core_max', 'value', CAST(w.cpu_busiest_core_max AS DOUBLE), 'detail', CAST(NULL AS STRING)),
    named_struct('metric', 'host_logical_cpus_max', 'value', CAST(w.host_logical_cpus_max AS DOUBLE), 'detail', CAST(NULL AS STRING)),
    named_struct('metric', 'cpu_per_connection', 'value', CAST(w.cpu_per_connection AS DOUBLE), 'detail', CAST(NULL AS STRING)),
    named_struct('metric', 'lfc_hits', 'value', CAST(w.lfc_hits AS DOUBLE), 'detail', CAST(NULL AS STRING)),
    named_struct('metric', 'lfc_misses', 'value', CAST(w.lfc_misses AS DOUBLE), 'detail', CAST(NULL AS STRING)),
    named_struct('metric', 'lfc_hit_rate', 'value', CAST(w.lfc_hit_rate AS DOUBLE), 'detail', CAST(NULL AS STRING)),
    named_struct('metric', 'shared_blks_hit', 'value', CAST(w.shared_blks_hit AS DOUBLE), 'detail', CAST(NULL AS STRING)),
    named_struct('metric', 'shared_blks_read', 'value', CAST(w.shared_blks_read AS DOUBLE), 'detail', CAST(NULL AS STRING)),
    named_struct('metric', 'cache_hit_rate', 'value', CAST(w.cache_hit_rate AS DOUBLE), 'detail', CAST(NULL AS STRING)),
    named_struct('metric', 'lfc_working_set_60m_max', 'value', CAST(w.lfc_working_set_60m_max AS DOUBLE), 'detail', CAST(NULL AS STRING)),
    named_struct('metric', 'lfc_size_limit_min', 'value', CAST(w.lfc_size_limit_min AS DOUBLE), 'detail', CAST(NULL AS STRING)),
    named_struct('metric', 'lfc_working_set_fit', 'value', CAST(w.lfc_working_set_fit AS DOUBLE), 'detail', CAST(NULL AS STRING))
  )) m AS metric, value, detail
)
SELECT n.hour AS ts, 'hour' AS grain, s.project_id, s.branch_id, s.endpoint_id,
  CAST(NULL AS STRING) AS compute_id, CAST(NULL AS STRING) AS datname,
  'endpoint' AS object_type, s.endpoint_id AS object_name, n.metric, n.value, n.detail,
  '__RUN_ID__' AS run_id, current_timestamp() AS loaded_at
FROM narrow n
JOIN scope s ON s.endpoint_uid = n.e_uid
WHERE n.value IS NOT NULL
