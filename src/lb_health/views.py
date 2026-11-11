"""Consumption views over lb_health_metrics.

Re-created on every deploy, so adding a metric is one change in metrics.py. The dashboard and the
Genie space read only these views and lb_health_thresholds.
"""
from __future__ import annotations

from .config import REVIEW_PERIODS, Config
from .metrics import DAILY_METRICS, HOURLY_METRICS, TRENDS

# UTC "yesterday" independent of the session time zone.
UTC_TODAY = "to_date(to_utc_timestamp(current_timestamp(), current_timezone()))"


def _trend_name_case(col: str) -> str:
    whens = " ".join(f"WHEN '{tid}' THEN '{name}'" for tid, _, name in TRENDS)
    return f"CASE {col} {whens} END"


def _pivot_cols(metrics: list[str]) -> str:
    return ",\n  ".join(f"max(CASE WHEN m.metric = '{m}' THEN m.value END) AS {m}" for m in metrics)


def view_statements(cfg: Config) -> dict[str, str]:
    M, T = cfg.metrics, cfg.thresholds
    # endpoint_type (read_write = primary, read_only = replica) per endpoint, stamped on the day rows
    ept = (f"(SELECT project_id, branch_id, endpoint_id, max_by(detail:endpoint_type::string, ts) AS endpoint_type "
           f"FROM {M} WHERE grain = 'day' AND object_type = 'endpoint' AND detail:endpoint_type IS NOT NULL "
           f"GROUP BY ALL)")
    v = {}

    v["lb_health_hourly"] = f"""
CREATE OR REPLACE VIEW {cfg.fq('lb_health_hourly')}
COMMENT 'One row per project/branch/endpoint/hour (UTC) with every hourly metric as a column. Ratios are fractions (0.70 = 70%).'
AS
SELECT m.project_id, m.branch_id, m.endpoint_id, e.endpoint_type, m.ts AS hour_ts, CAST(m.ts AS DATE) AS day,
  {_pivot_cols(HOURLY_METRICS)}
FROM {M} m
LEFT JOIN {ept} e USING (project_id, branch_id, endpoint_id)
WHERE m.grain = 'hour' AND m.object_type = 'endpoint'
GROUP BY ALL
"""

    v["lb_health_daily"] = f"""
CREATE OR REPLACE VIEW {cfg.fq('lb_health_daily')}
COMMENT 'One row per project/branch/endpoint/day (UTC) with one column per daily health trend (connection utilization, long idle-in-transaction sessions, idle-in-transaction share, CPU utilization, CPU per connection, cache hit rate, working set vs cache, bloat, dead rows, full scans of large tables, new sequential-scan queries).'
AS
SELECT m.project_id, m.branch_id, m.endpoint_id, e.endpoint_type, CAST(m.ts AS DATE) AS day,
  {_pivot_cols(DAILY_METRICS)},
  max(CASE WHEN m.metric = 't11_new_seq_scan_queries' THEN m.detail:status::string END) AS t11_status
FROM {M} m
LEFT JOIN {ept} e USING (project_id, branch_id, endpoint_id)
WHERE m.grain = 'day' AND m.object_type = 'endpoint'
GROUP BY ALL
"""

    v["lb_health_objects_latest"] = f"""
CREATE OR REPLACE VIEW {cfg.fq('lb_health_objects_latest')}
COMMENT 'Current state of every table and index from the most recent Postgres snapshot (bloat from the most recent bloat run). object_name is schema.table or schema.index.'
AS
WITH snap AS (
  SELECT * FROM {M} WHERE grain = 'snapshot' AND object_type IN ('table', 'index')
),
last_snap AS (
  SELECT project_id, branch_id, endpoint_id, datname, max(ts) AS last_ts
  FROM snap WHERE metric IN ('n_live_tup', 'index_bytes') GROUP BY ALL
),
obj AS (
  SELECT project_id, branch_id, endpoint_id, datname, object_type, object_name,
    max(ts) FILTER (WHERE metric IN ('n_live_tup', 'index_bytes')) AS snapshot_ts,
    max_by(value, ts) FILTER (WHERE metric = 'n_live_tup') AS n_live_tup,
    max_by(value, ts) FILTER (WHERE metric = 'n_dead_tup') AS n_dead_tup,
    max_by(value, ts) FILTER (WHERE metric = 'seq_scan_delta') AS seq_scan_delta,
    max_by(value, ts) FILTER (WHERE metric = 'seq_tup_read_delta') AS seq_tup_read_delta,
    max_by(value, ts) FILTER (WHERE metric = 'idx_scan_delta') AS idx_scan_delta,
    sum(value) FILTER (WHERE metric = 'seq_scan_delta' AND ts >= current_timestamp() - INTERVAL 7 DAYS) AS seq_scan_7d,
    sum(value) FILTER (WHERE metric = 'idx_scan_delta' AND ts >= current_timestamp() - INTERVAL 7 DAYS) AS idx_scan_7d,
    max_by(value, ts) FILTER (WHERE metric = 'index_bytes') AS index_bytes,
    max_by(value, ts) FILTER (WHERE metric = 'index_is_valid') AS index_is_valid,
    max_by(value, ts) FILTER (WHERE metric = 'index_backs_constraint') AS index_backs_constraint,
    max_by(detail:indisunique::boolean, ts) FILTER (WHERE metric = 'index_bytes') AS index_is_unique,
    max_by(detail:indisprimary::boolean, ts) FILTER (WHERE metric = 'index_bytes') AS index_is_primary,
    max_by(detail:index_columns::string, ts) FILTER (WHERE metric = 'index_bytes') AS index_columns,
    max_by(detail:indexdef::string, ts) FILTER (WHERE metric = 'index_bytes') AS indexdef,
    max_by(detail:table::string, ts) FILTER (WHERE metric = 'index_bytes') AS index_table,
    max_by(value, ts) FILTER (WHERE metric = 'bloat_pct') AS bloat_pct,
    max_by(value, ts) FILTER (WHERE metric = 'bloat_bytes') AS bloat_bytes,
    max_by(value, ts) FILTER (WHERE metric = 'bloat_is_na') AS bloat_is_na,
    max_by(detail:real_size::double, ts) FILTER (WHERE metric = 'bloat_pct') AS real_size,
    max(ts) FILTER (WHERE metric = 'bloat_pct') AS bloat_ts,
    max_by(value, ts) FILTER (WHERE metric = 'hours_since_autovacuum') AS hours_since_autovacuum,
    max_by(detail:last_autovacuum::timestamp, ts) FILTER (WHERE metric = 'hours_since_autovacuum') AS last_autovacuum,
    max_by(detail:last_vacuum::timestamp, ts) FILTER (WHERE metric = 'hours_since_autovacuum') AS last_vacuum,
    max_by(detail:last_autoanalyze::timestamp, ts) FILTER (WHERE metric = 'hours_since_autovacuum') AS last_autoanalyze
  FROM snap GROUP BY ALL
)
SELECT o.project_id, o.branch_id, o.endpoint_id, e.endpoint_type, o.datname, o.object_type,
  split_part(o.object_name, '.', 1) AS schema_name, o.object_name,
  CASE WHEN o.object_type = 'table' THEN o.object_name
       ELSE concat(split_part(o.object_name, '.', 1), '.', o.index_table) END AS table_name,
  o.n_live_tup, o.n_dead_tup, o.seq_scan_delta, o.seq_tup_read_delta, o.idx_scan_delta,
  o.seq_scan_7d, o.idx_scan_7d, o.index_bytes, o.index_is_valid, o.index_backs_constraint,
  o.index_is_unique, o.index_is_primary, o.index_columns, o.indexdef,
  o.bloat_pct, o.bloat_bytes, o.bloat_is_na, o.real_size, o.bloat_ts,
  o.hours_since_autovacuum, o.last_autovacuum, o.last_vacuum, o.last_autoanalyze, o.snapshot_ts
FROM obj o
JOIN last_snap l ON o.project_id = l.project_id AND o.branch_id = l.branch_id
  AND o.endpoint_id = l.endpoint_id AND o.datname <=> l.datname AND o.snapshot_ts = l.last_ts
LEFT JOIN {ept} e
  ON o.project_id = e.project_id AND o.branch_id = e.branch_id AND o.endpoint_id = e.endpoint_id
"""

    v["lb_health_index_findings"] = f"""
CREATE OR REPLACE VIEW {cfg.fq('lb_health_index_findings')}
COMMENT 'One row per flagged index per day: finding = invalid | unused | duplicate.'
AS
SELECT project_id, branch_id, endpoint_id, datname, CAST(ts AS DATE) AS day,
  regexp_replace(metric, '^index_', '') AS finding,
  object_name,
  detail:table::string AS table_name,
  detail:index_bytes::double AS index_bytes,
  detail:indexdef::string AS indexdef,
  CAST(detail:duplicate_of AS ARRAY<STRING>) AS duplicate_of,
  detail:observed_days::double AS observed_days,
  detail:first_seen::timestamp AS first_seen
FROM {M}
WHERE grain = 'day' AND object_type = 'index'
  AND metric IN ('index_invalid', 'index_unused', 'index_duplicate')
"""

    v["lb_health_seq_scan_queries"] = f"""
CREATE OR REPLACE VIEW {cfg.fq('lb_health_seq_scan_queries')}
COMMENT 'One row per query (queryid) per day that sequentially scanned a large table (from system.lakebase.plan_history).'
AS
SELECT project_id, branch_id, endpoint_id, datname, CAST(ts AS DATE) AS day,
  object_name AS queryid,
  detail:query_text::string AS query_text,
  CAST(detail:relations AS ARRAY<STRING>) AS relations,
  value AS scan_count,
  detail:avg_duration_ms::double AS avg_duration_ms,
  coalesce(detail:is_new::boolean, false) AS is_new
FROM {M}
WHERE grain = 'day' AND object_type = 'query' AND metric = 'seq_scan_query_count'
"""

    v["lb_health_review_summary"] = f"""
CREATE OR REPLACE VIEW {cfg.fq('lb_health_review_summary')}
COMMENT 'Health status per trend (trend_name) x endpoint x period_days (7/14). Period = [yesterday - period_days + 1, yesterday] UTC vs the equally long prior window. status: red (latest day breaches its reference line) | amber (breached earlier in the period) | green | info (no reference line: dead rows on large tables, or CPU per connection before a prior period exists) | unknown (no data) | not_available (new sequential-scan queries when query plans are masked).'
AS
WITH d AS (
  SELECT project_id, branch_id, endpoint_id, CAST(ts AS DATE) AS day, metric, value,
         detail:status::string AS detail_status
  FROM {M}
  WHERE grain = 'day' AND object_type = 'endpoint'
),
th AS (
  SELECT threshold_id AS trend_id, metric, value AS line, reference_type, direction
  FROM {T}
),
eps AS (SELECT DISTINCT project_id, branch_id, endpoint_id FROM d),
base AS (
  SELECT e.project_id, e.branch_id, e.endpoint_id, th.*, p.period_days,
    date_sub({UTC_TODAY}, 1) AS period_end,
    date_sub({UTC_TODAY}, p.period_days) AS period_start,
    date_sub({UTC_TODAY}, p.period_days + 1) AS prior_end,
    date_sub({UTC_TODAY}, 2 * p.period_days) AS prior_start
  FROM eps e CROSS JOIN th CROSS JOIN (SELECT explode(array({', '.join(str(x) for x in REVIEW_PERIODS)})) AS period_days) p
),
agg AS (
  SELECT b.project_id, b.branch_id, b.endpoint_id, b.trend_id, b.metric, b.line, b.reference_type,
    b.direction, b.period_days, b.period_start, b.period_end,
    avg(d.value) FILTER (WHERE d.day BETWEEN b.period_start AND b.period_end) AS avg_period,
    avg(d.value) FILTER (WHERE d.day BETWEEN b.prior_start AND b.prior_end) AS avg_prior,
    max(d.day) FILTER (WHERE d.day BETWEEN b.period_start AND b.period_end) AS latest_day
  FROM base b
  LEFT JOIN d ON d.project_id = b.project_id AND d.branch_id = b.branch_id
    AND d.endpoint_id = b.endpoint_id AND d.metric = b.metric
    AND d.day BETWEEN b.prior_start AND b.period_end
  GROUP BY ALL
),
ref AS (
  SELECT a.*,
    CASE a.reference_type WHEN 'static' THEN a.line WHEN 'prior_period_avg' THEN a.avg_prior END AS reference_value
  FROM agg a
),
judged AS (
  SELECT r.*, d.day, d.value, d.detail_status,
    CASE WHEN d.value IS NULL OR r.reference_value IS NULL THEN false
         WHEN r.direction = 'below_is_bad' THEN d.value < r.reference_value
         ELSE d.value > r.reference_value END AS breach
  FROM ref r
  LEFT JOIN d ON d.project_id = r.project_id AND d.branch_id = r.branch_id
    AND d.endpoint_id = r.endpoint_id AND d.metric = r.metric
    AND d.day BETWEEN r.period_start AND r.period_end
),
rolled AS (
  SELECT project_id, branch_id, endpoint_id, trend_id, metric, reference_type, direction,
    period_days, period_start, period_end, avg_period, avg_prior, reference_value, latest_day,
    max_by(value, day) FILTER (WHERE day = latest_day) AS latest_value,
    max_by(breach, day) FILTER (WHERE day = latest_day) AS latest_breach,
    max_by(detail_status, day) FILTER (WHERE day = latest_day) AS latest_detail_status,
    count(*) FILTER (WHERE breach) AS days_breached,
    count(value) AS days_with_data
  FROM judged
  GROUP BY ALL
)
SELECT r.period_days, r.trend_id, {_trend_name_case('r.trend_id')} AS trend_name, r.metric, r.project_id, r.branch_id, r.endpoint_id,
  e.endpoint_type, r.latest_value, r.latest_day, r.reference_value, r.reference_type, r.direction,
  r.avg_period, r.avg_prior,
  CASE WHEN r.avg_prior IS NULL OR r.avg_prior = 0 THEN NULL
       ELSE (r.avg_period - r.avg_prior) / abs(r.avg_prior) END AS pct_change,
  r.avg_period - r.avg_prior AS delta_vs_prior,
  r.days_breached, r.days_with_data, r.period_start, r.period_end,
  CASE
    WHEN r.trend_id = 'T11' AND r.latest_value IS NULL AND r.latest_detail_status = 'not_available' THEN 'not_available'
    WHEN r.reference_type = 'none' THEN CASE WHEN r.days_with_data = 0 THEN 'unknown' ELSE 'info' END
    WHEN r.latest_value IS NULL THEN 'unknown'
    WHEN r.reference_value IS NULL THEN 'info'
    WHEN r.latest_breach THEN 'red'
    WHEN r.days_breached >= 1 THEN 'amber'
    ELSE 'green'
  END AS status,
  CASE
    WHEN r.trend_id = 'T11' AND r.latest_value IS NULL AND r.latest_detail_status = 'not_available' THEN 5
    WHEN r.reference_type = 'none' THEN 4
    WHEN r.latest_value IS NULL THEN 3
    WHEN r.reference_value IS NULL THEN 4
    WHEN r.latest_breach THEN 0
    WHEN r.days_breached >= 1 THEN 1
    ELSE 2
  END AS status_rank
FROM rolled r
LEFT JOIN {ept} e
  ON r.project_id = e.project_id AND r.branch_id = e.branch_id AND r.endpoint_id = e.endpoint_id
"""
    return v
