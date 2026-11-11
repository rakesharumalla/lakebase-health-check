"""Metric-name constants, row builders and the pure formulas used by the job.

Everything in this module is plain Python (no Spark, no psycopg) so tests/test_formulas.py can
exercise exactly the logic the notebooks run.
"""
from __future__ import annotations

import json
import re
from collections import defaultdict
from datetime import datetime, timezone

# ---------------------------------------------------------------------------
# system.lakebase specifics (Beta — keep every column/encoding reference here or in sql/)
# ---------------------------------------------------------------------------
SYSTEM_SCHEMA = "system.lakebase"
MAX_COUNTER_INTERVAL_S = 3600          # counter rows with interval_s <= 0 or > this are discarded
UTIL_CLAMP_MAX = 1.05                  # cpu_util / busiest_core_util are clamped to [0, 1.05]

# active_session_history.backend_state is the Postgres BackendState enum (PG17 numbering):
#   0 undefined, 1 starting, 2 idle, 3 running, 4 idle in transaction, 5 fastpath,
#   6 idle in transaction (aborted), 7 disabled.
# Validated in dev: state 3 query_age tops out at statement_timeout (running), state 4 is sampled.
ASH_IDLE_IN_TXN_STATES = (4, 6)

# ---------------------------------------------------------------------------
# Metric catalog
# ---------------------------------------------------------------------------
HOURLY_METRICS = [
    "conn_count_avg", "conn_util_p95", "conn_util_max", "max_connections_min",
    "idle_in_txn_time_ms", "session_time_ms", "idle_in_txn_share",
    "idle_in_txn_max_age_ms", "idle_in_txn_over_60s_count", "idle_in_txn_over_300s_count",
    "cpu_util_avg", "cpu_util_p95", "cpu_util_max", "cpu_busiest_core_max",
    "host_logical_cpus_max", "cpu_per_connection",
    "lfc_hits", "lfc_misses", "lfc_hit_rate", "lfc_working_set_60m_max",
    "lfc_size_limit_min", "lfc_working_set_fit",
]
COVERAGE_METRICS = ["rollup_ok", "snapshot_ok", "snapshot_skipped"]

TABLE_SNAPSHOT_METRICS = [
    "n_live_tup", "n_dead_tup", "seq_scan", "seq_scan_delta", "seq_tup_read",
    "seq_tup_read_delta", "idx_scan", "idx_scan_delta", "hours_since_autovacuum",
]
INDEX_SNAPSHOT_METRICS = [
    "idx_scan", "idx_scan_delta", "index_bytes", "index_is_valid", "index_backs_constraint",
]
BLOAT_METRICS = ["bloat_pct", "bloat_bytes", "bloat_is_na"]
COUNTER_METRICS = {"seq_scan": "seq_scan_delta", "seq_tup_read": "seq_tup_read_delta",
                   "idx_scan": "idx_scan_delta"}
IDLE_IN_TXN_SNAPSHOT_METRICS = [
    "idle_in_txn_over_60s_count", "idle_in_txn_over_300s_count", "idle_in_txn_max_age_ms",
]

# (threshold_id, daily metric, customer-facing name). threshold_id is the key in lb_health_thresholds;
# the name is what the dashboard, Genie and README show.
TRENDS = [
    ("T1", "t1_conn_util_busiest_hour", "Connection utilization (busiest hour)"),
    ("T2", "t2_idle_in_txn_over_60s_count", "Long idle-in-transaction sessions"),
    ("T3", "t3_idle_in_txn_share", "Idle-in-transaction time share"),
    ("T4", "t4_cpu_util_busiest_hour", "CPU utilization (busiest hour)"),
    ("T5", "t5_cpu_per_connection", "CPU per connection"),
    ("T6", "t6_lfc_hit_rate", "Cache hit rate"),
    ("T7", "t7_working_set_fit", "Working set vs. cache size"),
    ("T8", "t8_max_bloat_pct", "Table and index bloat"),
    ("T9", "t9_dead_rows_large_tables", "Dead rows on large tables"),
    ("T10", "t10_seq_scan_ratio_large_tables", "Full scans of large tables"),
    ("T11", "t11_new_seq_scan_queries", "New sequential-scan queries"),
]
TREND_NAMES = {tid: name for tid, _, name in TRENDS}
DAILY_METRICS = [m for _, m, _ in TRENDS]
INDEX_FINDINGS = ["index_invalid", "index_unused", "index_unused_candidate", "index_duplicate"]
SEQ_SCAN_QUERY_METRIC = "seq_scan_query_count"

EXCLUDED_SCHEMAS = {"pg_catalog", "information_schema", "pg_toast"}

# lb_health_thresholds seed rows:
# (threshold_id, kind, metric, value, reference_type, direction, label, description)
THRESHOLD_SEED = [
    ("T1", "reference_line", "t1_conn_util_busiest_hour", 0.70, "static", "above_is_bad",
     "Connection utilization 70%", "Busiest-hour p95 of connections / max_connections"),
    ("T2", "reference_line", "t2_idle_in_txn_over_60s_count", 0.0, "any_above_zero", "above_is_bad",
     "Any idle-in-transaction > 1 min", "Distinct idle-in-transaction sessions older than cfg_long_xact_seconds"),
    ("T3", "reference_line", "t3_idle_in_txn_share", 0.10, "static", "above_is_bad",
     "Idle-in-transaction share 10%", "idle_in_transaction_time / session_time over the day"),
    ("T4", "reference_line", "t4_cpu_util_busiest_hour", 0.70, "static", "above_is_bad",
     "CPU 70%", "Busiest-hour p95 CPU utilization"),
    ("T5", "reference_line", "t5_cpu_per_connection", None, "prior_period_avg", "above_is_bad",
     "Prior 14-day average", "Day CPU utilization / day average connections"),
    ("T6", "reference_line", "t6_lfc_hit_rate", 0.99, "static", "below_is_bad",
     "Cache hit rate 99%", "Local file cache hits / (hits + misses)"),
    ("T7", "reference_line", "t7_working_set_fit", 0.80, "static", "above_is_bad",
     "Working set 80% of cache", "Max 60-minute working set / LFC size limit"),
    ("T8", "reference_line", "t8_max_bloat_pct", 30.0, "static", "above_is_bad",
     "Bloat 30%", "Max bloat percent across tables/indexes wasting >= cfg_bloat_min_wasted_bytes"),
    ("T9", "reference_line", "t9_dead_rows_large_tables", None, "none", "above_is_bad",
     "Look for steady growth", "Sum of dead rows over large tables (latest snapshot of the day)"),
    ("T10", "reference_line", "t10_seq_scan_ratio_large_tables", 10.0, "static", "above_is_bad",
     "10 full scans/day", "Max over large tables of rows read by seq scans / live rows"),
    ("T11", "reference_line", "t11_new_seq_scan_queries", 0.0, "any_above_zero", "above_is_bad",
     "Any new seq-scan query", "Queries that started sequentially scanning a large table"),
    ("cfg_large_table_min_rows", "config", None, 100000.0, "static", None,
     "Large table = n_live_tup >= 100 K", "Tables at or above this live-row count are 'large'"),
    ("cfg_bloat_min_wasted_bytes", "config", None, 104857600.0, "static", None,
     "Bloat considered when >= 100 MB wasted", "Minimum bloat_bytes for the table and index bloat trend"),
    ("cfg_unused_index_min_bytes", "config", None, 10485760.0, "static", None,
     "Unused index considered when >= 10 MB", "Minimum index size for index_unused"),
    ("cfg_unused_index_window_days", "config", None, 30.0, "static", None,
     "Days with zero scans to call an index unused", "Observation window for index_unused"),
    ("cfg_long_xact_seconds", "config", None, 60.0, "static", None,
     "Idle-in-transaction age threshold", "Seconds; sessions idle in a transaction longer than this count as long"),
    ("cfg_very_long_xact_seconds", "config", None, 300.0, "static", None,
     "Idle-in-transaction severe age threshold", "Seconds"),
]

# ---------------------------------------------------------------------------
# Row builder — column order matches lb_health_metrics
# ---------------------------------------------------------------------------
ROW_COLUMNS = ["ts", "grain", "project_id", "branch_id", "endpoint_id", "compute_id", "datname",
               "object_type", "object_name", "metric", "value", "detail", "run_id", "loaded_at"]


def to_json(detail) -> str | None:
    if detail is None:
        return None
    return json.dumps(detail, default=_json_default, separators=(",", ":"))


def _json_default(o):
    if isinstance(o, datetime):
        return o.astimezone(timezone.utc).isoformat()
    try:
        return float(o)  # Decimal
    except Exception:
        return str(o)


def as_float(v):
    if v is None:
        return None
    if isinstance(v, bool):
        return 1.0 if v else 0.0
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def make_row(*, ts, grain, endpoint: dict, metric, value, object_type="endpoint", object_name=None,
             compute_id=None, datname=None, detail=None, run_id="", loaded_at=None) -> tuple:
    """Build one lb_health_metrics row (detail serialised to JSON; parse_json on write)."""
    return (
        ts, grain, endpoint["project_id"], endpoint["branch_id"], endpoint["endpoint_id"],
        compute_id, datname, object_type,
        object_name if object_name is not None else endpoint["endpoint_id"],
        metric, as_float(value), to_json(detail), run_id or None,
        loaded_at or datetime.now(timezone.utc),
    )


# ---------------------------------------------------------------------------
# Counter / interval rules
# ---------------------------------------------------------------------------
def counter_delta(current, previous):
    """previous NULL → NULL; current < previous → current (stats reset); else difference."""
    if current is None:
        return None
    current = float(current)
    if previous is None:
        return None
    previous = float(previous)
    if current < previous:
        return current
    return current - previous


def interval_valid(interval_s) -> bool:
    """Counter interval rule: discard interval_s <= 0 or > 3600."""
    return interval_s is not None and 0 < interval_s <= MAX_COUNTER_INTERVAL_S


def clamp_util(x):
    """Clamp a utilisation fraction to [0, 1.05]; returns (value, was_clamped)."""
    if x is None:
        return None, False
    c = min(max(x, 0.0), UTIL_CLAMP_MAX)
    return c, c != x


def hour_bucket(ts: datetime) -> datetime:
    return ts.replace(minute=0, second=0, microsecond=0)


def is_complete_hour(hour_start: datetime, now: datetime) -> bool:
    from datetime import timedelta
    return hour_start + timedelta(hours=1) <= now


def hours_since(ts, now: datetime):
    if ts is None:
        return None
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return (now - ts).total_seconds() / 3600.0


# ---------------------------------------------------------------------------
# Daily trend formulas T1–T7 (from hourly values)
# ---------------------------------------------------------------------------
def _vals(hours, metric):
    return [h[metric] for h in hours if h.get(metric) is not None]


def _avg(xs):
    return sum(xs) / len(xs) if xs else None


def _safe_div(a, b):
    if a is None or b in (None, 0):
        return None
    return a / b


def day_trends_from_hours(hours: list[dict], idle_snapshots: list[dict] | None = None) -> dict:
    """hours: one dict per hour {metric: value}. Returns {daily_metric: value} (None if no input).

    idle_snapshots: if given (pg_activity fallback), T2 = max over snapshots instead of the
    sum over hours.
    """
    out = {}
    conn_p95 = _vals(hours, "conn_util_p95")
    out["t1_conn_util_busiest_hour"] = max(conn_p95) if conn_p95 else None

    if idle_snapshots is not None:
        snaps = [s["idle_in_txn_over_60s_count"] for s in idle_snapshots
                 if s.get("idle_in_txn_over_60s_count") is not None]
        out["t2_idle_in_txn_over_60s_count"] = max(snaps) if snaps else None
    else:
        c60 = _vals(hours, "idle_in_txn_over_60s_count")
        out["t2_idle_in_txn_over_60s_count"] = sum(c60) if c60 else None

    iit, st = _vals(hours, "idle_in_txn_time_ms"), _vals(hours, "session_time_ms")
    out["t3_idle_in_txn_share"] = _safe_div(sum(iit), sum(st)) if st else None

    cpu_p95 = _vals(hours, "cpu_util_p95")
    out["t4_cpu_util_busiest_hour"] = max(cpu_p95) if cpu_p95 else None

    # T5: day-level cpu_util_avg / day-level conn_count_avg (hours weighted equally)
    out["t5_cpu_per_connection"] = _safe_div(_avg(_vals(hours, "cpu_util_avg")),
                                             _avg(_vals(hours, "conn_count_avg")))

    hits, misses = _vals(hours, "lfc_hits"), _vals(hours, "lfc_misses")
    out["t6_lfc_hit_rate"] = _safe_div(sum(hits), sum(hits) + sum(misses)) if (hits or misses) else None

    fit = _vals(hours, "lfc_working_set_fit")
    out["t7_working_set_fit"] = max(fit) if fit else None
    return out


# ---------------------------------------------------------------------------
# T8–T10 (from snapshot rows)
# ---------------------------------------------------------------------------
def t8_max_bloat(objects: list[dict], min_wasted_bytes: float):
    """objects: {object_type, object_name, datname, bloat_pct, bloat_bytes, bloat_is_na} (one per object per snapshot).

    Returns (value, top10). value is None when no bloat rows exist, 0 when none qualify.
    """
    if not objects:
        return None, []
    best = {}
    for o in objects:
        if o.get("bloat_bytes") is None or o.get("bloat_pct") is None or o.get("bloat_is_na"):
            continue  # is_na: statistics unavailable, the estimate is meaningless
        if o["bloat_bytes"] < min_wasted_bytes:
            continue
        key = (o.get("datname"), o["object_type"], o["object_name"])
        if key not in best or o["bloat_pct"] > best[key]["bloat_pct"]:
            best[key] = o
    ranked = sorted(best.values(), key=lambda o: (-o["bloat_pct"], -o["bloat_bytes"]))
    top = [{"object_type": o["object_type"], "object_name": o["object_name"], "datname": o.get("datname"),
            "bloat_pct": o["bloat_pct"], "bloat_bytes": o["bloat_bytes"]} for o in ranked[:10]]
    return (ranked[0]["bloat_pct"] if ranked else 0.0), top


def t9_dead_rows(tables: list[dict], min_rows: float):
    """tables: latest snapshot of the day {object_name, datname, n_live_tup, n_dead_tup, last_autovacuum,
    hours_since_autovacuum}. Returns (value, detail_list)."""
    large = [t for t in tables if (t.get("n_live_tup") or 0) >= min_rows]
    if not tables:
        return None, []
    detail = sorted(
        ({"object_name": t["object_name"], "datname": t.get("datname"),
          "n_dead_tup": t.get("n_dead_tup"), "last_autovacuum": t.get("last_autovacuum"),
          "hours_since_autovacuum": t.get("hours_since_autovacuum")} for t in large),
        key=lambda d: -(d["n_dead_tup"] or 0))
    return float(sum(t.get("n_dead_tup") or 0 for t in large)), detail


def t10_seq_scan_ratio(tables: list[dict], min_rows: float):
    """tables: per table for the day {object_name, datname, n_live_tup (latest), seq_scan_delta (sum),
    seq_tup_read_delta (sum)}. full_scan_equiv = seq_tup_read_delta / n_live_tup."""
    if not tables:
        return None, []
    detail = []
    for t in tables:
        live = t.get("n_live_tup") or 0
        if live < min_rows or live <= 0:
            continue
        reads = t.get("seq_tup_read_delta")
        equiv = (reads / live) if reads is not None else None
        detail.append({"object_name": t["object_name"], "datname": t.get("datname"),
                       "seq_scan_delta": t.get("seq_scan_delta"),
                       "seq_tup_read_delta": reads, "full_scan_equiv": equiv})
    detail.sort(key=lambda d: -(d["full_scan_equiv"] or 0))
    vals = [d["full_scan_equiv"] for d in detail if d["full_scan_equiv"] is not None]
    return (max(vals) if vals else 0.0), detail


# ---------------------------------------------------------------------------
# T11 — new sequential-scan queries from plan_history
# ---------------------------------------------------------------------------
# EXPLAIN (FORMAT JSON) emits "Node Type" before "Relation Name" within the same node and child
# "Plans" after it, so a brace-free gap between the two keys stays inside one node.
SEQ_SCAN_REGEX = r'"Node Type": "Seq Scan",[^{}]*?"Relation Name": "([^"]+)"'
_SEQ_SCAN_RE = re.compile(SEQ_SCAN_REGEX)
REDACTED = "<REDACTED>"


def seq_scan_relations(plan_json: str | None) -> list[str]:
    if not plan_json or plan_json == REDACTED:
        return []
    return sorted(set(_SEQ_SCAN_RE.findall(plan_json)))


def new_seq_scan_queries(day_pairs: list[dict], prior_pairs: set, large_tables: set) -> list[dict]:
    """day_pairs: {datname, queryid, relation, count, avg_duration_ms, query_text} for the day.
    prior_pairs: {(datname, queryid, relation)} seen in the previous 7 days.
    large_tables: {(datname, table_name)} (table name without schema — plans carry bare names).
    Returns the new (queryid, relation) entries on large tables."""
    out = []
    for p in day_pairs:
        if (p["datname"], p["relation"]) not in large_tables:
            continue
        if (p["datname"], p["queryid"], p["relation"]) in prior_pairs:
            continue
        out.append(p)
    return out


# ---------------------------------------------------------------------------
# Index classification
# ---------------------------------------------------------------------------
def classify_unused_index(*, scans_in_window, index_bytes, backs_constraint, observed_days,
                          min_bytes: float, window_days: float):
    """Return 'index_unused', 'index_unused_candidate' or None.

    scans_in_window: sum of idx_scan_delta over the window across all endpoints of the branch
    (None = no deltas observed yet).
    """
    if backs_constraint:
        return None
    if index_bytes is None or index_bytes < min_bytes:
        return None
    if scans_in_window is not None and scans_in_window > 0:
        return None
    if observed_days is not None and observed_days >= window_days and scans_in_window is not None:
        return "index_unused"
    return "index_unused_candidate"


def find_duplicate_indexes(indexes: list[dict]) -> dict:
    """indexes: {datname, schema, table, object_name, index_columns, indexprs, indpred, index_bytes,
    indisunique}. Returns {(datname, object_name): detail} for every index in a duplicate group."""
    groups = defaultdict(list)
    for i in indexes:
        if not i.get("index_columns") and not i.get("indexprs"):
            continue
        key = (i.get("datname"), i.get("schema"), i.get("table"), i.get("index_columns"),
               i.get("indexprs"), i.get("indpred"))
        groups[key].append(i)
    out = {}
    for members in groups.values():
        if len(members) < 2:
            continue
        for m in members:
            others = [o for o in members if o["object_name"] != m["object_name"]]
            out[(m.get("datname"), m["object_name"])] = {
                "table": m.get("table"),
                "duplicate_of": [o["object_name"] for o in others],
                "sizes": {o["object_name"]: o.get("index_bytes") for o in members},
                "is_unique": {o["object_name"]: bool(o.get("indisunique")) for o in members},
                "index_bytes": m.get("index_bytes"),
                "indexdef": m.get("indexdef"),
            }
    return out


# ---------------------------------------------------------------------------
# Idle-in-transaction (pg_stat_activity fallback)
# ---------------------------------------------------------------------------
def idle_in_txn_summary(rows: list[dict], long_s: float, very_long_s: float) -> dict:
    """rows: {xact_age_ms, ...} for sessions in 'idle in transaction'."""
    ages = [r["xact_age_ms"] for r in rows if r.get("xact_age_ms") is not None]
    return {
        "idle_in_txn_over_60s_count": float(sum(1 for a in ages if a > long_s * 1000)),
        "idle_in_txn_over_300s_count": float(sum(1 for a in ages if a > very_long_s * 1000)),
        "idle_in_txn_max_age_ms": float(max(ages)) if ages else 0.0,
    }
