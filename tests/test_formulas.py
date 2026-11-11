"""Unit tests for the pure formulas in lb_health (no Spark / Postgres needed).

Run:  uv run --with pytest pytest tests -q      (or: python -m pytest tests -q)
"""
import os
import sys
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from lb_health import config, lakebase_api, metrics, views  # noqa: E402

UTC = timezone.utc


# --- counter delta / reset ---------------------------------------------------------------------
def test_delta_normal():
    assert metrics.counter_delta(150, 100) == 50


def test_delta_no_previous_is_null():
    assert metrics.counter_delta(150, None) is None


def test_delta_reset_returns_current():
    # stats reset (or new compute after resume): counters restart from zero
    assert metrics.counter_delta(7, 1000) == 7


def test_delta_current_null():
    assert metrics.counter_delta(None, 10) is None


def test_delta_unchanged_is_zero():
    assert metrics.counter_delta(42, 42) == 0


# --- incremental rollup window (watermark per endpoint) ------------------------------------------
K1, K2 = ("p", "production", "ep1"), ("p", "dev", "ep2")
H = lambda d, h, m=0: datetime(2026, 11, d, h, m, tzinfo=UTC)  # noqa: E731


def test_window_first_run_loads_48h():
    start, end = config.rollup_window(H(10, 2, 7), {}, [K1])
    assert (start, end) == (H(8, 2), H(10, 2))


def test_window_normal_run_starts_overlap_before_watermark():
    # hourly schedule: last run processed up to 13:00, this run at 14:00:05
    now = datetime(2026, 11, 10, 14, 0, 5, tzinfo=UTC)
    start, end = config.rollup_window(now, {K1: H(10, 13)}, [K1])
    assert (start, end) == (H(10, 11), H(10, 14))
    h = start  # every hour in [start, end) is complete; the running hour is not
    while h < end:
        assert metrics.is_complete_hour(h, now)
        h += timedelta(hours=1)
    assert not metrics.is_complete_hour(end, now)


def test_window_daily_schedule():
    start, end = config.rollup_window(H(10, 2, 3), {K1: H(9, 2)}, [K1])
    assert (start, end) == (H(9, 0), H(10, 2))  # 24 h since the last run + 2 h overlap


def test_window_catches_up_after_gap():
    # job paused for 3 hours: watermark 10:00, run at 14:00 -> reload from 08:00
    start, end = config.rollup_window(H(10, 14), {K1: H(10, 10)}, [K1])
    assert (start, end) == (H(10, 8), H(10, 14))


def test_window_capped_at_max_catchup():
    start, end = config.rollup_window(H(30, 12), {K1: H(1, 0)}, [K1])
    assert end - start == timedelta(hours=config.MAX_CATCHUP_HOURS)


def test_window_earliest_endpoint_wins():
    # ep2 is new (no watermark) -> 48 h; ep1 is current -> 2 h. Shared window = earliest start.
    start, end = config.rollup_window(H(10, 14), {K1: H(10, 13)}, [K1, K2])
    assert start == H(8, 14)
    # endpoints outside scope are ignored
    start, _ = config.rollup_window(H(10, 14), {K1: H(10, 13), K2: H(1, 0)}, [K1])
    assert start == H(10, 11)


def test_window_rerun_same_hour_and_naive_minutes():
    # watermark already at the current hour (manual rerun) -> still re-reads the overlap
    start, end = config.rollup_window(H(10, 14, 40), {K1: H(10, 14)}, [K1])
    assert (start, end) == (H(10, 12), H(10, 14))


def test_window_backfill_overrides():
    start, end = config.rollup_window(H(20, 5), {K1: H(20, 4)}, [K1], backfill_days=15)
    assert (start, end) == (H(5, 5), H(20, 5))


def test_recompute_days():
    d = lambda day: datetime(2026, 11, day).date()  # noqa: E731
    assert config.recompute_days(H(10, 14), H(10, 11)) == [d(8), d(9)]           # minimum 2 days
    assert config.recompute_days(H(10, 14), H(5, 14)) == [d(5), d(6), d(7), d(8), d(9)]  # catch-up
    assert config.recompute_days(H(10, 14), None, backfill_days=4) == [d(6), d(7), d(8), d(9)]


def test_maintenance_due():
    assert config.maintenance_due(None, H(10, 2), timedelta(hours=20))
    assert not config.maintenance_due(H(10, 1), H(10, 2), timedelta(hours=20))
    assert config.maintenance_due(H(9, 2), H(10, 2), timedelta(hours=20))


def test_hour_bucket():
    assert metrics.hour_bucket(datetime(2026, 1, 1, 5, 59, 59, 999, tzinfo=UTC)) == datetime(2026, 1, 1, 5, tzinfo=UTC)


def test_complete_days_excludes_today():
    now = datetime(2026, 11, 10, 0, 30, tzinfo=UTC)
    assert config.complete_days(now, 2) == [datetime(2026, 11, 8).date(), datetime(2026, 11, 9).date()]


# --- counter interval guard ----------------------------------------------------------------------
def test_interval_guard():
    assert metrics.interval_valid(30)
    assert metrics.interval_valid(3600)
    assert not metrics.interval_valid(0)
    assert not metrics.interval_valid(-5)
    assert not metrics.interval_valid(3601)
    assert not metrics.interval_valid(None)


def test_clamp():
    assert metrics.clamp_util(1.3) == (1.05, True)
    assert metrics.clamp_util(-0.1) == (0.0, True)
    assert metrics.clamp_util(0.5) == (0.5, False)


# --- T1–T7 on a synthetic day --------------------------------------------------------------------
def _synthetic_hours():
    hours = []
    for h in range(24):
        busy = h == 14
        hours.append({
            "conn_util_p95": 0.9 if busy else 0.2,
            "idle_in_txn_over_60s_count": 2 if h in (3, 4) else 0,
            "idle_in_txn_time_ms": 100.0,
            "session_time_ms": 1000.0,
            "cpu_util_p95": 0.8 if busy else 0.1,
            "cpu_util_avg": 0.5 if busy else 0.1,
            "conn_count_avg": 10.0,
            "lfc_hits": 0.0,
            "lfc_misses": 10.0,
            "shared_blks_hit": 990.0,
            "shared_blks_read": 10.0,
            "lfc_working_set_fit": 0.85 if busy else 0.3,
        })
    return hours


def test_day_trends_t1_to_t7():
    t = metrics.day_trends_from_hours(_synthetic_hours())
    assert t["t1_conn_util_busiest_hour"] == 0.9
    assert t["t2_idle_in_txn_over_60s_count"] == 4
    assert abs(t["t3_idle_in_txn_share"] - 0.1) < 1e-12
    assert t["t4_cpu_util_busiest_hour"] == 0.8
    expected_cpu = (0.5 + 23 * 0.1) / 24
    assert abs(t["t5_cpu_per_connection"] - expected_cpu / 10.0) < 1e-12
    assert abs(t["t6_cache_hit_rate"] - 0.99) < 1e-12
    assert t["t7_working_set_fit"] == 0.85


def test_day_trends_fallback_t2_is_max_over_snapshots():
    t = metrics.day_trends_from_hours(_synthetic_hours(), idle_snapshots=[
        {"idle_in_txn_over_60s_count": 1}, {"idle_in_txn_over_60s_count": 3}])
    assert t["t2_idle_in_txn_over_60s_count"] == 3


def test_day_trends_empty():
    t = metrics.day_trends_from_hours([])
    assert all(v is None for v in t.values())


def test_day_trends_zero_session_time():
    t = metrics.day_trends_from_hours([{"idle_in_txn_time_ms": 0.0, "session_time_ms": 0.0}])
    assert t["t3_idle_in_txn_share"] is None


# --- T8–T10 --------------------------------------------------------------------------------------
MB = 1024 * 1024


def test_t8_bloat_filters_small_objects():
    objs = [
        {"object_type": "table", "object_name": "public.big", "bloat_pct": 40.0, "bloat_bytes": 500 * MB},
        {"object_type": "table", "object_name": "public.small", "bloat_pct": 90.0, "bloat_bytes": 1 * MB},
        {"object_type": "index", "object_name": "public.big_pkey", "bloat_pct": 35.0, "bloat_bytes": 200 * MB},
    ]
    v, top = metrics.t8_max_bloat(objs, 100 * MB)
    assert v == 40.0
    assert [o["object_name"] for o in top] == ["public.big", "public.big_pkey"]


def test_t8_skips_objects_without_statistics():
    objs = [{"object_type": "table", "object_name": "public.na", "bloat_pct": 0.0, "bloat_bytes": 500 * MB, "bloat_is_na": 1},
            {"object_type": "table", "object_name": "public.ok", "bloat_pct": 12.0, "bloat_bytes": 200 * MB, "bloat_is_na": 0}]
    v, top = metrics.t8_max_bloat(objs, 100 * MB)
    assert v == 12.0 and [o["object_name"] for o in top] == ["public.ok"]


def test_t8_none_qualify_is_zero_and_no_rows_is_none():
    assert metrics.t8_max_bloat([{"object_type": "table", "object_name": "a", "bloat_pct": 50, "bloat_bytes": 1}],
                                100 * MB)[0] == 0.0
    assert metrics.t8_max_bloat([], 100 * MB)[0] is None


def test_t9_dead_rows_only_large_tables():
    tables = [
        {"object_name": "public.big", "n_live_tup": 1_000_000, "n_dead_tup": 5000},
        {"object_name": "public.big2", "n_live_tup": 100_000, "n_dead_tup": 100},
        {"object_name": "public.small", "n_live_tup": 99_999, "n_dead_tup": 1_000_000},
    ]
    v, detail = metrics.t9_dead_rows(tables, 100_000)
    assert v == 5100
    assert [d["object_name"] for d in detail] == ["public.big", "public.big2"]


def test_t10_seq_scan_ratio():
    tables = [
        {"object_name": "public.big", "n_live_tup": 1_000_000, "seq_scan_delta": 12, "seq_tup_read_delta": 12_000_000},
        {"object_name": "public.mid", "n_live_tup": 200_000, "seq_scan_delta": 1, "seq_tup_read_delta": 200_000},
        {"object_name": "public.small", "n_live_tup": 10, "seq_scan_delta": 1000, "seq_tup_read_delta": 10_000},
    ]
    v, detail = metrics.t10_seq_scan_ratio(tables, 100_000)
    assert v == 12.0
    assert detail[0]["object_name"] == "public.big"
    assert len(detail) == 2


# --- T11 -----------------------------------------------------------------------------------------
PLAN = """[
  {
    "Plan": {
      "Node Type": "Hash Join",
      "Plans": [
        {
          "Node Type": "Seq Scan",
          "Parent Relationship": "Outer",
          "Parallel Aware": false,
          "Relation Name": "orders",
          "Alias": "o"
        },
        {
          "Node Type": "Index Scan",
          "Relation Name": "customers",
          "Index Name": "customers_pkey"
        }
      ]
    }
  }
]"""


def test_seq_scan_relations_from_plan():
    assert metrics.seq_scan_relations(PLAN) == ["orders"]
    assert metrics.seq_scan_relations(metrics.REDACTED) == []
    assert metrics.seq_scan_relations(None) == []


def test_new_seq_scan_queries():
    day = [
        {"datname": "db", "queryid": "1", "relation": "orders"},
        {"datname": "db", "queryid": "2", "relation": "orders"},
        {"datname": "db", "queryid": "3", "relation": "tiny"},
    ]
    prior = {("db", "1", "orders")}
    large = {("db", "orders")}
    new = metrics.new_seq_scan_queries(day, prior, large)
    assert [p["queryid"] for p in new] == ["2"]


# --- index classification ------------------------------------------------------------------------
def _unused(**kw):
    args = dict(scans_in_window=0, index_bytes=50 * MB, backs_constraint=False, observed_days=31,
                min_bytes=10 * MB, window_days=30)
    args.update(kw)
    return metrics.classify_unused_index(**args)


def test_unused_index():
    assert _unused() == "index_unused"


def test_unused_candidate_when_observed_too_short():
    assert _unused(observed_days=12) == "index_unused_candidate"


def test_unused_candidate_when_no_deltas_yet():
    assert _unused(scans_in_window=None) == "index_unused_candidate"


def test_not_unused_when_scanned_small_or_constraint():
    assert _unused(scans_in_window=3) is None
    assert _unused(index_bytes=1 * MB) is None
    assert _unused(backs_constraint=True) is None


def test_duplicate_indexes():
    idx = [
        {"datname": "db", "schema": "public", "table": "t", "object_name": "public.t_a_idx",
         "index_columns": "a", "indexprs": None, "indpred": None, "index_bytes": 10, "indisunique": False},
        {"datname": "db", "schema": "public", "table": "t", "object_name": "public.t_a_uq",
         "index_columns": "a", "indexprs": None, "indpred": None, "index_bytes": 12, "indisunique": True},
        {"datname": "db", "schema": "public", "table": "t", "object_name": "public.t_a_partial",
         "index_columns": "a", "indexprs": None, "indpred": "(a > 0)", "index_bytes": 5, "indisunique": False},
        {"datname": "db", "schema": "public", "table": "t", "object_name": "public.t_b_idx",
         "index_columns": "b", "indexprs": None, "indpred": None, "index_bytes": 5, "indisunique": False},
    ]
    d = metrics.find_duplicate_indexes(idx)
    assert set(d) == {("db", "public.t_a_idx"), ("db", "public.t_a_uq")}
    assert d[("db", "public.t_a_idx")]["duplicate_of"] == ["public.t_a_uq"]
    assert d[("db", "public.t_a_idx")]["is_unique"] == {"public.t_a_idx": False, "public.t_a_uq": True}


# --- idle-in-transaction fallback ----------------------------------------------------------------
def test_idle_in_txn_summary():
    s = metrics.idle_in_txn_summary([{"xact_age_ms": 30_000}, {"xact_age_ms": 90_000}, {"xact_age_ms": 400_000}],
                                    60, 300)
    assert s == {"idle_in_txn_over_60s_count": 2.0, "idle_in_txn_over_300s_count": 1.0,
                 "idle_in_txn_max_age_ms": 400_000.0}


# --- config ------------------------------------------------------------------------------------
def test_threshold_seed_covers_every_trend():
    ids = {t[0] for t in metrics.THRESHOLD_SEED}
    assert {t for t, _, _ in metrics.TRENDS} <= ids
    assert set(config.CONFIG_DEFAULTS) <= ids


def test_views_render():
    cfg = config.Config(project_id="p", retention_days=30,
                        trend_retention_days=90, catalog="c", schema="s", pg_lock_timeout_ms=1000,
                        pg_statement_timeout_ms=30000, pg_bloat_statement_timeout_ms=120000,
                        snapshot_all_databases=True, idle_in_txn_source="ash")
    v = views.view_statements(cfg)
    assert {"lb_health_hourly", "lb_health_daily", "lb_health_objects_latest", "lb_health_index_findings",
            "lb_health_seq_scan_queries", "lb_health_review_summary"} <= set(v)
    for m in metrics.HOURLY_METRICS:
        assert f" AS {m}" in v["lb_health_hourly"]


def test_ddl_statements_cover_every_object():
    from lb_health import ddl
    cfg = ddl._dummy_cfg()
    sql = "\n".join(ddl.statements(cfg))
    for obj in ddl.expected_objects():
        assert f"`{obj}`" in sql
    assert "WHEN NOT MATCHED THEN INSERT" in ddl.seed_statement(cfg)
    assert "WHEN MATCHED" not in ddl.seed_statement(cfg)  # customer edits are never overwritten


def test_trend_retention_minimum():
    import pytest
    from lb_health import ddl
    cfg = ddl._dummy_cfg()
    cfg.trend_retention_days = 59
    with pytest.raises(ValueError):
        cfg.validate()
    cfg.trend_retention_days = 60
    cfg.validate()


def test_backfill_range():
    import pytest
    from lb_health import config, ddl
    cfg = ddl._dummy_cfg()
    cfg.backfill_days = config.MAX_BACKFILL_DAYS
    cfg.validate()
    cfg.backfill_days = config.MAX_BACKFILL_DAYS + 1
    with pytest.raises(ValueError):
        cfg.validate()


def test_cache_hit_rate_counts_shared_buffers_and_caps_lfc_hits():
    # shared buffers serve almost everything; LFC hits never exceed the reads that missed shared buffers
    t = metrics.day_trends_from_hours([{"shared_blks_hit": 1_000_000.0, "shared_blks_read": 55.0,
                                        "lfc_hits": 0.0, "lfc_misses": 349.0}])
    assert abs(t["t6_cache_hit_rate"] - 1_000_000 / 1_000_055) < 1e-12
    t = metrics.day_trends_from_hours([{"shared_blks_hit": 900.0, "shared_blks_read": 100.0, "lfc_hits": 500.0}])
    assert t["t6_cache_hit_rate"] == 1.0
    assert metrics.day_trends_from_hours([{"lfc_hits": 5.0, "lfc_misses": 5.0}])["t6_cache_hit_rate"] is None


def test_cpu_per_connection_skipped_when_idle():
    t = metrics.day_trends_from_hours([{"cpu_util_avg": 0.02, "conn_count_avg": 0.0003}])
    assert t["t5_cpu_per_connection"] is None
    t = metrics.day_trends_from_hours([{"cpu_util_avg": 0.2, "conn_count_avg": 4.0}])
    assert abs(t["t5_cpu_per_connection"] - 0.05) < 1e-12


# --- scope: branch filter ----------------------------------------------------------------------
class _FakeApi:
    """Minimal stand-in for WorkspaceClient.api_client: one project with two branches."""
    def __init__(self):
        self.data = {
            "/api/2.0/postgres/projects/p1": {"uid": "proj-uid"},
            "/api/2.0/postgres/projects/p1/branches": {"branches": [
                {"name": "projects/p1/branches/production", "uid": "br-prod"},
                {"name": "projects/p1/branches/dev", "uid": "br-dev"}]},
            "/api/2.0/postgres/projects/p1/branches/production/endpoints": {"endpoints": [
                {"name": "projects/p1/branches/production/endpoints/ep1", "uid": "ep-1", "status": {}}]},
            "/api/2.0/postgres/projects/p1/branches/dev/endpoints": {"endpoints": [
                {"name": "projects/p1/branches/dev/endpoints/ep2", "uid": "ep-2", "status": {}}]},
        }

    def do(self, method, path, query=None, body=None):
        return self.data[path]


class _FakeW:
    api_client = _FakeApi()


def test_branch_filter_parsing():
    assert lakebase_api.parse_branch_filter("ALL") is None
    assert lakebase_api.parse_branch_filter(" all ") is None
    assert lakebase_api.parse_branch_filter("production, br-dev") == {"production", "br-dev"}


def test_list_endpoints_branch_filter():
    w = _FakeW()
    all_eps, _ = lakebase_api.list_endpoints(w, ["p1"])
    assert sorted(e["branch_id"] for e in all_eps) == ["dev", "production"]
    by_id, _ = lakebase_api.list_endpoints(w, ["p1"], {"production"})
    assert [e["endpoint_id"] for e in by_id] == ["ep1"]
    by_uid, _ = lakebase_api.list_endpoints(w, ["p1"], {"br-dev"})
    assert [e["endpoint_id"] for e in by_uid] == ["ep2"]
    none, _ = lakebase_api.list_endpoints(w, ["p1"], {"staging"})
    assert none == []


def test_empty_branch_id_rejected():
    cfg = config.Config(project_id="p", branch_id=" ",
                        retention_days=30, trend_retention_days=90, catalog="c", schema="s",
                        pg_lock_timeout_ms=1000, pg_statement_timeout_ms=30000,
                        pg_bloat_statement_timeout_ms=120000, snapshot_all_databases=True,
                        idle_in_txn_source="ash")
    try:
        cfg.validate()
        raise AssertionError("expected ValueError")
    except ValueError as e:
        assert "branch_id" in str(e)
