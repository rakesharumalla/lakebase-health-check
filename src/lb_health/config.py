"""Job parameters, constants and run-time configuration."""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

METRICS_TABLE = "lb_health_metrics"
THRESHOLDS_TABLE = "lb_health_thresholds"

# Incremental window for the system-table rollup (see rollup_window).
FIRST_RUN_HOURS = 48       # endpoint with no rollup_ok history yet
OVERLAP_HOURS = 2          # re-read before the last processed hour to pick up late-arriving telemetry
MAX_CATCHUP_HOURS = 7 * 24  # automatic catch-up limit (compute_counters / ASH retention); backfill_days can go further
IDLE_IN_TXN_SOURCES = ("ash", "pg_activity")
MIN_TREND_RETENTION_DAYS = 60
# system.lakebase retention is documented as 7 days but was observed longer; backfill loads whatever exists.
MAX_BACKFILL_DAYS = 30

_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_\-]*$")

# Defaults for the cfg_* rows in lb_health_thresholds (used when a row is missing).
CONFIG_DEFAULTS = {
    "cfg_large_table_min_rows": 100_000.0,
    "cfg_bloat_min_wasted_bytes": 104_857_600.0,
    "cfg_unused_index_min_bytes": 10_485_760.0,
    "cfg_unused_index_window_days": 30.0,
    "cfg_long_xact_seconds": 60.0,
    "cfg_very_long_xact_seconds": 300.0,
}

PARAM_DEFAULTS = {
    "project_id": "lb-health-check",
    "branch_id": "ALL",
    "retention_days": "30",
    "trend_retention_days": "90",
    "target_catalog_name": "main",
    "target_schema_name": "lb_health_check",
    "pg_lock_timeout_ms": "1000",
    "pg_statement_timeout_ms": "30000",
    "pg_bloat_statement_timeout_ms": "120000",
    "snapshot_all_databases": "true",
    "idle_in_txn_source": "ash",
    "backfill_days": "0",
    "run_id": "",
}


def _bool(v: str) -> bool:
    return str(v).strip().lower() in ("1", "true", "yes", "y", "on")


@dataclass
class Config:
    project_id: str
    retention_days: int
    trend_retention_days: int
    catalog: str
    schema: str
    pg_lock_timeout_ms: int
    pg_statement_timeout_ms: int
    pg_bloat_statement_timeout_ms: int
    snapshot_all_databases: bool
    idle_in_txn_source: str
    backfill_days: int = 0
    branch_id: str = "ALL"
    run_id: str = ""
    warnings: list = field(default_factory=list)

    @property
    def schema_fq(self) -> str:
        return f"`{self.catalog}`.`{self.schema}`"

    def fq(self, name: str) -> str:
        return f"{self.schema_fq}.`{name}`"

    @property
    def metrics(self) -> str:
        return self.fq(METRICS_TABLE)

    @property
    def thresholds(self) -> str:
        return self.fq(THRESHOLDS_TABLE)

    def validate(self) -> None:
        for name, v in (("target_catalog_name", self.catalog), ("target_schema_name", self.schema)):
            if not _IDENT.match(v):
                raise ValueError(f"{name}={v!r} is not a valid identifier")
        if self.idle_in_txn_source not in IDLE_IN_TXN_SOURCES:
            raise ValueError(f"idle_in_txn_source must be one of {IDLE_IN_TXN_SOURCES}")
        if self.retention_days < 1:
            raise ValueError("retention_days must be >= 1")
        if self.trend_retention_days < MIN_TREND_RETENTION_DAYS:
            raise ValueError(
                f"trend_retention_days must be >= {MIN_TREND_RETENTION_DAYS}: the 30-day review period compares "
                f"with the prior 30 days, and unused-index detection carries first_seen in day rows")
        if not 0 <= self.backfill_days <= MAX_BACKFILL_DAYS:
            raise ValueError(f"backfill_days must be between 0 and {MAX_BACKFILL_DAYS}")
        if not self.project_id.strip():
            raise ValueError("project_id is empty (use a comma-separated list or ALL)")
        if not self.branch_id.strip():
            raise ValueError("branch_id is empty (use a comma-separated list or ALL)")


def _widget(dbutils, name: str) -> str:
    try:
        v = dbutils.widgets.get(name)
    except Exception:  # widget not defined (e.g. interactive run)
        v = None
    return PARAM_DEFAULTS[name] if v in (None, "") else v


def _run_id(dbutils) -> str:
    try:
        ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
        tags = ctx.tags()
        for key in ("multitaskParentRunId", "jobRunId", "runId"):
            v = tags.get(key)
            if v is not None and v.isDefined():
                return str(v.get())
    except Exception:
        pass
    return ""


def load(dbutils) -> Config:
    """Read every job parameter through dbutils.widgets."""
    for name, default in PARAM_DEFAULTS.items():
        try:
            dbutils.widgets.text(name, default)
        except Exception:
            pass
    cfg = Config(
        project_id=_widget(dbutils, "project_id").strip(),
        branch_id=_widget(dbutils, "branch_id").strip(),
        retention_days=int(_widget(dbutils, "retention_days")),
        trend_retention_days=int(_widget(dbutils, "trend_retention_days")),
        catalog=_widget(dbutils, "target_catalog_name").strip(),
        schema=_widget(dbutils, "target_schema_name").strip(),
        pg_lock_timeout_ms=int(_widget(dbutils, "pg_lock_timeout_ms")),
        pg_statement_timeout_ms=int(_widget(dbutils, "pg_statement_timeout_ms")),
        pg_bloat_statement_timeout_ms=int(_widget(dbutils, "pg_bloat_statement_timeout_ms")),
        snapshot_all_databases=_bool(_widget(dbutils, "snapshot_all_databases")),
        idle_in_txn_source=_widget(dbutils, "idle_in_txn_source").strip().lower(),
        backfill_days=int(_widget(dbutils, "backfill_days") or 0),
        run_id=(_widget(dbutils, "run_id").strip() if "{{" not in _widget(dbutils, "run_id") else "")
               or _run_id(dbutils),
    )
    cfg.validate()
    return cfg


def read_config_values(spark, cfg: Config) -> dict:
    """cfg_* rows from lb_health_thresholds (read at run start, so edits need no redeploy)."""
    values = dict(CONFIG_DEFAULTS)
    try:
        rows = spark.sql(
            f"SELECT threshold_id, value FROM {cfg.thresholds} WHERE kind = 'config'"
        ).collect()
        for r in rows:
            if r["value"] is not None:
                values[r["threshold_id"]] = float(r["value"])
    except Exception as e:  # table missing on very first run → defaults
        print(f"WARN: could not read config thresholds, using defaults: {e}")
    return values


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def floor_hour(dt: datetime) -> datetime:
    return dt.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0)


def rollup_window(now: datetime, watermarks: dict, keys, backfill_days: int = 0) -> tuple[datetime, datetime]:
    """[start, end) of complete UTC hours for the system-table rollup.

    watermarks: endpoint key -> end of the last window that endpoint was rolled up to (None/missing = never).
    Per endpoint the window starts OVERLAP_HOURS before its watermark (late telemetry), FIRST_RUN_HOURS before
    `end` on its first run, and never more than MAX_CATCHUP_HOURS back. The shared window starts at the earliest
    endpoint start (rows are MERGEd, so re-reading hours is harmless). backfill_days widens it further.
    Only complete hours are written: end = start of the current hour.
    """
    end = floor_hour(now)
    floor = end - timedelta(hours=MAX_CATCHUP_HOURS)
    starts = []
    for k in keys:
        wm = watermarks.get(k)
        s = end - timedelta(hours=FIRST_RUN_HOURS) if wm is None else floor_hour(wm) - timedelta(hours=OVERLAP_HOURS)
        starts.append(max(s, floor))
    start = min(starts) if starts else end - timedelta(hours=FIRST_RUN_HOURS)
    start = min(start, end - timedelta(hours=OVERLAP_HOURS))
    if backfill_days:
        start = min(start, end - timedelta(days=backfill_days))
    return start, end


def recompute_days(now: datetime, window_start: datetime | None, backfill_days: int = 0) -> list:
    """Complete UTC days for the daily rollup: every day touched by the rollup window, at least the last 2,
    and the last backfill_days when set. The current partial day is never included."""
    today = now.astimezone(timezone.utc).date()
    n = 2
    if window_start is not None:
        n = max(n, (today - window_start.astimezone(timezone.utc).date()).days)
    n = max(n, backfill_days)
    return [today - timedelta(days=i) for i in range(n, 0, -1)]


def maintenance_due(last_run: datetime | None, now: datetime, interval: timedelta) -> bool:
    """True when a maintenance operation (VACUUM / OPTIMIZE) never ran or last ran more than `interval` ago."""
    return last_run is None or now - last_run >= interval


def complete_days(now: datetime, n: int = 2) -> list:
    """The last n complete UTC days (dates), oldest first. The current partial day is never included."""
    today = now.astimezone(timezone.utc).date()
    return [today - timedelta(days=i) for i in range(n, 0, -1)]


def ts_literal(dt: datetime) -> str:
    return "TIMESTAMP'" + dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S") + "'"


def sql_str_list(values) -> str:
    vals = [str(v).replace("'", "''") for v in values]
    return ", ".join(f"'{v}'" for v in vals) if vals else "NULL"
