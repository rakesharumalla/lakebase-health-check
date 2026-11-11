"""Job parameters, constants and run-time configuration."""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

METRICS_TABLE = "lb_health_metrics"
THRESHOLDS_TABLE = "lb_health_thresholds"

FREQUENCIES = ("daily", "hourly")
LOOKBACK_HOURS = {"daily": 48, "hourly": 3}
IDLE_IN_TXN_SOURCES = ("ash", "pg_activity")
MIN_TREND_RETENTION_DAYS = 60

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
    "project_name": "lb-health-check",
    "frequency": "daily",
    "schedule_cron": "0 0 2 * * ?",
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
    project_name: str
    frequency: str
    schedule_cron: str
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

    @property
    def lookback_hours(self) -> int:
        """48 h (daily) / 3 h (hourly); backfill_days > 0 widens it (system tables keep 7 days)."""
        return max(LOOKBACK_HOURS[self.frequency], self.backfill_days * 24)

    @property
    def daily_recompute_days(self) -> int:
        """Complete UTC days the daily rollup recomputes: 2, or backfill_days if larger."""
        return max(2, self.backfill_days)

    def validate(self) -> None:
        for name, v in (("target_catalog_name", self.catalog), ("target_schema_name", self.schema)):
            if not _IDENT.match(v):
                raise ValueError(f"{name}={v!r} is not a valid identifier")
        if self.frequency not in FREQUENCIES:
            raise ValueError(f"frequency must be one of {FREQUENCIES}, got {self.frequency!r}")
        if self.idle_in_txn_source not in IDLE_IN_TXN_SOURCES:
            raise ValueError(f"idle_in_txn_source must be one of {IDLE_IN_TXN_SOURCES}")
        if self.retention_days < 1:
            raise ValueError("retention_days must be >= 1")
        if self.trend_retention_days < MIN_TREND_RETENTION_DAYS:
            raise ValueError(
                f"trend_retention_days must be >= {MIN_TREND_RETENTION_DAYS}: the 30-day review period compares "
                f"with the prior 30 days, and unused-index detection carries first_seen in day rows")
        if not 0 <= self.backfill_days <= 7:
            raise ValueError("backfill_days must be between 0 and 7 (system.lakebase keeps 7 days)")
        if not self.project_name.strip():
            raise ValueError("project_name is empty (use a comma-separated list or ALL)")


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
        project_name=_widget(dbutils, "project_name").strip(),
        frequency=_widget(dbutils, "frequency").strip().lower(),
        schedule_cron=_widget(dbutils, "schedule_cron").strip(),
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


def cron_looks_hourly(cron: str) -> bool:
    """Quartz: sec min hour dom month dow [year]. Hourly when the hour field is a wildcard/step."""
    parts = cron.split()
    if len(parts) < 6:
        return False
    hour = parts[2]
    return hour == "*" or hour.startswith("*/") or hour.startswith("0/") or "," in hour


def schedule_warning(frequency: str, cron: str) -> str | None:
    hourly = cron_looks_hourly(cron)
    if frequency == "hourly" and not hourly:
        return f"frequency=hourly but schedule_cron={cron!r} looks daily; use '0 0 * * * ?'"
    if frequency == "daily" and hourly:
        return f"frequency=daily but schedule_cron={cron!r} looks hourly; use '0 0 2 * * ?'"
    return None


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


def hour_window(now: datetime, lookback_hours: int) -> tuple[datetime, datetime]:
    """[start, end) of complete hours to (re)compute: start = floor(now - lookback), end = floor(now).

    Only complete hours are written (hour_start + 1h <= now), which is exactly hour_start < end.
    """
    now = now.astimezone(timezone.utc)
    end = now.replace(minute=0, second=0, microsecond=0)
    start = (now - timedelta(hours=lookback_hours)).replace(minute=0, second=0, microsecond=0)
    return start, end


def complete_days(now: datetime, n: int = 2) -> list:
    """The last n complete UTC days (dates), oldest first. The current partial day is never included."""
    today = now.astimezone(timezone.utc).date()
    return [today - timedelta(days=i) for i in range(n, 0, -1)]


def ts_literal(dt: datetime) -> str:
    return "TIMESTAMP'" + dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S") + "'"


def sql_str_list(values) -> str:
    vals = [str(v).replace("'", "''") for v in values]
    return ", ".join(f"'{v}'" for v in vals) if vals else "NULL"
