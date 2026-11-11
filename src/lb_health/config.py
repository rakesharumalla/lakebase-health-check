"""Job parameters and constants."""
from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

METRICS_TABLE = "lb_health_metrics"
THRESHOLDS_TABLE = "lb_health_thresholds"

# Every run does the same work, whatever the schedule.
LOOKBACK_HOURS = 48          # hourly rollup re-reads (and MERGEs) the last 48 complete hours
RECOMPUTE_DAYS = 2           # daily rollup recomputes the last 2 complete UTC days
RETENTION_DAYS = 90          # every row older than this is deleted
REVIEW_PERIODS = (7, 14)     # review summary periods (days), each compared with the equally long prior period

# Postgres session guardrails for the snapshot.
PG_LOCK_TIMEOUT_MS = 1_000
PG_STATEMENT_TIMEOUT_MS = 30_000  # also covers the bloat queries

# Cutoffs used by the trends and findings.
LARGE_TABLE_MIN_ROWS = 100_000           # n_live_tup at or above this = large table
BLOAT_MIN_WASTED_BYTES = 100 * 1024**2   # bloat below 100 MB wasted is ignored by the bloat trend
UNUSED_INDEX_MIN_BYTES = 10 * 1024**2    # indexes smaller than 10 MB are never reported unused
UNUSED_INDEX_WINDOW_DAYS = 30            # zero scans for this many days = unused
LONG_XACT_SECONDS = 60                   # idle-in-transaction older than this = long
VERY_LONG_XACT_SECONDS = 300             # ... = very long

_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_\-]*$")

# Job parameters. project_id and branch_id have no default: the job fails fast without them.
PARAMS = ("project_id", "branch_id", "target_catalog_name", "target_schema_name", "run_id")


@dataclass
class Config:
    project_id: str
    branch_id: str
    catalog: str
    schema: str
    run_id: str = ""

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
        for name, v in (("project_id", self.project_id), ("branch_id", self.branch_id)):
            if not v.strip():
                raise ValueError(f"{name} is required (a single Lakebase {name.split('_')[0]} ID)")
            if "," in v or v.strip().upper() == "ALL":
                raise ValueError(f"{name}={v!r}: give a single ID; lists and ALL are not supported")
        for name, v in (("target_catalog_name", self.catalog), ("target_schema_name", self.schema)):
            if not _IDENT.match(v):
                raise ValueError(f"{name}={v!r} is not a valid identifier")


def _widget(dbutils, name: str) -> str:
    try:
        return (dbutils.widgets.get(name) or "").strip()
    except Exception:  # widget not defined (e.g. interactive run)
        return ""


def load(dbutils) -> Config:
    """Read the job parameters through dbutils.widgets and validate them."""
    for name in PARAMS:
        try:
            dbutils.widgets.text(name, "")
        except Exception:
            pass
    run_id = _widget(dbutils, "run_id")
    cfg = Config(
        project_id=_widget(dbutils, "project_id"),
        branch_id=_widget(dbutils, "branch_id"),
        catalog=_widget(dbutils, "target_catalog_name") or "main",
        schema=_widget(dbutils, "target_schema_name") or "lb_health_check",
        run_id="" if "{{" in run_id else run_id,
    )
    cfg.validate()
    return cfg


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def hour_window(now: datetime, lookback_hours: int = LOOKBACK_HOURS) -> tuple[datetime, datetime]:
    """[start, end) of complete UTC hours to (re)compute: end = start of the current hour."""
    end = now.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0)
    return end - timedelta(hours=lookback_hours), end


def complete_days(now: datetime, n: int = RECOMPUTE_DAYS) -> list:
    """The last n complete UTC days (dates), oldest first. The current partial day is never included."""
    today = now.astimezone(timezone.utc).date()
    return [today - timedelta(days=i) for i in range(n, 0, -1)]


def ts_literal(dt: datetime) -> str:
    return "TIMESTAMP'" + dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S") + "'"


def sql_str_list(values) -> str:
    vals = [str(v).replace("'", "''") for v in values]
    return ", ".join(f"'{v}'" for v in vals) if vals else "NULL"
