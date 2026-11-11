"""All DDL for the bundle: schema, tables, threshold seed, consumption views.

scripts/bootstrap_tables.py (the predeploy hook) runs `statements()` on the SQL warehouse on every
`bundle deploy`, so the tables and views exist before the Genie space is created.
"""
from __future__ import annotations

from .config import METRICS_TABLE, THRESHOLDS_TABLE, Config
from .metrics import THRESHOLD_SEED
from .store import metrics_ddl, thresholds_ddl
from .views import view_statements


def _sql_literal(v) -> str:
    if v is None:
        return "NULL"
    if isinstance(v, (int, float)):
        return repr(float(v))
    return "'" + str(v).replace("\\", "\\\\").replace("'", "\\'") + "'"


def seed_statement(cfg: Config) -> str:
    """Insert threshold rows that are missing; never overwrites customer edits."""
    values = ",\n".join("(" + ", ".join(_sql_literal(x) for x in row) + ")" for row in THRESHOLD_SEED)
    return f"""
MERGE INTO {cfg.thresholds} t
USING (SELECT * FROM VALUES {values} AS s(threshold_id, metric, value, reference_type, direction)) s
ON t.threshold_id = s.threshold_id
WHEN NOT MATCHED THEN INSERT (threshold_id, metric, value, reference_type, direction)
VALUES (s.threshold_id, s.metric, CAST(s.value AS DOUBLE), s.reference_type, s.direction)"""


def statements(cfg: Config) -> list[str]:
    """Full, idempotent DDL in dependency order."""
    return ([f"CREATE SCHEMA IF NOT EXISTS {cfg.schema_fq} COMMENT 'Lakebase Health Check'",
             metrics_ddl(cfg), thresholds_ddl(cfg), seed_statement(cfg)]
            + list(view_statements(cfg).values()))


def expected_objects() -> set[str]:
    return {METRICS_TABLE, THRESHOLDS_TABLE} | set(view_statements(_dummy_cfg()).keys())


def _dummy_cfg() -> Config:
    return Config(project_id="p", branch_id="b", catalog="c", schema="s")
