"""All DDL for the bundle: schema, tables, threshold seeds, consumption views.

Used in two places:
  * scripts/bootstrap_tables.py (predeploy hook) runs `statements()` on the SQL warehouse on every
    `bundle deploy`, which is when definitions change;
  * the job's resolve_scope task calls `ensure()`, which only runs the DDL when an object is missing
    (e.g. a manual run with a different target catalog/schema, or something was dropped).
"""
from __future__ import annotations

from .config import METRICS_TABLE, THRESHOLDS_TABLE, Config
from .metrics import THRESHOLD_SEED
from .store import FORWARD_COLUMNS, metrics_ddl, thresholds_ddl
from .views import view_statements


def _sql_literal(v) -> str:
    if v is None:
        return "NULL"
    if isinstance(v, (int, float)):
        return repr(float(v))
    return "'" + str(v).replace("\\", "\\\\").replace("'", "\\'") + "'"


def seed_statement(cfg: Config) -> str:
    """Insert threshold/config rows that are missing; never overwrites customer edits."""
    values = ",\n".join("(" + ", ".join(_sql_literal(x) for x in row) + ")" for row in THRESHOLD_SEED)
    return f"""
MERGE INTO {cfg.thresholds} t
USING (SELECT * FROM VALUES {values}
       AS s(threshold_id, kind, metric, value, reference_type, direction, label, description)) s
ON t.threshold_id = s.threshold_id
WHEN NOT MATCHED THEN INSERT (threshold_id, kind, metric, value, reference_type, direction, label,
                              description, updated_at)
VALUES (s.threshold_id, s.kind, s.metric, CAST(s.value AS DOUBLE), s.reference_type, s.direction, s.label,
        s.description, current_timestamp())"""


def migration_statements(cfg: Config) -> list[str]:
    """Idempotent data migrations for existing installations (no-ops on a fresh install)."""
    return [
        # Cache hit rate became an overall buffer hit rate: daily column t6_lfc_hit_rate -> t6_cache_hit_rate.
        # Keeps any customer-tuned value; old day rows are recomputed by the next run / backfill.
        f"""UPDATE {cfg.thresholds}
SET metric = 't6_cache_hit_rate',
    description = '(shared buffer hits + local file cache hits) / (shared buffer hits + shared buffer reads)',
    updated_at = current_timestamp()
WHERE threshold_id = 'T6' AND metric = 't6_lfc_hit_rate'""",
        f"DELETE FROM {cfg.metrics} WHERE grain = 'day' AND metric = 't6_lfc_hit_rate'",
    ]


def statements(cfg: Config) -> list[str]:
    """Full, idempotent DDL in dependency order (forward column adds are handled by ensure())."""
    return ([f"CREATE SCHEMA IF NOT EXISTS {cfg.schema_fq} COMMENT 'Lakebase Health Check'",
             metrics_ddl(cfg), thresholds_ddl(cfg), seed_statement(cfg)]
            + migration_statements(cfg)
            + list(view_statements(cfg).values()))


def expected_objects() -> set[str]:
    return {METRICS_TABLE, THRESHOLDS_TABLE} | set(view_statements(_dummy_cfg()).keys())


def _dummy_cfg() -> Config:
    return Config(project_id="ALL", retention_days=30,
                  trend_retention_days=90, catalog="c", schema="s", pg_lock_timeout_ms=1000,
                  pg_statement_timeout_ms=30000, pg_bloat_statement_timeout_ms=120000,
                  snapshot_all_databases=True, idle_in_txn_source="ash")


def missing(spark, cfg: Config) -> list[str]:
    """Names of expected tables/views (and 'seed rows', 'columns') that are absent. One or two queries."""
    try:
        present = {r["tableName"].lower() for r in spark.sql(f"SHOW TABLES IN {cfg.schema_fq}").collect()}
    except Exception:  # schema does not exist
        return sorted(expected_objects())
    gaps = sorted(o for o in expected_objects() if o.lower() not in present)
    if THRESHOLDS_TABLE in present:
        have = {r["threshold_id"] for r in spark.sql(f"SELECT threshold_id FROM {cfg.thresholds}").collect()}
        if {row[0] for row in THRESHOLD_SEED} - have:
            gaps.append("seed rows")
    if FORWARD_COLUMNS and METRICS_TABLE in present:
        cols = {f.name for f in spark.table(cfg.metrics).schema.fields}
        if any(c not in cols for c, _, _ in FORWARD_COLUMNS):
            gaps.append("columns")
    return gaps


def ensure(spark, cfg: Config) -> list[str]:
    """Create whatever is missing; returns what was missing (empty list = nothing to do)."""
    gaps = missing(spark, cfg)
    if not gaps:
        return []
    for stmt in statements(cfg):
        spark.sql(stmt)
    cols = {f.name for f in spark.table(cfg.metrics).schema.fields}
    for col, typ, comment in FORWARD_COLUMNS:
        if col not in cols:
            spark.sql(f"ALTER TABLE {cfg.metrics} ADD COLUMNS ({col} {typ} COMMENT '{comment}')")
    return gaps
