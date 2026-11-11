"""Delta writes: DDL, MERGE helpers."""
from __future__ import annotations

from .config import Config
from .metrics import ROW_COLUMNS

NATURAL_KEY_EQ = """
  t.ts = s.ts AND t.grain = s.grain AND t.project_id = s.project_id AND t.branch_id = s.branch_id
  AND t.endpoint_id = s.endpoint_id AND t.compute_id <=> s.compute_id AND t.datname <=> s.datname
  AND t.object_type = s.object_type AND t.object_name = s.object_name AND t.metric = s.metric
"""

def metrics_ddl(cfg: Config) -> str:
    return f"""
CREATE TABLE IF NOT EXISTS {cfg.metrics} (
  ts            TIMESTAMP   NOT NULL COMMENT 'Hour start (grain=hour), snapshot time (grain=snapshot), or day start UTC (grain=day)',
  grain         STRING      NOT NULL COMMENT 'hour | snapshot | day',
  project_id    STRING      NOT NULL COMMENT 'Lakebase project ID (projects/<project_id>)',
  branch_id     STRING      NOT NULL COMMENT 'Lakebase branch ID',
  endpoint_id   STRING      NOT NULL COMMENT 'Lakebase endpoint ID',
  compute_id    STRING      COMMENT 'NULL for grain=hour (summed across computes) and grain=day',
  datname       STRING      COMMENT 'Postgres database; NULL for endpoint-level metrics',
  object_type   STRING      NOT NULL COMMENT 'endpoint | table | index | query',
  object_name   STRING      NOT NULL COMMENT 'endpoint_id, schema.table, schema.index, or queryid',
  metric        STRING      NOT NULL COMMENT 'see the metric catalog in README.md',
  value         DOUBLE,
  detail        VARIANT     COMMENT 'Semi-structured supporting fields; query with detail:field',
  run_id        STRING      COMMENT 'Databricks job run id that wrote the row',
  loaded_at     TIMESTAMP   NOT NULL
)
USING DELTA
CLUSTER BY (grain, project_id, metric, ts)   -- liquid clustering allows max 4 columns; endpoint_id ('primary') is the least selective
COMMENT 'Lakebase Health Check — narrow metric store (hour / snapshot / day grains)'
TBLPROPERTIES (delta.enableChangeDataFeed = false, delta.autoOptimize.optimizeWrite = true)
"""


def thresholds_ddl(cfg: Config) -> str:
    return f"""
CREATE TABLE IF NOT EXISTS {cfg.thresholds} (
  threshold_id   STRING  NOT NULL COMMENT 'Trend ID, T1..T11',
  metric         STRING  NOT NULL COMMENT 'Daily metric in lb_health_metrics (grain = day) the reference applies to',
  value          DOUBLE  COMMENT 'Reference value; NULL unless reference_type = static',
  reference_type STRING  NOT NULL COMMENT 'static (compare with value) | prior_period_avg (compare with the prior period average) | none (informational)',
  direction      STRING  NOT NULL COMMENT 'above_is_bad | below_is_bad'
) USING DELTA
COMMENT 'Lakebase Health Check — reference value per trend (edit freely; the seed never overwrites)'
"""


def _schema():
    from pyspark.sql.types import (DoubleType, StringType, StructField, StructType, TimestampType)
    types = {"ts": TimestampType(), "value": DoubleType(), "loaded_at": TimestampType()}
    return StructType([StructField(c, types.get(c, StringType()), True) for c in ROW_COLUMNS])


def merge_rows(spark, cfg: Config, rows: list[tuple], view_name: str = "lbh_rows") -> int:
    """MERGE python rows (metrics.make_row tuples) into lb_health_metrics. Returns row count."""
    if not rows:
        return 0
    df = spark.createDataFrame(rows, schema=_schema())
    df.createOrReplaceTempView(view_name)
    merge_from_query(spark, cfg, f"SELECT * FROM {view_name}")
    return len(rows)


def merge_from_query(spark, cfg: Config, source_sql: str) -> None:
    """MERGE the result of source_sql (same columns as ROW_COLUMNS, detail as JSON string or VARIANT)."""
    spark.sql(f"""
MERGE INTO {cfg.metrics} AS t
USING (
  SELECT ts, grain, project_id, branch_id, endpoint_id, compute_id, datname, object_type, object_name,
         metric, value,
         CASE WHEN detail IS NULL THEN NULL ELSE parse_json(CAST(detail AS STRING)) END AS detail,
         run_id, loaded_at
  FROM ({source_sql})
) AS s
ON {NATURAL_KEY_EQ}
WHEN MATCHED THEN UPDATE SET t.value = s.value, t.detail = s.detail, t.run_id = s.run_id,
                             t.loaded_at = s.loaded_at
WHEN NOT MATCHED THEN INSERT *
""")
