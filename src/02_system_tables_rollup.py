# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · System tables rollup (metrics 1–3, grain = hour)
# MAGIC Rolls `system.lakebase` gauges/counters/ASH up to complete hours per endpoint over the
# MAGIC lookback window (48 h daily, 3 h hourly), MERGEs them, then writes one `rollup_ok` coverage
# MAGIC row per endpoint in scope.

# COMMAND ----------

import json
import os
import sys
from datetime import timedelta


def _add_src_to_path():
    try:
        nb = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
        sys.path.insert(0, os.path.dirname("/Workspace" + nb if not nb.startswith("/Workspace") else nb))
    except Exception:
        pass
    sys.path.insert(0, os.getcwd())


_add_src_to_path()

from lb_health import config, metrics, pg, store  # noqa: E402

cfg = config.load(dbutils)
spark.conf.set("spark.sql.session.timeZone", "UTC")
endpoints = json.loads(dbutils.jobs.taskValues.get(taskKey="resolve_scope", key="endpoints", debugValue="[]"))
cfg_values = config.read_config_values(spark, cfg)

now = config.utc_now()
start, end = config.hour_window(now, cfg.lookback_hours)
print(f"rollup window [{start}, {end}) lookback={cfg.lookback_hours}h endpoints={len(endpoints)}")

# COMMAND ----------

from pyspark.sql.types import StringType, StructField, StructType  # noqa: E402

scope_cols = ["project_id", "branch_id", "endpoint_id", "project_uid", "endpoint_uid", "endpoint_type", "state"]
spark.createDataFrame(
    [tuple(e.get(c) for c in scope_cols) for e in endpoints],
    StructType([StructField(c, StringType()) for c in scope_cols]),
).createOrReplaceTempView("lbh_scope")

project_uids = sorted({e["project_uid"] for e in endpoints if e.get("project_uid")})

# COMMAND ----------

if endpoints:
    sql = pg.load_sql("system_hourly_rollup.sql")
    subs = {
        "__START__": config.ts_literal(start),
        "__END__": config.ts_literal(end),
        "__CF_START__": config.ts_literal(start - timedelta(hours=1)),
        "__PROJECT_UIDS__": config.sql_str_list(project_uids),
        "__MAX_INTERVAL_S__": str(metrics.MAX_COUNTER_INTERVAL_S),
        "__CLAMP__": str(metrics.UTIL_CLAMP_MAX),
        "__IIT_STATES__": ", ".join(str(s) for s in metrics.ASH_IDLE_IN_TXN_STATES),
        "__LONG_MS__": str(int(cfg_values["cfg_long_xact_seconds"] * 1000)),
        "__VERY_LONG_MS__": str(int(cfg_values["cfg_very_long_xact_seconds"] * 1000)),
        "__RUN_ID__": cfg.run_id.replace("'", ""),
    }
    for k, v in subs.items():
        sql = sql.replace(k, v)

    hourly = spark.sql(sql)
    hourly.createOrReplaceTempView("lbh_hourly_rows")
    store.merge_from_query(spark, cfg, "SELECT * FROM lbh_hourly_rows")
    print("hourly rows merged")

# COMMAND ----------

# Coverage: one rollup_ok row per endpoint in scope (also for endpoints with zero telemetry rows).
written = {
    (r["project_id"], r["branch_id"], r["endpoint_id"]): (r["hours"], r["max_source_ts"])
    for r in spark.sql(f"""
        SELECT m.project_id, m.branch_id, m.endpoint_id, count(DISTINCT m.ts) AS hours,
               max(g.max_ts) AS max_source_ts
        FROM {cfg.metrics} m
        LEFT JOIN (
          SELECT s.project_id, s.branch_id, s.endpoint_id, max(c.ts) AS max_ts
          FROM system.lakebase.compute_gauges c JOIN lbh_scope s ON c.endpoint_id = s.endpoint_uid
          WHERE c.ts >= {config.ts_literal(start)} AND c.project_id IN ({config.sql_str_list(project_uids)})
          GROUP BY ALL
        ) g USING (project_id, branch_id, endpoint_id)
        WHERE m.grain = 'hour' AND m.metric <> 'rollup_ok'
          AND m.ts >= {config.ts_literal(start)} AND m.ts < {config.ts_literal(end)}
          AND m.project_id IN ({config.sql_str_list(sorted({e['project_id'] for e in endpoints}))})
        GROUP BY ALL
    """).collect()
} if endpoints else {}

run_hour = metrics.hour_bucket(now)
rows = []
for e in endpoints:
    hours, max_src = written.get((e["project_id"], e["branch_id"], e["endpoint_id"]), (0, None))
    rows.append(metrics.make_row(
        ts=run_hour, grain="hour", endpoint=e, metric="rollup_ok", value=hours,
        detail={"endpoint_type": e["endpoint_type"], "state": e["state"],
                "lookback_hours": cfg.lookback_hours, "max_source_ts": max_src,
                "window_start": start, "window_end": end, "endpoint_uid": e["endpoint_uid"]},
        run_id=cfg.run_id))
    print(f"  {e['project_id']}/{e['branch_id']}/{e['endpoint_id']}: {hours} complete hours, last telemetry {max_src}")
store.merge_rows(spark, cfg, rows, "lbh_rollup_ok")
