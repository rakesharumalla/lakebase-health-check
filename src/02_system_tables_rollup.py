# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · System tables rollup (metrics 1–3, grain = hour)
# MAGIC Rolls `system.lakebase` gauges/counters/ASH up to the last 48 complete hours per endpoint and MERGEs
# MAGIC them. Every run re-reads the same 48 h window, so late telemetry and missed runs within 48 h are filled in.

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

now = config.utc_now()
start, end = config.hour_window(now)
print(f"rollup window [{start}, {end}) endpoints={len(endpoints)}")

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
        # one project per run, so one Postgres version
        "__IIT_STATES__": ", ".join(str(s) for s in metrics.ash_idle_in_txn_states(endpoints[0].get("pg_version"))),
        "__LONG_MS__": str(config.LONG_XACT_SECONDS * 1000),
        "__VERY_LONG_MS__": str(config.VERY_LONG_XACT_SECONDS * 1000),
        "__RUN_ID__": cfg.run_id.replace("'", ""),
    }
    for k, v in subs.items():
        sql = sql.replace(k, v)

    hourly = spark.sql(sql)
    hourly.createOrReplaceTempView("lbh_hourly_rows")
    store.merge_from_query(spark, cfg, "SELECT * FROM lbh_hourly_rows")
    print("hourly rows merged")
