# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Retention
# MAGIC Deletes hour/snapshot rows older than `retention_days` and day rows older than
# MAGIC `trend_retention_days`; OPTIMIZE weekly (Sunday UTC) and VACUUM once a day.

# COMMAND ----------

import os
import sys


def _add_src_to_path():
    try:
        nb = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
        sys.path.insert(0, os.path.dirname("/Workspace" + nb if not nb.startswith("/Workspace") else nb))
    except Exception:
        pass
    sys.path.insert(0, os.getcwd())


_add_src_to_path()

from lb_health import config  # noqa: E402

cfg = config.load(dbutils)
spark.conf.set("spark.sql.session.timeZone", "UTC")
now = config.utc_now()

# COMMAND ----------

spark.sql(f"""
DELETE FROM {cfg.metrics}
WHERE grain IN ('hour', 'snapshot') AND ts < current_timestamp() - INTERVAL {int(cfg.retention_days)} DAYS
""")
spark.sql(f"""
DELETE FROM {cfg.metrics}
WHERE grain = 'day' AND ts < current_timestamp() - INTERVAL {int(cfg.trend_retention_days)} DAYS
""")
print(f"retention applied: hour/snapshot {cfg.retention_days}d, day {cfg.trend_retention_days}d")

# COMMAND ----------

# Maintenance runs once per day: every run in daily mode, the 02:00 UTC run in hourly mode.
maintenance_run = cfg.frequency == "daily" or now.hour == 2
if maintenance_run and now.isoweekday() == 7:  # Sunday (Spark dayofweek = 1)
    spark.sql(f"OPTIMIZE {cfg.metrics}")
    print("OPTIMIZE done")
if maintenance_run:
    spark.sql(f"VACUUM {cfg.metrics}")
    print("VACUUM done (default retention)")
