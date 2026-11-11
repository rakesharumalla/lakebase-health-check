# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Retention
# MAGIC Deletes hour/snapshot rows older than `retention_days` and day rows older than
# MAGIC `trend_retention_days`; VACUUM when the job's last VACUUM is over 20 h old and OPTIMIZE when its last
# MAGIC OPTIMIZE is over 6 days old (recorded as table properties), so any schedule works.

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

from datetime import datetime, timedelta  # noqa: E402

from lb_health import config  # noqa: E402
from lb_health.config import maintenance_due  # noqa: E402

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

# Maintenance cadence comes from the job's own record, not the schedule: VACUUM about daily, OPTIMIZE about
# weekly. The last run times are kept as table properties because VACUUM is not always written to the table
# history, and automatic compaction adds OPTIMIZE entries that are not the job's clustering OPTIMIZE.
props = {r["key"]: r["value"] for r in spark.sql(f"SHOW TBLPROPERTIES {cfg.metrics}").collect()}


def _last(key):
    v = props.get(key)
    try:
        return datetime.fromisoformat(v) if v else None
    except ValueError:
        return None


for op, key, interval in (("OPTIMIZE", "lb_health.last_optimize", timedelta(days=6)),
                          ("VACUUM", "lb_health.last_vacuum", timedelta(hours=20))):
    last = _last(key)
    if maintenance_due(last, now, interval):
        spark.sql(f"{op} {cfg.metrics}")
        spark.sql(f"ALTER TABLE {cfg.metrics} SET TBLPROPERTIES ('{key}' = '{config.utc_now().isoformat()}')")
        print(f"{op} done (previous: {last or 'never'})")
    else:
        print(f"{op} skipped: last run {last}, next after {last + interval}")
