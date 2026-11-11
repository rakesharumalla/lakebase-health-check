# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Retention
# MAGIC Deletes every row older than 90 days. File compaction and VACUUM are left to predictive optimization.

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

# COMMAND ----------

spark.sql(f"""
DELETE FROM {cfg.metrics}
WHERE ts < current_timestamp() - INTERVAL {config.RETENTION_DAYS} DAYS
""")
print(f"retention applied: rows older than {config.RETENTION_DAYS} days deleted")
