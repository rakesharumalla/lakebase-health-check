# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Resolve scope
# MAGIC Lists the endpoints (primary and read replicas) of the configured `project_id` / `branch_id` and
# MAGIC writes them as the `endpoints` task value used by every downstream task. Fails fast when the project
# MAGIC or branch is missing or has no endpoints. Tables and views are created by the predeploy bootstrap.

# COMMAND ----------

import json
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

from databricks.sdk import WorkspaceClient  # noqa: E402

from lb_health import config, lakebase_api  # noqa: E402

cfg = config.load(dbutils)
print(f"project_id={cfg.project_id} branch_id={cfg.branch_id} target={cfg.catalog}.{cfg.schema}")

# COMMAND ----------

w = WorkspaceClient()
try:
    endpoints = lakebase_api.list_branch_endpoints(w, cfg.project_id, cfg.branch_id)
except Exception as e:
    raise RuntimeError(
        f"Could not read projects/{cfg.project_id}/branches/{cfg.branch_id}: {e}. Check the IDs (the last part of "
        f"projects/PROJECT/branches/BRANCH) and that the run identity has CAN USE on the project.") from e
if not endpoints:
    raise RuntimeError(f"projects/{cfg.project_id}/branches/{cfg.branch_id} has no endpoints")

for e in endpoints:
    print(f"  {e['project_id']}/{e['branch_id']}/{e['endpoint_id']}  type={e['endpoint_type']} "
          f"state={e['state']} uid={e['endpoint_uid']}")

dbutils.jobs.taskValues.set(key="endpoints", value=json.dumps(endpoints))
