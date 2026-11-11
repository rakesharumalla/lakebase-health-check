# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Resolve scope
# MAGIC Projects → branches → endpoints for the configured `project_name` (or `ALL`).
# MAGIC Writes task values `endpoints` and `project_ids` used by every downstream task.
# MAGIC Also checks that the target tables/views exist and creates only what is missing: the full DDL
# MAGIC runs on every `bundle deploy` (predeploy bootstrap), so on a normal run this is a single check.

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

from lb_health import config, ddl, lakebase_api  # noqa: E402

cfg = config.load(dbutils)
print(f"project_name={cfg.project_name} frequency={cfg.frequency} cron={cfg.schedule_cron} "
      f"target={cfg.catalog}.{cfg.schema}")

warning = config.schedule_warning(cfg.frequency, cfg.schedule_cron)
if warning:
    print(f"WARNING: {warning}")

# COMMAND ----------

w = WorkspaceClient()
project_ids = lakebase_api.resolve_project_ids(w, cfg.project_name)
if not project_ids:
    raise RuntimeError(f"No Lakebase projects resolved from project_name={cfg.project_name!r}")

endpoints, errors = lakebase_api.list_endpoints(w, project_ids)
for e in errors:
    print(f"WARNING: {e}")

resolved_projects = sorted({e["project_id"] for e in endpoints})
if not resolved_projects:
    raise RuntimeError(
        f"Zero projects resolved (requested {project_ids}). Check the project IDs and that the run "
        f"identity has CAN USE on them. Errors: {errors}")

missing = sorted(set(project_ids) - set(resolved_projects))
if missing:
    print(f"WARNING: projects with no reachable endpoints: {missing}")

for e in endpoints:
    print(f"  {e['project_id']}/{e['branch_id']}/{e['endpoint_id']}  type={e['endpoint_type']} "
          f"state={e['state']} uid={e['endpoint_uid']}")
print(f"{len(endpoints)} endpoints in {len(resolved_projects)} projects")

# COMMAND ----------

# Tables / views: created by the predeploy bootstrap; recreate here only if something is missing
# (e.g. a manual run with a different target_catalog_name / target_schema_name, or a dropped object).
spark.conf.set("spark.sql.session.timeZone", "UTC")
created = ddl.ensure(spark, cfg)
print(f"created missing objects in {cfg.catalog}.{cfg.schema}: {created}" if created
      else f"all tables and views present in {cfg.catalog}.{cfg.schema}")

dbutils.jobs.taskValues.set(key="endpoints", value=json.dumps(endpoints))
dbutils.jobs.taskValues.set(key="project_ids", value=json.dumps(resolved_projects))
dbutils.jobs.taskValues.set(key="identity", value=lakebase_api.current_identity(w))
