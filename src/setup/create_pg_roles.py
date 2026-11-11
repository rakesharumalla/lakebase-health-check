# Databricks notebook source
# MAGIC %md
# MAGIC # OPTIONAL one-time setup · Postgres roles for the run identity
# MAGIC Run interactively **as a project owner** (not part of the scheduled job). For every branch of
# MAGIC every project in scope it:
# MAGIC 1. creates an OAuth Postgres role for the run identity (`identity_type` SERVICE_PRINCIPAL or USER)
# MAGIC    through the Lakebase API, if the role does not exist yet;
# MAGIC 2. connects as the owner to each running read-write endpoint and runs
# MAGIC    `GRANT pg_read_all_stats` (and optionally `pg_read_all_data`, see README §Prerequisites).
# MAGIC
# MAGIC Endpoints that are not running are reported and skipped (they are not woken up); re-run the
# MAGIC notebook later, or grant manually.

# COMMAND ----------

# MAGIC %pip install -q "psycopg[binary]>=3.1"

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("project_name", "lb-health-check", "Project IDs (comma-separated) or ALL")
dbutils.widgets.text("run_identity", "", "SP application ID or user e-mail the job runs as")
dbutils.widgets.dropdown("identity_type", "SERVICE_PRINCIPAL", ["SERVICE_PRINCIPAL", "USER"], "Identity type")
dbutils.widgets.text("role_id", "lb-health", "Lakebase role resource ID")
dbutils.widgets.dropdown("grant_read_all_data", "false", ["false", "true"], "Also GRANT pg_read_all_data")
dbutils.widgets.dropdown("dry_run", "true", ["true", "false"], "Dry run")

# COMMAND ----------

import os
import sys

try:
    nb = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    sys.path.insert(0, os.path.dirname(os.path.dirname("/Workspace" + nb if not nb.startswith("/Workspace") else nb)))
except Exception:
    sys.path.insert(0, os.path.dirname(os.getcwd()))

import time  # noqa: E402

import psycopg  # noqa: E402
from databricks.sdk import WorkspaceClient  # noqa: E402

from lb_health import lakebase_api  # noqa: E402

project_name = dbutils.widgets.get("project_name")
identity = dbutils.widgets.get("run_identity").strip()
identity_type = dbutils.widgets.get("identity_type")
role_id = dbutils.widgets.get("role_id").strip()
grant_all_data = dbutils.widgets.get("grant_read_all_data") == "true"
dry_run = dbutils.widgets.get("dry_run") == "true"
if not identity:
    raise ValueError("run_identity is required")

w = WorkspaceClient()
owner = lakebase_api.current_identity(w)
projects = lakebase_api.resolve_project_ids(w, project_name)
endpoints, errors = lakebase_api.list_endpoints(w, projects)
for e in errors:
    print("WARNING:", e)

# COMMAND ----------

branches = sorted({f"projects/{e['project_id']}/branches/{e['branch_id']}" for e in endpoints})
for branch in branches:
    existing = lakebase_api.list_role_names(w, branch)
    if identity in existing:
        print(f"{branch}: role for {identity} exists")
        continue
    print(f"{branch}: creating role {role_id} for {identity} ({identity_type})" + (" [dry run]" if dry_run else ""))
    if not dry_run:
        op = lakebase_api.create_role(w, branch, role_id, identity, identity_type)
        for _ in range(60):  # long-running operation
            if op.get("done"):
                break
            time.sleep(5)
            op = w.api_client.do("GET", f"/api/2.0/postgres/{op['name']}")
        if op.get("error"):
            raise RuntimeError(f"{branch}: create role failed: {op['error']}")

# COMMAND ----------

grants = ["pg_read_all_stats"] + (["pg_read_all_data"] if grant_all_data else [])
quoted = '"' + identity.replace('"', '""') + '"'
for e in endpoints:
    if e["endpoint_type"] != "read_write":
        continue
    if not lakebase_api.is_running(e):
        print(f"SKIP {e['endpoint_name']}: state={e['state']} (not woken up)")
        continue
    for g in grants:
        stmt = f"GRANT {g} TO {quoted}"
        print(f"{e['endpoint_name']}: {stmt}" + (" [dry run]" if dry_run else ""))
        if dry_run:
            continue
        token = lakebase_api.database_password(w, e["endpoint_name"])
        with psycopg.connect(host=e["host"], dbname="databricks_postgres", user=owner, password=token,
                             sslmode="require", autocommit=True) as conn:
            conn.execute(stmt)
