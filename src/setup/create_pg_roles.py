# Databricks notebook source
# MAGIC %md
# MAGIC # OPTIONAL one-time setup · Postgres role for the run identity
# MAGIC Run interactively **as a project owner** (not part of the scheduled job). It creates an OAuth Postgres
# MAGIC role for the run identity on the branch through the Lakebase API, if the role does not exist yet, and
# MAGIC prints the SQL that gives the role read access for the bloat estimates.
# MAGIC
# MAGIC The snapshot itself needs no grant: `pg_stat_user_tables` / `pg_stat_user_indexes` are readable by any
# MAGIC role (verified with a service principal). Bloat reads `pg_stats`, which only shows tables the role may
# MAGIC `SELECT`. `GRANT pg_read_all_stats` is not needed and fails in Lakebase (the owner has no ADMIN option
# MAGIC on that role).

# COMMAND ----------

dbutils.widgets.text("project_id", "", "Project ID")
dbutils.widgets.text("branch_id", "", "Branch ID")
dbutils.widgets.text("run_identity", "", "SP application ID or user e-mail the job runs as")
dbutils.widgets.dropdown("identity_type", "SERVICE_PRINCIPAL", ["SERVICE_PRINCIPAL", "USER"], "Identity type")
dbutils.widgets.text("role_id", "lb-health", "Lakebase role resource ID")
dbutils.widgets.text("schemas", "public", "Schemas to monitor for bloat (comma-separated)")
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

from databricks.sdk import WorkspaceClient  # noqa: E402

from lb_health import lakebase_api  # noqa: E402

project_id = dbutils.widgets.get("project_id").strip()
branch_id = dbutils.widgets.get("branch_id").strip()
identity = dbutils.widgets.get("run_identity").strip()
identity_type = dbutils.widgets.get("identity_type")
role_id = dbutils.widgets.get("role_id").strip()
schemas = [s.strip() for s in dbutils.widgets.get("schemas").split(",") if s.strip()]
dry_run = dbutils.widgets.get("dry_run") == "true"
if not (project_id and branch_id and identity):
    raise ValueError("project_id, branch_id and run_identity are required")

w = WorkspaceClient()
branch = f"projects/{project_id}/branches/{branch_id}"
lakebase_api.list_branch_endpoints(w, project_id, branch_id)  # fails early if the branch is not visible

# COMMAND ----------

if identity in lakebase_api.list_role_names(w, branch):
    print(f"{branch}: role for {identity} exists")
else:
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

quoted = '"' + identity.replace('"', '""') + '"'
print("Run as the owner of the monitored tables, in each database, so bloat can be estimated:\n")
for s in schemas:
    print(f'GRANT USAGE ON SCHEMA "{s}" TO {quoted};')
    print(f'GRANT SELECT ON ALL TABLES IN SCHEMA "{s}" TO {quoted};')
    print(f'ALTER DEFAULT PRIVILEGES IN SCHEMA "{s}" GRANT SELECT ON TABLES TO {quoted};')
