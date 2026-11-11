#!/usr/bin/env python3
"""Pre-deploy bootstrap: create the schema, tables, threshold seeds and consumption views.

Runs as the bundle's `experimental.scripts.predeploy` hook so the Genie space (whose tables must
already exist when it is created) deploys cleanly on the very first `bundle deploy`. It renders the
same DDL the job uses (src/lb_health/ddl.py) and executes it on the SQL warehouse
through the Databricks CLI, which inherits the bundle's authentication. Idempotent.

Usage: python3 scripts/bootstrap_tables.py --catalog C --schema S --warehouse WAREHOUSE_ID [--profile P]
"""
import argparse
import json
import os
import subprocess
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "src"))

from lb_health import config, ddl  # noqa: E402


PROFILE = []


def _cli(args):
    r = subprocess.run(["databricks", *args, *PROFILE], capture_output=True, text=True)
    if r.returncode != 0:
        raise RuntimeError(f"databricks {' '.join(args[:3])} failed: {r.stderr.strip() or r.stdout.strip()}")
    return json.loads(r.stdout)


def execute(warehouse_id, sql, timeout_s=600):
    body = {"warehouse_id": warehouse_id, "statement": sql, "wait_timeout": "50s",
            "on_wait_timeout": "CONTINUE", "disposition": "INLINE", "format": "JSON_ARRAY"}
    resp = _cli(["api", "post", "/api/2.0/sql/statements", "--json", json.dumps(body)])
    deadline = time.time() + timeout_s
    while resp.get("status", {}).get("state") in ("PENDING", "RUNNING"):
        if time.time() > deadline:
            raise TimeoutError(f"statement {resp.get('statement_id')} still running after {timeout_s}s")
        time.sleep(3)
        resp = _cli(["api", "get", f"/api/2.0/sql/statements/{resp['statement_id']}"])
    state = resp.get("status", {}).get("state")
    if state != "SUCCEEDED":
        err = resp.get("status", {}).get("error", {}).get("message", resp)
        raise RuntimeError(f"{state}: {err}\n--- statement ---\n{sql.strip()[:400]}")


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--catalog", required=True)
    ap.add_argument("--schema", required=True)
    ap.add_argument("--warehouse", required=True)
    ap.add_argument("--profile", nargs="?", const="", default="", help="Databricks CLI profile (the bundle passes workspace.profile)")
    ap.add_argument("--print-only", action="store_true", help="print the SQL instead of executing it")
    a = ap.parse_args()
    if a.profile:
        PROFILE[:] = ["--profile", a.profile]

    cfg = config.Config(project_id="ALL", retention_days=30,
                        trend_retention_days=90, catalog=a.catalog, schema=a.schema, pg_lock_timeout_ms=1000,
                        pg_statement_timeout_ms=30000, pg_bloat_statement_timeout_ms=120000,
                        snapshot_all_databases=True, idle_in_txn_source="ash")
    cfg.validate()
    stmts = ddl.statements(cfg)
    if a.print_only:
        print(";\n".join(s.strip() for s in stmts) + ";")
        return
    print(f"lakebase-health-check bootstrap: {len(stmts)} statements on {a.catalog}.{a.schema} "
          f"(warehouse {a.warehouse})")
    for s in stmts:
        execute(a.warehouse, s)
    print("lakebase-health-check bootstrap: tables and views ready")


if __name__ == "__main__":
    main()
