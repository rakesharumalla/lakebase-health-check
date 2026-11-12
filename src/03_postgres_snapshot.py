# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Postgres snapshot (metrics 4–6, grain = snapshot)
# MAGIC For each running endpoint of the branch (primary and read replicas): connect read-only over OAuth with
# MAGIC lock/statement timeouts, snapshot `pg_stat_user_tables`, indexes and the ioguix bloat estimates in every
# MAGIC database, compute counter deltas and MERGE. Endpoints that are not running are skipped so the job never
# MAGIC wakes a scaled-to-zero compute.
# MAGIC
# MAGIC Rows from the endpoints that worked are always saved; the task then fails if any running endpoint
# MAGIC had an error, so the job-failure e-mail reports it.

# COMMAND ----------

# MAGIC %pip install -q "psycopg[binary]>=3.1"

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import json
import os
import sys
import time
import traceback


def _add_src_to_path():
    try:
        nb = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
        sys.path.insert(0, os.path.dirname("/Workspace" + nb if not nb.startswith("/Workspace") else nb))
    except Exception:
        pass
    sys.path.insert(0, os.getcwd())


_add_src_to_path()

from databricks.sdk import WorkspaceClient  # noqa: E402

from lb_health import config, lakebase_api, metrics, pg, store  # noqa: E402

cfg = config.load(dbutils)
spark.conf.set("spark.sql.session.timeZone", "UTC")
endpoints = json.loads(dbutils.jobs.taskValues.get(taskKey="resolve_scope", key="endpoints", debugValue="[]"))

now = config.utc_now()
# One snapshot time per run, identical across endpoints. Aligned to the run hour so a rerun
# within the same hour overwrites (idempotent) instead of adding a second snapshot.
snap_ts = metrics.hour_bucket(now)
project_list = config.sql_str_list(sorted({e["project_id"] for e in endpoints}))
print(f"snapshot ts={snap_ts} endpoints={len(endpoints)}")

# COMMAND ----------

# Previous cumulative counters per object (latest snapshot strictly before this one), with the compute that
# reported them: a different compute now means the endpoint restarted and its counters started from zero.
prev = {}
compute_fallback = {}
if endpoints:
    for r in spark.sql(f"""
        SELECT project_id, branch_id, endpoint_id, datname, object_type, object_name, metric,
               max_by(value, ts) AS value, max_by(compute_id, ts) AS compute_id
        FROM {cfg.metrics}
        WHERE grain = 'snapshot' AND metric IN ('seq_scan', 'seq_tup_read', 'idx_scan')
          AND project_id IN ({project_list})
          AND ts < {config.ts_literal(snap_ts)}
        GROUP BY ALL
    """).collect():
        prev[(r["project_id"], r["branch_id"], r["endpoint_id"], r["datname"], r["object_type"],
              r["object_name"], r["metric"])] = (r["value"], r["compute_id"])

    uids = config.sql_str_list([e["endpoint_uid"] for e in endpoints])
    for r in spark.sql(f"""
        SELECT endpoint_id, max_by(compute_id, ts) AS compute_id
        FROM system.lakebase.compute_gauges
        WHERE ts >= current_timestamp() - INTERVAL 2 HOURS AND endpoint_id IN ({uids})
        GROUP BY endpoint_id
    """).collect():
        compute_fallback[r["endpoint_id"]] = r["compute_id"]
print(f"previous counters: {len(prev)}")

# COMMAND ----------

TABLES_SQL = pg.load_sql("pg_tables_snapshot.sql")
INDEXES_SQL = pg.load_sql("pg_indexes_snapshot.sql")
TABLE_BLOAT_SQL = pg.load_sql("table_bloat.sql")
BTREE_BLOAT_SQL = pg.load_sql("btree_bloat.sql")
DATABASES_SQL = pg.load_sql("pg_databases.sql")


def counter_rows(e, datname, object_type, object_name, cur_values: dict, row, compute_id, detail=None):
    """Raw cumulative counter + delta row for each counter metric present in cur_values."""
    out = []
    for metric, delta_metric in metrics.COUNTER_METRICS.items():
        if metric not in cur_values:
            continue
        cur = cur_values[metric]
        p, p_compute = prev.get((e["project_id"], e["branch_id"], e["endpoint_id"], datname, object_type,
                                 object_name, metric), (None, None))
        restarted = bool(compute_id and p_compute and compute_id != p_compute)
        out.append(row(metric, cur, detail))
        out.append(row(delta_metric, metrics.counter_delta(cur, p, restarted), detail))
    return out


def snapshot_database(e, conn, datname, compute_id, errors):
    rows, n_tables, n_indexes = [], 0, 0

    def mk(object_type, object_name):
        def row(metric, value, detail=None):
            return metrics.make_row(ts=snap_ts, grain="snapshot", endpoint=e, metric=metric, value=value,
                                    object_type=object_type, object_name=object_name,
                                    compute_id=compute_id, datname=datname, detail=detail, run_id=cfg.run_id)
        return row

    def attempt(name, sql, timeout=None):
        try:
            return pg.run_query(conn, sql, timeout)
        except Exception as ex:  # log and continue with the remaining queries
            msg = f"{e['endpoint_name']} db={datname} query={name}: {type(ex).__name__}: {ex}".strip()
            print(f"ERROR {msg}")
            errors.append(msg[:500])
            return None

    tables = attempt("pg_tables_snapshot", TABLES_SQL) or []
    for t in tables:
        if t["schemaname"] in metrics.EXCLUDED_SCHEMAS:
            continue
        n_tables += 1
        name = f"{t['schemaname']}.{t['tblname']}"
        row = mk("table", name)
        rows += [row("n_live_tup", t["n_live_tup"]), row("n_dead_tup", t["n_dead_tup"])]
        rows += counter_rows(e, datname, "table", name,
                             {k: t[k] for k in ("seq_scan", "seq_tup_read", "idx_scan") if t.get(k) is not None},
                             row, compute_id)
        rows.append(row("hours_since_autovacuum", metrics.hours_since(t["last_autovacuum"], now),
                        {k: t[k] for k in ("last_vacuum", "last_autovacuum", "last_analyze", "last_autoanalyze")}))

    indexes = attempt("pg_indexes_snapshot", INDEXES_SQL) or []
    for i in indexes:
        n_indexes += 1
        name = f"{i['schemaname']}.{i['idxname']}"
        detail = {"table": i["tblname"], "index_columns": i["index_columns"], "indkey": i["indkey"],
                  "indpred": i["indpred"], "indexprs": i["indexprs"], "indexdef": i["indexdef"],
                  "indisunique": i["indisunique"], "indisprimary": i["indisprimary"]}
        row = mk("index", name)
        rows += counter_rows(e, datname, "index", name,
                             {"idx_scan": i["idx_scan"]} if i.get("idx_scan") is not None else {}, row, compute_id,
                             detail)
        rows.append(row("index_bytes", i["index_bytes"], detail))
        rows.append(row("index_is_valid", bool(i["indisvalid"] and i["indisready"]), detail))
        rows.append(row("index_backs_constraint", bool(i["backs_constraint"]), detail))

    for qname, sql, otype in (("table_bloat", TABLE_BLOAT_SQL, "table"),
                              ("btree_bloat", BTREE_BLOAT_SQL, "index")):
        for b in attempt(qname, sql) or []:
            if b["schemaname"] in metrics.EXCLUDED_SCHEMAS:
                continue
            name = f"{b['schemaname']}.{b['idxname'] if otype == 'index' else b['tblname']}"
            row = mk(otype, name)
            is_na = bool(b["is_na"])
            detail = {"real_size": b["real_size"], "extra_size": b["extra_size"],
                      "extra_pct": b["extra_pct"], "fillfactor": b["fillfactor"]}
            if otype == "index":
                detail["table"] = b["tblname"]
            # btree_bloat can return a negative estimate for small/compact indexes; floor at 0
            pct = 0.0 if is_na or b["bloat_pct"] is None else max(float(b["bloat_pct"]), 0.0)
            rows.append(row("bloat_pct", pct, detail))
            rows.append(row("bloat_bytes", b["bloat_size"], detail))
            rows.append(row("bloat_is_na", is_na))
    return rows, n_tables, n_indexes


# COMMAND ----------

w = WorkspaceClient()
identity = lakebase_api.current_identity(w)
all_rows, failures = [], []

for e in endpoints:
    if not lakebase_api.is_running(e):
        print(f"SKIP {e['endpoint_name']} (state={e['state']}, not woken up)")
        continue

    t0 = time.time()
    errors, databases, ep_rows = [], [], []
    n_tables = n_indexes = 0
    try:
        if not e.get("host"):
            raise RuntimeError("endpoint has no host")
        password = lakebase_api.database_password(w, e["endpoint_name"])
        connect = lambda db: pg.connect(e["host"], db, identity, password,  # noqa: E731
                                        config.PG_LOCK_TIMEOUT_MS, config.PG_STATEMENT_TIMEOUT_MS)
        with connect("databricks_postgres") as conn:
            try:
                compute_id = pg.scalar(conn, "SELECT current_setting('neon.compute_id', true)") or None
            except Exception:
                compute_id = None
            compute_id = compute_id or compute_fallback.get(e["endpoint_uid"])
            databases = [r["datname"] for r in pg.run_query(conn, DATABASES_SQL)]

        for db in databases:
            try:
                with connect(db) as conn:
                    rows, nt, ni = snapshot_database(e, conn, db, compute_id, errors)
                    ep_rows += rows
                    n_tables += nt
                    n_indexes += ni
            except Exception as ex:
                errors.append(f"connect db={db}: {type(ex).__name__}: {ex}"[:500])
    except Exception as ex:
        errors.append(f"{type(ex).__name__}: {ex}"[:500])
        traceback.print_exc()

    all_rows += ep_rows
    if errors:
        failures.append(f"{e['endpoint_name']}: " + "; ".join(errors))
    print(f"{'FAIL' if errors else 'OK  '} {e['endpoint_name']}: dbs={len(databases)} tables={n_tables} "
          f"indexes={n_indexes} errors={len(errors)} rows={len(ep_rows)} {time.time() - t0:.1f}s")

# COMMAND ----------

# A rerun in the same hour replaces that hour's snapshot. compute_id is part of the row key, so rows from
# an earlier run on a different compute (the endpoint restarted in between) are removed first.
written = sorted({(r[2], r[3], r[4]) for r in all_rows})  # (project_id, branch_id, endpoint_id)
if written:
    spark.sql(f"""
        DELETE FROM {cfg.metrics}
        WHERE grain = 'snapshot' AND ts = {config.ts_literal(snap_ts)}
          AND ({" OR ".join(f"(project_id = '{p}' AND branch_id = '{b}' AND endpoint_id = '{x}')" for p, b, x in written)})
    """)
n = store.merge_rows(spark, cfg, all_rows, "lbh_snapshot_rows")
print(f"merged {n} snapshot rows")
if failures:
    raise RuntimeError("Postgres snapshot had errors (rows from the rest were saved):\n" + "\n".join(failures))
