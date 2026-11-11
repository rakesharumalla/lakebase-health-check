# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Postgres snapshot (metrics 4–6, grain = snapshot)
# MAGIC For each running endpoint: connect read-only over OAuth with lock/statement timeouts, snapshot
# MAGIC `pg_stat_user_tables`, indexes and (on the first run of each UTC day per endpoint) the ioguix bloat
# MAGIC estimates, compute counter deltas and MERGE. Endpoints that are not running are skipped so the
# MAGIC job never wakes a scaled-to-zero compute.
# MAGIC
# MAGIC A failure on one endpoint does not fail the task; the task fails only if every attempted
# MAGIC endpoint failed.

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
from datetime import timedelta


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
cfg_values = config.read_config_values(spark, cfg)

now = config.utc_now()
# One snapshot time per run, identical across endpoints. Aligned to the run hour so a rerun
# within the same hour overwrites (idempotent) instead of adding a second snapshot.
snap_ts = metrics.hour_bucket(now)
day_start = snap_ts.replace(hour=0)
project_list = config.sql_str_list(sorted({e["project_id"] for e in endpoints}))
print(f"snapshot ts={snap_ts} endpoints={len(endpoints)} all_databases={cfg.snapshot_all_databases}")

# COMMAND ----------

# Previous cumulative counters per object (latest snapshot strictly before this one).
prev = {}
bloat_done = set()
compute_fallback = {}
if endpoints:
    for r in spark.sql(f"""
        SELECT project_id, branch_id, endpoint_id, datname, object_type, object_name, metric,
               max_by(value, ts) AS value
        FROM {cfg.metrics}
        WHERE grain = 'snapshot' AND metric IN ('seq_scan', 'seq_tup_read', 'idx_scan')
          AND project_id IN ({project_list})
          AND ts < {config.ts_literal(snap_ts)}
          AND ts >= {config.ts_literal(snap_ts - timedelta(days=cfg.retention_days))}
        GROUP BY ALL
    """).collect():
        prev[(r["project_id"], r["branch_id"], r["endpoint_id"], r["datname"], r["object_type"],
              r["object_name"], r["metric"])] = r["value"]

    # Bloat estimates are heavy: run them once per UTC day per endpoint, whatever the schedule.
    for r in spark.sql(f"""
        SELECT DISTINCT project_id, branch_id, endpoint_id FROM {cfg.metrics}
        WHERE grain = 'snapshot' AND project_id IN ({project_list})
          AND ts >= {config.ts_literal(day_start)}
          AND (metric = 'bloat_pct'
               OR (metric = 'snapshot_ok' AND coalesce(detail:bloat_ran::boolean, false)))
    """).collect():
        bloat_done.add((r["project_id"], r["branch_id"], r["endpoint_id"]))

    uids = config.sql_str_list([e["endpoint_uid"] for e in endpoints])
    for r in spark.sql(f"""
        SELECT endpoint_id, max_by(compute_id, ts) AS compute_id
        FROM system.lakebase.compute_gauges
        WHERE ts >= current_timestamp() - INTERVAL 2 HOURS AND endpoint_id IN ({uids})
        GROUP BY endpoint_id
    """).collect():
        compute_fallback[r["endpoint_id"]] = r["compute_id"]
print(f"previous counters: {len(prev)}; endpoints with bloat already today: {len(bloat_done)}")

# COMMAND ----------

TABLES_SQL = pg.load_sql("pg_tables_snapshot.sql")
INDEXES_SQL = pg.load_sql("pg_indexes_snapshot.sql")
TABLE_BLOAT_SQL = pg.load_sql("table_bloat.sql")
BTREE_BLOAT_SQL = pg.load_sql("btree_bloat.sql")
ACTIVITY_SQL = pg.load_sql("pg_activity_snapshot.sql")
DATABASES_SQL = pg.load_sql("pg_databases.sql")


def counter_rows(e, datname, object_type, object_name, cur_values: dict, row, detail=None):
    """Raw cumulative counter + delta row for each counter metric present in cur_values."""
    out = []
    for metric, delta_metric in metrics.COUNTER_METRICS.items():
        if metric not in cur_values:
            continue
        cur = cur_values[metric]
        p = prev.get((e["project_id"], e["branch_id"], e["endpoint_id"], datname, object_type, object_name, metric))
        out.append(row(metric, cur, detail))
        out.append(row(delta_metric, metrics.counter_delta(cur, p), detail))
    return out


def snapshot_database(e, conn, datname, compute_id, do_bloat, errors):
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
                             row)
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
                             {"idx_scan": i["idx_scan"]} if i.get("idx_scan") is not None else {}, row, detail)
        rows.append(row("index_bytes", i["index_bytes"], detail))
        rows.append(row("index_is_valid", bool(i["indisvalid"] and i["indisready"]), detail))
        rows.append(row("index_backs_constraint", bool(i["backs_constraint"]), detail))

    if do_bloat:
        for qname, sql, otype in (("table_bloat", TABLE_BLOAT_SQL, "table"),
                                  ("btree_bloat", BTREE_BLOAT_SQL, "index")):
            res = attempt(qname, sql, cfg.pg_bloat_statement_timeout_ms)
            for b in res or []:
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
all_rows, attempted, failed = [], 0, 0

for e in endpoints:
    key = (e["project_id"], e["branch_id"], e["endpoint_id"])
    base_detail = {"endpoint_type": e["endpoint_type"], "state": e["state"]}
    if not lakebase_api.is_running(e):
        all_rows.append(metrics.make_row(ts=snap_ts, grain="snapshot", endpoint=e, metric="snapshot_skipped",
                                         value=1, detail={**base_detail, "reason": "endpoint_not_running"},
                                         run_id=cfg.run_id))
        print(f"SKIP {e['endpoint_name']} (state={e['state']})")
        continue

    attempted += 1
    t0 = time.time()
    errors, databases, ep_rows = [], [], []
    n_tables = n_indexes = 0
    do_bloat = key not in bloat_done
    fatal = False
    compute_id = None
    try:
        if not e.get("host"):
            raise RuntimeError("endpoint has no host")
        password = lakebase_api.database_password(w, e["endpoint_name"])
        connect = lambda db: pg.connect(e["host"], db, identity, password,  # noqa: E731
                                        cfg.pg_lock_timeout_ms, cfg.pg_statement_timeout_ms)
        with connect("databricks_postgres") as conn:
            try:
                compute_id = pg.scalar(conn, "SELECT current_setting('neon.compute_id', true)") or None
            except Exception:
                compute_id = None
            compute_id = compute_id or compute_fallback.get(e["endpoint_uid"])
            databases = ([r["datname"] for r in pg.run_query(conn, DATABASES_SQL)]
                         if cfg.snapshot_all_databases else ["databricks_postgres"])
            if cfg.idle_in_txn_source == "pg_activity":
                try:
                    act = pg.run_query(conn, ACTIVITY_SQL)
                    summary = metrics.idle_in_txn_summary(act, cfg_values["cfg_long_xact_seconds"],
                                                          cfg_values["cfg_very_long_xact_seconds"])
                    top = sorted(act, key=lambda r: -(r["xact_age_ms"] or 0))[:5]
                    for m, v in summary.items():
                        ep_rows.append(metrics.make_row(
                            ts=snap_ts, grain="snapshot", endpoint=e, metric=m, value=v, compute_id=compute_id,
                            detail={"top": [{k: r[k] for k in ("pid", "datname", "xact_age_ms", "wait_event_type",
                                                                "wait_event", "blocking_pids")} for r in top]}
                            if m == "idle_in_txn_over_60s_count" else None,
                            run_id=cfg.run_id))
                except Exception as ex:
                    errors.append(f"pg_activity_snapshot: {ex}"[:500])

        for db in databases:
            try:
                with connect(db) as conn:
                    rows, nt, ni = snapshot_database(e, conn, db, compute_id, do_bloat, errors)
                    ep_rows += rows
                    n_tables += nt
                    n_indexes += ni
            except Exception as ex:
                errors.append(f"connect db={db}: {type(ex).__name__}: {ex}"[:500])
        if databases and not ep_rows and errors:
            fatal = True
    except Exception as ex:
        fatal = True
        errors.append(f"{type(ex).__name__}: {ex}"[:500])
        traceback.print_exc()

    if fatal:
        failed += 1
    all_rows += ep_rows
    all_rows.append(metrics.make_row(
        ts=snap_ts, grain="snapshot", endpoint=e, metric="snapshot_ok", value=0 if fatal else 1,
        compute_id=compute_id,
        detail={**base_detail, "host": e.get("host"), "databases": databases, "tables": n_tables,
                "indexes": n_indexes, "bloat_ran": bool(do_bloat and not fatal), "errors": errors,
                "duration_ms": int((time.time() - t0) * 1000), "compute_id": compute_id},
        run_id=cfg.run_id))
    print(f"{'FAIL' if fatal else 'OK  '} {e['endpoint_name']}: dbs={len(databases)} tables={n_tables} "
          f"indexes={n_indexes} bloat={do_bloat} errors={len(errors)} rows={len(ep_rows)}")

# COMMAND ----------

n = store.merge_rows(spark, cfg, all_rows, "lbh_snapshot_rows")
print(f"merged {n} snapshot rows; attempted={attempted} failed={failed}")
if attempted and failed == attempted:
    raise RuntimeError(f"Postgres snapshot failed on all {attempted} running endpoints; see snapshot_ok detail.errors")
