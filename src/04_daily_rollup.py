# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Daily rollup (grain = day)
# MAGIC Recomputes the last 2 complete UTC days per endpoint: the 11 daily health trends, the schema-health
# MAGIC lists (invalid / unused / duplicate indexes) and the seq-scan query list.
# MAGIC The current partial day is never written. SQL does the heavy aggregation; the formulas are the
# MAGIC pure functions in `lb_health.metrics` (covered by tests/test_formulas.py).

# COMMAND ----------

import json
import os
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone


def _add_src_to_path():
    try:
        nb = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
        sys.path.insert(0, os.path.dirname("/Workspace" + nb if not nb.startswith("/Workspace") else nb))
    except Exception:
        pass
    sys.path.insert(0, os.getcwd())


_add_src_to_path()

from lb_health import config, metrics, store  # noqa: E402

cfg = config.load(dbutils)
spark.conf.set("spark.sql.session.timeZone", "UTC")
endpoints = json.loads(dbutils.jobs.taskValues.get(taskKey="resolve_scope", key="endpoints", debugValue="[]"))
if not endpoints:
    dbutils.notebook.exit("no endpoints in scope")

now = config.utc_now()
days = config.complete_days(now)
day_ts = {d: datetime(d.year, d.month, d.day, tzinfo=timezone.utc) for d in days}
range_start = day_ts[days[0]]
range_end = day_ts[days[-1]] + timedelta(days=1)
projects = config.sql_str_list(sorted({e["project_id"] for e in endpoints}))
project_uids = config.sql_str_list(sorted({e["project_uid"] for e in endpoints if e.get("project_uid")}))
ep_by_key = {(e["project_id"], e["branch_id"], e["endpoint_id"]): e for e in endpoints}
M = cfg.metrics
LARGE = config.LARGE_TABLE_MIN_ROWS
print(f"days={days} large_table_min_rows={LARGE}")

from pyspark.sql.types import StringType, StructField, StructType  # noqa: E402

scope_cols = ["project_id", "branch_id", "endpoint_id", "project_uid", "endpoint_uid", "endpoint_type"]
spark.createDataFrame([tuple(e.get(c) for c in scope_cols) for e in endpoints],
                      StructType([StructField(c, StringType()) for c in scope_cols])
                      ).createOrReplaceTempView("lbh_scope")


def ekey(r):
    return (r["project_id"], r["branch_id"], r["endpoint_id"])


rows = []


def day_row(e_key, d, metric, value, detail=None, object_type="endpoint", object_name=None, datname=None):
    e = ep_by_key.get(e_key) or {"project_id": e_key[0], "branch_id": e_key[1], "endpoint_id": e_key[2]}
    if object_type == "endpoint":  # the views read primary/replica from here
        detail = {**(detail or {}), "endpoint_type": e.get("endpoint_type")}
    rows.append(metrics.make_row(ts=day_ts[d], grain="day", endpoint=e, metric=metric, value=value,
                                 object_type=object_type, object_name=object_name, datname=datname,
                                 detail=detail, run_id=cfg.run_id))


# COMMAND ----------

# T1–T7 from hourly rows
hourly = defaultdict(lambda: defaultdict(dict))  # (ekey, day) -> hour -> {metric: value}
for r in spark.sql(f"""
    SELECT project_id, branch_id, endpoint_id, ts, metric, value FROM {M}
    WHERE grain = 'hour' AND object_type = 'endpoint' AND metric IS NOT NULL
      AND project_id IN ({projects})
      AND ts >= {config.ts_literal(range_start)} AND ts < {config.ts_literal(range_end)}
""").collect():
    hourly[(ekey(r), r["ts"].date())][r["ts"]][r["metric"]] = r["value"]

for (k, d), hours in hourly.items():
    trends = metrics.day_trends_from_hours(list(hours.values()))
    for m, v in trends.items():
        if v is not None:
            day_row(k, d, m, v, {"hours": len(hours)})

# COMMAND ----------

# T8 bloat, T9 dead rows, T10 seq-scan ratio from snapshot rows
bloat = defaultdict(list)
for r in spark.sql(f"""
    SELECT project_id, branch_id, endpoint_id, datname, object_type, object_name, ts,
           max(value) FILTER (WHERE metric = 'bloat_pct') AS bloat_pct,
           max(value) FILTER (WHERE metric = 'bloat_bytes') AS bloat_bytes,
           max(value) FILTER (WHERE metric = 'bloat_is_na') AS bloat_is_na
    FROM {M}
    WHERE grain = 'snapshot' AND metric IN ('bloat_pct', 'bloat_bytes', 'bloat_is_na') AND project_id IN ({projects})
      AND ts >= {config.ts_literal(range_start)} AND ts < {config.ts_literal(range_end)}
    GROUP BY ALL
""").collect():
    bloat[(ekey(r), r["ts"].date())].append(r.asDict())
for (k, d), objs in bloat.items():
    v, top = metrics.t8_max_bloat(objs, config.BLOAT_MIN_WASTED_BYTES)
    if v is not None:
        day_row(k, d, "t8_max_bloat_pct", v, {"top": top})

tables = defaultdict(list)
for r in spark.sql(f"""
    SELECT project_id, branch_id, endpoint_id, datname, object_name, CAST(ts AS DATE) AS day,
      max_by(value, ts) FILTER (WHERE metric = 'n_live_tup') AS n_live_tup,
      max_by(value, ts) FILTER (WHERE metric = 'n_dead_tup') AS n_dead_tup,
      max_by(value, ts) FILTER (WHERE metric = 'hours_since_autovacuum') AS hours_since_autovacuum,
      max_by(detail:last_autovacuum::string, ts) FILTER (WHERE metric = 'hours_since_autovacuum') AS last_autovacuum,
      sum(value) FILTER (WHERE metric = 'seq_scan_delta') AS seq_scan_delta,
      sum(value) FILTER (WHERE metric = 'seq_tup_read_delta') AS seq_tup_read_delta
    FROM {M}
    WHERE grain = 'snapshot' AND object_type = 'table' AND project_id IN ({projects})
      AND ts >= {config.ts_literal(range_start)} AND ts < {config.ts_literal(range_end)}
    GROUP BY ALL
""").collect():
    tables[(ekey(r), r["day"])].append(r.asDict())
for (k, d), tbls in tables.items():
    v9, d9 = metrics.t9_dead_rows(tbls, LARGE)
    if v9 is not None:
        day_row(k, d, "t9_dead_rows_large_tables", v9, {"tables": d9})
    v10, d10 = metrics.t10_seq_scan_ratio(tbls, LARGE)
    if v10 is not None:
        day_row(k, d, "t10_seq_scan_ratio_large_tables", v10, {"tables": d10})

# COMMAND ----------

# T11 + seq-scan query list from plan_history.
# Large tables per branch/database as of the end of each day (bare table name: plans carry relname).
large_by_day = defaultdict(set)  # day -> {(project, branch, datname, relname)}
for d in days:
    d_end = day_ts[d] + timedelta(days=1)
    for r in spark.sql(f"""
        SELECT project_id, branch_id, datname, object_name, max_by(value, ts) AS n_live_tup
        FROM {M}
        WHERE grain = 'snapshot' AND object_type = 'table' AND metric = 'n_live_tup'
          AND project_id IN ({projects}) AND ts < {config.ts_literal(d_end)}
        GROUP BY ALL
    """).collect():
        if (r["n_live_tup"] or 0) >= LARGE:
            relname = r["object_name"].split(".", 1)[-1]
            large_by_day[d].add((r["project_id"], r["branch_id"], r["datname"], relname))

plan_start = range_start - timedelta(days=7)
pairs = defaultdict(list)  # (ekey, day) -> [pair]
coverage = {}
try:
    for r in spark.sql(f"""
        SELECT s.project_id, s.branch_id, s.endpoint_id, CAST(p.ts AS DATE) AS day,
          count(*) AS total, count_if(p.plan_json = '{metrics.REDACTED}' OR p.query_text = '{metrics.REDACTED}') AS masked
        FROM system.lakebase.plan_history p JOIN lbh_scope s ON p.endpoint_id = s.endpoint_uid
        WHERE p.ts >= {config.ts_literal(range_start)} AND p.ts < {config.ts_literal(range_end)}
          AND p.project_id IN ({project_uids})
        GROUP BY ALL
    """).collect():
        coverage[(ekey(r), r["day"])] = (r["total"], r["masked"])

    for r in spark.sql(f"""
        SELECT s.project_id, s.branch_id, s.endpoint_id, p.datname, CAST(p.ts AS DATE) AS day,
          CAST(p.queryid AS STRING) AS queryid, rel AS relation, count(*) AS count,
          avg(p.duration_ms) AS avg_duration_ms, any_value(left(p.query_text, 500)) AS query_text
        FROM system.lakebase.plan_history p
        JOIN lbh_scope s ON p.endpoint_id = s.endpoint_uid
        LATERAL VIEW explode(regexp_extract_all(p.plan_json, '{metrics.SEQ_SCAN_REGEX}', 1)) x AS rel
        WHERE p.ts >= {config.ts_literal(plan_start)} AND p.ts < {config.ts_literal(range_end)}
          AND p.project_id IN ({project_uids})
          AND p.plan_json <> '{metrics.REDACTED}' AND p.queryid IS NOT NULL
        GROUP BY ALL
    """).collect():
        pairs[(ekey(r), r["day"])].append(r.asDict())
    plan_ok = True
except Exception as ex:  # no SELECT on plan_history, or schema drift
    print(f"WARNING: plan_history unavailable: {ex}")
    plan_ok = False

for d in days:
    for k in ep_by_key:
        total, masked = coverage.get((k, d), (0, 0))
        if not plan_ok or (total > 0 and masked == total):
            reason = ("plan_json masked; add run identity to databricks_pii_access" if plan_ok
                      else "system.lakebase.plan_history not readable by the run identity")
            day_row(k, d, "t11_new_seq_scan_queries", None, {"status": "not_available", "reason": reason})
            continue
        large = {(dn, rel) for (p, b, dn, rel) in large_by_day[d] if (p, b) == k[:2]}
        today = pairs.get((k, d), [])
        prior = {(p["datname"], p["queryid"], p["relation"])
                 for i in range(1, 8) for p in pairs.get((k, d - timedelta(days=i)), [])}
        new = metrics.new_seq_scan_queries(today, prior, large)
        if total == 0 and not today and (k, d) not in hourly:
            continue  # endpoint had no activity and no telemetry that day
        new_ids = sorted({p["queryid"] for p in new})
        day_row(k, d, "t11_new_seq_scan_queries", len(new_ids),
                {"status": "ok", "plans": total, "new": [
                    {"queryid": p["queryid"], "query_text": p["query_text"], "relation": p["relation"],
                     "datname": p["datname"], "count": p["count"], "avg_duration_ms": p["avg_duration_ms"]}
                    for p in new][:50]})

        # all seq-scan queries on large tables that day (not just new ones)
        by_query = defaultdict(list)
        for p in today:
            if (p["datname"], p["relation"]) in large:
                by_query[(p["datname"], p["queryid"])].append(p)
        new_set = {(p["datname"], p["queryid"]) for p in new}
        for (dn, qid), ps in by_query.items():
            cnt = max(p["count"] for p in ps)  # plan rows for the query (each row may scan several relations)
            day_row(k, d, metrics.SEQ_SCAN_QUERY_METRIC, cnt, {
                "query_text": ps[0]["query_text"], "relations": sorted({p["relation"] for p in ps}),
                "avg_duration_ms": max(p["avg_duration_ms"] or 0 for p in ps), "is_new": (dn, qid) in new_set,
            }, object_type="query", object_name=qid, datname=dn)

# COMMAND ----------

# Index findings, written under the primary (read_write) endpoint of each branch; scans are summed
# across every endpoint of the branch, since replicas keep independent counters.
primary = {}
for e in sorted(endpoints, key=lambda e: (e["endpoint_type"] != "read_write", e["endpoint_id"])):
    primary.setdefault((e["project_id"], e["branch_id"]), (e["project_id"], e["branch_id"], e["endpoint_id"]))
WINDOW = config.UNUSED_INDEX_WINDOW_DAYS

for d in days:
    d_end = day_ts[d] + timedelta(days=1)
    w_start = d_end - timedelta(days=WINDOW)
    scans = {}
    for r in spark.sql(f"""
        SELECT project_id, branch_id, datname, object_name, sum(value) AS scans, count(value) AS n
        FROM {M}
        WHERE grain = 'snapshot' AND object_type = 'index' AND metric = 'idx_scan_delta'
          AND project_id IN ({projects})
          AND ts >= {config.ts_literal(w_start)} AND ts < {config.ts_literal(d_end)}
        GROUP BY ALL
    """).collect():
        scans[(r["project_id"], r["branch_id"], r["datname"], r["object_name"])] = r["scans"] if r["n"] else None

    latest = defaultdict(list)
    for r in spark.sql(f"""
        WITH idx AS (
          SELECT * FROM {M}
          WHERE grain = 'snapshot' AND object_type = 'index' AND project_id IN ({projects})
            AND ts < {config.ts_literal(d_end)}
        ),
        last_snap AS (
          SELECT project_id, branch_id, endpoint_id, datname, max(ts) AS last_ts
          FROM idx WHERE metric = 'index_bytes' GROUP BY ALL
        )
        SELECT i.project_id, i.branch_id, i.endpoint_id, i.datname, i.object_name,
          min(i.ts) AS first_seen, max(i.ts) FILTER (WHERE i.metric = 'index_bytes') AS last_ts,
          max_by(i.value, i.ts) FILTER (WHERE i.metric = 'index_bytes') AS index_bytes,
          max_by(i.value, i.ts) FILTER (WHERE i.metric = 'index_is_valid') AS index_is_valid,
          max_by(i.value, i.ts) FILTER (WHERE i.metric = 'index_backs_constraint') AS backs_constraint,
          max_by(i.detail:table::string, i.ts) FILTER (WHERE i.metric = 'index_bytes') AS tbl,
          max_by(i.detail:index_columns::string, i.ts) FILTER (WHERE i.metric = 'index_bytes') AS index_columns,
          max_by(i.detail:indexprs::string, i.ts) FILTER (WHERE i.metric = 'index_bytes') AS indexprs,
          max_by(i.detail:indpred::string, i.ts) FILTER (WHERE i.metric = 'index_bytes') AS indpred,
          max_by(i.detail:indexdef::string, i.ts) FILTER (WHERE i.metric = 'index_bytes') AS indexdef,
          max_by(i.detail:indisunique::boolean, i.ts) FILTER (WHERE i.metric = 'index_bytes') AS indisunique,
          max(l.last_ts) AS endpoint_last_ts
        FROM idx i JOIN last_snap l ON i.project_id = l.project_id AND i.branch_id = l.branch_id
          AND i.endpoint_id = l.endpoint_id AND i.datname <=> l.datname
        GROUP BY ALL
    """).collect():
        k = ekey(r)
        if primary.get(k[:2]) != k or r["last_ts"] != r["endpoint_last_ts"]:
            continue  # not the branch primary, or index absent from the latest snapshot (dropped)
        latest[k].append(r.asDict())

    for k, idxs in latest.items():
        for i in idxs:
            base = {"table": i["tbl"], "index_bytes": i["index_bytes"], "indexdef": i["indexdef"]}
            if i["index_is_valid"] == 0:
                day_row(k, d, "index_invalid", 1, base, object_type="index",
                        object_name=i["object_name"], datname=i["datname"])
            first = i["first_seen"]  # earliest stored snapshot (snapshots are kept 90 days > the 30-day window)
            if first.tzinfo is None:
                first = first.replace(tzinfo=timezone.utc)
            observed = (d_end - first).total_seconds() / 86400.0
            finding = metrics.classify_unused_index(
                scans_in_window=scans.get((k[0], k[1], i["datname"], i["object_name"])),
                index_bytes=i["index_bytes"], backs_constraint=bool(i["backs_constraint"]),
                observed_days=observed, min_bytes=config.UNUSED_INDEX_MIN_BYTES, window_days=WINDOW)
            if finding:
                day_row(k, d, finding, 1, {**base, "observed_days": round(observed, 2), "first_seen": first,
                                           "window_days": WINDOW},
                        object_type="index", object_name=i["object_name"], datname=i["datname"])
        dups = metrics.find_duplicate_indexes([
            {"datname": i["datname"], "schema": i["object_name"].split(".", 1)[0], "table": i["tbl"],
             "object_name": i["object_name"], "index_columns": i["index_columns"], "indexprs": i["indexprs"],
             "indpred": i["indpred"], "index_bytes": i["index_bytes"], "indisunique": i["indisunique"],
             "indexdef": i["indexdef"]} for i in idxs])
        for (dn, name), detail in dups.items():
            day_row(k, d, "index_duplicate", 1, detail, object_type="index", object_name=name, datname=dn)

# COMMAND ----------

# Replace the recomputed days for the endpoints in scope (removes findings that no longer apply),
# then MERGE the fresh rows.
ep_filter = " OR ".join(
    f"(project_id = '{p}' AND branch_id = '{b}' AND endpoint_id = '{e}')" for (p, b, e) in ep_by_key)
spark.sql(f"""
    DELETE FROM {M}
    WHERE grain = 'day' AND ts >= {config.ts_literal(range_start)} AND ts < {config.ts_literal(range_end)}
      AND ({ep_filter})
""")
n = store.merge_rows(spark, cfg, rows, "lbh_daily_rows")
print(f"daily rows written: {n}")
