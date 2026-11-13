# Lakebase Health Check

A Databricks Asset Bundle that keeps 90 days of health history for one Lakebase branch and presents it for a
periodic health review.

| Component | What it does |
|---|---|
| **Job `lakebase_health`** | Serverless job, every 4 hours. Rolls `system.lakebase` telemetry up to hourly values (last 48 h, MERGE), snapshots the Postgres statistics views and bloat estimates on the branch's primary and read replicas, recomputes the last 2 complete UTC days of trends, and deletes rows older than 90 days. |
| **Tables** | `lb_health_metrics` (narrow metric store, grains `hour` / `snapshot` / `day`) and `lb_health_thresholds` (reference value per trend), plus 6 views. |
| **Dashboard** | *Lakebase Health Review*: review summary, daily trends with reference lines, schema health. |
| **Genie space** | *Lakebase Health Investigator*, for ad-hoc questions over the views. |
| **Alerts** | 8 Databricks SQL alerts on `system.lakebase`, every 5 minutes, independent of the job (see below). |

The job only reads: Postgres sessions are read-only with a 1 s lock timeout and a 30 s statement timeout, and
endpoints that are scaled to zero are skipped, not woken.

## Install

See **[INSTALL.md](INSTALL.md)** for the full guide (requirements, grants, sharing, updating, troubleshooting).
In short:

```bash
git clone https://github.com/rakesharumalla/lakebase-health-check.git && cd lakebase-health-check
databricks auth login --host https://<your-workspace>.cloud.databricks.com --profile <profile-name>
./scripts/deploy.sh     # asks for project, branch, catalog, schema, warehouse and service principal
```

Review summary and Daily trends need a complete UTC day: a project created today shows them after the first
run after 00:00 UTC.

## Running a health review

1. Open **Lakebase Health Review**, pick the review period (7 or 14 days) and, if needed, one endpoint.
2. **Review summary**: red = the latest day breaches its reference, amber = an earlier day in the period did,
   green = no breach, info = no reference line. The table compares the period average with the prior period.
3. **Daily trends**: the actual value per endpoint and the reference line (orange) for each trend.
4. **Schema health**: most bloated objects, large tables with dead rows, sequential-scan queries, invalid,
   unused and duplicate indexes.
5. Ask **Lakebase Health Investigator** follow-ups, for example *"Which trends are red this period?"* or
   *"Which hours had CPU p95 above 70%?"*.
6. Adjust a reference when it does not fit the workload, e.g.
   `UPDATE <catalog>.<schema>.lb_health_thresholds SET value = 0.8 WHERE threshold_id = 'T4'`. The dashboard uses it
   immediately and judges past days against it; redeploys keep your edits.

## Trends

Each trend is judged per endpoint over the review period (7 or 14 days ending yesterday, UTC) against its row in
`lb_health_thresholds`.

| ID | Trend | Reference |
|---|---|---|
| T1 | Connection utilization (busiest-hour p95 of connections / max_connections) | above 70% |
| T2 | Long idle-in-transaction sessions (> 60 s, from `active_session_history`) | above 0 |
| T3 | Idle-in-transaction time share (`database_counters`) | above 10% |
| T4 | CPU utilization (busiest-hour p95) | above 70% |
| T5 | CPU per connection | above the prior-period average |
| T6 | Cache hit rate ((shared hits + LFC hits) / (shared hits + shared reads)) | below 99% |
| T7 | Working set vs. cache size (60-min working set / LFC limit) | above 80% |
| T8 | Table and index bloat (max %, objects wasting ≥ 100 MB) | above 30% |
| T9 | Dead rows on large tables (≥ 100K live rows) | none — look for growth |
| T10 | Full scans of large tables (rows read by seq scans / live rows, per day) | above 10 |
| T11 | New sequential-scan queries on large tables (`plan_history`) | above 0 |
| T12 | Idle session share ((session − active − idle-in-transaction time) / session time, `database_counters`) | none — informational |
| T13 | Active session share (active time / session time, `database_counters`) | below 10% |

Lakebase ends idle-in-transaction sessions after 5 minutes by default (`idle_in_transaction_session_timeout`), so
sessions over 300 s only appear where that setting was raised. Unused indexes are ≥ 10 MB with zero scans for 30
days, summed across the primary and replicas.

## Alerts

Databricks SQL alerts in `resources/lb_health_alerts.yml`, evaluated every 5 minutes on `warehouse_id` as the job's
service principal. They query `system.lakebase` directly, so they work even when the job is down. Severity is in
the title; recipients are the e-mails given to `deploy.sh`, notified only when an alert fires (once per incident).

| ID | Severity | Fires when |
|---|---|---|
| R1 | High | Connections (summed across databases) / max_connections > 85% on the latest 2 samples |
| R2 | High | A client transaction is open > 1 min (shows the blocking pid) |
| R2b | High | ≥ 5 sessions idle in transaction on every sample of the last 60 s |
| R3 | Med | 15-min average CPU > 85%, or busiest core > 95% |
| R4 | Low | 30-min cache hit rate (1 − LFC misses / (shared hits + shared reads)) < 95%, ≥ 10K block requests |
| R5 | Med | Any deadlock in the last 10 min |
| R6 | Med | Storage > 80% of the project's size limit |
| R7 | Med | No telemetry from the branch for 15 min (its computes stopped or suspended, or telemetry off) |

Alerts cover the same branch as the job. R3 fires about 13 minutes after CPU saturates (15-minute average).
`max_connections` follows the autoscaling maximum, so R1's denominator only changes when the autoscaling range
does. R7 looks at the branch as a whole, so one replica scaling to zero does not set it off. ASH codes depend on the Postgres version (client backends
are `backend_type` 4 on PG 16 and 1 from 17; idle in transaction is state 3 up to PG 17 and 4 from 18), so the
alerts use the `pg_version` variable, which `deploy.sh` reads from the project. `alerts_pause_status` pauses them.

## Parameters, tables and views

| Parameter | Default | Meaning |
|---|---|---|
| `project_id` | — (required) | Lakebase project ID |
| `branch_id` | — (required) | Branch ID (e.g. `production`) |
| `target_catalog_name` | `main` | Catalog for the tables and views |
| `target_schema_name` | `lb_health_check` | Schema (created if missing) |
| `failure_notification_emails` | `[]` | E-mails notified when the job fails |

`warehouse_id` (bundle variable) is used by the dashboard, the Genie space and the predeploy bootstrap.

`lb_health_metrics` has one row per `(ts, grain, project_id, branch_id, endpoint_id, compute_id, datname,
object_type, object_name, metric)` with `value DOUBLE` and `detail VARIANT`. Ratios are fractions (0.70 = 70%);
bloat is a percent.

| View | One row per |
|---|---|
| `lb_health_hourly` | endpoint × hour, all hourly metrics as columns |
| `lb_health_daily` | endpoint × day, trends `t1_…`–`t13_…` as columns |
| `lb_health_review_summary` | trend × endpoint × review period (7 / 14): status, latest value, averages, days breached |
| `lb_health_objects_latest` | table or index in the latest snapshot: rows, scans, size, bloat, autovacuum |
| `lb_health_index_findings` | flagged index × day: `invalid`, `unused`, `duplicate` |
| `lb_health_seq_scan_queries` | query × day that sequentially scanned a large table |

## Development

Unit tests: `uv run --with pytest pytest tests -q`. The telemetry SQL is in `src/lb_health/sql/`, the formulas in
`src/lb_health/metrics.py`, the views in `src/lb_health/views.py`. Deploy to a test workspace with
`./scripts/deploy.sh --target dev`. The bloat SQL is from ioguix/pgsql-bloat-estimation (BSD-2, see
`src/lb_health/sql/`).
