# Lakebase Health Check

A Databricks Asset Bundle that keeps 90 days of health history for one Lakebase branch and presents it for a
periodic health review.

| Component | What it does |
|---|---|
| **Job `lakebase_health`** | Serverless job, every 4 hours. Rolls `system.lakebase` telemetry up to hourly values (last 48 h, MERGE), snapshots the Postgres statistics views and bloat estimates on the branch's primary and read replicas, recomputes the last 2 complete UTC days of trends, and deletes rows older than 90 days. |
| **Tables** | `lb_health_metrics` (narrow metric store, grains `hour` / `snapshot` / `day`) and `lb_health_thresholds` (reference value per trend), plus 6 views. |
| **Dashboard** | *Lakebase Health Review*: review summary, daily trends with reference lines, schema health. |
| **Genie space** | *Lakebase Health Investigator*, for ad-hoc questions over the views. |

The job only reads: Postgres sessions are read-only with a 1 s lock timeout and a 30 s statement timeout, and
endpoints that are scaled to zero are skipped, not woken.

## Prerequisites

- Databricks CLI 1.3+ and `python3` on the machine that deploys; a SQL warehouse ID.
- The workspace preview **Lakebase Observability in System Tables** enabled (check:
  `SELECT max(ts) FROM system.lakebase.compute_gauges`).
- A run identity (a service principal is recommended) with:
  - `USE CATALOG` on `system`, `USE SCHEMA` + `SELECT` on `system.lakebase`, `USE CATALOG` on the target catalog;
  - **CAN USE** on the Lakebase project;
  - a Postgres role on the branch (`src/setup/create_pg_roles.py`, run as a project owner, `dry_run=true` first).
    The snapshot needs no further grant. For the bloat estimates the role needs `USAGE` + `SELECT` on the
    monitored schemas (the notebook prints the SQL), because `pg_stats` only shows readable tables — without it
    every table reports *bloat unknown*. `pg_read_all_stats` is not needed (and Lakebase does not let the owner
    grant it);
  - tables that were never `ANALYZE`d have no statistics and also report *bloat unknown*; run `ANALYZE` on them;
  - optional: membership of `databricks_pii_access`, otherwise query plans are masked and the *New sequential-scan
    queries* trend shows *not available*.

## Deploy

**Guided:** `./scripts/deploy.sh` asks for the profile, project and branch, catalog and schema, warehouse and
service principal (it creates one if needed). It then grants the principal what it needs, deploys, runs the job
once and checks the job, dashboard and Genie space. Answers are saved in `local/deploy.<target>.yml`
(git-ignored), so later `databricks bundle deploy` runs keep them. `system.lakebase` grants need a metastore admin;
the script reports them if it cannot apply them.

**Manual:**
1. Set `project_id`, `branch_id`, `warehouse_id` (and optionally `target_catalog_name`, `target_schema_name`,
   `failure_notification_emails`) for the `default` target in `databricks.yml`, or pass `--var`. To run as a
   service principal, uncomment the job-level `run_as` and `permissions` blocks in the target.
2. `databricks bundle deploy --profile <profile>` (the predeploy step creates the schema, tables and views), then
   `databricks bundle run lakebase_health --profile <profile>`.
3. Grant the run identity `USE SCHEMA`, `SELECT` and `MODIFY` on the new schema, and give reviewers `SELECT` on
   the views plus access to the dashboard and Genie space.

The first run loads the last 48 hours; the 14-day review is complete after 28 days of history. To change the
cadence, edit the cron in `resources/lakebase_health.job.yml`. Uninstall with `databricks bundle destroy`
(the schema and its tables stay; drop them separately).

## Parameters

| Parameter | Default | Meaning |
|---|---|---|
| `project_id` | — (required) | Lakebase project ID, the last part of `projects/<project_id>` |
| `branch_id` | — (required) | Branch ID, the last part of `projects/<p>/branches/<branch_id>` (e.g. `production`) |
| `target_catalog_name` | `main` | Catalog for the tables and views |
| `target_schema_name` | `lb_health_check` | Schema (created if missing) |
| `failure_notification_emails` | `[]` | E-mails notified when the job fails |

`warehouse_id` (bundle variable) is used by the dashboard, the Genie space and the predeploy bootstrap.
`schedule_pause_status` pauses the schedule; the `dev` target defaults to `PAUSED`. The job fails if any running
endpoint cannot be snapshotted (rows from the others are kept).

## Trends

Each trend is judged per endpoint over the review period (7 or 14 days ending yesterday, UTC) against its row in
`lb_health_thresholds`: **red** = the latest day breaches, **amber** = an earlier day breached, **green** = none.

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

Change a reference with `UPDATE lb_health_thresholds SET value = … WHERE threshold_id = 'T4'`; the dashboard picks
it up immediately and judges past days against the new value. Redeploys never overwrite edits. Schema health also
lists invalid indexes, unused indexes (≥ 10 MB, zero scans for 30 days) and duplicate indexes.

## Tables and views

`lb_health_metrics` has one row per `(ts, grain, project_id, branch_id, endpoint_id, compute_id, datname,
object_type, object_name, metric)` with `value DOUBLE` and `detail VARIANT`. IDs are the Lakebase API IDs.
Ratios are fractions (0.70 = 70%); bloat is a percent.

| View | One row per |
|---|---|
| `lb_health_hourly` | endpoint × hour, all hourly metrics as columns |
| `lb_health_daily` | endpoint × day, trends `t1_…`–`t11_…` as columns |
| `lb_health_review_summary` | trend × endpoint × review period (7 / 14): status, latest value, averages, days breached |
| `lb_health_objects_latest` | table or index in the latest snapshot: rows, scans, size, bloat, autovacuum |
| `lb_health_index_findings` | flagged index × day: `invalid`, `unused`, `duplicate` |
| `lb_health_seq_scan_queries` | query × day that sequentially scanned a large table |

## Development

Unit tests: `uv run --with pytest pytest tests -q`. The telemetry SQL is in `src/lb_health/sql/`, the formulas in
`src/lb_health/metrics.py`, the views in `src/lb_health/views.py`. Deploy to a test workspace with
`-t dev --profile <dev-profile>`; keep personal values in `.databricks/bundle/dev/variable-overrides.json`
(git-ignored). The bloat SQL is from ioguix/pgsql-bloat-estimation (BSD-2, see `src/lb_health/sql/`).
