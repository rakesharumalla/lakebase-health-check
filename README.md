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

## Getting started

### Step 1 — Check what you need

| What | Why |
|---|---|
| A Mac or Linux machine (Windows: WSL) with `bash`, `git`, `python3` and the [Databricks CLI](https://docs.databricks.com/dev-tools/cli/install.html) 1.3 or later | runs the deploy script |
| **CAN MANAGE** on the Lakebase project | grants the run identity access to the project and creates its Postgres role |
| Workspace admin, or an existing service principal you hold **Service principal: User** on | the job runs as a service principal |
| `CREATE SCHEMA` on a Unity Catalog catalog (or an existing schema you can write to) | holds the tables and views |
| A SQL warehouse you can use | dashboard, Genie space and table setup |
| A metastore admin, once (unless `account users` can already read `system.lakebase`) | grants the service principal read access to `system.lakebase` |

### Step 2 — Turn on Lakebase telemetry

In the workspace **Previews** page, enable **Lakebase Observability in System Tables**. Then check that data
arrives (SQL editor):

```sql
SELECT project_name, max(ts) FROM system.lakebase.compute_gauges GROUP BY 1;
```

### Step 3 — Log in with the Databricks CLI

```bash
databricks auth login --host https://<your-workspace>.cloud.databricks.com --profile <profile-name>
```

Find the project and branch IDs you want to monitor:

```bash
databricks postgres list-projects --profile <profile-name>                      # projects/<project_id>
databricks postgres list-branches projects/<project_id> --profile <profile-name>  # .../branches/<branch_id>
```

### Step 4 — Get the code

```bash
git clone https://github.com/rakesharumalla/lakebase-health-check.git
cd lakebase-health-check
```

### Step 5 — Run the guided deploy

```bash
./scripts/deploy.sh
```

The script asks for:

| Prompt | Answer |
|---|---|
| CLI profile | the profile from step 3 |
| Project ID / Branch ID | from step 3 (for example `my-project` / `production`) |
| Catalog / Schema | where the tables go (defaults `main` / `lb_health_check`) |
| SQL warehouse ID | pick one from the list it prints |
| Failure e-mails | optional, comma-separated |
| Service principal | an existing one (name or application ID), or `new` to create `lakebase-health-check` |
| `databricks_pii_access` | `y` to unmask query plans for the *New sequential-scan queries* trend |

It then gives the service principal CAN USE on the project, catalog access and a Postgres role, deploys the job,
dashboard and Genie space, runs the job once (about 5 minutes) and prints their links. If it cannot grant you
**Service principal: User** on the principal, it shows where to do it and waits. Your answers are saved in
`local/deploy.default.yml` (git-ignored); re-running the script is safe.

### Step 6 — Finish the grants the script cannot make

1. **`system.lakebase` access** — if the script warned about it, a metastore admin runs:
   ```sql
   GRANT USE CATALOG ON CATALOG system TO `<sp-application-id>`;
   GRANT USE SCHEMA, SELECT ON SCHEMA system.lakebase TO `<sp-application-id>`;
   ```
2. **Bloat estimates** — connected to the branch as the owner of the monitored tables, in each database:
   ```sql
   GRANT USAGE ON SCHEMA <schema> TO "<sp-application-id>";
   GRANT SELECT ON ALL TABLES IN SCHEMA <schema> TO "<sp-application-id>";
   ANALYZE;   -- tables never analyzed have no statistics and report "bloat unknown"
   ```
   Without the grant every table reports *bloat unknown*. `pg_read_all_stats` is not needed.

### Step 7 — Check the result and share it

- Open the dashboard link. The first run loads the last 48 hours; trends fill in from then on, and the 14-day
  review is complete after 28 days.
- The job runs every 4 hours (UTC). Failures are e-mailed to the addresses from step 5.
- Share the dashboard (**Share** → *Can view*; data is read with the publisher's credentials) and the Genie space
  (*Can run*). Genie queries run as the user, so reviewers also need `USE CATALOG`, `USE SCHEMA` and `SELECT` on
  the schema and **CAN USE** on the warehouse.

**Manual deploy (instead of the script):** set `project_id`, `branch_id` and `warehouse_id` for the `default`
target in `databricks.yml` (or pass `--var`), uncomment the job-level `run_as` and `permissions` blocks, then run
`databricks bundle deploy --profile <profile>` and `databricks bundle run lakebase_health --profile <profile>`, and
grant the service principal `USE SCHEMA`, `SELECT` and `MODIFY` on the new schema.

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

## Updating and uninstalling

- **Update:** `git pull`, then re-run `./scripts/deploy.sh` (or `databricks bundle deploy --profile <profile>`,
  which reuses `local/deploy.default.yml`).
- **Change the cadence:** edit `quartz_cron_expression` in `resources/lakebase_health.job.yml` and redeploy.
- **Uninstall:** `databricks bundle destroy --profile <profile>` removes the job, dashboard and Genie space. The
  data stays; drop it with `DROP SCHEMA <catalog>.<schema> CASCADE`. Remove the service principal's Postgres role
  with `databricks postgres delete-role projects/<p>/branches/<b>/roles/<role-id>` if no longer needed.

## Troubleshooting

| Symptom | Fix |
|---|---|
| `Cannot bind the service principal provided in 'run_as'` | Grant yourself **Service principal: User** on it (Settings → Identity and access → Service principals → *name* → Permissions), then re-run. |
| `The user '<sp>' does not have access to Databricks Workspace` | Give the service principal the **Workspace access** entitlement (Settings → Identity and access → Service principals → *name* → Configurations). The script now does this when you are a workspace admin. |
| `Could not read projects/<p>/branches/<b>` | Check the IDs (step 3) and that the service principal has CAN USE on the project. |
| No hourly rows, or an empty dashboard | Telemetry is not arriving (step 2), or the service principal cannot read `system.lakebase` (step 6.1). |
| Schema health shows *bloat unknown* | Step 6.2: grant `SELECT` and run `ANALYZE`. |
| *New sequential-scan queries* shows *not available* | Query plans are masked: add the service principal to `databricks_pii_access`. |
| Job failed in `postgres_snapshot` | The error lists the endpoint; rows from the other endpoints were saved. Check the Postgres role exists on the branch. |

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

Lakebase ends idle-in-transaction sessions after 5 minutes by default (`idle_in_transaction_session_timeout`), so
sessions over 300 s only appear where that setting was raised. Unused indexes are ≥ 10 MB with zero scans for 30
days, summed across the primary and replicas.

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
| `lb_health_daily` | endpoint × day, trends `t1_…`–`t11_…` as columns |
| `lb_health_review_summary` | trend × endpoint × review period (7 / 14): status, latest value, averages, days breached |
| `lb_health_objects_latest` | table or index in the latest snapshot: rows, scans, size, bloat, autovacuum |
| `lb_health_index_findings` | flagged index × day: `invalid`, `unused`, `duplicate` |
| `lb_health_seq_scan_queries` | query × day that sequentially scanned a large table |

## Development

Unit tests: `uv run --with pytest pytest tests -q`. The telemetry SQL is in `src/lb_health/sql/`, the formulas in
`src/lb_health/metrics.py`, the views in `src/lb_health/views.py`. Deploy to a test workspace with
`./scripts/deploy.sh --target dev`. The bloat SQL is from ioguix/pgsql-bloat-estimation (BSD-2, see
`src/lb_health/sql/`).
