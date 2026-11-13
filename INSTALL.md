# Installing Lakebase Health Check

Step-by-step guide to deploy the bundle into your workspace. For what it monitors and how to run a
review, see the [README](README.md).

## Step 1 — Check what you need

| What | Why |
|---|---|
| A Mac or Linux machine (Windows: WSL) with `bash`, `git`, `python3` and the [Databricks CLI](https://docs.databricks.com/dev-tools/cli/install.html) 1.3 or later | runs the deploy script |
| **CAN MANAGE** on the Lakebase project | grants the run identity access to the project and creates its Postgres role |
| Workspace admin, or an existing service principal you hold **Service principal: User** on | the job runs as a service principal |
| `CREATE SCHEMA` on a Unity Catalog catalog (or an existing schema you can write to) | holds the tables and views |
| A SQL warehouse you can use | dashboard, Genie space and table setup |
| The service principal has **CAN USE** on that warehouse (the script grants it if you can manage the warehouse; otherwise a warehouse owner or admin grants it once) | the alerts run on it as the service principal |
| A metastore admin, once (unless `account users` can already read `system.lakebase`) | grants the service principal read access to `system.lakebase` |

## Step 2 — Turn on Lakebase telemetry

In the workspace **Previews** page, enable **Lakebase Observability in System Tables**. Then check that data
arrives (SQL editor):

```sql
SELECT project_name, max(ts) FROM system.lakebase.compute_gauges GROUP BY 1;
```

## Step 3 — Log in with the Databricks CLI

```bash
databricks auth login --host https://<your-workspace>.cloud.databricks.com --profile <profile-name>
```

Find the project and branch IDs you want to monitor:

```bash
databricks postgres list-projects --profile <profile-name>                      # projects/<project_id>
databricks postgres list-branches projects/<project_id> --profile <profile-name>  # .../branches/<branch_id>
```

## Step 4 — Get the code

```bash
git clone https://github.com/rakesharumalla/lakebase-health-check.git
cd lakebase-health-check
```

## Step 5 — Run the guided deploy

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
| E-mails | optional, comma-separated: notified of job failures and alerts |
| Service principal | an existing one (name or application ID), or `new` to create `lakebase-health-check` |
| `databricks_pii_access` | `y` to unmask query plans for the *New sequential-scan queries* trend |

It then gives the service principal CAN USE on the project, catalog access and a Postgres role, deploys the job,
dashboard and Genie space, runs the job once (about 5 minutes) and prints their links. If it cannot grant you
**Service principal: User** on the principal, it shows where to do it and waits. Your answers are saved in
`local/deploy.default.yml` (git-ignored); re-running the script is safe.

## Step 6 — Finish the grants the script cannot make

These are two different places: 6.1 runs in the **lakehouse** (Unity Catalog), 6.2 runs **inside Lakebase
Postgres**. Unity Catalog grants do not reach Postgres, and the two use different SQL.

1. **`system.lakebase` access — lakehouse.** Only if the script warned about it. A metastore admin runs this in
   the Databricks **SQL editor** (on a SQL warehouse); identifiers use backticks:
   ```sql
   GRANT USE CATALOG ON CATALOG system TO `<sp-application-id>`;
   GRANT USE SCHEMA, SELECT ON SCHEMA system.lakebase TO `<sp-application-id>`;   -- USAGE also works for USE SCHEMA
   ```
2. **Bloat estimates — Lakebase Postgres.** The bloat queries read Postgres's own `pg_stats`, which only shows
   tables the role can `SELECT`. Connect to the branch (the Lakebase project's **SQL editor**, or `psql`) as the
   owner of the monitored tables and run, in each database and for each schema to monitor (Postgres syntax:
   double quotes, and `SELECT` is granted on tables, not on the schema):
   ```sql
   GRANT USAGE ON SCHEMA <schema> TO "<sp-application-id>";
   GRANT SELECT ON ALL TABLES IN SCHEMA <schema> TO "<sp-application-id>";
   ALTER DEFAULT PRIVILEGES IN SCHEMA <schema> GRANT SELECT ON TABLES TO "<sp-application-id>";  -- future tables
   ANALYZE;   -- tables never analyzed have no statistics and report "bloat unknown"
   ```
   Without these every table reports *bloat unknown*. `pg_read_all_stats` is not needed.

## Step 7 — Check the result and share it

- Open the dashboard link. What you see first depends on how much history the project has:
  - **Schema health** shows data after the first run (it uses the latest Postgres snapshot).
  - **Review summary** and **Daily trends** use **complete UTC days** only. The first run loads up to the last
    48 hours, so an existing project shows its last two days right away. A project created today shows them after
    the first run after 00:00 UTC tomorrow; until then those pages are empty. Hourly values are already in
    `lb_health_hourly` (ask Genie, e.g. *"Show connection utilization by hour today"*).
  - Comparisons with the prior period need history: the 7-day review is complete after 14 days, the 14-day review
    after 28. *CPU per connection* shows *info* until a prior period exists.
- The job runs every 4 hours (UTC). Failures are e-mailed to the addresses from step 5.
- Share the dashboard (**Share** → *Can view*; data is read with the publisher's credentials) and the Genie space
  (*Can run*). Genie queries run as the user, so reviewers also need `USE CATALOG`, `USE SCHEMA` and `SELECT` on
  the schema and **CAN USE** on the warehouse.

**Manual deploy (instead of the script):** set `project_id`, `branch_id`, `warehouse_id` and `pg_version` (the
project's Postgres major version, used by the alerts) for the `default` target in `databricks.yml` (or pass `--var`), uncomment the job-level `run_as` and `permissions` blocks, then run
`databricks bundle deploy --profile <profile>` and `databricks bundle run lakebase_health --profile <profile>`, and
grant the service principal `USE SCHEMA`, `SELECT` and `MODIFY` on the new schema.

## Updating and uninstalling

- **Update:** `git pull`, then re-run `./scripts/deploy.sh` (or `databricks bundle deploy --profile <profile>`,
  which reuses `local/deploy.default.yml`).
- **Alert cost:** the alerts run every 5 minutes, which keeps their SQL warehouse from auto-stopping. Point
  `warehouse_id` at a warehouse that already runs all day, or expect it to stay up around the clock.
- **Pause the alerts:** set the `alerts_pause_status` variable to `PAUSED` and redeploy (the job keeps running).
- **Change the cadence:** edit `quartz_cron_expression` in `resources/lakebase_health.job.yml` and redeploy.
- **Uninstall:** `databricks bundle destroy --profile <profile>` removes the job, dashboard, Genie space and alerts. The
  data stays; drop it with `DROP SCHEMA <catalog>.<schema> CASCADE`. Remove the service principal's Postgres role
  with `databricks postgres delete-role projects/<p>/branches/<b>/roles/<role-id>` if no longer needed.

## Troubleshooting

| Symptom | Fix |
|---|---|
| `Cannot bind the service principal provided in 'run_as'` | Grant yourself **Service principal: User** on it (Settings → Identity and access → Service principals → *name* → Permissions), then re-run. |
| `The user '<sp>' does not have access to Databricks Workspace` | Give the service principal the **Workspace access** entitlement (Settings → Identity and access → Service principals → *name* → Configurations). The script now does this when you are a workspace admin. |
| `Could not read projects/<p>/branches/<b>` | Check the IDs (step 3) and that the service principal has CAN USE on the project. |
| Review summary and Daily trends are empty, Schema health has data | No complete UTC day yet (new project): they fill in after the first run after 00:00 UTC (step 7 above). |
| No hourly rows, or an empty dashboard | Telemetry is not arriving (step 2), or the service principal cannot read `system.lakebase` (step 6.1). |
| Schema health shows *bloat unknown* | Step 6.2: grant `SELECT` and run `ANALYZE`. |
| *New sequential-scan queries* shows *not available* | Query plans are masked: add the service principal to `databricks_pii_access`. |
| An alert is in *Error* | The service principal needs CAN USE on the warehouse (SQL Warehouses → *warehouse* → Permissions) and SELECT on `system.lakebase` (step 6.1). |
| Job failed in `postgres_snapshot` | The error lists the endpoint; rows from the other endpoints were saved. Check the Postgres role exists on the branch. |
