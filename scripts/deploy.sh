#!/usr/bin/env bash
# Lakebase Health Check — guided deployment.
#
# Asks for the settings, prepares the run identity (service principal), deploys the bundle, runs the job
# once and checks that the job, the dashboard and the Genie space are in place.
#
#   ./scripts/deploy.sh                 # interactive
#   ./scripts/deploy.sh --target dev    # deploy another bundle target (default: default)
#
# Every prompt can be answered in advance with an environment variable (for unattended runs):
#   LBH_PROFILE LBH_PROJECT_ID LBH_BRANCH_ID LBH_CATALOG LBH_SCHEMA LBH_WAREHOUSE_ID LBH_EMAILS
#   LBH_SP (application ID or display name; "new" = create) LBH_SP_NAME (name for a new SP)
#   LBH_ACCOUNT_ID LBH_PII_ACCESS (y/n) LBH_YES=1 (skip the final confirmation)
#
# Your choices are written to local/deploy.<target>.yml (git-ignored), which the bundle includes, so a
# later plain `databricks bundle deploy -t <target>` keeps them.
set -euo pipefail
cd "$(dirname "$0")/.."

TARGET="default"
while [[ $# -gt 0 ]]; do
  case "$1" in
    -t|--target) TARGET="$2"; shift 2 ;;
    -h|--help) sed -n '2,17p' "$0"; exit 0 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done

# ---------- helpers ---------------------------------------------------------------------------------
bold() { printf '\033[1m%s\033[0m\n' "$*"; }
ok()   { printf '  \033[32m✓\033[0m %s\n' "$*"; }
warn() { printf '  \033[33m!\033[0m %s\n' "$*"; }
die()  { printf '\033[31mERROR:\033[0m %s\n' "$*" >&2; exit 1; }

ask() {  # ask VAR "Prompt" [default] — uses $VAR from the environment when set
  local var="$1" prompt="$2" def="${3:-}" val="${!1:-}"
  if [[ -z "$val" ]]; then
    if [[ -n "$def" ]]; then read -r -p "  $prompt [$def]: " val; val="${val:-$def}"
    else read -r -p "  $prompt: " val; fi
  fi
  printf -v "$var" '%s' "$val"
}

json() {  # json 'python expression on d' < json
  python3 -c "import json,sys; d=json.load(sys.stdin); v=($1); print('' if v is None else v)"
}

dbx() { databricks "$@" --profile "$LBH_PROFILE"; }

sql() {  # sql "statement" → prints the state (and the error), returns 1 on failure
  local body out state
  body=$(python3 -c 'import json,sys; print(json.dumps({"warehouse_id": sys.argv[1], "statement": sys.argv[2], "wait_timeout": "50s"}))' "$LBH_WAREHOUSE_ID" "$1")
  out=$(dbx api post /api/2.0/sql/statements --json "$body")
  state=$(echo "$out" | json 'd["status"]["state"]')
  if [[ "$state" != "SUCCEEDED" ]]; then
    SQL_ERROR=$(echo "$out" | json 'd["status"].get("error", {}).get("message", d["status"])')
    return 1
  fi
  SQL_RESULT="$out"
}

sp_user_help() {  # instructions for the 'Service principal: User' role the deployer needs for run_as
  cat <<HELP

  To deploy a job that runs as the service principal, YOUR user needs the 'Service principal: User'
  role on it (being a workspace admin, or having created it, is not enough):
    1. Open ${HOST:-the workspace} > Settings > Identity and access > Service principals
    2. Open '${SP_DISPLAY:-$SP_APP_ID}' (application ID $SP_APP_ID) > Permissions
    3. Grant access: $DEPLOYER, role 'Service principal: User'
  If you cannot change its permissions, ask an account or workspace admin to grant it
  (account console > User management > Service principals > '${SP_DISPLAY:-$SP_APP_ID}' > Permissions).

HELP
}

wait_for_sp_user() {  # interactive: pause until the user has granted the role; unattended: stop
  sp_user_help
  if [[ "${LBH_YES:-}" == "1" ]]; then die "grant the role, then re-run the script"; fi
  read -r -p "  Press Enter once the role is granted (Ctrl-C to stop) " _
}

# ---------- 0. prerequisites ------------------------------------------------------------------------
bold "Lakebase Health Check — deploy (target: $TARGET)"
command -v databricks >/dev/null || die "Databricks CLI not found (https://docs.databricks.com/dev-tools/cli/install.html)"
command -v python3 >/dev/null || die "python3 not found"

# ---------- 1. settings -----------------------------------------------------------------------------
bold "1. Workspace"
ask LBH_PROFILE "CLI profile for the target workspace (list them with: databricks auth profiles)"
[[ -n "$LBH_PROFILE" ]] || die "a profile is required"
DEPLOYER=$(dbx current-user me -o json | json 'd["userName"]') || die "cannot authenticate with profile $LBH_PROFILE (run: databricks auth login --profile $LBH_PROFILE)"
HOST=$(databricks auth env --profile "$LBH_PROFILE" 2>/dev/null | json 'd["env"].get("DATABRICKS_HOST","")' || true)
ok "authenticated as $DEPLOYER ${HOST:+on $HOST}"

bold "2. Lakebase scope"
ask LBH_PROJECT_ID "Lakebase project ID (projects/<project_id>)"
ask LBH_BRANCH_ID "Branch ID (projects/<project>/branches/<branch_id>)" "production"
dbx api get "/api/2.0/postgres/projects/$LBH_PROJECT_ID/branches/$LBH_BRANCH_ID" >/dev/null 2>&1 \
  || die "branch projects/$LBH_PROJECT_ID/branches/$LBH_BRANCH_ID not found or not visible to $DEPLOYER"
EPS=$(dbx api get "/api/2.0/postgres/projects/$LBH_PROJECT_ID/branches/$LBH_BRANCH_ID/endpoints" \
  | json '", ".join(e["name"].split("/")[-1] + " (" + (e.get("status") or {}).get("endpoint_type","?").replace("ENDPOINT_TYPE_","").lower() + ")" for e in d.get("endpoints", []))')
ok "branch found; endpoints: ${EPS:-none}"
# For the alerts: system.lakebase keys rows by the project UID, and ASH state codes depend on the Postgres version.
PROJECT=$(dbx api get "/api/2.0/postgres/projects/$LBH_PROJECT_ID")
PROJECT_UUID=$(echo "$PROJECT" | json 'd.get("uid")')
PG_VERSION=$(echo "$PROJECT" | json '(d.get("status") or {}).get("pg_version")')
[[ -n "$PROJECT_UUID" && -n "$PG_VERSION" ]] || die "could not read the UID / Postgres version of project $LBH_PROJECT_ID"
ok "project UID $PROJECT_UUID, Postgres $PG_VERSION"

bold "3. Where to store the data"
ask LBH_CATALOG "Unity Catalog catalog" "main"
ask LBH_SCHEMA "Schema (created if missing)" "lb_health_check"
dbx catalogs get "$LBH_CATALOG" >/dev/null 2>&1 || die "catalog $LBH_CATALOG not found or not visible"
ok "catalog $LBH_CATALOG found"
echo "  SQL warehouses:"
dbx warehouses list -o json | json '"\n".join("    " + w["id"] + "  " + w["name"] + "  (" + str(w.get("state", "")) + ")" for w in (d if isinstance(d, list) else d.get("warehouses", [])))'
ask LBH_WAREHOUSE_ID "SQL warehouse ID (dashboard, Genie, table setup)"
dbx warehouses get "$LBH_WAREHOUSE_ID" >/dev/null 2>&1 || die "warehouse $LBH_WAREHOUSE_ID not found"
ask LBH_EMAILS "E-mails to notify of job failures and alerts (comma-separated, optional)" "-"
[[ "$LBH_EMAILS" == "-" ]] && LBH_EMAILS=""

bold "4. Run identity (service principal)"
ask LBH_SP "Service principal application ID or display name (\"new\" to create one)" "new"
if [[ "$LBH_SP" == "new" ]]; then
  ask LBH_SP_NAME "Display name for the new service principal" "lakebase-health-check"
  EXISTING=$(dbx service-principals list --filter "displayName eq \"$LBH_SP_NAME\"" -o json | json '(d if isinstance(d, list) else d.get("Resources", []) or [{}])[0].get("applicationId") if (d if isinstance(d, list) else d.get("Resources")) else None')
  if [[ -n "$EXISTING" ]]; then
    SP_APP_ID="$EXISTING"; ok "service principal $LBH_SP_NAME already exists ($SP_APP_ID); reusing it"
  else
    SP_APP_ID=$(dbx service-principals create --display-name "$LBH_SP_NAME" -o json | json 'd["applicationId"]')
    ok "created service principal $LBH_SP_NAME ($SP_APP_ID)"
  fi
else
  SP_APP_ID=$(dbx service-principals list --filter "applicationId eq \"$LBH_SP\"" -o json \
    | json '(d if isinstance(d, list) else d.get("Resources", []))[0]["applicationId"] if (d if isinstance(d, list) else d.get("Resources")) else None' 2>/dev/null || true)
  if [[ -z "$SP_APP_ID" ]]; then
    SP_APP_ID=$(dbx service-principals list --filter "displayName eq \"$LBH_SP\"" -o json \
      | json '(d if isinstance(d, list) else d.get("Resources", []))[0]["applicationId"] if (d if isinstance(d, list) else d.get("Resources")) else None' 2>/dev/null || true)
  fi
  [[ -n "$SP_APP_ID" ]] || die "service principal '$LBH_SP' not found in this workspace"
  ok "using service principal $SP_APP_ID"
fi
SP_DISPLAY=$(dbx service-principals list --filter "applicationId eq \"$SP_APP_ID\"" -o json \
  | json '(d if isinstance(d, list) else d.get("Resources", []))[0].get("displayName")' 2>/dev/null || true)
ask LBH_PII_ACCESS "Add it to databricks_pii_access (unmasked query plans for the 'new seq-scan queries' trend)? y/n" "n"

bold "Summary"
cat <<EOF
  target            $TARGET  (profile $LBH_PROFILE)
  project / branch  $LBH_PROJECT_ID / $LBH_BRANCH_ID
  tables            $LBH_CATALOG.$LBH_SCHEMA   warehouse $LBH_WAREHOUSE_ID
  runs as           $SP_APP_ID
  failure e-mails   ${LBH_EMAILS:-none}
EOF
if [[ "${LBH_YES:-}" != "1" ]]; then
  read -r -p "  Continue? [y/N] " go; [[ "$go" =~ ^[Yy] ]] || die "cancelled"
fi

# ---------- 2. grants for the service principal ------------------------------------------------------
bold "5. Access for the service principal"
# Service principal: User for the deployer, so the job can run as the SP.
ACCOUNT_ID="${LBH_ACCOUNT_ID:-}"
if [[ -z "$ACCOUNT_ID" ]] && sql "SELECT account_id FROM system.billing.usage LIMIT 1"; then
  ACCOUNT_ID=$(echo "$SQL_RESULT" | json '(d.get("result", {}).get("data_array") or [[None]])[0][0]')
fi
[[ -n "$ACCOUNT_ID" ]] || ask ACCOUNT_ID "Databricks account ID (needed to grant you 'Service principal: User')"
RS_NAME="accounts/$ACCOUNT_ID/servicePrincipals/$SP_APP_ID/ruleSets/default"
if RS=$(dbx api get "/api/2.0/preview/accounts/access-control/rule-sets?name=$RS_NAME&etag=" 2>/dev/null); then
  HAS=$(echo "$RS" | python3 -c '
import json, sys
d = json.load(sys.stdin); me = "users/" + sys.argv[1]
print(any(me in r.get("principals", []) for r in d.get("grant_rules", [])
          if r.get("role") in ("roles/servicePrincipal.user", "roles/servicePrincipal.manager")))' "$DEPLOYER")
  if [[ "$HAS" == "True" ]]; then
    ok "you have 'Service principal: User' on $SP_APP_ID"
  else
    BODY=$(echo "$RS" | python3 -c '
import json, sys
d = json.load(sys.stdin); rules = d.get("grant_rules", [])
rules.append({"role": "roles/servicePrincipal.user", "principals": ["users/" + sys.argv[1]]})
print(json.dumps({"name": sys.argv[2], "rule_set": {"name": sys.argv[2], "etag": d["etag"], "grant_rules": rules}}))' "$DEPLOYER" "$RS_NAME")
    dbx api put /api/2.0/preview/accounts/access-control/rule-sets --json "$BODY" >/dev/null \
      && ok "granted you 'Service principal: User' on $SP_APP_ID" \
      || { warn "could not grant you 'Service principal: User' on $SP_APP_ID automatically"; wait_for_sp_user; }
  fi
else
  warn "cannot check whether you have 'Service principal: User' on $SP_APP_ID (no access to its rules)"
  if [[ "${LBH_YES:-}" == "1" ]]; then
    warn "continuing; the deploy step stops with instructions if the role is missing"
  else
    sp_user_help
    read -r -p "  Already granted? Press Enter to continue (Ctrl-C to stop) " _
  fi
fi

# Workspace access entitlement: without it the job cannot start ("does not have access to Databricks
# Workspace"). Some workspaces do not give it to new service principals by default.
SP_SCIM=$(dbx service-principals list --filter "applicationId eq \"$SP_APP_ID\"" -o json)
SP_ID=$(echo "$SP_SCIM" | json '(d if isinstance(d, list) else d.get("Resources", []))[0]["id"]')
HAS_WS=$(echo "$SP_SCIM" | json 'any(e.get("value") == "workspace-access" for e in ((d if isinstance(d, list) else d.get("Resources", []))[0].get("entitlements") or []))')
if [[ "$HAS_WS" == "True" ]]; then
  ok "service principal has Workspace access"
elif dbx service-principals patch "$SP_ID" --json '{"schemas":["urn:ietf:params:scim:api:messages:2.0:PatchOp"],"Operations":[{"op":"add","path":"entitlements","value":[{"value":"workspace-access"}]}]}' >/dev/null 2>&1; then
  ok "gave the service principal Workspace access"
else
  warn "could not give the service principal Workspace access (needs a workspace admin):"
  warn "Settings > Identity and access > Service principals > '${SP_DISPLAY:-$SP_APP_ID}' > Configurations > Workspace access"
  if [[ "${LBH_YES:-}" == "1" ]]; then die "grant Workspace access, then re-run the script"; fi
  read -r -p "  Press Enter once it is granted (Ctrl-C to stop) " _
fi

# Lakebase project: CAN USE (database credentials).
dbx api patch "/api/2.0/permissions/database-projects/$LBH_PROJECT_ID" \
  --json "{\"access_control_list\":[{\"service_principal_name\":\"$SP_APP_ID\",\"permission_level\":\"CAN_USE\"}]}" >/dev/null \
  && ok "CAN USE on project $LBH_PROJECT_ID" || die "could not grant CAN USE on project $LBH_PROJECT_ID (you need CAN MANAGE)"

# SQL warehouse: CAN USE (the alerts run on it as the service principal; the job itself uses serverless compute).
dbx api patch "/api/2.0/permissions/warehouses/$LBH_WAREHOUSE_ID" \
  --json "{\"access_control_list\":[{\"service_principal_name\":\"$SP_APP_ID\",\"permission_level\":\"CAN_USE\"}]}" >/dev/null \
  && ok "CAN USE on warehouse $LBH_WAREHOUSE_ID" || die "could not grant CAN USE on warehouse $LBH_WAREHOUSE_ID (you need CAN MANAGE)"

# Unity Catalog: system.lakebase and the target catalog.
for stmt in "GRANT USE CATALOG ON CATALOG system TO \`$SP_APP_ID\`" \
            "GRANT USE SCHEMA, SELECT ON SCHEMA system.lakebase TO \`$SP_APP_ID\`"; do
  if sql "$stmt"; then ok "$stmt"; else
    warn "$stmt — not granted ($SQL_ERROR). A metastore admin must run it, unless the SP already has access (e.g. through 'account users')."
  fi
done
sql "GRANT USE CATALOG ON CATALOG \`$LBH_CATALOG\` TO \`$SP_APP_ID\`" && ok "USE CATALOG on $LBH_CATALOG" \
  || die "GRANT USE CATALOG on $LBH_CATALOG failed: $SQL_ERROR"

# Postgres role on the branch.
BRANCH="projects/$LBH_PROJECT_ID/branches/$LBH_BRANCH_ID"
if dbx postgres list-roles "$BRANCH" -o json | python3 -c '
import json, sys
d = json.load(sys.stdin); d = d if isinstance(d, list) else d.get("roles", [])
sys.exit(0 if any((r.get("status") or {}).get("postgres_role") == sys.argv[1] for r in d) else 1)' "$SP_APP_ID"; then
  ok "Postgres role for $SP_APP_ID exists on $BRANCH"
else
  ROLE_ID="lbh-$(echo "$SP_APP_ID" | cut -c1-8)"
  dbx postgres create-role "$BRANCH" --role-id "$ROLE_ID" \
    --json "{\"spec\":{\"identity_type\":\"SERVICE_PRINCIPAL\",\"postgres_role\":\"$SP_APP_ID\",\"auth_method\":\"LAKEBASE_OAUTH_V1\"}}" >/dev/null \
    && ok "created Postgres role $ROLE_ID on $BRANCH" || die "could not create the Postgres role on $BRANCH"
fi

if [[ "$LBH_PII_ACCESS" =~ ^[Yy] ]]; then
  GID=$(dbx groups list --filter 'displayName eq "databricks_pii_access"' -o json \
    | json '(d if isinstance(d, list) else d.get("Resources", []))[0]["id"] if (d if isinstance(d, list) else d.get("Resources")) else None' 2>/dev/null || true)
  if [[ -n "$GID" ]] && dbx groups patch "$GID" --json "{\"schemas\":[\"urn:ietf:params:scim:api:messages:2.0:PatchOp\"],\"Operations\":[{\"op\":\"add\",\"path\":\"members\",\"value\":[{\"value\":\"$SP_ID\"}]}]}" >/dev/null 2>&1; then
    ok "added to databricks_pii_access"
  else
    warn "could not add the SP to databricks_pii_access (group missing or no permission); query plans stay masked"
  fi
fi

# ---------- 3. deploy -------------------------------------------------------------------------------
bold "6. Deploy"
mkdir -p local
EMAILS_YAML=$(python3 -c 'import json,sys; print(json.dumps([e.strip() for e in sys.argv[1].split(",") if e.strip()]))' "$LBH_EMAILS")
SUBS_YAML=$(python3 -c 'import json,sys; print(json.dumps([{"user_email": e.strip()} for e in sys.argv[1].split(",") if e.strip()]))' "$LBH_EMAILS")
# Every alert in resources/lb_health_alerts.yml runs as the service principal too. An alert that runs as a
# service principal needs CAN_RUN on itself (the bundle-wide CAN_VIEW below only gives CAN_READ): without it every
# evaluation ends in ERROR.
ALERTS_RUN_AS=$(python3 -c '
import re, sys
keys = re.findall(r"^    (lb_r\w+):$", open("resources/lb_health_alerts.yml").read(), re.M)
print("\n".join(f"        {k}:\n          run_as:\n            service_principal_name: \"{sys.argv[1]}\"\n"
                f"          permissions:\n            - service_principal_name: \"{sys.argv[1]}\"\n              level: CAN_RUN"
                for k in keys))' "$SP_APP_ID")
cat > "local/deploy.$TARGET.yml" <<EOF
# Written by scripts/deploy.sh — git-ignored. Re-run the script, or edit and run
# 'databricks bundle deploy -t $TARGET --profile $LBH_PROFILE'.
targets:
  $TARGET:
    resources:
      jobs:
        lakebase_health:
          run_as:                # job only: the dashboard embeds the deployer's credentials
            service_principal_name: "$SP_APP_ID"
      alerts:
$ALERTS_RUN_AS
    permissions:
      - user_name: "$DEPLOYER"
        level: CAN_MANAGE
      - service_principal_name: "$SP_APP_ID"
        level: CAN_VIEW          # read access to the deployed notebooks
    variables:
      project_id: "$LBH_PROJECT_ID"
      branch_id: "$LBH_BRANCH_ID"
      target_catalog_name: "$LBH_CATALOG"
      target_schema_name: "$LBH_SCHEMA"
      warehouse_id: "$LBH_WAREHOUSE_ID"
      failure_notification_emails: $EMAILS_YAML
      project_uuid: "$PROJECT_UUID"
      pg_version: "$PG_VERSION"
      alert_subscriptions: $SUBS_YAML
EOF
ok "wrote local/deploy.$TARGET.yml"
dbx bundle validate -t "$TARGET" >/dev/null || { dbx bundle validate -t "$TARGET"; die "bundle validation failed"; }
ok "bundle validated"
DEPLOY_LOG=$(mktemp)
while true; do
  set +e
  dbx bundle deploy -t "$TARGET" 2>&1 | tee "$DEPLOY_LOG"
  rc=${PIPESTATUS[0]}
  set -e
  [[ $rc -eq 0 ]] && break
  if grep -q "Cannot bind the service principal" "$DEPLOY_LOG"; then
    warn "deploy stopped: you do not have 'Service principal: User' on $SP_APP_ID yet"
    warn "(anything already created, such as the dashboard or Genie space, is kept; the retry adds the rest)"
    wait_for_sp_user
    continue
  fi
  rm -f "$DEPLOY_LOG"; die "bundle deploy failed (see the output above)"
done
rm -f "$DEPLOY_LOG"
ok "bundle deployed"

for stmt in "GRANT USE SCHEMA, SELECT, MODIFY ON SCHEMA \`$LBH_CATALOG\`.\`$LBH_SCHEMA\` TO \`$SP_APP_ID\`"; do
  sql "$stmt" && ok "$stmt" || die "$stmt failed: $SQL_ERROR"
done

# ---------- 4. first run and checks ------------------------------------------------------------------
bold "7. First run (about 5 minutes)"
if dbx bundle run lakebase_health -t "$TARGET"; then ok "job run succeeded"; else
  die "the job run failed; open the run URL above for the task output"
fi

bold "8. Checks"
SUMMARY=$(dbx bundle summary -t "$TARGET" -o json)
JOB_ID=$(echo "$SUMMARY" | json 'd["resources"]["jobs"]["lakebase_health"]["id"]')
DASH_ID=$(echo "$SUMMARY" | json 'd["resources"]["dashboards"]["lb_health_review"]["id"]')
GENIE_ID=$(echo "$SUMMARY" | json 'list(d["resources"]["genie_spaces"].values())[0]["id"]')
N_ALERTS=$(echo "$SUMMARY" | json 'sum(1 for a in d["resources"].get("alerts", {}).values() if a.get("id"))')
[[ "$N_ALERTS" == "8" ]] && ok "8 alerts deployed (every 5 minutes, run as $SP_APP_ID)" || warn "expected 8 alerts, found $N_ALERTS"
RUN_AS=$(dbx jobs get "$JOB_ID" -o json | json '(d["settings"].get("run_as") or {}).get("service_principal_name") or d.get("run_as_user_name")')
[[ "$RUN_AS" == "$SP_APP_ID" ]] && ok "job $JOB_ID runs as $SP_APP_ID" || warn "job $JOB_ID runs as $RUN_AS"
dbx lakeview get-published "$DASH_ID" >/dev/null 2>&1 && ok "dashboard published ($DASH_ID)" || die "dashboard $DASH_ID is not published"
dbx api get "/api/2.0/genie/spaces/$GENIE_ID" >/dev/null 2>&1 && ok "Genie space available ($GENIE_ID)" || die "Genie space $GENIE_ID not found"
if sql "SELECT count(*) FROM \`$LBH_CATALOG\`.\`$LBH_SCHEMA\`.lb_health_metrics WHERE grain = 'hour'"; then
  ok "hourly rows loaded: $(echo "$SQL_RESULT" | json 'd["result"]["data_array"][0][0]')"
fi

bold "Done"
cat <<EOF
  Job:        ${HOST}/jobs/$JOB_ID
  Dashboard:  ${HOST}/dashboardsv3/$DASH_ID/published
  Genie:      ${HOST}/genie/rooms/$GENIE_ID

  For bloat estimates the Postgres role needs read access to the monitored tables. In Lakebase (not the
  Databricks SQL editor): connect to the branch as the table owner and,
  in each database, run for every schema to monitor:
    GRANT USAGE ON SCHEMA <schema> TO "$SP_APP_ID";
    GRANT SELECT ON ALL TABLES IN SCHEMA <schema> TO "$SP_APP_ID";
  Tables that were never ANALYZEd report "bloat unknown"; run ANALYZE on them.
EOF
