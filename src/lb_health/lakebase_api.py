"""Lakebase (Postgres) control-plane helpers: projects → branches → endpoints, credentials.

Calls the /api/2.0/postgres REST API through WorkspaceClient.api_client, so it works with whatever
databricks-sdk version the serverless environment ships (no in-notebook SDK upgrade, which can
conflict with the preinstalled Spark Connect / protobuf stack).

ID conventions (verified against system.lakebase in dev):
  * API project ID   (projects/<project_id>)   == system.lakebase.*.project_name
  * API project uid                              == system.lakebase.*.project_id
  * API branch uid   (br-...)                    == system.lakebase.*.branch_id
  * API endpoint uid (ep-...)                    == system.lakebase.*.endpoint_id
lb_health_metrics stores the human-readable API IDs (project_id, branch_id, endpoint_id); the uids are
carried in the scope so the rollup can join telemetry.
"""
from __future__ import annotations

from typing import Iterable, Iterator

API = "/api/2.0/postgres"
RUNNING_STATES = {"active", "degraded"}


def _last(name: str | None) -> str:
    return (name or "").rstrip("/").split("/")[-1]


def _paged(w, path: str, key: str) -> Iterator[dict]:
    query = {"page_size": 100}
    while True:
        res = w.api_client.do("GET", path, query=query) or {}
        yield from res.get(key, [])
        token = res.get("next_page_token")
        if not token:
            return
        query = {"page_size": 100, "page_token": token}


def resolve_project_ids(w, project_name: str) -> list[str]:
    if project_name.strip().upper() == "ALL":
        return sorted(_last(p.get("name")) for p in _paged(w, f"{API}/projects", "projects"))
    return [p.strip() for p in project_name.split(",") if p.strip()]


def list_endpoints(w, project_ids: Iterable[str]) -> tuple[list[dict], list[str]]:
    """Return (endpoints, errors). Each endpoint is a JSON-serialisable dict."""
    endpoints, errors = [], []
    for pid in project_ids:
        try:
            project = w.api_client.do("GET", f"{API}/projects/{pid}") or {}
        except Exception as e:
            errors.append(f"project {pid}: {e}")
            continue
        try:
            branches = list(_paged(w, f"{API}/projects/{pid}/branches", "branches"))
        except Exception as e:
            errors.append(f"project {pid}: list branches failed: {e}")
            continue
        for b in branches:
            try:
                eps = list(_paged(w, f"{API}/{b['name']}/endpoints", "endpoints"))
            except Exception as e:
                errors.append(f"branch {b.get('name')}: list endpoints failed: {e}")
                continue
            for ep in eps:
                st = ep.get("status") or {}
                hosts = st.get("hosts") or {}
                endpoints.append({
                    "project_id": pid,
                    "project_uid": project.get("uid"),
                    "branch_id": _last(b.get("name")),
                    "branch_uid": b.get("uid"),
                    "endpoint_id": _last(ep.get("name")),
                    "endpoint_uid": ep.get("uid"),
                    "endpoint_name": ep.get("name"),
                    "host": hosts.get("host") or hosts.get("read_only_host"),
                    "endpoint_type": (st.get("endpoint_type") or "unknown").replace("ENDPOINT_TYPE_", "").lower(),
                    "state": (st.get("current_state") or "unknown").lower(),
                })
    return endpoints, errors


def is_running(endpoint: dict) -> bool:
    return endpoint.get("state") in RUNNING_STATES


def current_identity(w) -> str:
    """Postgres role name of the run identity: SP application ID or user e-mail."""
    return w.current_user.me().user_name


def database_password(w, endpoint_name: str) -> str:
    res = w.api_client.do("POST", f"{API}/credentials", body={"endpoint": endpoint_name}) or {}
    return res["token"]


def list_role_names(w, branch_name: str) -> set[str]:
    return {(r.get("spec") or {}).get("postgres_role") for r in _paged(w, f"{API}/{branch_name}/roles", "roles")}


def create_role(w, branch_name: str, role_id: str, identity: str, identity_type: str) -> dict:
    """Start role creation (long-running operation); returns the operation."""
    return w.api_client.do("POST", f"{API}/{branch_name}/roles", query={"role_id": role_id},
                           body={"spec": {"identity_type": identity_type, "postgres_role": identity}})
