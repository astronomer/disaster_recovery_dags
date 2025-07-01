import os
import json
import logging
import requests

ASTRO_ORGANIZATION_ID = os.getenv("ASTRO_ORGANIZATION_ID")
WORKSPACES_JSON_PATH = os.path.join("include", "workspaces_to_backup.json")
BASE_URL = f"https://api.astronomer.io/platform/v1beta1/organizations/{ASTRO_ORGANIZATION_ID}"
IAM_BASE_URL = f"https://api.astronomer.io/iam/v1beta1/organizations/{ASTRO_ORGANIZATION_ID}"
HEADERS = {
    "Authorization": f"Bearer {os.getenv('ASTRO_API_TOKEN')}",
    "Content-Type": "application/json"
}

log = logging.getLogger(__name__)


def get_summary(endpoint: str, result_key: str, params=None):
    if params is None:
        params = {}
    params["limit"] = 1000
    log.info(f"🔍 Fetching summary from {endpoint}?limit=1000")
    resp = requests.get(endpoint, headers=HEADERS, params=params)
    resp.raise_for_status()
    results = resp.json().get(result_key, [])
    log.info(f"✅ Retrieved {len(results)} {result_key}")
    return results


def get_detail(base_endpoint: str, resource_id: str):
    url = f"{base_endpoint}/{resource_id}"
    log.debug(f"🔍 Fetching details from {url}")
    try:
        resp = requests.get(url, headers=HEADERS)
        if resp.status_code == 404:
            log.warning(f"⚠️ Resource not found (likely deleted): {resource_id}")
            return None
        resp.raise_for_status()
        return resp.json()
    except requests.exceptions.RequestException as e:
        log.error(f"❌ Failed to fetch details from {url}: {e}")
        return None

def get_source_deployments(all_deployments: list, workspace_entries: list) -> list:
    """Return deployments that live in source workspaces (defined in workspace_entries)."""
    source_workspace_ids = {
        entry["source_workspace_id"] for entry in workspace_entries
    }

    filtered = [
        deployment
        for deployment in all_deployments
        if deployment.get("workspaceId") in source_workspace_ids
    ]

    log.info(f"📦 Found {len(filtered)} deployments in source workspaces.")
    for dep in filtered:
        log.info(
            f"   • {dep.get('id')} — {dep.get('name')} (Workspace: {dep.get('workspace_id')})"
        )

    return filtered

def is_relevant_token(token: dict, source_workspace_ids: set, source_deployment_ids: set) -> bool:
    token_id = token.get("id")
    roles = token.get("roles", [])
    if not roles:
        log.debug(f"❌ Token {token_id} has no roles.")
        return False

    for role in roles:
        entity_type = role.get("entityType")
        entity_id = role.get("entityId")
        log.info(
            f"🔍 Token {token_id} Role:\n"
            f"   • Type: {entity_type}\n"
            f"   • ID: {entity_id}"
        )
        if entity_type == "WORKSPACE" and entity_id in source_workspace_ids:
            log.info(f"✅ Token {token_id} is relevant via workspace {entity_id}")
            return True
        if entity_type == "DEPLOYMENT" and entity_id in source_deployment_ids:
            log.info(f"✅ Token {token_id} is relevant via deployment {entity_id}")
            return True

    log.info(f"❌ Token {token_id} rejected: no matching workspace or deployment")
    return False

def get_workspace_name_to_id_map() -> dict:
    """Fetch all workspaces and return a mapping of name → id using existing helpers."""
    workspaces = get_summary(f"{BASE_URL}/workspaces", "workspaces")
    name_to_id = {}
    for ws_summary in workspaces:
        ws_id = ws_summary.get("id")
        if not ws_id:
            continue
        ws_detail = get_detail(f"{BASE_URL}/workspaces", ws_id)
        if ws_detail:
            name = ws_detail.get("name")
            if name:
                name_to_id[name] = ws_id
    log.info(f"🔁 Built workspace name → ID map with {len(name_to_id)} entries")
    return name_to_id

def generate_organization_hierarchy():
    log.info("🔧 Fetching organization resource summaries...")

    # Load backup plan
    with open(WORKSPACES_JSON_PATH) as f:
        workspace_entries = json.load(f)

    # Fetch summaries
    all_workspaces = get_summary(f"{BASE_URL}/workspaces", "workspaces")
    all_deployments = get_summary(f"{BASE_URL}/deployments", "deployments")
    all_tokens_summary = get_summary(f"{IAM_BASE_URL}/tokens", "tokens")

    # Identify source workspace and deployment IDs
    source_workspace_ids = {entry["source_workspace_id"] for entry in workspace_entries}
    source_deployments = get_source_deployments(all_deployments, workspace_entries)
    source_deployment_ids = {dep["id"] for dep in source_deployments}

    # Skip ORGANIZATION scoped tokens early
    summary_filtered = [
        t for t in all_tokens_summary if t.get("type") != "ORGANIZATION"
    ]

    # Retrieve full token details
    detailed_tokens = []
    for token in summary_filtered:
        token_id = token.get("id")
        detailed = get_detail(f"{IAM_BASE_URL}/tokens", token_id)
        if detailed:
            detailed_tokens.append(detailed)

    # Filter by relevance
    relevant_tokens = [
        token for token in detailed_tokens
        if is_relevant_token(token, source_workspace_ids, source_deployment_ids)
    ]

    # Log what we retained
    log.info(f"✅ Retained {len(relevant_tokens)} relevant tokens out of {len(all_tokens_summary)} total")
    for token in relevant_tokens:
        log.info(
            f"🆔 {token.get('id')}\n"
            f"   • Name: {token.get('name')}\n"
            f"   • Type: {token.get('type')}\n"
            f"   • Roles: {json.dumps(token.get('roles'), indent=2)}\n"
        )

    # Test workspace name → ID mapping
    name_id_map = get_workspace_name_to_id_map()
    for name, id_ in name_id_map.items():
        log.info(f"🔗 Workspace '{name}' → {id_}")

    # Enrich backup plan with backup_workspace_id
    for entry in workspace_entries:
        bkp_name = entry.get("backup_workspace_name")
        entry["backup_workspace_id"] = name_id_map.get(bkp_name)

    # Return full organization context for downstream token replication
    return {
        "organization_id": ASTRO_ORGANIZATION_ID,
        "workspaces": workspace_entries,
        "tokens": relevant_tokens,  # already filtered
        "deployments": source_deployments,  # if needed later
    }

