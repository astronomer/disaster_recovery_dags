import os
import requests
import logging
import json
from airflow.utils.log.logging_mixin import LoggingMixin
import time

# Setup logging
log = logging.getLogger(__name__)
log.setLevel(logging.INFO)

ASTRO_API_TOKEN = os.getenv("ASTRO_API_TOKEN")
ASTRO_ORGANIZATION_ID = os.getenv("ASTRO_ORGANIZATION_ID")

if not ASTRO_API_TOKEN or not ASTRO_ORGANIZATION_ID:
    raise EnvironmentError("Missing ASTRO_API_TOKEN or ASTRO_ORGANIZATION_ID.")

HEADERS = {
    "Authorization": f"Bearer {ASTRO_API_TOKEN}",
    "Content-Type": "application/json"
}

BASE_URL = f"https://api.astronomer.io/platform/v1beta1/organizations/{ASTRO_ORGANIZATION_ID}"
IAM_BASE_URL = f"https://api.astronomer.io/iam/v1beta1/organizations/{ASTRO_ORGANIZATION_ID}"
WORKSPACES_URL = f"{BASE_URL}/workspaces"
DEPLOYMENTS_URL = f"{BASE_URL}/deployments"
WORKSPACES_JSON_PATH = os.path.join(os.path.dirname(__file__), "workspaces_to_backup.json")


def get_workspace_details_by_names(names):
    params = [("names", name) for name in names]
    resp = requests.get(WORKSPACES_URL, headers=HEADERS, params=params)
    resp.raise_for_status()
    return resp.json().get("workspaces", [])


def get_all_deployments():
    resp = requests.get(DEPLOYMENTS_URL, headers=HEADERS)
    resp.raise_for_status()
    return resp.json().get("deployments", [])


def get_workspace_hierarchy_with_deployments():
    with open(WORKSPACES_JSON_PATH, "r") as f:
        workspace_entries = json.load(f)

    workspace_names = {
        entry["source_workspace_name"] for entry in workspace_entries
    } | {
        entry["backup_workspace_name"] for entry in workspace_entries
    }

    log.info(f"Requesting metadata for workspaces: {workspace_names}")
    matched_workspaces = get_workspace_details_by_names(list(workspace_names))
    log.info(f"Resolved {len(matched_workspaces)} workspace names to IDs.")
    name_to_id = {ws["name"]: ws["id"] for ws in matched_workspaces}

    all_deployments = get_all_deployments()

    result = []
    for entry in workspace_entries:
        src_name = entry["source_workspace_name"]
        bkp_name = entry["backup_workspace_name"]
        src_id = name_to_id.get(src_name)
        bkp_id = name_to_id.get(bkp_name)

        if not src_id or not bkp_id:
            log.warning(f"[WARN] Skip {entry}, missing workspace IDs")
            continue

        src_deps = [
            {"deployment_id": d["id"], "deployment_name": d["name"]}
            for d in all_deployments if d["workspaceId"] == src_id
        ]
        bkp_deps = [
            {"deployment_id": d["id"], "deployment_name": d["name"]}
            for d in all_deployments if d["workspaceId"] == bkp_id
        ]

        result.append({
            "source": {
                "workspace_name": src_name,
                "workspace_id": src_id,
                "deployments": src_deps
            },
            "backup": {
                "workspace_name": bkp_name,
                "workspace_id": bkp_id,
                "deployments": bkp_deps
            }
        })

    return result

def retrieve_tokens(workspace_id: str = None, deployment_id: str = None) -> list:
    """
    Retrieves tokens for the given workspace_id or deployment_id (or both).
    """
    url = f"https://api.astronomer.io/iam/v1beta1/organizations/{ASTRO_ORGANIZATION_ID}/tokens"
    params = {}

    if workspace_id:
        params["workspaceId"] = workspace_id
    if deployment_id:
        params["deploymentId"] = deployment_id

    log.info(f"🔍 Retrieving tokens...")
    log.info(f"GET {url}")
    log.info(f"Params: {params}")
    log.info(f"Headers: [Authorization: Bearer ***REDACTED***]")
    log.info("")

    try:
        response = requests.get(url, headers=HEADERS, params=params, timeout=10)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        log.error(f"Failed to retrieve tokens: {e}")
        return []

    try:
        tokens = response.json().get("tokens", [])
        log.info(f"Retrieved {len(tokens)} tokens.")
        return tokens
    except Exception as e:
        log.error(f"Failed to parse token response JSON: {e}")
        return []


def get_token_details(token_id: str) -> dict | None:
    """
    Retrieves detailed information for a single token.
    """
    url = f"https://api.astronomer.io/iam/v1beta1/organizations/{ASTRO_ORGANIZATION_ID}/tokens/{token_id}"

    log.info("🔎 Fetching token details...")
    log.info(f"GET {url}")
    log.info(f"Headers: [Authorization: Bearer ***REDACTED***]")
    log.info("")

    try:
        response = requests.get(url, headers=HEADERS, timeout=10)
        response.raise_for_status()
        data = response.json()
    except requests.exceptions.RequestException as e:
        log.error(f"Failed to fetch token details: {e}")
        return None
    except Exception as e:
        log.error(f"Failed to parse token details JSON: {e}")
        return None

    log.info("📄 Token details received:")
    log.info(json.dumps(data, indent=2))
    log.info("")

    return data


def get_detailed_tokens(workspace_id: str = None, deployment_id: str = None) -> list:
    """
    Returns detailed information for all tokens that have roles in the specified
    workspace or deployment. Only one of workspace_id or deployment_id should be provided.
    """
    if not workspace_id and not deployment_id:
        raise ValueError("You must provide either workspace_id or deployment_id.")
    if workspace_id and deployment_id:
        raise ValueError("Only one of workspace_id or deployment_id can be provided at a time.")

    scope = f"workspace {workspace_id}" if workspace_id else f"deployment {deployment_id}"
    log.info(f"Retrieving tokens for {scope}...")

    raw_tokens = retrieve_tokens(workspace_id=workspace_id, deployment_id=deployment_id)
    log.info(f"Found {len(raw_tokens)} tokens with roles in the {scope}.")

    detailed_tokens = []
    for token in raw_tokens:
        token_id = token.get("id")
        if not token_id:
            log.warning("Skipping token without ID.")
            continue

        details = get_token_details(token_id)
        if details:
            detailed_tokens.append(details)
            log.info(f"✔ Retrieved details for token '{token.get('name')}' ({token_id})")
        else:
            log.warning(f"✘ Failed to retrieve details for token {token_id}")

    return detailed_tokens

def get_workspace_hierarchy_with_deployments_with_tokens():
    log.info("Loading backup plan...")
    with open(WORKSPACES_JSON_PATH, "r") as f:
        workspace_entries = json.load(f)

    workspace_names = {
        entry["source_workspace_name"] for entry in workspace_entries
    } | {
        entry["backup_workspace_name"] for entry in workspace_entries
    }

    log.info(f"Requesting metadata for workspaces: {workspace_names}")
    matched_workspaces = get_workspace_details_by_names(list(workspace_names))
    log.info(f"Resolved {len(matched_workspaces)} workspace names to IDs.")
    name_to_id = {ws["name"]: ws["id"] for ws in matched_workspaces}

    all_deployments = get_all_deployments()

    result = []
    for entry in workspace_entries:
        src_name = entry["source_workspace_name"]
        bkp_name = entry["backup_workspace_name"]
        src_id = name_to_id.get(src_name)
        bkp_id = name_to_id.get(bkp_name)

        if not src_id or not bkp_id:
            log.warning(f"[WARN] Skip {entry}, missing workspace IDs")
            continue

        # Add source deployments with tokens
        src_deps = []
        for d in all_deployments:
            if d["workspaceId"] == src_id:
                tokens = get_detailed_tokens(deployment_id=d["id"])
                src_deps.append({
                    "deployment_id": d["id"],
                    "deployment_name": d["name"],
                    "tokens": tokens
                })

        # Add backup deployments with tokens
        bkp_deps = []
        for d in all_deployments:
            if d["workspaceId"] == bkp_id:
                tokens = get_detailed_tokens(deployment_id=d["id"])
                bkp_deps.append({
                    "deployment_id": d["id"],
                    "deployment_name": d["name"],
                    "tokens": tokens
                })

        # Workspace-level tokens
        src_tokens = get_detailed_tokens(workspace_id=src_id)
        bkp_tokens = get_detailed_tokens(workspace_id=bkp_id)

        result.append({
            "source": {
                "workspace_name": src_name,
                "workspace_id": src_id,
                "tokens": src_tokens,
                "deployments": src_deps,
            },
            "backup": {
                "workspace_name": bkp_name,
                "workspace_id": bkp_id,
                "tokens": bkp_tokens,
                "deployments": bkp_deps,
            },
        })

    return result


def log_token_recreation_plan():
    plan = get_workspace_hierarchy_with_deployments_with_tokens()
    print(plan)


def create_token_for_backup_deployments():
    pass