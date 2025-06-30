import os
import json
import requests

ASTRO_API_TOKEN = os.getenv("ASTRO_API_TOKEN")
ORG_ID = os.getenv("ASTRO_ORGANIZATION_ID")

DEPLOYMENTS_URL = f"https://api.astronomer.io/platform/v1beta1/organizations/{ORG_ID}/deployments"
WORKSPACES_URL = f"https://api.astronomer.io/platform/v1beta1/organizations/{ORG_ID}/workspaces"

HEADERS = {
    "Authorization": f"Bearer {ASTRO_API_TOKEN}",
    "Content-Type": "application/json"
}


def get_deployments(mode="source", workspace_list_path="include/workspaces_to_backup.json"):
    with open(workspace_list_path, "r") as f:
        workspace_entries = json.load(f)

    # Step 1: Get all deployments in the org
    deployments_response = requests.get(DEPLOYMENTS_URL, headers=HEADERS)
    deployments_response.raise_for_status()
    all_deployments = deployments_response.json()["deployments"]

    # Step 2: Filter based on mode
    if mode == "source":
        source_workspace_ids = {entry["source_workspace_id"] for entry in workspace_entries}
        filtered_deployments = [
            d for d in all_deployments if d.get("workspaceId") in source_workspace_ids
        ]
    elif mode == "backup":
        # Get all workspaces
        workspaces_response = requests.get(WORKSPACES_URL, headers=HEADERS)
        workspaces_response.raise_for_status()
        all_workspaces = workspaces_response.json()["workspaces"]

        # Build expected backup names from JSON
        backup_workspace_names = {entry["backup_workspace_name"] for entry in workspace_entries}

        # Find matching workspace IDs
        backup_workspace_ids = {
            w["id"] for w in all_workspaces if w["name"] in backup_workspace_names
        }

        filtered_deployments = [
            d for d in all_deployments if d.get("workspaceId") in backup_workspace_ids
        ]
    else:
        raise ValueError("Mode must be either 'source' or 'backup'")

    # Return simplified deployment info
    return [
        {
            "deployment_id": d.get("id"),
            "deployment_name": d.get("name"),
            "workspace_id": d.get("workspaceId"),
        }
        for d in filtered_deployments
    ]