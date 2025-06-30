import os
import requests
import json

ASTRO_API_TOKEN = os.getenv("ASTRO_API_TOKEN")
ASTRO_ORGANIZATION_ID = os.getenv("ASTRO_ORGANIZATION_ID")

HEADERS = {
    "Authorization": f"Bearer {ASTRO_API_TOKEN}",
    "Content-Type": "application/json"
}

WORKSPACES_JSON_PATH = os.path.join(os.path.dirname(__file__), "workspaces_to_backup.json")

def create_backup_workspaces(context):
    with open(WORKSPACES_JSON_PATH, "r") as f:
        workspace_entries = json.load(f)

    created = []

    # Fetch existing workspace names
    existing_url = f"https://api.astronomer.io/platform/v1beta1/organizations/{ASTRO_ORGANIZATION_ID}/workspaces"
    existing_resp = requests.get(existing_url, headers=HEADERS)
    existing_resp.raise_for_status()
    existing_workspaces = existing_resp.json().get("workspaces", [])
    existing_names = {ws["name"] for ws in existing_workspaces}

    for entry in workspace_entries:
        source_id = entry["source_workspace_id"]
        backup_name = entry["backup_workspace_name"]

        if backup_name in existing_names:
            print(f"Skipping creation: Workspace '{backup_name}' already exists.")
            continue

        print(f"Creating backup workspace for {source_id} as {backup_name}")

        payload = {
            "name": backup_name,
            "description": f"Backup for workspace {source_id}",
            "cicdEnforcedDefault": True
        }

        url = f"https://api.astronomer.io/platform/v1beta1/organizations/{ASTRO_ORGANIZATION_ID}/workspaces"
        response = requests.post(url, headers=HEADERS, json=payload)

        if response.status_code == 201:
            print(f"Successfully created backup workspace: {backup_name}")
            created.append(backup_name)
        else:
            print(f"Failed to create backup workspace {backup_name}. Status: {response.status_code}, Message: {response.text}")