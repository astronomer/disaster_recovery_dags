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

    for entry in workspace_entries:
        source_id = entry["source_workspace_id"]
        backup_label = entry["backup_workspace_label"]

        print(f"Creating backup workspace for {source_id} as {backup_label}")

        payload = {
            "name": backup_label,
            "description": f"Backup for workspace {source_id}",
            "cicdEnforcedDefault": True
        }

        url = f"https://api.astronomer.io/platform/v1beta1/organizations/{ASTRO_ORGANIZATION_ID}/workspaces"
        response = requests.post(url, headers=HEADERS, json=payload)

        print("Response status:", response.status_code)
        print("Response body:", response.text)

        if response.status_code >= 300:
            raise Exception(f"Failed to create backup workspace for {source_id}:\n{response.text}")

        result = response.json()
        created.append({
            "source_workspace_id": source_id,
            "backup_workspace_id": result["id"],
            "backup_workspace_label": result["name"]
        })

    return created