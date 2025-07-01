import os
import requests
import json
from airflow.exceptions import AirflowException, AirflowSkipException


ASTRO_API_TOKEN = os.getenv("ASTRO_API_TOKEN")
ASTRO_ORGANIZATION_ID = os.getenv("ASTRO_ORGANIZATION_ID")

HEADERS = {
    "Authorization": f"Bearer {ASTRO_API_TOKEN}",
    "Content-Type": "application/json"
}

WORKSPACES_JSON_PATH = os.path.join(os.path.dirname(__file__), "workspaces_to_backup.json")

def create_backup_workspaces(source_workspace_id, backup_workspace_name, context):
    existing_url = f"https://api.astronomer.io/platform/v1beta1/organizations/{ASTRO_ORGANIZATION_ID}/workspaces"
    existing_resp = requests.get(existing_url, headers=HEADERS)
    existing_resp.raise_for_status()
    existing_workspaces = existing_resp.json().get("workspaces", [])

    existing_workspace = next(
        (ws for ws in existing_workspaces if ws.get("id") == source_workspace_id),
        None,
    )

    backup_workspace = next(
        (ws for ws in existing_workspaces if ws.get("name") == backup_workspace_name),
        None,
    )

    if backup_workspace:
        ti = context['ti']
        ti.xcom_push(key="return_value", value={"source_workspace_id": source_workspace_id, "backup_workspace_id": backup_workspace.get("id")})
        raise AirflowSkipException(f"Backup workspace '{backup_workspace_name}' already exists. Skipping creation.")

    if existing_workspace:
        source_workspace_description = existing_workspace.get("description", "")
        source_workspace_cicd_default = existing_workspace.get("cicdEnforcedDefault", True)
        print(f"Creating backup workspace for {source_workspace_id} as {backup_workspace_name}")

        payload = {
            "name": backup_workspace_name,
            "description": f"Backup for workspace {source_workspace_id} - {source_workspace_description}",
            "cicdEnforcedDefault": source_workspace_cicd_default
        }

        url = f"https://api.astronomer.io/platform/v1beta1/organizations/{ASTRO_ORGANIZATION_ID}/workspaces"
        response = requests.post(url, headers=HEADERS, json=payload)
        
        if response.status_code in (201, 200):
            print(f"Successfully created backup workspace: {backup_workspace_name}")
            ti = context['ti']
            ti.xcom_push(key="return_value", value={"source_workspace_id": source_workspace_id, "backup_workspace_id": response.json().get("id")})
        else:
            print(f"Failed to create backup workspace {backup_workspace_name}. Status: {response.status_code}, Message: {response.text}")
    else:
        raise AirflowException(f"Source workspace {source_workspace_id} not found in existing workspaces. Cannot create backup.")


def map_source_workpaces_to_backup(workspaces):
    with open(WORKSPACES_JSON_PATH, "r", encoding="utf-8") as f:
        workspace_entries = json.load(f)

    mapped_workspaces = []

    for entry in workspace_entries:
        workspace_source_id = entry["source_workspace_id"]
        workspace_backup_name = entry["backup_workspace_name"]

        if workspace_source_id in workspaces:
            mapped_workspaces.append({
                "source_workspace_id": workspace_source_id,
                "backup_workspace_name": workspace_backup_name
            })
        else:
            print(f"Source workspace {workspace_source_id} not found in provided workspaces.")

    return mapped_workspaces
