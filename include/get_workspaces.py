import os
import json
import requests
from collections import defaultdict

ASTRO_API_TOKEN = os.getenv("ASTRO_API_TOKEN")
ORG_ID = os.getenv("ASTRO_ORGANIZATION_ID")

WORKSPACES_URL = f"https://api.astronomer.io/platform/v1beta1/organizations/{ORG_ID}/workspaces"
DEPLOYMENTS_URL = f"https://api.astronomer.io/platform/v1beta1/organizations/{ORG_ID}/deployments"

HEADERS = {
    "Authorization": f"Bearer {ASTRO_API_TOKEN}",
    "Content-Type": "application/json"
}

def get_workspaces():
    workspaces = requests.get(WORKSPACES_URL, params={"limit": 1000}, headers=HEADERS)
    workspaces.raise_for_status()
    all_workspaces = workspaces.json()["workspaces"]
    return {workspace["id"] for workspace in all_workspaces}

def get_deployment_mappings(source_workspace_id, backup_workspace_name):
    all_deployments = requests.get(DEPLOYMENTS_URL, headers=HEADERS)
    all_deployments.raise_for_status()

    deployment_mappings = defaultdict(dict)
    for deployment_specs in all_deployments.json().get("deployments", []):
        if deployment_specs["workspaceId"] == source_workspace_id:
            deployment_id = deployment_specs["id"]
            deployment_name = deployment_specs["name"]
            deployment_url = deployment_specs["webServerUrl"].split("?")[0]
            deployment_mappings[deployment_name].update({
                "source_deployment_id": deployment_id,
                "source_workspace_id": source_workspace_id,
                "_deployment_name": deployment_name,
                "source_deployment_url": deployment_url,
            })
        elif deployment_specs["workspaceName"] == backup_workspace_name:
            deployment_id = deployment_specs["id"]
            backup_workspace_id = deployment_specs["workspaceId"]
            deployment_name = deployment_specs["name"]
            deployment_url = deployment_specs["webServerUrl"].split("?")[0]
            deployment_mappings[deployment_name].update({
                "backup_deployment_id": deployment_id,
                "backup_workspace_id": backup_workspace_id,
                "backup_deployment_url": deployment_url,
            })

    filtered_mappings = [
        mapping for mapping in deployment_mappings.values()
        if "source_deployment_id" in mapping and "backup_deployment_id" in mapping
    ]

    return filtered_mappings
