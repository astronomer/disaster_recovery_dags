import os
import json
import requests

ASTRO_API_URL = "https://api.astronomer.io/hub/graphql"
ASTRO_API_TOKEN = os.getenv("ASTRO_API_TOKEN")

if not ASTRO_API_TOKEN:
    raise EnvironmentError("Missing ASTRO_API_TOKEN in environment.")

HEADERS = {
    "Authorization": f"Bearer {ASTRO_API_TOKEN}",
    "Content-Type": "application/json"
}


def get_deployments(workspace_list_path="include/workspaces_to_backup.json"):
    with open(workspace_list_path, "r") as f:
        workspaces = json.load(f)

    all_deployments = []

    for entry in workspaces:
        ws_id = entry["source_workspace_id"]
        label = entry.get("backup_workspace_label", "")

        query = {
            "query": """
                query GetDeployments($workspaceId: Id!) {
                    workspace(id: $workspaceId) {
                        id
                        label
                        deployments {
                            id
                            label
                            releaseName
                            type
                        }
                    }
                }
            """,
            "variables": {
                "workspaceId": ws_id
            }
        }

        response = requests.post(
            ASTRO_API_URL,
            headers=HEADERS,
            data=json.dumps(query)
        )

        if response.status_code != 200:
            raise Exception(f"GraphQL call failed for {ws_id}: {response.text}")

        result = response.json()
        deployments = result.get("data", {}).get("workspace", {}).get("deployments", [])

        for d in deployments:
            all_deployments.append({
                "workspace_id": ws_id,
                "backup_workspace_label": label,
                "deployment_id": d.get("id"),
                "deployment_name": d.get("label"),
                "release_name": d.get("releaseName"),
                "deployment_type": d.get("type"),
            })

    return all_deployments
