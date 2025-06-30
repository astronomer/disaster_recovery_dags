import os
import json
import requests
import logging

log = logging.getLogger(__name__)
ASTRO_API_URL = "https://api.astronomer.io/platform/v1beta1"

def create_backup_deployments():
    # Load environment variables
    ASTRO_TOKEN = os.environ["ASTRO_API_TOKEN"]
    ORG_ID = os.environ["ASTRO_ORGANIZATION_ID"]
    NEW_CLUSTER_ID = os.environ["NEW_CLUSTER_ID"]

    HEADERS = {
        "Authorization": f"Bearer {ASTRO_TOKEN}",
        "Content-Type": "application/json",
    }

    # Load workspaces_to_backup.json
    file_path = os.path.join(os.path.dirname(__file__), "workspaces_to_backup.json")
    with open(file_path, "r") as f:
        workspace_map = json.load(f)

    # Map source_workspace_id → backup_workspace_name
    workspace_map_dict = {
        entry["source_workspace_id"]: entry["backup_workspace_name"]
        for entry in workspace_map
    }

    # Fetch deployments
    deployments_url = f"{ASTRO_API_URL}/organizations/{ORG_ID}/deployments"
    log.info(f"🌐 GET {deployments_url}")
    response = requests.get(deployments_url, headers=HEADERS)
    response.raise_for_status()
    deployments = response.json().get("deployments", [])
    log.info(f"✅ Found {len(deployments)} deployments in org")

    # Filter deployments
    filtered_deployments = [
        d for d in deployments if d["workspaceId"] in workspace_map_dict
    ]
    log.info(f"🔎 {len(filtered_deployments)} deployments match the workspaces to back up")

    # Fetch all workspaces
    workspaces_url = f"{ASTRO_API_URL}/organizations/{ORG_ID}/workspaces"
    log.info(f"🌐 GET {workspaces_url}")
    resp_ws = requests.get(workspaces_url, headers=HEADERS)
    resp_ws.raise_for_status()
    all_workspaces = resp_ws.json().get("workspaces", [])
    name_to_id = {
        ws.get("name"): ws.get("id")
        for ws in all_workspaces
        if "name" in ws and "id" in ws
    }
    log.info(f"✅ Found {len(name_to_id)} valid workspaces (with name + id)")

    for dep in filtered_deployments:
        deployment_id = dep["id"]
        source_ws = dep["workspaceId"]
        backup_name = workspace_map_dict[source_ws]

        log.info(f"🔍 Checking source workspace ID: {source_ws}")
        verify_source = requests.get(
            f"{ASTRO_API_URL}/organizations/{ORG_ID}/workspaces/{source_ws}",
            headers=HEADERS,
        )
        log.info(f"→ Source workspace status: {verify_source.status_code}")
        if verify_source.status_code == 404:
            log.warning(f"⚠️ Source workspace '{source_ws}' not found — skipping.")
            continue

        # Get deployment details (no "deployment" key in response)
        detail_url = f"{ASTRO_API_URL}/organizations/{ORG_ID}/deployments/{deployment_id}"
        log.info(f"🌐 GET {detail_url}")
        detail_resp = requests.get(detail_url, headers=HEADERS)
        if detail_resp.status_code != 200:
            log.warning(f"⚠️ Could not retrieve details for deployment {deployment_id}")
            continue
        deployment_data = detail_resp.json()

        # Get backup workspace ID
        backup_workspace_id = name_to_id.get(backup_name)
        if not backup_workspace_id:
            log.warning(f"⚠️ Skipping {deployment_id} - no workspace with name '{backup_name}'")
            continue

        # Create backup payload
        payload = {
            "name": f"backup-{deployment_data['name']}",
            "description": f"Backup of {deployment_data['name']}",
            "workspaceId": backup_workspace_id,
            "clusterId": NEW_CLUSTER_ID,
            "runtimeVersion": deployment_data["runtimeVersion"],
            "astroRuntimeVersion": deployment_data.get("astroRuntimeVersion"),
            "dagDeployEnabled": deployment_data.get("dagDeployEnabled", False),
            "isDagDeployEnabled": deployment_data.get("isDagDeployEnabled", False),
            "isCicdEnforced": deployment_data.get("isCicdEnforced", False),
            "isHighAvailability": deployment_data.get("isHighAvailability", False),
            "schedulerSize": deployment_data.get("schedulerSize"),
            "executor": deployment_data["executor"],
            "environmentVariables": deployment_data.get("environmentVariables", []),
            "type": "DEDICATED",
            "resourceQuotaCpu": deployment_data.get("resourceQuotaCpu"),
            "resourceQuotaMemory": deployment_data.get("resourceQuotaMemory"),
            "defaultTaskPodCpu": deployment_data.get("defaultTaskPodCpu"),
            "defaultTaskPodMemory": deployment_data.get("defaultTaskPodMemory"),
            "workerQueues": deployment_data.get("workerQueues", []),
            "isDevelopmentMode": True,  # ✅ Added line
        }

        log.info(f"📦 Creating backup deployment with payload:\n{json.dumps(payload, indent=2)}")

        create_url = f"{ASTRO_API_URL}/organizations/{ORG_ID}/deployments"
        create_resp = requests.post(create_url, headers=HEADERS, json=payload)
        if create_resp.status_code != 201:
            log.error(f"❌ Failed to create backup: {create_resp.status_code} {create_resp.text}")
        else:
            created = create_resp.json()
            log.info(f"✅ Created backup deployment: {created['id']} ({created['name']})")