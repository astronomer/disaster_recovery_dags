import os
import json
import requests
import logging
from airflow.exceptions import AirflowException, AirflowSkipException


log = logging.getLogger(__name__)
ASTRO_API_URL = "https://api.astronomer.io/platform/v1beta1"
ASTRO_TOKEN = os.environ["ASTRO_API_TOKEN"]
ORG_ID = os.environ["ASTRO_ORGANIZATION_ID"]
NEW_CLUSTER_ID = os.environ["NEW_CLUSTER_ID"]
HEADERS = {
        "Authorization": f"Bearer {ASTRO_TOKEN}",
        "Content-Type": "application/json",
    }

def get_source_deployments_payload(source_workspace_id, backup_workspace_id, context):
    deployments_url = f"{ASTRO_API_URL}/organizations/{ORG_ID}/deployments?workspaceIds={source_workspace_id}"
    response = requests.get(deployments_url, headers=HEADERS)
    response.raise_for_status()
    deployments = response.json().get("deployments", [])

    deployments_mapping = []
    for deployment in deployments:
        deployment_id = deployment.get("id")
        deployment_detail_url = f"{ASTRO_API_URL}/organizations/{ORG_ID}/deployments/{deployment_id}"
        response = requests.get(deployment_detail_url, headers=HEADERS)
        response.raise_for_status()
        deployment_details = response.json()
        payload = {
            "name": deployment_details.get("name"),
            "workspaceId": backup_workspace_id,
            "clusterId": NEW_CLUSTER_ID,
            "runtimeVersion": deployment_details.get("runtimeVersion"),
            "astroRuntimeVersion": deployment_details.get("astroRuntimeVersion"),
            "dagDeployEnabled": deployment_details.get("dagDeployEnabled", False),
            "isDagDeployEnabled": deployment_details.get("isDagDeployEnabled", False),
            "isCicdEnforced": deployment_details.get("isCicdEnforced", False),
            "isHighAvailability": deployment_details.get("isHighAvailability", False),
            "schedulerSize": deployment_details.get("schedulerSize"),
            "executor": deployment_details.get("executor"),
            "environmentVariables": deployment_details.get("environmentVariables", []),
            "type": "DEDICATED",
            "resourceQuotaCpu": deployment_details.get("resourceQuotaCpu"),
            "resourceQuotaMemory": deployment_details.get("resourceQuotaMemory"),
            "defaultTaskPodCpu": deployment_details.get("defaultTaskPodCpu"),
            "defaultTaskPodMemory": deployment_details.get("defaultTaskPodMemory"),
            "workerQueues": deployment_details.get("workerQueues", []),
            "isDevelopmentMode": True,
            "contactEmails": deployment_details.get("contactEmails", [])
        }

        deployments_mapping.append({
            "source_deployment_id": deployment_details.get("id"),
            "deployment_payload": payload
        })

    return deployments_mapping


def create_backup_deployments(deployment_payload, context):
    create_url = f"{ASTRO_API_URL}/organizations/{ORG_ID}/deployments"
    create_resp = requests.post(create_url, headers=HEADERS, json=deployment_payload)
    if "already exists" in create_resp.text:
        url = f"{ASTRO_API_URL}/organizations/{ORG_ID}/deployments?names={deployment_payload.get('name')}&workspaceIds={deployment_payload.get('workspaceId')}"
        resp = requests.get(url, headers=HEADERS)
        print(resp.text)
        existing_deployment = resp.json().get("deployments", [])
        context["ti"].xcom_push(key="return_value", value=existing_deployment[0].get('id'))
        raise AirflowSkipException(f"Deployment {deployment_payload.get('name')} Already exists! Skipping.")
        
    elif create_resp.status_code in (201, 200):
        created = create_resp.json()
        print(f"Created backup deployment: {created['id']} ({created['name']})")
        return created['id']
    else:
        raise AirflowException(f"Failed to create backup: {create_resp.status_code} {create_resp.text}")


def create_token_for_backup_deployments():
    pass