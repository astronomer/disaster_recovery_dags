import requests
from airflow.models import Variable
import os
import logging

log = logging.getLogger(__name__)

ASTRO_API_URL = "https://api.astronomer.io/platform/v1beta1"
ASTRO_TOKEN = os.environ["ASTRO_API_TOKEN"]
ORG_ID = os.environ["ASTRO_ORGANIZATION_ID"]

HEADERS = {
    "Authorization": f"Bearer {ASTRO_TOKEN}",
    "Content-Type": "application/json",
}

def get_recent_deploys(deployment_id: str, limit: int = 3) -> list[dict]:
    """
    Fetch the most recent deploys for a given deployment ID.

    Args:
        deployment_id (str): The source deployment ID.
        limit (int): The number of recent deploys to return. Defaults to 3.

    Returns:
        list of dict: Deploy metadata including fields required to replicate the deploy on a backup deployment.
    """
    url = f"{ASTRO_API_URL}/organizations/{ORG_ID}/deployments/{deployment_id}/deploys"
    response = requests.get(url, headers=HEADERS)
    response.raise_for_status()

    all_deploys = response.json().get("deploys", [])
    sorted_deploys = sorted(all_deploys, key=lambda d: d.get("createdAt", ""), reverse=True)

    recent_deploys = []

    for d in sorted_deploys[:limit]:
        bundle_info = d.get("bundles", [{}])[0]  # Assume first bundle is relevant

        deploy_data = {
            "id": d.get("id"),
            "createdAt": d.get("createdAt"),
            "imageRepository": d.get("imageRepository"),
            "imageTag": d.get("imageTag"),
            "bundleMountPath": d.get("bundleMountPath"),
            "bundleType": bundle_info.get("bundleType"),
            "bundleTarballVersion": bundle_info.get("currentVersion"),
            "dagTarballVersion": d.get("dagTarballVersion"),
        }
        recent_deploys.append(deploy_data)

    log.info(f"🔍 Found {len(recent_deploys)} recent deploys for deployment {deployment_id}")
    return recent_deploys

def create_deploy_from_source(source_deploy: dict, target_deployment_id: str) -> dict:
    """
    Create a new deploy on a backup deployment using metadata from a source deploy.

    Args:
        source_deploy (dict): A deploy entry returned by get_recent_deploys().
        target_deployment_id (str): The deployment ID of the backup deployment.

    Returns:
        dict: Metadata about the newly created deploy including id, upload URLs, and tarball versions.
    """
    url = f"{ASTRO_API_URL}/organizations/{ORG_ID}/deployments/{target_deployment_id}/deploys"

    payload = {
        "bundleMountPath": source_deploy["bundleMountPath"],
        "bundleType": source_deploy["bundleType"],
        "description": f"Backup of deploy {source_deploy['id']} from source deployment",
        "type": "IMAGE_AND_DAG"
    }

    response = requests.post(url, headers=HEADERS, json=payload)
    response.raise_for_status()

    deploy = response.json()

    log.info(f"🚀 Created new deploy {deploy['id']} on backup deployment {target_deployment_id}")

    return {
        "id": deploy["id"],
        "deploymentId": deploy["deploymentId"],
        "bundleUploadUrl": deploy.get("bundleUploadUrl"),
        "dagsUploadUrl": deploy.get("dagsUploadUrl"),
        "bundleMountPath": deploy.get("bundleMountPath"),
        "dagTarballVersion": deploy.get("dagTarballVersion"),
        "imageRepository": deploy.get("imageRepository"),
        "imageTag": deploy.get("imageTag"),
    }

def finalize_deploy(deployment_id: str, deploy_id: str, source_deploy: dict) -> dict:
    """
    Finalize a deploy on a backup deployment using tarball versions from a source deploy.

    Args:
        deployment_id (str): The ID of the backup deployment.
        deploy_id (str): The ID of the deploy to finalize.
        source_deploy (dict): A source deploy dict returned by get_recent_deploys().

    Returns:
        dict: Metadata about the finalized deploy including status and versions.
    """
    url = f"{ASTRO_API_URL}/organizations/{ORG_ID}/deployments/{deployment_id}/deploys/{deploy_id}/finalize"

    payload = {
        "bundleTarballVersion": source_deploy["bundleTarballVersion"],
        "dagTarballVersion": source_deploy["dagTarballVersion"]
    }

    response = requests.post(url, headers=HEADERS, json=payload)
    response.raise_for_status()

    deploy = response.json()

    log.info(f"✅ Finalized deploy {deploy_id} on deployment {deployment_id} with status {deploy.get('status')}")

    return {
        "id": deploy.get("id"),
        "status": deploy.get("status"),
        "deploymentId": deploy.get("deploymentId"),
        "bundleMountPath": deploy.get("bundleMountPath"),
        "dagTarballVersion": deploy.get("dagTarballVersion"),
        "imageRepository": deploy.get("imageRepository"),
        "imageTag": deploy.get("imageTag"),
        "updatedAt": deploy.get("updatedAt"),
    }

def replicate_deploy_to_backup():
    """
    Orchestrates replication of selected recent deploys from source deployments
    to their associated backup deployments using Airflow Variable `backupDeployOffset`.
    """
    from airflow.models import Variable
    from include.get_workspace_hierarchy import get_workspace_hierarchy_with_deployments
    import logging

    log = logging.getLogger(__name__)
    backup_offset = int(Variable.get("backupDeployOffset", default_var=1))

    if backup_offset not in [1, 2, 3]:
        raise ValueError("Airflow Variable 'backupDeployOffset' must be 1, 2, or 3")

    workspace_hierarchy = get_workspace_hierarchy_with_deployments()

    for entry in workspace_hierarchy:
        for source_deploy in entry["source_deployments"]:
            source_deploy_id = source_deploy["id"]
            backup_deployments = entry["backup_deployments"]
            backup_deploy = next(
                (d for d in backup_deployments if d["source_deployment_id"] == source_deploy_id),
                None,
            )

            if not backup_deploy:
                log.warning(f"⚠️ No matching backup deployment found for source deployment {source_deploy_id}")
                continue

            try:
                recent_deploys = get_recent_deploys(source_deploy_id, limit=3)
                if len(recent_deploys) < backup_offset:
                    log.warning(f"⚠️ Only found {len(recent_deploys)} deploys for deployment {source_deploy_id}, skipping.")
                    continue

                selected_deploy = recent_deploys[backup_offset - 1]

                new_deploy = create_deploy_from_source(
                    source_deploy=selected_deploy,
                    target_deployment_id=backup_deploy["id"]
                )

                finalize_result = finalize_deploy(
                    deployment_id=backup_deploy["id"],
                    deploy_id=new_deploy["id"],
                    source_deploy=selected_deploy
                )

                log.info(f"✅ Replicated deploy {selected_deploy['id']} to backup deployment {backup_deploy['id']}")
            except Exception as e:
                log.exception(f"❌ Failed to replicate deploy for source deployment {source_deploy_id}: {e}")