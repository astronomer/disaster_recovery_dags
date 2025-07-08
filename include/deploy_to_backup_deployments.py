# import requests
# from airflow.models import Variable
# import os
# import logging

# log = logging.getLogger(__name__)

# ASTRO_API_URL = "https://api.astronomer.io/platform/v1beta1"
# ASTRO_TOKEN = os.environ["ASTRO_API_TOKEN"]
# ORG_ID = os.environ["ASTRO_ORGANIZATION_ID"]

# HEADERS = {
#     "Authorization": f"Bearer {ASTRO_TOKEN}",
#     "Content-Type": "application/json",
# }

# def get_recent_deploys(deployment_id: str, limit: int = 3) -> list[dict]:
#     """
#     Fetch the most recent deploys for a given deployment ID.

#     Args:
#         deployment_id (str): The source deployment ID.
#         limit (int): The number of recent deploys to return. Defaults to 3.

#     Returns:
#         list of dict: Deploy metadata including fields required to replicate the deploy on a backup deployment.
#     """
#     url = f"{ASTRO_API_URL}/organizations/{ORG_ID}/deployments/{deployment_id}/deploys"
#     response = requests.get(url, headers=HEADERS)
#     response.raise_for_status()

#     all_deploys = response.json().get("deploys", [])
#     sorted_deploys = sorted(all_deploys, key=lambda d: d.get("createdAt", ""), reverse=True)

#     recent_deploys = []

#     for d in sorted_deploys[:limit]:
#         bundle_info = d.get("bundles", [{}])[0]  # Assume first bundle is relevant

#         deploy_data = {
#             "id": d.get("id"),
#             "createdAt": d.get("createdAt"),
#             "imageRepository": d.get("imageRepository"),
#             "imageTag": d.get("imageTag"),
#             "bundleMountPath": d.get("bundleMountPath"),
#             "bundleType": bundle_info.get("bundleType"),
#             "bundleTarballVersion": bundle_info.get("currentVersion"),
#             "dagTarballVersion": d.get("dagTarballVersion"),
#         }
#         recent_deploys.append(deploy_data)

#     log.info(f"🔍 Found {len(recent_deploys)} recent deploys for deployment {deployment_id}")
#     return recent_deploys

# def create_deploy_from_source(source_deploy: dict, target_deployment_id: str) -> dict:
#     """
#     Create a new deploy on a backup deployment using metadata from a source deploy.

#     Args:
#         source_deploy (dict): A deploy entry returned by get_recent_deploys().
#         target_deployment_id (str): The deployment ID of the backup deployment.

#     Returns:
#         dict: Metadata about the newly created deploy including id, upload URLs, and tarball versions.
#     """
#     url = f"{ASTRO_API_URL}/organizations/{ORG_ID}/deployments/{target_deployment_id}/deploys"

#     payload = {
#         "bundleMountPath": source_deploy["bundleMountPath"],
#         "bundleType": source_deploy["bundleType"],
#         "description": f"Backup of deploy {source_deploy['id']} from source deployment",
#         "type": "IMAGE_AND_DAG"
#     }

#     response = requests.post(url, headers=HEADERS, json=payload)
#     response.raise_for_status()

#     deploy = response.json()

#     log.info(f"🚀 Created new deploy {deploy['id']} on backup deployment {target_deployment_id}")

#     return {
#         "id": deploy["id"],
#         "deploymentId": deploy["deploymentId"],
#         "bundleUploadUrl": deploy.get("bundleUploadUrl"),
#         "dagsUploadUrl": deploy.get("dagsUploadUrl"),
#         "bundleMountPath": deploy.get("bundleMountPath"),
#         "dagTarballVersion": deploy.get("dagTarballVersion"),
#         "imageRepository": deploy.get("imageRepository"),
#         "imageTag": deploy.get("imageTag"),
#     }

# def finalize_deploy(deployment_id: str, deploy_id: str, source_deploy: dict) -> dict:
#     """
#     Finalize a deploy on a backup deployment using tarball versions from a source deploy.

#     Args:
#         deployment_id (str): The ID of the backup deployment.
#         deploy_id (str): The ID of the deploy to finalize.
#         source_deploy (dict): A source deploy dict returned by get_recent_deploys().

#     Returns:
#         dict: Metadata about the finalized deploy including status and versions.
#     """
#     url = f"{ASTRO_API_URL}/organizations/{ORG_ID}/deployments/{deployment_id}/deploys/{deploy_id}/finalize"

#     payload = {
#         "bundleTarballVersion": source_deploy["bundleTarballVersion"],
#         "dagTarballVersion": source_deploy["dagTarballVersion"]
#     }

#     response = requests.post(url, headers=HEADERS, json=payload)
#     response.raise_for_status()

#     deploy = response.json()

#     log.info(f"✅ Finalized deploy {deploy_id} on deployment {deployment_id} with status {deploy.get('status')}")

#     return {
#         "id": deploy.get("id"),
#         "status": deploy.get("status"),
#         "deploymentId": deploy.get("deploymentId"),
#         "bundleMountPath": deploy.get("bundleMountPath"),
#         "dagTarballVersion": deploy.get("dagTarballVersion"),
#         "imageRepository": deploy.get("imageRepository"),
#         "imageTag": deploy.get("imageTag"),
#         "updatedAt": deploy.get("updatedAt"),
#     }

# def replicate_deploy_to_backup():
#     """
#     Orchestrates replication of selected recent deploys from source deployments
#     to their associated backup deployments using Airflow Variable `backupDeployOffset`.
#     """
#     from airflow.models import Variable
#     from include.get_workspace_hierarchy import get_workspace_hierarchy_with_deployments
#     import logging

#     log = logging.getLogger(__name__)
#     backup_offset = int(Variable.get("backupDeployOffset", default_var=1))

#     if backup_offset not in [1, 2, 3]:
#         raise ValueError("Airflow Variable 'backupDeployOffset' must be 1, 2, or 3")

#     workspace_hierarchy = get_workspace_hierarchy_with_deployments()

#     for entry in workspace_hierarchy:
#         for source_deploy in entry["source_deployments"]:
#             source_deploy_id = source_deploy["id"]
#             backup_deployments = entry["backup_deployments"]
#             backup_deploy = next(
#                 (d for d in backup_deployments if d["source_deployment_id"] == source_deploy_id),
#                 None,
#             )

#             if not backup_deploy:
#                 log.warning(f"⚠️ No matching backup deployment found for source deployment {source_deploy_id}")
#                 continue

#             try:
#                 recent_deploys = get_recent_deploys(source_deploy_id, limit=3)
#                 if len(recent_deploys) < backup_offset:
#                     log.warning(f"⚠️ Only found {len(recent_deploys)} deploys for deployment {source_deploy_id}, skipping.")
#                     continue

#                 selected_deploy = recent_deploys[backup_offset - 1]

#                 new_deploy = create_deploy_from_source(
#                     source_deploy=selected_deploy,
#                     target_deployment_id=backup_deploy["id"]
#                 )

#                 finalize_result = finalize_deploy(
#                     deployment_id=backup_deploy["id"],
#                     deploy_id=new_deploy["id"],
#                     source_deploy=selected_deploy
#                 )

#                 log.info(f"✅ Replicated deploy {selected_deploy['id']} to backup deployment {backup_deploy['id']}")
#             except Exception as e:
#                 log.exception(f"❌ Failed to replicate deploy for source deployment {source_deploy_id}: {e}")

import requests
import os
import subprocess
import logging
from datetime import datetime

log = logging.getLogger(__name__)

ASTRO_API_URL = "https://api.astronomer.io/platform/v1beta1"
ASTRO_TOKEN = os.environ["ASTRO_API_TOKEN"]
ORG_ID = os.environ["ASTRO_ORGANIZATION_ID"]

HEADERS = {
    "Authorization": f"Bearer {ASTRO_TOKEN}",
    "Content-Type": "application/json",
}
def do_image_deploy(source_image_tag, source_registry_link, description, source_deployment_id, backup_deployment_id):
    print(f"Initiating Image Deploy Process for deployment {backup_deployment_id}")
    deploy_url = f"{ASTRO_API_URL}/organizations/{ORG_ID}/deployments/{backup_deployment_id}/deploys"
    payload = {
        "type": "IMAGE_AND_DAG",
        "description": description
    }
    response = requests.post(deploy_url, headers=HEADERS, json=payload)
    response.raise_for_status()
    deploy_initialized = response.json()
    backup_registry_link = deploy_initialized.get("imageRepository")
    backup_image_tag = deploy_initialized.get("imageTag")
    dags_upload_url = deploy_initialized.get("dagsUploadUrl")
    deploy_id = deploy_initialized.get("id")

    print("Logging into Docker...")
    subprocess.run(["docker", "login", source_registry_link,"-u", "cli", "-p", ASTRO_TOKEN], check=True)
    print("Docker login successful.")

    print(f"Pulling Docker image {source_registry_link}:{source_image_tag}")
    print(f"{source_registry_link}:{source_image_tag}")
    subprocess.run(["docker", "pull", f"{source_registry_link}:{source_image_tag}"], check=True)
    print("Docker image pulled successfully.")

    subprocess.run(
        [
            "docker", "tag",
            f"{source_registry_link}:{source_image_tag}",
            f"{backup_registry_link}:{backup_image_tag}"
        ],
        check=True
    )

    print(f"Pushing Docker image {backup_registry_link}:{backup_image_tag}")
    subprocess.run(["docker", "push", f"{backup_registry_link}:{backup_image_tag}"], check=True)
    print("Docker image pushed successfully.")

    print("Downloading DAGs...")
    s3_url = f"s3://af-demo-dags-bucket/{source_deployment_id}.tar.gz"
    resp = requests.get(s3_url, stream=True)
    resp.raise_for_status()

    print(f"Uploading tar file {source_deployment_id}.tar.gz")
    put_headers = {
        "x-ms-blob-type": "BlockBlob",
        "Content-Type": "application/x-gtar"
    }
    put_resp = requests.put(dags_upload_url, headers=put_headers, data=open(f"{source_deployment_id}.tar.gz", "rb"))
    put_resp.raise_for_status()
    response = put_resp.json()
    print(response)
    print("DAGs uploaded successfully.")

    deploy_url = f"{ASTRO_API_URL}/organizations/{ORG_ID}/deployments/{backup_deployment_id}/deploys/{deploy_id}/finalize"
    response = requests.post(deploy_url, headers=HEADERS)
    response.raise_for_status()
    response = response.json()
    print("Image deploy finalized successfully.")
    print(f"Image deploy for deployment {backup_deployment_id} completed successfully.")

    print(response)


def do_dag_deploy(description, source_deployment_id, backup_deployment_id):
    print(f"Initiating Image Deploy Process for deployment {backup_deployment_id}")
    deploy_url = f"{ASTRO_API_URL}/organizations/{ORG_ID}/deployments/{backup_deployment_id}/deploys"
    payload = {
        "type": "DAG_ONLY",
        "description": description
    }
    response = requests.post(deploy_url, headers=HEADERS, json=payload)
    response.raise_for_status()
    deploy_initialized = response.json()
    dags_upload_url = deploy_initialized.get("dagsUploadUrl")
    deploy_id = deploy_initialized.get("id")
    
    print("Downloading DAGs...")

    url = f"https://af-demo-dags-bucket.s3.amazonaws.com/{source_deployment_id}.tar.gz"
    resp = requests.get(url, stream=True)
    resp.raise_for_status()

    with open(f"{source_deployment_id}.tar.gz", "wb") as f:
        for chunk in resp.iter_content(1024 * 64):
            f.write(chunk)

    print(f"Uploading tar file {source_deployment_id}.tar.gz")
    put_headers = {
        "x-ms-blob-type": "BlockBlob",
        "Content-Type": "application/x-gtar"
    }

    put_resp = requests.put(dags_upload_url, headers=put_headers, data=open(f"{source_deployment_id}.tar.gz", "rb"))
    put_resp.raise_for_status()
    version_id = put_resp.headers.get("x-ms-version-id")
    print(f"DAGs uploaded successfully. Version ID: {version_id}")

    deploy_url = f"{ASTRO_API_URL}/organizations/{ORG_ID}/deployments/{backup_deployment_id}/deploys/{deploy_id}/finalize"
    response = requests.post(deploy_url, headers=HEADERS, json={"dagTarballVersion": version_id})
    response.raise_for_status()
    response = response.json()
    print("DAG deploy finalized successfully.")

def parse_iso(ts: str) -> datetime:
    """Parse an ISO 8601 timestamp ending with 'Z' into a datetime."""
    return datetime.fromisoformat(ts.replace('Z', '+00:00'))


def replicate_deploy_to_backup(source_deployment_id: str, backup_deployment_id: str, context):
    source_deployment_deploy_detail_url = f"{ASTRO_API_URL}/organizations/{ORG_ID}/deployments/{source_deployment_id}/deploys"
    backup_deployment_deploy_detail_url = f"{ASTRO_API_URL}/organizations/{ORG_ID}/deployments/{backup_deployment_id}/deploys"
    
    response = requests.get(source_deployment_deploy_detail_url, headers=HEADERS)
    response.raise_for_status()
    source_deployment_deploy_details = response.json()

    response = requests.get(backup_deployment_deploy_detail_url, headers=HEADERS)
    response.raise_for_status()
    backup_deployment_deploy_details = response.json()

    latest_source_deploy = {}
    for d in source_deployment_deploy_details['deploys']:
        if d["status"] != "DEPLOYED":
            continue
        deploy_type = d['type']
        created_at = parse_iso(d['createdAt'])
        if deploy_type not in latest_source_deploy or created_at > latest_source_deploy[deploy_type][0]:
            latest_source_deploy[deploy_type] = (created_at, d)
    latest_deploy = {t: entry[1] for t, entry in latest_source_deploy.items()}
    latest_dag_deploy_source = latest_deploy.get("DAG_ONLY")
    latest_image_deploy_source = latest_deploy.get("IMAGE_AND_DAG")

    latest_backup_deploy = {}
    for d in backup_deployment_deploy_details['deploys']:
        if d["status"] != "DEPLOYED":
            continue
        deploy_type = d['type']
        created_at = parse_iso(d['createdAt'])
        if deploy_type not in latest_backup_deploy or created_at > latest_backup_deploy[deploy_type][0]:
            latest_backup_deploy[deploy_type] = (created_at, d)
    latest_deploy = {t: entry[1] for t, entry in latest_backup_deploy.items()}
    latest_dag_deploy_backup = latest_deploy.get("DAG_ONLY")
    latest_image_deploy_backup = latest_deploy.get("IMAGE_AND_DAG")


    # if not latest_image_deploy_backup or (latest_image_deploy_source.get("description") != latest_image_deploy_backup.get("description")):
    #     description = latest_image_deploy_source.get("description")
    #     source_registry_link = latest_image_deploy_source.get("imageRepository")
    #     source_image_tag = latest_image_deploy_source.get("imageTag")
    #     print("Mismatch in Image Deploys! Deploying image to backup deployment...")
    #     do_image_deploy(source_image_tag, source_registry_link, description, source_deployment_id, backup_deployment_id)
    # else:
    #     print("Image Deploys are in sync.")

    # if not latest_dag_deploy_backup or (latest_dag_deploy_source.get("description") == latest_dag_deploy_backup.get("description")):
    #     description = latest_dag_deploy_source.get("description")
    #     print("Mismatch in DAG Deploys! Deploying DAG to backup deployment...")
    #     do_dag_deploy(description, source_deployment_id, backup_deployment_id)
    # else:
    #     print("DAG Deploys are in sync.")

    # To Do: Remove later
    description = latest_dag_deploy_source.get("description")
    print("Mismatch in DAG Deploys! Deploying DAG to backup deployment...")
    do_dag_deploy(description, source_deployment_id, backup_deployment_id)
