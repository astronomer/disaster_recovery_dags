# pylint: disable=line-too-long
import datetime
import os
import json
import requests
import urllib3
import logging
from typing import List, Dict, Any

log = logging.getLogger(__name__)

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Path to the workspaces_to_backup.json file
WORKSPACES_JSON_PATH = os.path.join(os.path.dirname(__file__), "workspaces_to_backup.json")
ASTRO_API_TOKEN = os.getenv("ASTRO_API_TOKEN")

def get_headers(token: str) -> dict:
    """
    Get header with Authorization token
    """
    return {"Authorization": f"Bearer {token}"}

# def unpause_dag(airflow_base_url: str, astro_token: str, dag_id: str) -> None:
#     """
#     Unpause dag in Airflow deployment
#     """
#     url = f"{airflow_base_url}/api/starship/dags"
#     payload = {"dag_id": dag_id, "is_paused": False}
#     try:
#         response = requests.patch(
#             url,
#             json=payload,
#             headers=get_headers(astro_token),
#             verify=False,
#             timeout=300,
#         )
#         response.raise_for_status()
#         log.info(f"Successfully unpaused dag: {dag_id} on {airflow_base_url}")
#     except requests.exceptions.RequestException as e:
#         log.error(f"Failed to unpause dag {dag_id} on {airflow_base_url}: {e}")
#         if e.response:
#             log.error(f"Response: {e.response.text}")
#         raise

# def pause_dag(airflow_base_url: str, astro_token: str, dag_id: str) -> None:
#     """
#     Pause dag in Airflow deployment
#     """
#     url = f"{airflow_base_url}/api/starship/dags"
#     payload = {"dag_id": dag_id, "is_paused": True}
#     try:
#         response = requests.patch(
#             url,
#             json=payload,
#             headers=get_headers(astro_token),
#             verify=False,
#             timeout=300,
#         )
#         response.raise_for_status()
#         log.info(f"Successfully paused dag: {dag_id} on {airflow_base_url}")
#     except requests.exceptions.RequestException as e:
#         log.error(f"Failed to pause dag {dag_id} on {airflow_base_url}: {e}")
#         if e.response:
#             log.error(f"Response: {e.response.text}")
#         raise

def post_target_dag_runs(target_airflow_base_url: str, dag_runs: List[dict], dag_id: str) -> None:
    """
    Post dag runs to target Airflow
    """
    url = f"{target_airflow_base_url}/api/starship/dag_runs"
    data = {"dag_runs": dag_runs}
    try:
        response = requests.post(
            url,
            json=data,
            headers=get_headers(ASTRO_API_TOKEN),
            verify=False,
            timeout=300,
        )
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        if e.response is not None:
            if e.response.status_code == 409:
                log.warning(f"Dag runs already exist in target Airflow for dag_id: {dag_id}")
            else:
                log.error(f"Failed to post dag runs for dag_id: {dag_id}. Confirm dag runs in target Airflow. Could be a false failure! {e}")
                if e.response:
                    log.error(f"Response: {e.response.text}")
                raise
        else:
            log.error(f"An unexpected error occurred while posting dag runs for {dag_id}: {e}")
            raise

def post_target_task_instances(target_airflow_base_url: str, ti: List[dict], dag_id: str) -> None:
    """
    Post task instances to target Airflow
    """
    url = f"{target_airflow_base_url}/api/starship/task_instances"
    data = {"task_instances": ti}
    try:
        response = requests.post(
            url,
            json=data,
            headers=get_headers(ASTRO_API_TOKEN),
            verify=False,
            timeout=300,
        )
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        if e.response is not None:
            if e.response.status_code == 409:
                log.warning(f"Task instances already exist in target Airflow for dag_id: {dag_id}")
            else:
                log.error(f"Failed to post task instances for dag_id: {dag_id}. Confirm task instances in target Airflow. Could be a false failure! {e}")
                if e.response:
                    log.error(f"Response: {e.response.text}")
                raise
        else:
            log.error(f"An unexpected error occurred while posting task instances for {dag_id}: {e}")
            raise

def get_source_dag_runs(source_airflow_base_url: str, dag_id: str, offset: int, max_obj_fetch_num_per_req: int) -> dict:
    """
    Get dag runs from source Airflow
    """
    url = f"{source_airflow_base_url}/api/starship/dag_runs"
    params = {"dag_id": dag_id, "offset": offset, "limit": max_obj_fetch_num_per_req}
    response = requests.get(
        url,
        params=params,
        headers=get_headers(ASTRO_API_TOKEN),
        verify=False,
        timeout=300,
    )
    response.raise_for_status()
    json_response = response.json()
    return json_response.get("dag_runs", [])

def get_source_task_instances(source_airflow_base_url: str, dag_id: str, offset: int, max_obj_fetch_num_per_req: int) -> dict:
    """
    Get task instances from source Airflow
    """
    url = f"{source_airflow_base_url}/api/starship/task_instances"
    params = {"dag_id": dag_id, "offset": offset, "limit": max_obj_fetch_num_per_req}
    response = requests.get(
        url,
        params=params,
        headers=get_headers(ASTRO_API_TOKEN),
        verify=False,
        timeout=300,
    )
    response.raise_for_status()
    result = response.json()
    return result.get("task_instances", [])


def get_all_source_dag_runs(source_airflow_base_url: str, dag_id: str, max_obj_fetch_num_per_req: int) -> List[dict]:
    """
    Get all dag runs for a dag_id.
    """
    offset = 0
    all_dag_runs = []
    while True:
        log.info(f"Getting dag runs for dag_id: {dag_id}, offset: {offset}")
        dag_runs = get_source_dag_runs(source_airflow_base_url, dag_id=dag_id, offset=offset, max_obj_fetch_num_per_req=max_obj_fetch_num_per_req)
        all_dag_runs.extend(dag_runs)
        if len(dag_runs) < max_obj_fetch_num_per_req: # No more pages if fewer than limit are returned
            break
        offset += max_obj_fetch_num_per_req
    return all_dag_runs

def get_all_source_task_instances(source_airflow_base_url: str, dag_id: str, max_obj_fetch_num_per_req: int) -> List[dict]:
    """
    Get all task instances for a dag_id.
    """
    offset = 0
    all_task_instances = []
    while True:
        log.info(f"Getting task instances for dag_id: {dag_id}, offset: {offset}")
        task_instances = get_source_task_instances(source_airflow_base_url, dag_id=dag_id, offset=offset, max_obj_fetch_num_per_req=max_obj_fetch_num_per_req)
        all_task_instances.extend(task_instances)
        if len(task_instances) < max_obj_fetch_num_per_req: # No more pages if fewer than limit are returned
            break
        offset += max_obj_fetch_num_per_req
    return all_task_instances

def get_source_variables(source_airflow_base_url: str, source_astro_token: str) -> List[dict]:
    """
    Get all variables from source Airflow
    """
    url = f"{source_airflow_base_url}/api/starship/variable"
    response = requests.get(
        url,
        headers=get_headers(source_astro_token),
        verify=False,
        timeout=300,
    )
    response.raise_for_status()
    result = response.json()
    return result.get("variables", [])

def post_target_variables(target_airflow_base_url: str, target_astro_token: str, variables: List[dict]) -> None:
    """
    Post variables to target Airflow
    """
    url = f"{target_airflow_base_url}/api/starship/variable"
    data = {"variables": variables}
    try:
        response = requests.post(
            url,
            json=data,
            headers=get_headers(target_astro_token),
            verify=False,
            timeout=300,
        )
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        if e.response is not None:
            if e.response.status_code == 409:
                log.warning(f"Variables already exist in target Airflow, skipping existing variables.")
            else:
                log.error(f"Failed to post variables to target Airflow: {e}")
                if e.response:
                    log.error(f"Response: {e.response.text}")
                raise
        else:
            log.error(f"An unexpected error occurred while posting variables: {e}")
            raise

def sync_airflow_variables(
    source_airflow_base_url: str,
    target_airflow_base_url: str,
    dry_run: bool,
    max_obj_post_num_per_req: int
) -> None:
    """
    Synchronizes Airflow Variables from source to target.
    """
    log.info("Starting Airflow Variable migration.")
    source_variables = get_source_variables(source_airflow_base_url, ASTRO_API_TOKEN)
    log.info(f"Found {len(source_variables)} variables in source Airflow.")

    if dry_run:
        log.info("Dry run mode. Skipping syncing variables to target Airflow.")
        return

    if not source_variables:
        log.info("No variables to sync.")
        return

    for i in range(0, len(source_variables), max_obj_post_num_per_req):
        batch = source_variables[i : i + max_obj_post_num_per_req]
        post_target_variables(target_airflow_base_url, ASTRO_API_TOKEN, batch)
        log.info(f"Completed {(i/len(source_variables)*100):.2f}% for variables")
    log.info("Completed 100% for variables")
    log.info(f"Synced {len(source_variables)} variables to target Airflow.")
    log.info("-" * 80)

def get_all_source_dags(source_airflow_base_url: str, max_obj_fetch_num_per_req: int) -> List[str]:
    """
    Fetches all DAG IDs from the source Airflow deployment.
    """
    url = f"{source_airflow_base_url}/api/starship/dags"
    offset = 0
    all_dag_ids = []
    while True:
        log.info(f"Getting DAGs from source, offset: {offset}")
        params = {"offset": offset, "limit": max_obj_fetch_num_per_req}
        response = requests.get(
            url,
            params=params,
            headers=get_headers(ASTRO_API_TOKEN),
            verify=False,
            timeout=300,
        )
        response.raise_for_status()
        dags_data = response.json().get("dags", [])
        
        # Extract dag_id from each DAG object
        dag_ids_batch = [d.get("dag_id") for d in dags_data if d.get("dag_id")]
        all_dag_ids.extend(dag_ids_batch)

        if len(dags_data) < max_obj_fetch_num_per_req:
            break
        offset += max_obj_fetch_num_per_req
    log.info(f"Found {len(all_dag_ids)} DAGs in source Airflow.")
    return all_dag_ids


def migrate_airflow_data(
    astro_data_list: List[Dict[str, Any]], # Renamed argument to reflect it's a list
    max_obj_post_num_per_req: int,
    max_obj_fetch_num_per_req: int,
    dry_run: bool = False,
):
    """
    Main function to orchestrate the Airflow data migration based on mapped deployments.
    It expects the XCom output from a previous task as a LIST of 'astro_data' entries.
    """

    # Load the workspace mapping file (still needed for consistency checks and logic)
    if not os.path.exists(WORKSPACES_JSON_PATH):
        raise FileNotFoundError(f"workspaces_to_backup.json not found at: {WORKSPACES_JSON_PATH}")
    with open(WORKSPACES_JSON_PATH, "r") as f:
        workspace_map_entries = json.load(f)
    
    # Create a lookup for source workspace IDs to backup workspace names
    workspace_name_map = {entry["source_workspace_id"]: entry["backup_workspace_name"] for entry in workspace_map_entries}

    log.info("Starting Airflow data migration across mapped deployments.")
    log.info(f"Astro Data List:\n{json.dumps(astro_data_list, indent=2)}")

    # Iterate over each entry in the astro_data_list
    for astro_data_entry in astro_data_list: # Loop here
        source_data = astro_data_entry["source"]
        backup_data = astro_data_entry["backup"]

        # Prepare lookup dictionaries for deployments from current entry
        source_deployments_by_name = {d["deployment_name"]: d for d in source_data["deployments"]}
        backup_deployments_by_name = {(d["deployment_name"]): d for d in backup_data["deployments"]}

        # Now, iterate through source deployments relevant to this mapping entry
        for source_deployment_name, source_deployment_data in source_deployments_by_name.items():
            target_deployment_name = source_deployment_name # Target deployment name is the same as source.

            target_deployment_data = backup_deployments_by_name.get(target_deployment_name)

            if not target_deployment_data:
                log.warning(f"Could not find a backup deployment named '{target_deployment_name}'. Skipping migration for this deployment pair.")
                continue
            
            # Extract source and target details
            source_airflow_base_url = f"https://{source_deployment_data['deployment_url']}"
            target_airflow_base_url = f"https://{target_deployment_data['deployment_url']}"


            log.info(f"\n--- Migrating from Source Deployment: '{source_deployment_name}' ({source_deployment_name}) ---")
            log.info(f"--- To Backup Deployment: '{target_deployment_data['deployment_name']}' ({target_deployment_data['deployment_id']}) ---\n")
            log.info(f"Source Airflow Base URL: {source_airflow_base_url}")
            log.info(f"Target Airflow Base URL: {target_airflow_base_url}")

            # 1. Sync Airflow Variables for this deployment pair
            sync_airflow_variables(
                source_airflow_base_url=source_airflow_base_url,
                target_airflow_base_url=target_airflow_base_url,
                dry_run=dry_run,
                max_obj_post_num_per_req=max_obj_post_num_per_req
            )

            # 2. Get all DAG IDs from the source Airflow deployment
            all_source_dag_ids = get_all_source_dags(source_airflow_base_url, max_obj_fetch_num_per_req)
            log.info(f"Number of DAGs to migrate from '{source_deployment_name}': {len(all_source_dag_ids)}")

            # 3. Loop through all DAGs and sync dag runs and task instances
            for dag_id in all_source_dag_ids:
                log.info(f"Processing DAG: {dag_id}")

                # Get all dag runs for the dag_id
                log.info(f"Getting all dag runs for dag_id: {dag_id}")
                dag_runs = get_all_source_dag_runs(
                    source_airflow_base_url,
                    dag_id,
                    max_obj_fetch_num_per_req
                )

                if len(dag_runs) == 0:
                    log.info(f"No dag runs to sync for dag_id: {dag_id}")
                else:
                    log.info(f"Found {len(dag_runs)} dag runs for dag_id: {dag_id}")
                    if dry_run:
                        log.info("Dry run mode. Skipping syncing dag runs to target Airflow")
                    else:
                        for i in range(0, len(dag_runs), max_obj_post_num_per_req):
                            post_target_dag_runs(target_airflow_base_url, dag_runs[i : i + max_obj_post_num_per_req], dag_id)
                            log.info(f"Completed {(i/len(dag_runs)*100):.2f}% for dag runs")
                        log.info("Completed 100% for dag runs")
                        log.info(f"Synced {len(dag_runs)} dag runs to target Airflow")
                log.info("-" * 80)

                # Get all task instances for the dag_id
                log.info(f"Getting all task instances for dag_id: {dag_id}")
                task_instances = get_all_source_task_instances(
                    source_airflow_base_url,
                    dag_id,
                    max_obj_fetch_num_per_req
                )
                log.info(f"Found {len(task_instances)} task instances for dag_id: {dag_id}")

                if dry_run:
                    log.info("Dry run mode. Skipping syncing task instances to target Airflow")
                else:
                    if len(task_instances) > 0:
                        for i in range(0, len(task_instances), max_obj_post_num_per_req):
                            post_target_task_instances(target_airflow_base_url, task_instances[i : i + max_obj_post_num_per_req], dag_id)
                            log.info(f"Completed {(i/len(task_instances)*100):.2f}% for task instances")
                        log.info("Completed 100% for task instances")
                        log.info(f"Synced {len(task_instances)} task instances to target Airflow")
                    else:
                        log.info(f"No task instances found for dag_id: {dag_id}")
                log.info("-" * 80)

    log.info("Finished syncing dag runs, task instances, and variables across all mapped deployments!")
