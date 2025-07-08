import os
import logging
from typing import Dict, List

import requests

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
log = logging.getLogger(__name__)

MAX_OBJ_FETCH_NUM_PER_REQ = int(os.environ.get("MAX_OBJ_FETCH_NUM_PER_REQ", "100"))


def _get_header(token: str = None) -> Dict[str, str]:
    astro_api_token = token if token else os.getenv("ASTRO_API_TOKEN")
    if not astro_api_token:
        raise ValueError("ASTRO_API_TOKEN environment variable is not set.")

    return {
        "Authorization": f"Bearer {astro_api_token}",
    }


def _post_header(token: str = None) -> Dict[str, str]:
    post_header = _get_header(token)
    post_header["Content-Type"] = "application/json"
    return post_header


###########
# VARIABLES
###########
def get_starship_variables(deployment_url: str, token: str = None) -> List[Dict[str, str]]:
    url = f"https://{deployment_url}/api/starship/variables"
    response = requests.get(
        url,
        headers=_get_header(token),
        timeout=30,
    )
    response.raise_for_status()
    log.info(f"Successfully fetched variables from {deployment_url}")
    return response.json()


def set_starship_variable(deployment_url: str, variable: Dict[str, str], token: str = None) -> Dict[str, str]:
    url = f"https://{deployment_url}/api/starship/variables"
    # if "value" in variable:
    #     variable["val"] = variable.pop("value")
    log.info(f"Making request to {url} with data:\n{variable}")

    try:
        response = requests.post(
            url,
            json=variable,
            headers=_post_header(token),
            timeout=30,
        )
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        if e.response is not None:
            if e.response.status_code == 409:
                log.warning(
                    f"Variable `{variable['key']}` already exists in target Airflow, skipping existing variables.")
                # do a check on variable value
            else:
                log.error(f"Failed to post variable {variable['key']} to target Airflow: {e}")
                if e.response:
                    log.error(f"Response: {e.response.text}")
                raise
        else:
            log.error(f"An unexpected error occurred while posting variables: {e}")
            raise


def migrate_variables(source_deployment_url: str, target_deployment_url: str, token: str = None) -> None:
    source_variables = get_starship_variables(source_deployment_url, token)

    i = 0
    for variable in source_variables:
        set_starship_variable(target_deployment_url, variable)
        i += 1
        log.info(f"Completed {(i / len(source_variables) * 100):.2f}% for variables")
    log.info("Completed 100% for variables")
    log.info(f"Synced {len(source_variables)} variables to target Airflow.")
    log.info("-" * 80)


###########
# POOLS
###########
def get_starship_pools(deployment_url: str, token: str = None) -> List[Dict[str, str]]:
    url = f"https://{deployment_url}/api/starship/pools"
    response = requests.get(
        url,
        headers=_get_header(token),
        timeout=30,
    )
    response.raise_for_status()
    log.info(f"Successfully fetched pools from {deployment_url}")
    return response.json()


def set_starship_pool(deployment_url: str, pool: Dict[str, str], token: str = None) -> Dict[str, str]:
    url = f"https://{deployment_url}/api/starship/pools"
    for key in ("deferred_slots", "occupied_slots", "open_slots", "queued_slots", "scheduled_slots"):
        pool.pop(key, None)

    log.info(f"Making request to {url} with data:\n{pool}")

    try:
        response = requests.post(
            url,
            json=pool,
            headers=_get_header(token),
            timeout=30,
        )
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        if e.response is not None:
            if e.response.status_code == 409:
                log.warning(f"Pool `{pool['name']}` already exists in target Airflow, skipping existing pools.")
            else:
                log.error(f"Failed to post pool {pool['name']} to target Airflow: {e}")
                if e.response:
                    log.error(f"Response: {e.response.text}")
                raise
        else:
            log.error(f"An unexpected error occurred while posting pools: {e}")
            raise


def migrate_pools(source_deployment_url: str, target_deployment_url: str, token: str = None) -> None:
    source_pools = get_starship_pools(source_deployment_url, token)

    i = 0
    for pool in source_pools:
        set_starship_pool(target_deployment_url, pool)
        i += 1
        log.info(f"Completed {(i / len(source_pools) * 100):.2f}% for pools")
    log.info("Completed 100% for pools")
    log.info(f"Synced {len(source_pools)} pools to target Airflow.")
    log.info("-" * 80)


##########
# DAG History
##########
def get_all_dags(deployment_url: str, token: str) -> List[str]:
    url = f"https://{deployment_url}/api/starship/dags"
    response = requests.get(
        url,
        headers=_get_header(token),
        timeout=60,
    )
    response.raise_for_status()
    log.info(f"Successfully fetched DAGs from {deployment_url}")
    result = response.json()
    return [dag["dag_id"] for dag in result]


def get_dag_runs(deployment_url, dag_id, offset, token: str = None):
    url = f"https://{deployment_url}/api/starship/dag_runs"
    params = {"dag_id": dag_id, "offset": offset, "limit": MAX_OBJ_FETCH_NUM_PER_REQ}
    response = requests.get(
        url,
        params=params,
        headers=_get_header(token),
        timeout=60,
    )
    response.raise_for_status()
    result = response.json()
    return result.get("dag_runs", [])


def get_all_dag_runs(deployment_url, dag_id, token: str = None):
    offset = 0
    all_dag_runs = []
    while True:
        log.info(f"Getting dag runs for dag_id: {dag_id}, offset: {offset}")
        dag_runs = get_dag_runs(deployment_url, dag_id, offset, token)
        all_dag_runs.extend(dag_runs)
        if len(dag_runs) < MAX_OBJ_FETCH_NUM_PER_REQ:
            break
        offset += MAX_OBJ_FETCH_NUM_PER_REQ
    return all_dag_runs


def post_dag_run(deployment_url, dag_id, dag_run, token: str = None):
    url = f"https://{deployment_url}/api/starship/dag_runs"
    data = {"dag_runs": [dag_run]}
    log.info(f"Making request to {url} with data:\n{data}")
    try:
        response = requests.post(
            url,
            json=data,
            headers=_post_header(token),
            timeout=30,
        )
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        if e.response is not None:
            if e.response.status_code == 409:
                log.warning(
                    f"Dag run already exists in target Airflow for dag_id: {dag_id}, run_id: {dag_run.get('run_id')}")
            else:
                log.error(f"Failed to post individual dag run for dag_id: {dag_id}: {e}")
                if e.response:
                    log.error(f"Response: {e.response.text}")
                raise
        else:
            log.error(f"An unexpected error occurred while posting dag runs for {dag_id}: {e}")
            raise


def migrate_dag_runs(source_deployment_url, target_deployment_url, token: str = None):
    # ['basic_dag', 'example_astronauts', 'dag_with_custom_pool', 'dag_with_variable']
    all_dag_ids = get_all_dags(source_deployment_url, token)

    for dag_id in all_dag_ids:
        log.info(f"Getting all dag runs for dag_id: {dag_id}")
        dag_runs = get_all_dag_runs(source_deployment_url, dag_id)
        if not dag_runs:
            log.info(f"No DAG Runs to sync for dag_id: {dag_id}")
            continue

        for i, dag_run in enumerate(dag_runs):
            post_dag_run(target_deployment_url, dag_id, dag_run, token)
            log.info(f"Posted {i}/{len(dag_runs)} dag runs for dag_id: {dag_id}")
        log.info(f"Completed 100% for DAG Runs of dag_id: {dag_id}")
        log.info(f"Synced {len(dag_runs)} DAG Runs to target Airflow")
        log.info("-" * 80)


##########
# Task History
##########
def get_task_instances(deployment_url, dag_id, offset, token: str = None):
    url = f"https://{deployment_url}/api/starship/task_instances"
    params = {"dag_id": dag_id, "offset": offset, "limit": MAX_OBJ_FETCH_NUM_PER_REQ}
    log.info(f"Making request to {url} with params: {params}")
    response = requests.get(
        url,
        params=params,
        headers=_get_header(token),
        timeout=60,
    )
    response.raise_for_status()
    result = response.json()
    return result.get("task_instances", [])


def get_all_task_instances(deployment_url, dag_id, token: str = None):
    offset = 0
    all_task_instances = []
    while True:
        log.info(f"Getting Task Instances for dag_id: {dag_id}, offset: {offset}")
        task_instances = get_task_instances(deployment_url, dag_id, offset, token)
        all_task_instances.extend(task_instances)
        if len(task_instances) < MAX_OBJ_FETCH_NUM_PER_REQ:
            break
        offset += MAX_OBJ_FETCH_NUM_PER_REQ
    return all_task_instances


def post_task_instance(deployment_url, dag_id, task_instance, token: str = None):
    url = f"https://{deployment_url}/api/starship/task_instances"
    data = {"task_instances": [task_instance]}
    log.info(f"Posting individual task_instance to {url} with data:\n{data}")
    try:
        response = requests.post(
            url,
            json=data,
            headers=_post_header(token),
            timeout=30,
        )
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        if e.response is not None:
            if e.response.status_code == 409:
                log.warning(
                    f"Task Instance already exists for dag_id: {dag_id}, task_id: {task_instance.get('task_id')}, execution_date: {task_instance.get('execution_date')}")
            else:
                log.error(f"Failed to post individual task instance for dag_id: {dag_id}: {e}")
                if e.response:
                    log.error(f"Response: {e.response.text}")
                raise
        else:
            log.error(f"An unexpected error occurred while posting Task Instances for {dag_id}: {e}")
            raise


def migrate_task_instances(source_deployment_url, target_deployment_url, token: str = None):
    # ['basic_dag', 'example_astronauts', 'dag_with_custom_pool', 'dag_with_variable']
    all_dag_ids = get_all_dags(source_deployment_url, token)

    for dag_id in all_dag_ids:
        log.info(f"Getting all Task Instances for dag_id: {dag_id}")
        task_instances = get_all_task_instances(source_deployment_url, dag_id)
        if not task_instances:
            log.info(f"No Task Instances to sync for dag_id: {dag_id}")
            continue

        for i, task_instance in enumerate(task_instances, 1):
            post_task_instance(target_deployment_url, dag_id, task_instance, token)
            log.info(f"Posted {i}/{len(task_instances)} task instances")
        log.info(f"Completed 100% for task instances of dag_id: {dag_id}")
        log.info(f"Synced {len(task_instances)} task instances to target Airflow")
        log.info("-" * 80)


def migrate_metadata():
    pass