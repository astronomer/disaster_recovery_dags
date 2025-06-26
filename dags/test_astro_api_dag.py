from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import requests
import os
import logging

ASTRO_API_BASE = "https://api.astronomer.io/platform"
ASTRO_API_TOKEN = os.getenv("ASTRO_API_TOKEN")
ASTRO_ORGANIZATION_ID = os.getenv("ASTRO_ORGANIZATION_ID")
ASTRO_WORKSPACE_ID_TO_VALIDATE = os.getenv("ASTRO_WORKSPACE_ID_TO_VALIDATE")

def list_workspaces():
    headers = {
        "Authorization": f"Bearer {ASTRO_API_TOKEN}",
        "Content-Type": "application/json"
    }
    url = f"{ASTRO_API_BASE}/v1beta1/organizations/{ASTRO_ORGANIZATION_ID}/workspaces"
    response = requests.get(url, headers=headers)
    response.raise_for_status()

    try:
        workspaces = response.json()
        logging.info("📋 Workspaces in org %s:", ASTRO_ORGANIZATION_ID)
        for ws in workspaces:
            if isinstance(ws, dict) and ws.get("organizationId") == ASTRO_ORGANIZATION_ID:
                logging.info("  %s: %s", ws.get("id"), ws.get("label"))
    except Exception as e:
        logging.error("Failed to parse workspace list: %s", str(e))
        raise

def validate_workspace_id():
    if not ASTRO_ORGANIZATION_ID or not ASTRO_WORKSPACE_ID_TO_VALIDATE:
        raise ValueError("ASTRO_ORGANIZATION_ID and ASTRO_WORKSPACE_ID_TO_VALIDATE must be set")

    headers = {
        "Authorization": f"Bearer {ASTRO_API_TOKEN}",
        "Content-Type": "application/json"
    }
    url = f"{ASTRO_API_BASE}/v1beta1/organizations/{ASTRO_ORGANIZATION_ID}/workspaces"
    response = requests.get(url, headers=headers)
    response.raise_for_status()

    workspaces = response.json()
    logging.info("🔍 Raw workspace response:\n%s", workspaces)

    match = next(
        (
            ws for ws in workspaces
            if isinstance(ws, dict)
            and ws.get("organizationId") == ASTRO_ORGANIZATION_ID
            and ws.get("id") == ASTRO_WORKSPACE_ID_TO_VALIDATE
        ),
        None
    )

    if not match:
        raise ValueError(f"❌ Workspace ID {ASTRO_WORKSPACE_ID_TO_VALIDATE} not found in org {ASTRO_ORGANIZATION_ID}")
    else:
        logging.info("✅ Workspace ID %s found with label: %s", match['id'], match.get('label'))

with DAG(
    dag_id="validate_workspace_id_dag",
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=["validation"],
) as dag:
    list_task = PythonOperator(
        task_id="list_workspaces",
        python_callable=list_workspaces,
    )

    validate_task = PythonOperator(
        task_id="validate_workspace_id",
        python_callable=validate_workspace_id,
    )

    list_task >> validate_task