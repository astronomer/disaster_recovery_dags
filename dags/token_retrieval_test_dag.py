from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import logging
import json
from include.assign_tokens_to_backups import get_workspace_hierarchy_with_deployments_with_tokens

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "catchup": False,
}

with DAG(
    dag_id="test_workspace_token_hierarchy",
    default_args=default_args,
    schedule_interval=None,
    description="Test DAG to log workspaces, deployments, and token details",
    tags=["test", "tokens", "hierarchy"],
) as dag:

    def log_workspace_token_hierarchy():
        log = logging.getLogger("airflow.task")
        hierarchy = get_workspace_hierarchy_with_deployments_with_tokens()

        # Pretty-print JSON for readability
        log.info("====== WORKSPACE TOKEN HIERARCHY ======")
        log.info(json.dumps(hierarchy, indent=2))
        log.info("=======================================")

    log_hierarchy = PythonOperator(
        task_id="log_workspace_token_hierarchy",
        python_callable=log_workspace_token_hierarchy,
    )