from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from include.get_deployments import get_deployments
from include.create_backup_workspaces import create_backup_workspaces
from airflow.models.xcom_arg import XComArg
from include.create_backup_deployments import create_backup_deployments

default_args = {
    "owner": "airflow",
    "retries": 3,
    "retry_delay": 60,
}

with DAG(
    dag_id="dr_maintenance_dag",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    description="Disaster recovery: create backup workspaces from primary deployments",
) as dag:

    get_deployments_task = PythonOperator(
        task_id="get_deployments",
        python_callable=get_deployments,
    )

    create_backup_workspaces_task = PythonOperator(
        task_id="create_backup_workspaces",
        python_callable=create_backup_workspaces,
        op_args=[XComArg(get_deployments_task)],
    )

    create_backup_deployments_task = PythonOperator(
        task_id="create_backup_deployments",
        python_callable=create_backup_deployments,
    )

    get_deployments_task >> create_backup_workspaces_task >> create_backup_deployments_task