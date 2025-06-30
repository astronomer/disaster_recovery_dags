import os
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.xcom_arg import XComArg
from include import assign_tokens_to_backups
from include.get_deployments import get_deployments
from include.create_backup_workspaces import create_backup_workspaces
from include.create_backup_deployments import create_backup_deployments
from include.manage_backup_hibernation import manage_backup_hibernation
from include.assign_tokens_to_backups import log_token_recreation_plan
from include.migrate_airflow_data import migrate_airflow_data

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
        op_kwargs={"mode": "source"},
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

    unhibernate_backup_deployments_task = PythonOperator(
        task_id="unhibernate_backup_deployments",
        python_callable=manage_backup_hibernation,
        op_kwargs={"deployment_set": "backup", "action": "unhibernate"},
    )

    log_token_recreation_plan_task = PythonOperator(
        task_id="log_token_recreation_plan",
        python_callable=log_token_recreation_plan,
    )

    migrate_airflow_data_task = PythonOperator(
        task_id="migrate_airflow_metadata",
        python_callable=migrate_airflow_data,
        op_kwargs={
            "astro_data": XComArg(log_token_recreation_plan_task),
            "max_obj_post_num_per_req": int(os.environ.get("MAX_OBJ_POST_NUM_PER_REQ", "50")),
            "max_obj_fetch_num_per_req": int(os.environ.get("MAX_OBJ_FETCH_NUM_PER_REQ", "100")),
            "dry_run": (os.environ.get("DRY_RUN", "False").lower() == "true"),
        },
    )

    # hibernate_backup_deployments_task = PythonOperator(
    #     task_id="hibernate_backup_deployments",
    #     python_callable=manage_backup_hibernation,
    #     op_kwargs={"deployment_set": "backup", "action": "hibernate"},
    # )

    # Set task dependencies
    get_deployments_task >> create_backup_workspaces_task >> create_backup_deployments_task >> unhibernate_backup_deployments_task >> log_token_recreation_plan_task >> migrate_airflow_data_task
