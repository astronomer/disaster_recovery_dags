"""
This DAG migrates metadata using Starship.
"""
from datetime import datetime
from collections import defaultdict
from typing import List, Dict

from airflow.models import DAG
from airflow.decorators import task
from airflow.operators.python import get_current_context
from airflow.exceptions import AirflowException

from include.create_backup_workspaces import map_source_workpaces_to_backup
from include.get_workspaces import get_workspaces, get_deployment_mappings
from include.manage_backup_hibernation import manage_backup_hibernation
from include.migrate_with_starship import migrate_variables, migrate_pools, migrate_dag_runs, migrate_task_instances

with DAG(
        "starship_migration_dag",
        description="Migrate metadata using Starship",
        start_date=datetime(2025, 1, 1),
        schedule="@daily",
        catchup=False,
        default_args={
            "owner": "astro",
            "retries": 0,
        },
        doc_md=__doc__,
        # params={
        #     # TODO: add on-recovery
        # }
) as dag:
    # @task(task_id="get_source_workspaces")
    # def get_source_workspaces_task():
    #     return get_workspaces()


    # @task(task_id="map_source_workspaces_to_backup")
    # def map_source_workpaces_to_backup_task(workspaces):
    #     mapped_workspaces = map_source_workpaces_to_backup(workspaces)
    #     return mapped_workspaces


    # @task.python(task_id="generate_deployment_mappings")
    # def generate_deployment_mappings_task(workspace_maps):
    #     all_deployment_mappings = []

    #     for workspace_map in workspace_maps:
    #         source_ws_id = workspace_map["source_workspace_id"]
    #         backup_ws_name = workspace_map["backup_workspace_name"]

    #         deployment_mappings = get_deployment_mappings(source_ws_id, backup_ws_name)

    #         if not deployment_mappings:
    #             print(f"No deployment mappings found for workspace pair: {source_ws_id} -> {backup_ws_name}")
    #             continue

    #         all_deployment_mappings.extend(deployment_mappings)

    #     if not all_deployment_mappings:
    #         print("No deployment mappings found for any workspace pairs.")
    #         return []

    #     print(all_deployment_mappings)
    #     return all_deployment_mappings


    # @task.python(task_id="unhibernate_backup_deployments_task", map_index_template="{{ backup_deployment }}")
    # def unhibernate_backup_deployments_task(deployment_mapping: Dict[str, str]):
    #     backup_dep_id = deployment_mapping.get("backup_deployment_id")
    #     deployment_name = deployment_mapping.get("deployment_name")

    #     if not backup_dep_id:
    #         raise AirflowException(f"Missing backup_deployment_id for mapping '{deployment_name}'. Cannot unhibernate.")

    #     context = get_current_context()
    #     context["backup_deployment"] = f"{deployment_name} (ID: {backup_dep_id})"

    #     print(f"Processing backup deployment '{deployment_name}' (ID: {backup_dep_id}) for unhibernation.")

    #     manage_backup_hibernation(
    #         deployment_id=backup_dep_id,
    #         action="unhibernate",
    #     )

    #     return


    @task(task_id="starship_variables_migration", map_index_template="{{ source_deployment }}")
    def starship_variables_migration_task(deployment_mapping: List[Dict[str, str]]):
        source_deployment_id = deployment_mapping.get("source_deployment_id")
        source_deployment_url = deployment_mapping.get("source_deployment_url")
        target_deployment_url = deployment_mapping.get("backup_deployment_url")

        context = get_current_context()
        context["source_deployment"] = f"{deployment_mapping.get('_deployment_name')} (ID: {source_deployment_id})"

        if not source_deployment_url or not target_deployment_url:
            raise AirflowException(f"Missing source or target deployment URL in mapping: {deployment_mapping}")

        migrate_variables(
            source_deployment_url=source_deployment_url,
            target_deployment_url=target_deployment_url,
        )


    @task(task_id="starship_pools_migration", map_index_template="{{ source_deployment }}")
    def starship_pools_migration_task(deployment_mapping: List[Dict[str, str]]):
        source_deployment_id = deployment_mapping.get("source_deployment_id")
        source_deployment_url = deployment_mapping.get("source_deployment_url")
        target_deployment_url = deployment_mapping.get("backup_deployment_url")

        context = get_current_context()
        context["source_deployment"] = f"{deployment_mapping.get('_deployment_name')} (ID: {source_deployment_id})"

        if not source_deployment_url or not target_deployment_url:
            raise AirflowException(f"Missing source or target deployment URL in mapping: {deployment_mapping}")

        migrate_pools(
            source_deployment_url=source_deployment_url,
            target_deployment_url=target_deployment_url,
        )


    @task(task_id="starship_dag_runs_migration", map_index_template="{{ source_deployment }}")
    def starship_dag_runs_migration_task(deployment_mapping: List[Dict[str, str]]):
        source_deployment_id = deployment_mapping.get("source_deployment_id")
        source_deployment_url = deployment_mapping.get("source_deployment_url")
        target_deployment_url = deployment_mapping.get("backup_deployment_url")

        context = get_current_context()
        context["source_deployment"] = f"{deployment_mapping.get('_deployment_name')} (ID: {source_deployment_id})"

        if not source_deployment_url or not target_deployment_url:
            raise AirflowException(f"Missing source or target deployment URL in mapping: {deployment_mapping}")

        migrate_dag_runs(
            source_deployment_url=source_deployment_url,
            target_deployment_url=target_deployment_url,
        )


    @task(task_id="starship_task_instances_migration", map_index_template="{{ source_deployment }}")
    def starship_task_instances_migration_task(deployment_mapping: List[Dict[str, str]]):
        source_deployment_id = deployment_mapping.get("source_deployment_id")
        source_deployment_url = deployment_mapping.get("source_deployment_url")
        target_deployment_url = deployment_mapping.get("backup_deployment_url")

        context = get_current_context()
        context["source_deployment"] = f"{deployment_mapping.get('_deployment_name')} (ID: {source_deployment_id})"

        if not source_deployment_url or not target_deployment_url:
            raise AirflowException(f"Missing source or target deployment URL in mapping: {deployment_mapping}")

        migrate_task_instances(
            source_deployment_url=source_deployment_url,
            target_deployment_url=target_deployment_url,
        )


    @task(task_id="hibernate_backup_deployments_task", map_index_template="{{ backup_deployment }}")
    def hibernate_backup_deployments_task(deployment_mapping: Dict[str, str]):
        backup_dep_id = deployment_mapping.get("backup_deployment_id")
        deployment_name = deployment_mapping.get("deployment_name")

        if not backup_dep_id:
            raise AirflowException(f"Missing backup_deployment_id for mapping '{deployment_name}'. Cannot unhibernate.")

        context = get_current_context()
        context["backup_deployment"] = f"{deployment_name} (ID: {backup_dep_id})"

        print(f"Processing backup deployment '{deployment_name}' (ID: {backup_dep_id}) for unhibernation.")

        manage_backup_hibernation(
            deployment_id=backup_dep_id,
            action="hibernate",
        )

        return


    # unhibernate_backup_deployments() >> migrate_variables_task >> migrate_connections_task >> migrate_pools_task >> migrate_dag_history_task
    all_deployment_mappings = generate_deployment_mappings_task(
        map_source_workpaces_to_backup_task(get_source_workspaces_task()))
    unhibernate_backup_deployments_task.expand(
        deployment_mapping=all_deployment_mappings
    ) >> [
        starship_variables_migration_task.expand(deployment_mapping=all_deployment_mappings),
        starship_pools_migration_task.expand(deployment_mapping=all_deployment_mappings),
        starship_dag_runs_migration_task.expand(deployment_mapping=all_deployment_mappings),
        starship_task_instances_migration_task.expand(deployment_mapping=all_deployment_mappings)
    ] >> hibernate_backup_deployments_task.expand(
        deployment_mapping=all_deployment_mappings
    )

