from datetime import datetime
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from include.get_workspaces import get_workspaces
from include.create_backup_workspaces import create_backup_workspaces, map_source_workpaces_to_backup
from include.create_backup_deployments import create_backup_deployments, get_source_deployments_payload
from include.manage_backup_hibernation import manage_backup_hibernation
from include.assign_tokens_to_backups import create_token_for_backup_deployments
from include.deploy_to_backup_deployments import deploy_dags_to_backup_deployments
from include.migrate_with_starship import migrate_metadata

@dag(
    dag_id="dr_maintenance_dag_new",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    default_args={
        "owner": "astro",
        "retries": 1,
        "retry_delay": 5,
    },
    description="Disaster recovery: create backup workspaces from primary deployments",
    tags=["Disaster Recovery", "Maintenance", "Example DAG"],
)
def dr_maintenance_dag():
    @task(task_id="get_source_workspaces")
    def get_source_workspaces_task():
        return get_workspaces()
    
    @task(task_id="map_source_workspaces_to_backup")
    def map_source_workpaces_to_backup_task(workspaces):
        mapped_workspaces = map_source_workpaces_to_backup(workspaces)
        return mapped_workspaces

    @task(map_index_template="{{ source_workspace_id }}")
    def create_backup_workspaces_task(workspace):
        context = get_current_context()
        source_workspace_id = workspace.get("source_workspace_id")
        backup_workspace_name = workspace.get("backup_workspace_name")
        context["source_workspace_id"] = f"Backup for Workspace ID - {source_workspace_id}"
        create_backup_workspaces(source_workspace_id, backup_workspace_name, context)

    @task(trigger_rule="none_failed", map_index_template="{{ source_workspace_id }}")
    def get_source_deployments_task(workspace_ids):
        context = get_current_context()
        source_workspace_id = workspace_ids.get("source_workspace_id")
        backup_workspace_id = workspace_ids.get("backup_workspace_id")
        context["source_workspace_id"] = f"Deployments for Workspace ID - {source_workspace_id}"
        workspace_deployments_mapping = get_source_deployments_payload(source_workspace_id, backup_workspace_id, context)
        return workspace_deployments_mapping
    
    @task
    def create_deployment_payloads(nested: list[list[dict]]) -> list[dict]:
        return [item for sublist in nested for item in sublist]

    @task(trigger_rule="none_failed", map_index_template="{{ source_deployment_id }}")
    def create_backup_deployments_task(deployment):
        context = get_current_context()
        source_deployment_id = deployment.get("source_deployment_id")
        deployment_payload = deployment.get("deployment_payload")
        context["source_deployment_id"] = f"Backup for Deployment ID - {source_deployment_id}"
        create_backup_deployments(deployment_payload, context)

    @task(trigger_rule="none_failed", map_index_template="{{ backup_deployment_id }}")
    def manage_backup_hibernation_task(action, deployment_id):
        context = get_current_context()
        context["backup_deployment_id"] = deployment_id
        manage_backup_hibernation(deployment_id, action)

    @task(trigger_rule="none_failed", map_index_template="{{ backup_deployment_id }}")
    def create_and_set_token_for_backup_deployments_task(deployment_id):
        context = get_current_context()
        context["backup_deployment_id"] = deployment_id
        create_token_for_backup_deployments(context)

    @task(trigger_rule="none_failed", map_index_template="{{ backup_deployment_id }}")
    def deploy_dags_to_backup_deployments_task(deployment_id):
        context = get_current_context()
        context = get_current_context()
        context["backup_deployment_id"] = deployment_id
        deploy_dags_to_backup_deployments(context)

    @task(trigger_rule="none_failed", map_index_template="{{ backup_deployment_id }}")
    def migrate_metadata_to_backup_deployments_task(deployment_id):
        context = get_current_context()
        context["backup_deployment_id"] = deployment_id
        migrate_metadata(deployment_id, context)

    workspaces = get_source_workspaces_task()
    mapped_workspaces = map_source_workpaces_to_backup_task(workspaces)

    workspace_id_mapping = create_backup_workspaces_task.expand(workspace=mapped_workspaces)
    deployments = get_source_deployments_task.expand(workspace_ids=workspace_id_mapping)

    deployments_payload = create_deployment_payloads(deployments)

    created_deployments_ids = create_backup_deployments_task.expand(deployment=deployments_payload)

    unhibernate_task = manage_backup_hibernation_task.override(task_id="unhibernate_backup_deployments").partial(action="unhibernate").expand(deployment_id=created_deployments_ids)
    
    hibernate_task = manage_backup_hibernation_task.override(task_id="hibernate_backup_deployments").partial(action="hibernate").expand(deployment_id=created_deployments_ids)

    create_token = create_and_set_token_for_backup_deployments_task.expand(deployment_id=created_deployments_ids)

    deploy_dags = deploy_dags_to_backup_deployments_task.expand(deployment_id=deployments_payload)

    migrate_dags_metadata = migrate_metadata_to_backup_deployments_task.expand(deployment_id=deployments_payload)

    unhibernate_task >> create_token >> deploy_dags >> migrate_dags_metadata >> hibernate_task

dr_maintenance_dag()