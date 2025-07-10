# Disaster Recovery DAGs for Astronomer

This project provides an automated and modular way to manage **backup deployments** for selected Astronomer workspaces using the Astro API. It includes a fully orchestrated DAG (`dr_maintenance_dag`) and helper scripts to replicate, update, and hibernate backup deployments for disaster recovery.

---

## 🔧 Purpose

This project ensures that critical Astronomer deployments have up-to-date and immediately usable backups.

- Creates and maintains **backup deployments** in a target cluster.
- Schedules backup deployment **replication, updates, and hibernation**.
- **Clones API tokens** from source deployments/workspaces and applies equivalent roles in the backup environment.
- Ensures DAGs and environment are replicated using **historical deploys**.
- Uses [Astro Starship](https://docs.astronomer.io/astro/starship) to **migrate metadata DBs** from source to backup deployments.

---

## ✅ Requirements

Before running the DAG or any manual scripts, ensure the following are available:

- **Astro API Token** with `Organization Admin` permissions
- **Astro Organization ID**
- **Target Cluster ID** where backup deployments should be created

These values should be stored as environment variables, as demonstrated by `.env_example`

## 🚀 `dr_maintenance_dag` Breakdown

This DAG orchestrates the full disaster recovery workflow. Below is a walkthrough of each task:

| Task ID                          | Description |
|----------------------------------|-------------|
| `get_source_workspaces_task`     | Loads the list of source workspaces to back up from `workspaces_to_backup.json`. |
| `map_source_workpaces_to_backup_task` | Maps source to backup workspace names and prepares them for ID resolution. |
| `create_backup_workspaces_task`  | Creates backup workspaces if they do not already exist. |
| `get_source_deployments_task`    | Fetches deployments from source workspaces using Astro API. |
| `create_backup_deployments_task` | Creates backup deployments in the target cluster with identical configurations. |
| `manage_backup_hibernation_task` (as `unhibernate_backup_deployments`) | Unhibernates backup deployments to prepare them for deploy and metadata sync. |
| `replicate_deploy_to_backup_task`| Uses recent deploy history to replicate DAGs, environment, and code to the backup deployment. |
| `starship_migration_task`        | Migrates metadata DBs (including variables, pools, DAG runs, task instances) from source to backup using Astro Starship. |
| `manage_backup_hibernation_task` (commented out as `hibernate_backup_deployments`) | (Optional) Re-hibernates the backup deployments after migration completes. |

## 🧰 Scripts in `/include`

Each script in the `include` folder can be run independently to manually replicate the DAG logic. They handle tasks like workspace creation, deployment replication, hibernation control, and metadata migration.

| Script | Description | Astro API Endpoints Used |
|--------|-------------|---------------------------|
| `get_workspaces.py` | Loads workspace mappings from JSON, resolves names to IDs using Astro API. | [List Workspaces](https://www.astronomer.io/docs/api/platform-api-reference/workspace/list-workspaces) |
| `create_backup_workspaces.py` | Creates backup workspaces from JSON config. Maps source to backup workspace IDs. | [Create Workspace](https://www.astronomer.io/docs/api/platform-api-reference/workspace/create-workspace) |
| `create_backup_deployments.py` | Clones source deployments into backup workspaces with matching config. | [List Deployments](https://www.astronomer.io/docs/api/platform-api-reference/deployment/list-deployments), [Get Deployment](https://www.astronomer.io/docs/api/platform-api-reference/deployment/get-deployment), [Create Deployment](https://www.astronomer.io/docs/api/platform-api-reference/deployment/create-deployment) |
| `manage_backup_hibernation.py` | Unhibernates or hibernates deployments. Polls status until deployments are healthy. | [Override Hibernation](https://www.astronomer.io/docs/api/platform-api-reference/deployment/override-hibernation), [Get Deployment](https://www.astronomer.io/docs/api/platform-api-reference/deployment/get-deployment) |
| `deploy_to_backup_deployments.py` | Lists deploy history and replicates chosen deploys to backup deployments. Finalizes the deploy. | [List Deploys](https://www.astronomer.io/docs/api/platform-api-reference/deploy/list-deploys), [Create Deploy](https://www.astronomer.io/docs/api/platform-api-reference/deploy/create-deploy), [Finalize Deploy](https://www.astronomer.io/docs/api/platform-api-reference/deploy/finalize-deploy) |
| `migrate_with_starship.py` | Uses Astro Starship to migrate metadata: variables, pools, DAG runs, task instances. | [Starship CLI Docs](https://docs.astronomer.io/astro/starship) |
| `assign_tokens_to_backups.py` | Creates new API tokens for backup workspaces/deployments, mirroring names and roles from source. | [Create Token](https://www.astronomer.io/docs/api/platform-api-reference/auth/create-token) |