from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import requests

def fetch_workspace_direct():
    workspace_id = "clr2kw5rp00db01h86b627mxa"
    token = os.getenv("ASTRO_API_TOKEN")

    if not token:
        raise ValueError("ASTRO_API_TOKEN is not set in the environment")

    url = f"https://api.astronomer.io/platform/v1beta1/workspaces/{workspace_id}"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    response = requests.get(url, headers=headers)

    print(f"Status Code: {response.status_code}")
    try:
        json_response = response.json()
        print(f"Response JSON:\n{json_response}")
    except Exception as e:
        print(f"Failed to parse JSON: {e}")
        print(f"Raw Response:\n{response.text}")

    if response.status_code != 200:
        raise Exception(f"Workspace fetch failed with status {response.status_code}")

with DAG(
    dag_id="debug_fetch_workspace_dag",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    schedule=None,
    tags=["debug", "astro"],
) as dag:

    fetch_workspace = PythonOperator(
        task_id="fetch_workspace_directly",
        python_callable=fetch_workspace_direct,
    )