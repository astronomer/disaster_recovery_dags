import os
import time
import requests
from include.get_deployments import get_deployments

ASTRO_API_TOKEN = os.getenv("ASTRO_API_TOKEN")
ORG_ID = os.getenv("ASTRO_ORGANIZATION_ID")

if not ASTRO_API_TOKEN or not ORG_ID:
    raise EnvironmentError("Missing ASTRO_API_TOKEN or ASTRO_ORGANIZATION_ID in environment.")

HEADERS = {
    "Authorization": f"Bearer {ASTRO_API_TOKEN}",
    "Content-Type": "application/json",
}

BASE_URL = "https://api.astronomer.io/platform/v1beta1"


def wait_for_deployment_state(deployment_id, status, max_attempts=10, delay=15):
    """Polls until the deployment reaches one of the expected status values or fails."""
    if isinstance(status, str):
        target_states = [status]
    else:
        target_states = status

    for attempt in range(max_attempts):
        url = f"{BASE_URL}/organizations/{ORG_ID}/deployments/{deployment_id}"
        resp = requests.get(url, headers=HEADERS)
        if resp.status_code != 200:
            print(f"⚠️ Could not check status for deployment {deployment_id}")
            return False
        current = resp.json().get("status")
        print(f"⏳ Deployment {deployment_id} status: {current}")
        if current in target_states:
            return True
        if current == "FAILED":
            print(f"❌ Deployment {deployment_id} entered FAILED state.")
            return False
        time.sleep(delay)

    print(f"❌ Deployment {deployment_id} did not reach one of {target_states} after polling.")
    return False


def manage_backup_hibernation(deployment_set="backup", action="hibernate"):
    if deployment_set not in ["source", "backup"]:
        raise ValueError("deployment_set must be 'source' or 'backup'")
    if action not in ["hibernate", "unhibernate"]:
        raise ValueError("action must be 'hibernate' or 'unhibernate'")

    deployments = get_deployments(mode=deployment_set)
    print(f"🔍 Managing hibernation for {len(deployments)} {deployment_set} deployments - action: {action}")

    for d in deployments:
        deployment_id = d["deployment_id"]
        deployment_name = d["deployment_name"]
        hibernation_url = f"{BASE_URL}/organizations/{ORG_ID}/deployments/{deployment_id}/hibernation-override"

        print(f"🌐 POST {hibernation_url} → {action.upper()} {deployment_name} ({deployment_id})")

        try:
            response = requests.post(
                hibernation_url,
                headers=HEADERS,
                json={"isHibernating": action == "hibernate"}
            )

            if response.status_code == 200:
                print(f"✅ Triggered {action} for {deployment_name}")
            else:
                print(f"❌ Failed to trigger {action} for {deployment_name} ({deployment_id})")
                print(f"    • Status: {response.status_code}")
                print(f"    • Body: {response.text}")
                continue

        except Exception as e:
            print(f"🔥 Exception while attempting to {action} {deployment_name}: {str(e)}")
            continue

        # Wait for final expected state
        target = ["HIBERNATING"] if action == "hibernate" else ["HEALTHY", "READY"]
        if wait_for_deployment_state(deployment_id, status=target):
            print(f"✅ {deployment_name} reached target state {target}")
        else:
            print(f"❌ {deployment_name} did not reach target state {target}")