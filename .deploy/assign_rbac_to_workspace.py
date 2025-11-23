import os
import argparse
import requests
import json

from pathlib import Path
from azure.identity import ClientSecretCredential
from typing import Dict, Any, List

FABRIC_SCOPE = "https://api.fabric.microsoft.com/.default"
FABRIC_BASE_URL = "https://api.fabric.microsoft.com/v1"

root = Path(__file__).resolve().parents[1]

def get_token():
    credential = ClientSecretCredential(
        tenant_id=os.getenv("AZURE_TENANT_ID"),
        client_id=os.getenv("AZURE_CLIENT_ID"),
        client_secret=os.getenv("AZURE_CLIENT_SECRET")
    )
    return credential.get_token(FABRIC_SCOPE).token

def get_headers(token: str) -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

def list_workspace_role_assignments(workspace_id: str, headers: Dict[str, str]) -> List[Dict[str, Any]]:
    url = f"{FABRIC_BASE_URL}/workspaces/{workspace_id}/roleAssignments"
    assignments: List[Dict[str, Any]] = []

    while url:
        resp = requests.get(url, headers=headers)

        if not resp.ok:
            print("\n[Fabric API error]")
            print("URL:", resp.url)
            print("Status code:", resp.status_code)
            print("Response text:", resp.text)
            try:
                print("Response JSON:", resp.json())
            except Exception:
                pass
            resp.raise_for_status()

        data = resp.json()
        assignments.extend(data.get("value", []))
        url = data.get("continuationUri")

    return assignments

def add_workspace_role_assignment(
    workspace_id: str,
    identity: str,
    principal_type: str,
    role: str,
    headers: Dict[str, str],
) -> None:
    url = f"{FABRIC_BASE_URL}/workspaces/{workspace_id}/roleAssignments"
    payload = {
        "principal": {
            "id": identity,
            "type": principal_type,
        },
        "role": role,
    }
    resp = requests.post(url, headers=headers, json=payload)
    if resp.status_code not in (200, 201):
        raise RuntimeError(
            f"Failed to add role assignment for {identity} in workspace {workspace_id}: "
            f"{resp.status_code} {resp.text}"
        )

def update_workspace_role_assignment(
    workspace_id: str,
    assignment_id: str,
    new_role: str,
    headers: Dict[str, str],
) -> None:
    url = f"{FABRIC_BASE_URL}/workspaces/{workspace_id}/roleAssignments/{assignment_id}"
    payload = {
        "role": new_role
    }
    resp = requests.patch(url, headers=headers, json=payload)
    if resp.status_code not in (200, 204):
        raise RuntimeError(
            f"Failed to update role assignment {assignment_id} in workspace {workspace_id}: "
            f"{resp.status_code} {resp.text}"
        )
    
def delete_workspace_role_assignment(
    workspace_id: str,
    assignment_id: str,
    headers: Dict[str, str],
) -> None:
    url = f"{FABRIC_BASE_URL}/workspaces/{workspace_id}/roleAssignments/{assignment_id}"
    resp = requests.delete(url, headers=headers)
    if resp.status_code != 200:
        raise RuntimeError(
            f"Failed to delete role assignment {assignment_id} in workspace {workspace_id}: "
            f"{resp.status_code} {resp.text}"
        )

    
def apply_workspace_rbac_for_env(config: Dict[str, Any], environment:str, headers: Dict[str, str]) -> None:
    config = config.get(environment)
    entities = config.get("workspace_permission_entities")
    workspace_id = config.get("workspace_id")
    if entities is None:
        raise ValueError(f"No payloads found for environment '{environment}' in config")
    
    print("Listing existing role assignments")
    existing = list_workspace_role_assignments(workspace_id, headers)
    existing_map: Dict[str, Dict[str, Any]] = {}
    for a in existing:
        principal = a.get("principal", {})
        pid = principal.get("id")
        if pid:
            existing_map[pid] = a

    desired_principals = {e["identity"] for e in entities}

    for entity in entities:
        new_entity_identity = entity["identity"]
        new_entity_type = entity["type"]
        new_entity_role = entity["role"]
        new_entity_name = entity["name"]

        existing_entity = existing_map.get(new_entity_identity)
        if existing_entity:
            existing_entity_object_id = existing_entity.get("id")
            existing_entity_role = existing_entity.get("role")
            existing_entity_principal = existing_entity.get("principal")
            existing_entity_identity = existing_entity_principal.get("id")
            existing_entity_type = existing_entity_principal.get("type")
            
            if existing_entity_role == new_entity_role:
                print(f"  - {existing_entity_identity} already has role '{existing_entity_role}' → skipping")
                continue
            else: 
                print(f"  - Updating {existing_entity_identity} from role '{existing_entity_role}' to '{new_entity_role}'")
                update_workspace_role_assignment(
                    workspace_id=workspace_id,
                    assignment_id=existing_entity_object_id,
                    new_role=new_entity_role,
                    headers=headers,
                )
        else:
            print(f"  - Adding role '{new_entity_role}' for {new_entity_identity} ({new_entity_type}) - {new_entity_name}")
            add_workspace_role_assignment(
                workspace_id=workspace_id,
                identity=new_entity_identity,
                principal_type=new_entity_type,
                role=new_entity_role,
                headers=headers,
            )

    print("Removing role assignments not present in config")
    for principal_id, assignment in existing_map.items():
        if principal_id in desired_principals:
            continue  # still desired, keep it

        assignment_id = assignment.get("id")
        role = assignment.get("role")
        principal = assignment.get("principal", {})
        p_type = principal.get("type")

        print(f"  - Removing role '{role}' for {principal_id} ({p_type})")
        delete_workspace_role_assignment(
            workspace_id=workspace_id,
            assignment_id=assignment_id,
            headers=headers,
        )

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--environment", required=True, help="DEV | TEST | PROD")
    parser.add_argument("--workspace", required=True, help="Workspace like: orchestration | store | integration")
    args = parser.parse_args()

    env = args.environment.upper()
    workspace = args.workspace.lower()

    print(
        "=======================================",
        "Deploying ACL using fabric cli",
        f"Environment: {env}",
        f"Workspace: {workspace}",
        "=======================================",
        sep="\n"
    )
    rbac_file_path = os.path.join(root, f"workspace/{workspace}/acl.json")
    print(f"Loading ACL file: {rbac_file_path}")
    with open(rbac_file_path, "r", encoding="utf-8") as f:
        cfg = json.load(f)

    if cfg is None:
        raise RuntimeError("Config was not loaded correctly")

    print("Fetching Fabric Token")
    token = get_token()
    print("Generating Headers")
    headers = get_headers(token)

    print(f"Applying workspace RBAC for environment: {env}")
    apply_workspace_rbac_for_env(cfg, env, headers)
    print("Done.")


if __name__ == "__main__":
    main()