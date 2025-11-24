import os
import pytest
import requests
from datetime import datetime, timedelta, UTC
from azure.identity import ClientSecretCredential
from typing import Dict

DFS_SCOPE = "https://storage.azure.com/.default"

def get_token():
    credential = ClientSecretCredential(
        tenant_id=os.getenv("AZURE_TENANT_ID"),
        client_id=os.getenv("AZURE_CLIENT_ID"),
        client_secret=os.getenv("AZURE_CLIENT_SECRET")
    )
    return credential.get_token(DFS_SCOPE).token


def get_headers(token: str) -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "x-ms-version": "2021-08-06",
    }

def list_files_in_folder(workspace_id: str, lakehouse_id: str, relative_path: str = None):
    """
    Lists files under Files/<relative_path> using OneLake DFS REST.
    Equivalent of 'dir' on that folder.
    """

    base = f"https://onelake.dfs.fabric.microsoft.com/{workspace_id}/{lakehouse_id}/Files"
    # Append relative path if provided
    if relative_path:
        # No leading slash
        relative_path = relative_path.lstrip("/")
        url = f"{base}/{relative_path}?resource=filesystem&recursive=false"
    else:
        url = f"{base}?resource=filesystem&recursive=false"

    token = get_token()

    headers = get_headers(token)

    resp = requests.get(url, headers=headers)
    if resp.status_code != 200:
        raise AssertionError(
            f"Failed to list files: {resp.status_code} {resp.text}"
        )

    # OneLake DFS responses follow ADLS Gen2 directory listing format:
    # { "value": [ { "name": "...", "isDirectory": true/false, "lastModified": "...", ... }, ... ] }
    data = resp.json()
    # If not wrapped in 'value', adapt with resp.json()["paths"] depending on actual shape
    entries = data.get("value", data.get("paths", []))
    return entries

@pytest.fixture(scope="session")
def env():
    return os.environ["FABRIC_ENVIRONMENT"].upper()

@pytest.fixture(scope="session")
def lakehouse_ids(env):
    return os.environ["STORE_WS_ID"], os.environ["PROD_LH1_ID"]

def test_root_folder_has_files(env, lakehouse_ids):
    ws_id, lh_id = lakehouse_ids

    if env == "DEV":
        min_files = 1
    elif env == "TEST":
        min_files = 10
    elif env == "PROD":
        min_files = 100

    entries = list_files_in_folder(ws_id, lh_id, "/")
    files = [e for e in entries if not e.get("isDirectory", False)]

    assert len(files) >= min_files, (
        f"[{env}] Too few files in root: {len(files)} < {min_files}"
    )

def test_root_folder_has_recent_file(env, lakehouse_ids):
    ws_id, lh_id = lakehouse_ids

    if env == "DEV":
        max_age_days = 7
    else:
        max_age_days = 1

    entries = list_files_in_folder(ws_id, lh_id, "/")
    files = [e for e in entries if not e.get("isDirectory", False)]
    assert files, f"[{env}] No files in root"

    cutoff = datetime.now(UTC) - timedelta(days=max_age_days)

    ok = False
    for f in files:
        lm_str = f.get("lastModified") or f.get("lastModifiedTime")
        if not lm_str:
            continue

        try:
            lm = datetime.strptime(lm_str, "%a, %d %b %Y %H:%M:%S GMT").replace(tzinfo=UTC)
        except ValueError:
            try:
                lm = datetime.fromisoformat(lm_str.replace("Z", "+00:00"))
            except Exception:
                continue

        if lm >= cutoff:
            ok = True
            break

    assert ok, f"[{env}] No recent files (<= {max_age_days} days) in root"