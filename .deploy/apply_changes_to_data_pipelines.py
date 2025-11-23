from azure.identity import ClientSecretCredential
import requests
import argparse

FABRIC_SCOPE = "https://api.fabric.microsoft.com/.default"
BASE_URL = "https://api.fabric.microsoft.com/v1"


def get_token(
        cli_id:str,
        cli_secret:str,
        tenant_id:str
):
    credential = ClientSecretCredential(
        client_id=cli_id,
        client_secret=cli_secret,
        tenant_id=tenant_id
    )

    token = credential.get_token(FABRIC_SCOPE)
    return token.token


def get_all_dataflow_data(access_token:str, workspace_id:str):
    url = f"{BASE_URL}/workspaces/{workspace_id}/dataflows"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": f"application/json",
    }
    dataflows = []
    continuation = None

    while True:
        params = {}
        if continuation:
            params["continuationToken"] = continuation
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        payload = response.json()

        continuation = payload.get("continuationToken")

        dataflows.extend(payload.get("value", []))

        if not continuation:
            break
    return dataflows

def run_apply_changes(access_token, workspace_id, dataflow_id):
    url = f"{BASE_URL}/workspaces/{workspace_id}/dataflows/{dataflow_id}/jobs/ApplyChanges/instances"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": f"application/json",
    }
    resp = requests.post(url=url, headers=headers)
    if resp.status_code != 202:
        raise RuntimeError(
            f"ApplyChanges failed for dataflow {dataflow_id}"
            f"{resp.status_code} {resp.text}"
            )
    return resp.headers.get("Location"), resp.headers.get("Retry-After")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--tenant-id", required=True)
    parser.add_argument("--client-id", required=True)
    parser.add_argument("--client-secret", required=True)
    parser.add_argument("--workspace-id", required=True)
    args = parser.parse_args()

    token = get_token(
        tenant_id=args.tenant_id,
        cli_id=args.client_id,
        cli_secret=args.client_secret,
    )

    print(f"Listing dataflows in workspace {args.workspace_id}...")
    dataflows = get_all_dataflow_data(token, args.workspace_id)

    if not dataflows:
        print("No dataflows found.")
        return

    for df in dataflows:
        df_id = df["id"]
        df_name = df.get("displayName", "")
        print(f"Triggering ApplyChanges for dataflow {df_name} ({df_id}) ...")

        try:
            location, retry_after = run_apply_changes(
                token, args.workspace_id, df_id
            )
            print(
                f"  -> Accepted (202). Location={location}, Retry-After={retry_after}"
            )
        except Exception as ex:
            print(f"  -> ERROR: {ex}")


if __name__ == "__main__":
    main()