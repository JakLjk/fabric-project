from fabric_cicd import deploy_with_config, append_feature_flag
from azure.identity import ClientSecretCredential
import argparse
from pathlib import Path
import os

print("\n==========================================================")

root = Path(__file__).resolve().parents[1]

parser = argparse.ArgumentParser()
parser.add_argument("--environment", required=True)
parser.add_argument("--workspace", required=True)
args = parser.parse_args()

tenant_id = os.getenv("AZURE_TENANT_ID")
client_id = os.getenv("AZURE_CLIENT_ID")
client_secret = os.getenv("AZURE_CLIENT_SECRET")
print(
    f"Deploying items using fabric CICD module"
    f"Environment: {args.environment}"
    f"Workspace: {args.workspace}")

print("Getting credentials")
credential = ClientSecretCredential(
    tenant_id=tenant_id,
    client_id=client_id,
    client_secret=client_secret
)

print("Appending feature flags")
append_feature_flag("enable_experimental_features")
append_feature_flag("enable_config_deploy")


config_file_path = os.path.join(root, f"workspace/{args.workspace}/config.yml")
print(f"Deploying with config file: {config_file_path}")

deploy_with_config(
    config_file_path=config_file_path,
    environment=args.environment.upper(),
    token_credential=credential,
)