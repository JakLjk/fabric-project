import os
from azure.identity import ClientSecretCredential

def get_token(
        tenant_id:str,
        client_id:str,
        client_secret:str,
        scope:str="https://api.fabric.microsoft.com/.default"):
    credential = ClientSecretCredential(
        tenant_id=tenant_id,
        client_id=client_id,
        client_secret=client_secret
    )
    return credential.get_token(scope).token
