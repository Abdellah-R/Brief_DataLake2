import os
from azure.storage.filedatalake import DataLakeServiceClient, generate_directory_sas, DirectorySasPermissions
from azure.identity import ClientSecretCredential
from datetime import datetime, timedelta, timezone
from azure.keyvault.secrets import SecretClient
from dotenv import load_dotenv


load_dotenv()

# Chargement des variables d'environnement
TENANT_ID = os.getenv("TENANT_ID")
CLIENT_ID = os.getenv("SP_ID_SECONDARY")
SP_SECONDARY_PASSWORD = os.getenv("SP_SECONDARY_PASSWORD") 
STORAGE_ACCOUNT_NAME = os.getenv("STORAGE_ACCOUNT_NAME")
CONTAINER_NAME = os.getenv("CONTAINER_NAME")


def generate_sas_token_for_directory(directory_name: str):

    credential = ClientSecretCredential(
        tenant_id=TENANT_ID, 
        client_id=CLIENT_ID, 
        client_secret=SP_SECONDARY_PASSWORD
    )

    dl_service_client = DataLakeServiceClient(
        account_url=f"https://{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/",
        credential=credential
    )

    start_time = datetime.now(timezone.utc)
    expiry_time = start_time + timedelta(hours=1)
    user_delegation_key = dl_service_client.get_user_delegation_key(start_time, expiry_time)

    sas_token = generate_directory_sas(
        account_name=STORAGE_ACCOUNT_NAME,
        file_system_name=CONTAINER_NAME,
        directory_name=directory_name,
        credential=user_delegation_key,
        start=start_time,
        expiry=expiry_time,
        permission=DirectorySasPermissions(read=True, write=True),
    )
    
    return sas_token
