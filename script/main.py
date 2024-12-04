import os
import requests
from azure.storage.filedatalake import DataLakeServiceClient
from azure.identity import ClientSecretCredential
from dotenv import load_dotenv
from azure.keyvault.secrets import SecretClient

load_dotenv()

TENANT_ID = os.getenv("TENANT_ID")
CLIENT_ID = os.getenv("SP_ID_SECONDARY")
SP_SECONDARY_PASSWORD = os.getenv("SP_SECONDARY_PASSWORD")
KEYVAULT_URL = os.getenv("KEYVAULT_URL")
SECRET_NAME = os.getenv("SECRET_NAME")
STORAGE_ACCOUNT_NAME = os.getenv("STORAGE_ACCOUNT_NAME")
CONTAINER_NAME = os.getenv("CONTAINER_NAME")

credential_sp2 = ClientSecretCredential(
        tenant_id=TENANT_ID,
        client_id=CLIENT_ID,
        client_secret=SP_SECONDARY_PASSWORD
    )

keyvault_swd_sp1 = SecretClient(vault_url=KEYVAULT_URL, credential=credential_sp2)
credential_sp1 = keyvault_swd_sp1.get_secret(SECRET_NAME)._value


def upload_file_to_datalake(file_path, directory_name, file_name):

    credential = ClientSecretCredential(
        tenant_id=TENANT_ID,
        client_id=CLIENT_ID,
        client_secret=credential_sp1
    )

    dl_service_client = DataLakeServiceClient(
        account_url=f"https://{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/",
        credential=credential
    )

    file_system_client = dl_service_client.get_file_system_client(CONTAINER_NAME)


    directory_client = file_system_client.get_directory_client(directory_name)

    file_client = directory_client.get_file_client(file_name)


    with open(file_path, "rb") as file:
        file_client.upload_data(file, overwrite=True)
    
    print(f"Le fichier {file_name} a été téléchargé dans le répertoire {directory_name}.")

if __name__ == "__main__":
    file_path = input("Entrez le chemin local du dossier contenant le fichier : ")
    directory_name = input("Entrez le nom du répertoire dans Azure Data Lake : ")
    file_name = input("Entrez le nom du fichier : ")
    upload_file_to_datalake(file_path, directory_name, file_name)
