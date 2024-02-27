import datetime
from azure.storage.blob import BlobServiceClient
import os

from dotenv import load_dotenv
import pytz
load_dotenv()


account_name = os.getenv("ACCOUNT_NAME")
account_key = os.getenv("ACCOUNT_KEY")
container_name = os.getenv("CONTAINER_NAME")
base_path = os.getenv("BASE_PATH")
no_of_days = os.getenv("NO_OF_DAYS")

def get_container_client():
    blob_service_client = BlobServiceClient(account_url=f"https://{account_name}.blob.core.windows.net", credential=account_key)
    container_client = blob_service_client.get_container_client(container_name)
    return container_client

def get_files_to_change_tier():

    threshold_date = datetime.datetime.now(pytz.utc) - datetime.timedelta(days=int(no_of_days))

    container_client = get_container_client()
    if base_path == "":
        blobs = container_client.list_blobs()
    else:
        blobs = container_client.list_blobs(name_starts_with=base_path)

    files_to_change_tier = []

    for blob in blobs:
        blob_properties = container_client.get_blob_client(blob.name).get_blob_properties()
        last_access_time = blob_properties['last_modified']

        if last_access_time < threshold_date:
            files_to_change_tier.append(blob.name)
    return files_to_change_tier

def change_tier(files_to_change_tier):
    container_client = get_container_client()
    for file in files_to_change_tier:
        blob_client = container_client.get_blob_client(file)
        blob_client.set_standard_blob_tier("Cool")
        print(f"Tier of {file} changed to Cool")
    print("All files have been changed to Cool tier")



if __name__ == "__main__":
    files_to_change_tier = get_files_to_change_tier()    
    print(files_to_change_tier)
    change_tier(files_to_change_tier)

