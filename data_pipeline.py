from azure.storage.blob import BlobServiceClient
import boto3
from prefect import flow, task, get_run_logger
import os

def list_azure_blob_files(container_name, pattern):
    """List files in an Azure blob container.

    Args:
        container_name (str): Name of the Azure blob container.
        prefix (str): Prefix to filter files.

    Returns:
        list: List of files.
    """
    blob_service_client = BlobServiceClient.from_connection_string(
        os.environ["AZURE_STORAGE_CONNECTION_STRING"]
    )

    container_client = blob_service_client.get_container_client(container_name)
    blob_list = container_client.list_blobs(name_starts_with='202')
    return [blob.name for blob in blob_list if blob.name[-len(pattern):] == pattern]

def get_azure_blob_file(container_name, blob_name, local_path):
    """Download a file from an Azure blob container.

    Args:
        container_name (str): Name of the Azure blob container.
        blob_name (str): Name of the blob.
        local_path (str): Local path to save the file.
    """
    blob_service_client = BlobServiceClient.from_connection_string(
        os.environ["AZURE_STORAGE_CONNECTION_STRING"]
    )
    container_client = blob_service_client.get_container_client(container_name)
    blob_client = container_client.get_blob_client(blob_name)
    with open(local_path, "wb") as file:
        download_stream = blob_client.download_blob()
        file.write(download_stream.readall())

def list_s3_files(bucket_name, prefix):
    """List files in an S3 bucket.

    Args:
        bucket_name (str): Name of the S3 bucket.
        prefix (str): Prefix to filter files.

    Returns:
        list: List of files.
    """
    s3 = boto3.resource("s3")
    bucket = s3.Bucket(bucket_name)
    return [obj.key for obj in bucket.objects.filter(Prefix=prefix) if obj.key[-3:] == 'csv']

def upload_to_s3(bucket_name, local_path, s3_path):
    """Upload a file to an S3 bucket.

    Args:
        bucket_name (str): Name of the S3 bucket.
        local_path (str): Local path to the file.
        s3_path (str): S3 path to save the file.
    """
    s3 = boto3.resource("s3")
    bucket = s3.Bucket(bucket_name)
    bucket.upload_file(local_path, s3_path)

def find_missing_files(azure_files, s3_files):
    """Find missing files between Azure and S3.

    Args:
        azure_files (list): List of files in Azure.
        s3_files (list): List of files in S3.

    Returns:
        list: List of missing files.
    """
    return list(set(azure_files) - set(s3_files))

@task
def upload_missing_files(file_type):
    # file_type = "activity" or "customer"

    logger = get_run_logger()

    # List files in the Azure blob container
    azure_files = list_azure_blob_files("mk-inbound", "_" + file_type + ".csv")
    azure_files = [file_type + "/" + f for f in azure_files]

    # List files in the S3 bucket
    s3_files = list_s3_files("ibex-input", file_type)

    # Find missing files between Azure and S3
    missing_files = find_missing_files(azure_files, s3_files)

    # exclude files before 2022-12
    start = missing_files[0].find('/') + 1
    end = start + 7
    missing_files = [f for f in missing_files if f[start:end] > '2022-11']

    if missing_files == []:
        logger.info(f"No missing {file_type} files")
    else: 
        # Upload missing files to S3
        for file in missing_files:
            logger.info(f'Uploading {file}')

            file = file[start:]
            # Download a file from the Azure blob container
            get_azure_blob_file("mk-inbound", file, file)
            upload_to_s3("ibex-input", file, file_type + "/" + file)

            # delete the file after uploading
            os.remove(file)

@flow(name="Data Pipeline")
def pipeline():
    logger = get_run_logger()
    logger.info("Copying activity data to S3")
    upload_missing_files("activity")

    logger.info("Copying customer data to S3")
    upload_missing_files("customer")

if __name__=='__main__':
    pipeline()