import os
import boto3
from botocore.exceptions import BotoCoreError, ClientError
from boto3.s3.transfer import TransferConfig
from dotenv import load_dotenv

load_dotenv()

def test_vast_s3_connection():
    """
    Connects to a VAST S3-compatible bucket using environment variables.
    Lists objects in the bucket and uploads 'sample2.txt' using multipart upload.
    Prints the results.
    """
    access_key = os.getenv("ACCESS_KEY")
    secret_key = os.getenv("SECRET_KEY")
    host_base = os.getenv("HOST_BASE")
    host_bucket = os.getenv("HOST_BUCKET")
    endpoint_url = os.getenv("ENDPOINT_URL")
    bucket_name = os.getenv("BUCKET_NAME")
    use_https = "false"
    use_ssl = "false"

    # Transfer configuration for multipart upload as per VAST S3 Documentation
    config = TransferConfig(
        multipart_threshold=100 * 1024 * 1024,  # Start multipart for files > 100MB
        multipart_chunksize=50 * 1024 * 1024    # Each part is 50MB
    )

    if not all([access_key, secret_key, host_base, host_bucket, bucket_name]):
        print("Missing environment variables.")
        return

    try:
        protocol = "https" if use_https.lower() == "true" else "http"
        s3 = boto3.client(
            "s3",
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            endpoint_url=f"{protocol}://{host_base}"
        )

        # List objects to test connection
        resp = s3.list_objects_v2(Bucket=bucket_name)
        print(f"Connected to bucket {bucket_name}")

        if resp.get("Contents"):
            print("Objects found:")
            for obj in resp["Contents"]:
                print(f" - {obj['Key']}")
        else:
            print("Bucket is empty")

        object_name = "sample2.txt"
        try:
            s3.upload_file(
                Filename="./sample2.txt",
                Bucket=bucket_name,
                Key=object_name,
                Config=config
            )
            print(f"Successfully uploaded {object_name} to bucket {bucket_name}.")
        except Exception as e:
            print(f"Upload failed: {e}")

        # Refresh object list after upload
        resp = s3.list_objects_v2(Bucket=bucket_name)
        print("Objects found" if resp.get("Contents") else "Bucket is empty")

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    test_vast_s3_connection()
