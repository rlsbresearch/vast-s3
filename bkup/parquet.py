import boto3
from botocore.exceptions import BotoCoreError, ClientError
from dotenv import load_dotenv
import os

#load_dotenv()

def test_vast_s3_connection():
    #access_key = os.getenv("ACCESS_KEY")
    #secret_key = os.getenv("SECRET_KEY")
    #host_base = os.getenv("HOST_BASE")
    #host_bucket = os.getenv("HOST_BUCKET")
    #use_https = os.getenv("USE_HTTPS")
    access_key = "4HC127RYRPT117LS0568"
    secret_key = "gCYD2COaVpGWk2g/uwlf4+DIR5FV6r9lcecDRGLy"
    host_base = "172.28.215.20"
    host_bucket = "172.28.215.20"
    endpoint_url = "https://172.28.215.20"
    bucket_name = "azawada-bucket01"
    use_https = "false"
    use_ssl = "false"


    if not all([access_key, secret_key, host_base, host_bucket, bucket_name]):
        print("Missing environment variables.")
        return

    try:
        protocol = "https" if use_https.lower() == "true" else "http"
        s3 = boto3.client(
            's3',
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            endpoint_url=f"{protocol}://{host_base}"
        )
        # List objects to test connection
        resp = s3.list_objects_v2(Bucket=bucket_name)
        print(f"Connected to bucket {bucket_name}")
        if resp.get('Contents'):
            print("Objects found:")
            for obj in resp['Contents']:
                print(f" - {obj['Key']}")
        else:
            print("Bucket is empty")

        # Here is my attempt to upload the sample.txt
        file_path = "./sample.txt"
        object_name = "sample.txt"
        if not os.path.isfile(file_path):
            print(f"File {file_path} does not exist.")
            return
        with open(file_path, "rb") as f:
            s3.upload_fileobj(f, bucket_name, object_name)
        print(f"Uploaded {file_path} to {bucket_name}/{object_name}")

        # Refresh object list after upload
        resp = s3.list_objects_v2(Bucket=bucket_name)
        print("Objects found" if resp.get('Contents') else "Bucket is empty")

    except Exception as e:
        print(f"Error: {e}")

if __name__ == '__main__':
    test_vast_s3_connection()
