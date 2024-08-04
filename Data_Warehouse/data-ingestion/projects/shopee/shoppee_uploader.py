import os

import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

s3 = boto3.client('s3')
bucket_name = 'spider.truongnd'
prefix = 'shopee/raw'
data_path = './output'


def file_exists_in_s3(bucket: str, key: str) -> bool:
    """Check if a file exists in an S3 bucket."""
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except s3.exceptions.ClientError as e:
        if e.response['Error']['Code'] == '404':
            return False
        else:
            raise


def upload_files_to_s3(local_directory: str, bucket: str, s3_prefix: str):
    """Upload JSON files from a local directory to an S3 bucket."""
    try:
        for root, dirs, files in os.walk(local_directory):
            for file in files:
                if file.endswith('.json'):
                    local_file_path = os.path.join(root, file)
                    s3_key = f"{s3_prefix}/{file}"

                    if file_exists_in_s3(bucket, s3_key):
                        print(f"File {file} already exists in S3. Stopping the upload process.")
                        return

                    print(f"Uploading {local_file_path} to s3://{bucket}/{s3_key}")
                    s3.upload_file(local_file_path, bucket, s3_key)
                    print(f"Uploaded {file} to {s3_key}")
    except NoCredentialsError:
        print("Credentials not available for AWS S3.")
    except PartialCredentialsError:
        print("Incomplete credentials provided for AWS S3.")
    except Exception as e:
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    upload_files_to_s3(data_path, bucket_name, prefix)
