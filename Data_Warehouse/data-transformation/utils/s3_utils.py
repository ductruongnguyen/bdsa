from typing import List, Dict, Any

import boto3
import os

S3_CLIENT = boto3.client('s3')
BUCKET_NAME = 'spider.truongnd'


def get_most_recent_file_s3(prefix: str, file_prefix: str) -> Any:
    objects = S3_CLIENT.list_objects_v2(Bucket=BUCKET_NAME, Prefix=prefix)
    recent_files = [
        obj for obj in objects.get('Contents', [])
        if str(obj['Key']).split('/')[-1].startswith(f'{file_prefix}-')
    ]
    if recent_files:
        recent_files.sort(key=lambda obj: obj['LastModified'], reverse=True)
        return recent_files[0]
    else:
        return None


def get_list_most_recent_files_s3(prefix: str, file_prefix: str, file_date_time: str) -> List[Dict[str, Any]]:
    objects = S3_CLIENT.list_objects_v2(Bucket=BUCKET_NAME, Prefix=prefix)
    recent_files = [
        obj for obj in objects.get('Contents', [])
        if str(obj['Key']).split('/')[-1].startswith(f'{file_prefix}-{file_date_time}')
    ]
    if recent_files:
        return recent_files
    else:
        return []


def download_files_from_s3(recent_files: List[Dict[str, Any]], local_dir: str):
    for file in recent_files:
        file_key = file['Key']
        local_file_path = os.path.join(local_dir, os.path.basename(file_key))
        S3_CLIENT.download_file(BUCKET_NAME, file_key, local_file_path)
        print(f'Downloaded {file_key} to {local_file_path}')


def read_file_from_s3(file_key: str) -> str:
    try:
        file_obj = S3_CLIENT.get_object(Bucket=BUCKET_NAME, Key=file_key)
        return file_obj["Body"].read().decode('utf-8')
    except Exception as e:
        print(f"Error reading file from S3: {e}")
        return ""
