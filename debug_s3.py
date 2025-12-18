#!/usr/bin/env python3

import os
import boto3
import json
from botocore.exceptions import ClientError

# Configuration - use environment variables for security
S3_ENDPOINT = os.getenv('S3_ENDPOINT', 'https://storage.yandexcloud.net')
BUCKET_NAME = os.getenv('BUCKET_NAME', 'lecture-notes-storage')
ACCESS_KEY = os.getenv('SA_KEY_ID') or os.getenv('STORAGE_ACCESS_KEY')
SECRET_KEY = os.getenv('SA_SECRET') or os.getenv('STORAGE_SECRET_KEY')

# Check if credentials are available
if not ACCESS_KEY or not SECRET_KEY:
    print("ERROR: Missing credentials. Please set environment variables:")
    print("  SA_KEY_ID and SA_SECRET")
    print("  or STORAGE_ACCESS_KEY and STORAGE_SECRET_KEY")
    exit(1)

# Initialize S3 client
s3_client = boto3.client(
    's3',
    endpoint_url=S3_ENDPOINT,
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
    region_name='ru-central1'
)

def check_bucket_and_tasks():
    try:
        # Check bucket info
        print("Checking bucket info...")
        response = s3_client.head_bucket(Bucket=BUCKET_NAME)
        print(f"Bucket {BUCKET_NAME} exists and is accessible")

        # List objects with tasks/ prefix
        print("\nListing objects in tasks/ directory...")
        response = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix='tasks/')

        if 'Contents' in response:
            print(f"Found {len(response['Contents'])} objects:")
            for obj in response['Contents']:
                print(f"  - {obj['Key']} (Size: {obj['Size']} bytes)")

                # Try to read the first object
                if obj['Key'].endswith('.json'):
                    print(f"\nReading {obj['Key']}:")
                    obj_response = s3_client.get_object(Bucket=BUCKET_NAME, Key=obj['Key'])
                    content = obj_response['Body'].read().decode('utf-8')
                    task_data = json.loads(content)
                    print(json.dumps(task_data, indent=2))
                    break
        else:
            print("No objects found in tasks/ directory")

    except ClientError as e:
        print(f"ClientError: {e}")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == '__main__':
    check_bucket_and_tasks()