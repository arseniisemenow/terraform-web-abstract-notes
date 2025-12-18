#!/usr/bin/env python3

print("Testing imports...")

try:
    from flask import Flask
    print("✓ Flask import successful")
except ImportError as e:
    print(f"✗ Flask import failed: {e}")
    exit(1)

try:
    import boto3
    print("✓ boto3 import successful")
except ImportError as e:
    print(f"✗ boto3 import failed: {e}")
    exit(1)

try:
    from botocore.exceptions import ClientError
    print("✓ ClientError import successful")
except ImportError as e:
    print(f"✗ ClientError import failed: {e}")
    exit(1)

print("All imports successful!")
print("Testing Flask app creation...")

app = Flask(__name__)
print("✓ Flask app created successfully")

print("Testing S3 client creation with defaults...")
try:
    s3_client = boto3.client(
        's3',
        endpoint_url='https://storage.yandexcloud.net',
        aws_access_key_id='test',
        aws_secret_access_key='test'
    )
    print("✓ S3 client created successfully")
except Exception as e:
    print(f"✗ S3 client creation failed: {e}")

print("All tests passed!")