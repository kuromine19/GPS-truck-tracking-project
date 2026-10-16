"""Tạo bucket và các tiền tố lake trên MinIO. Chạy một lần bởi service minio-init."""
import os
import time

import boto3
from botocore.client import Config

ENDPOINT = os.environ.get("S3_ENDPOINT", "http://minio:9000")
BUCKET = os.environ.get("LAKE_BUCKET", "gps")
PREFIXES = ["bronze/", "silver/", "gold/", "ref/", "quarantine/", "_checkpoints/"]

s3 = boto3.client(
    "s3",
    endpoint_url=ENDPOINT,
    aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
    aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    region_name=os.environ.get("AWS_REGION", "us-east-1"),
    config=Config(s3={"addressing_style": "path"}),
)

for attempt in range(1, 31):
    try:
        existing = {b["Name"] for b in s3.list_buckets().get("Buckets", [])}
        break
    except Exception as exc:  # MinIO chưa sẵn sàng
        print(f"[{attempt}/30] chờ MinIO tại {ENDPOINT}: {exc}")
        time.sleep(2)
else:
    raise SystemExit("Không kết nối được MinIO")

if BUCKET not in existing:
    s3.create_bucket(Bucket=BUCKET)
    print(f"Đã tạo bucket {BUCKET}")
for prefix in PREFIXES:
    s3.put_object(Bucket=BUCKET, Key=f"{prefix}.keep", Body=b"")
print(f"OK: s3a://{BUCKET}/ với các tiền tố {', '.join(PREFIXES)}")
