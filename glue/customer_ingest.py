"""
customer-ingest  |  Thornquist Outfitters data lake  (owner: Ruchi)
Glue Python shell job, scheduled every Monday 06:00 UTC: copies the weekly customer extract
from landing/customers/ to raw/customers/ingest_date=YYYY-MM-DD/, unchanged.
"""
import boto3
from datetime import datetime, timezone

BUCKET = "thornquist-lake-764541939057"
SOURCE_PREFIX = "landing/customers/"
INGEST_DATE = datetime.now(timezone.utc).strftime("%Y-%m-%d")
TARGET_PREFIX = f"raw/customers/ingest_date={INGEST_DATE}/"

s3 = boto3.client("s3")
copied = 0

resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=SOURCE_PREFIX)
for obj in resp.get("Contents", []):
    key = obj["Key"]
    if not key.endswith(".csv"):
        continue
    file_name = key.split("/")[-1]
    s3.copy_object(
        Bucket=BUCKET,
        CopySource={"Bucket": BUCKET, "Key": key},
        Key=TARGET_PREFIX + file_name,
    )
    print(f"Copied {key} -> {TARGET_PREFIX + file_name}")
    copied += 1

print(f"customer-ingest complete: {copied} file(s) copied for {INGEST_DATE}")