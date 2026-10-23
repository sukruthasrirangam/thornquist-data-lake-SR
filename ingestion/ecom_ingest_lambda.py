"""
ecom-ingest  |  Thornquist Outfitters data lake  (owner: Ruchi)
Lambda triggered by S3 uploads to landing/ecommerce/*.json: converts the JSON array of orders
to JSON Lines (one order per line), adds _source_file and _ingest_ts, writes to raw/ecommerce/.
"""
import json
import boto3
from datetime import datetime, timezone
from urllib.parse import unquote_plus

s3 = boto3.client("s3")

def lambda_handler(event, context):
    results = []
    for record in event["Records"]:
        bucket = record["s3"]["bucket"]["name"]
        key = unquote_plus(record["s3"]["object"]["key"])

        # Read the JSON array from landing/
        body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
        orders = json.loads(body)

        # Convert to JSON Lines (one order per line), keep data as-is
        ingest_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        lines = []
        for order in orders:
            order["_source_file"] = key
            order["_ingest_ts"] = datetime.now(timezone.utc).isoformat()
            lines.append(json.dumps(order))

        file_name = key.split("/")[-1].replace(".json", ".jsonl")
        out_key = f"raw/ecommerce/ingest_date={ingest_date}/{file_name}"
        s3.put_object(Bucket=bucket, Key=out_key, Body="\n".join(lines).encode("utf-8"))

        print(f"Converted {len(orders)} orders: {key} -> {out_key}")
        results.append({"source": key, "target": out_key, "orders": len(orders)})

    return {"statusCode": 200, "body": results}