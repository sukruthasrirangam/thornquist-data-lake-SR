#!/bin/bash
# POS ingestion: upload daily CSVs to landing, then copy to raw with an ingest_date partition  (owner: Ruchi)
BUCKET=thornquist-lake-764541939057
INGEST_DATE=$(date -u +%Y-%m-%d)
DATA_DIR=~/thornquist-data-lake-SR/data

for f in $DATA_DIR/pos_sales_*.csv; do
  name=$(basename "$f")
  aws s3 cp "$f" s3://$BUCKET/landing/pos/$name
  aws s3 cp s3://$BUCKET/landing/pos/$name s3://$BUCKET/raw/pos/ingest_date=$INGEST_DATE/$name
done
echo "POS ingestion complete for $INGEST_DATE"
