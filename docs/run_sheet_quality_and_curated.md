# Run sheet: quality gate, curated layer, queries, governance (Sukrutha's part)

Run in **Ruchi's lab (account 764541939057)**, in **CloudShell**, from the repo folder.
Each step lists the **📸 screenshot** to take for `evidence/`. Allow about 45 minutes.

```bash
cd ~/thornquist-data-lake-SR && git pull
export BUCKET=thornquist-lake-764541939057
export ROLE=arn:aws:iam::764541939057:role/LabRole
```

---

## 1. Upload the scripts and create the two Glue jobs

```bash
aws s3 cp glue/dq_gate.py            s3://$BUCKET/scripts/dq_gate.py
aws s3 cp glue/trusted_to_curated.py s3://$BUCKET/scripts/trusted_to_curated.py

aws glue create-job --name dq-gate --role $ROLE \
  --command Name=glueetl,ScriptLocation=s3://$BUCKET/scripts/dq_gate.py,PythonVersion=3 \
  --glue-version 4.0 --worker-type G.1X --number-of-workers 2 --timeout 10 \
  --default-arguments "{\"--BUCKET\":\"$BUCKET\",\"--RUN_SELF_TEST\":\"true\",\"--enable-continuous-cloudwatch-log\":\"true\"}"

aws glue create-job --name trusted-to-curated --role $ROLE \
  --command Name=glueetl,ScriptLocation=s3://$BUCKET/scripts/trusted_to_curated.py,PythonVersion=3 \
  --glue-version 4.0 --worker-type G.1X --number-of-workers 2 --timeout 10 \
  --default-arguments "{\"--BUCKET\":\"$BUCKET\",\"--enable-continuous-cloudwatch-log\":\"true\"}"
```

## 2. Run the quality gate

```bash
RUN=$(aws glue start-job-run --job-name dq-gate --query JobRunId --output text)
# repeat until SUCCEEDED (about 1–2 minutes)
aws glue get-job-run --job-name dq-gate --run-id $RUN --query JobRun.JobRunState --output text
```

Expected log summary (Glue → ETL jobs → dq-gate → Runs → **Output logs**):
```
checked=1705 passed=1601 quarantined=104 customers_flagged=4
ecommerce  orphan_customer   rows=90  revenue=$18,361.87
ecommerce  duplicate_record  rows=15  revenue=$2,943.95
pos        duplicate_record  rows=3   revenue=$720.27
----- rule self-test -----   10 x PASS
```
📸 Job run **Succeeded** + the output log with the summary and the 10 PASS lines.

## 3. Run the curated job

```bash
RUN=$(aws glue start-job-run --job-name trusted-to-curated --query JobRunId --output text)
aws glue get-job-run --job-name trusted-to-curated --run-id $RUN --query JobRun.JobRunState --output text
aws s3 ls s3://$BUCKET/curated/ --recursive
```
Expected: `curated/sales_fact/year=2026/month=8/channel=in-store/` and `.../channel=online/`, plus `curated/customer_dim/`.
📸 The S3 listing showing the partition folders.

## 4. Catalog the new zones (3 crawlers)

```bash
for DB in thornquist_trusted thornquist_quality thornquist_curated; do
  aws glue create-database --database-input Name=$DB; done

aws glue create-crawler --name trusted-crawler --role $ROLE --database-name thornquist_trusted \
  --targets "{\"S3Targets\":[{\"Path\":\"s3://$BUCKET/trusted/sales_lines/\"},{\"Path\":\"s3://$BUCKET/trusted/customers/\"}]}"
aws glue create-crawler --name quality-crawler --role $ROLE --database-name thornquist_quality \
  --targets "{\"S3Targets\":[{\"Path\":\"s3://$BUCKET/quarantine/quarantined_sales/\"},{\"Path\":\"s3://$BUCKET/quality/dq_metrics/\"},{\"Path\":\"s3://$BUCKET/quality/rule_self_test/\"}]}"
aws glue create-crawler --name curated-crawler --role $ROLE --database-name thornquist_curated \
  --schedule "cron(0 8 * * ? *)" \
  --targets "{\"S3Targets\":[{\"Path\":\"s3://$BUCKET/curated/sales_fact/\"},{\"Path\":\"s3://$BUCKET/curated/customer_dim/\"}]}"

for C in trusted-crawler quality-crawler curated-crawler; do aws glue start-crawler --name $C; done
# wait ~1 minute, then check all three say READY
aws glue list-crawls --crawler-name curated-crawler --query "Crawls[0].State" --output text
```
Expected tables: `thornquist_trusted.sales_lines`, `customers` · `thornquist_quality.quarantined_sales`, `dq_metrics`, `rule_self_test` · `thornquist_curated.sales_fact`, `customer_dim`.
📸 Glue → Tables, filtered to `thornquist_`.

## 5. Athena workgroup (skip if `analyst-wg` already exists)

```bash
aws athena create-work-group --name analyst-wg --configuration \
  "ResultConfiguration={OutputLocation=s3://$BUCKET/athena-results/analyst/},EnforceWorkGroupConfiguration=true,PublishCloudWatchMetricsEnabled=true,BytesScannedCutoffPerQuery=1073741824"
```
📸 Athena → Workgroups → analyst-wg (shows the 1 GB per-query limit).

## 6. Queries (Athena query editor, workgroup **analyst-wg**)

Run each query separately; 📸 every result.
- `sql/20_quality_queries.sql`: QA1 (why rows were quarantined + revenue at risk), QA2 (cost of bad data), QA3 (self-test), QA4 (the rejected rows)
- `sql/10_business_queries.sql`: Q1–Q6

Expected headline from Q1: online average order **$469.06** vs in-store **$204.73** (2.3×).

## 7. Bonus 1: CSV vs Parquet benchmark

1. Run B1 and B2a/B2b from `sql/30_benchmark.sql` (creates the two benchmark tables).
2. Run each test query on all three tables. For each, open **Query stats** and note *Data scanned* and *Run time*.
3. Fill the table in `docs/benchmark.md`. 📸 The Query stats of Test 1 on CSV vs on `sales_fact`.

## 8. Bonus 2: Ontario-only access

Run `sql/40_ontario_row_filter.sql` (view + the two demo queries). 📸 Both results: 5 regions vs 1.
Optional, **after all recordings are done**: the Lake Formation data filter in `governance/lakeformation_permissions.md`.

## 9. Bucket policy (governance, enforced)

```bash
aws s3api put-bucket-policy --bucket $BUCKET --policy file://governance/bucket_policy.json
aws s3 ls s3://$BUCKET/raw/ --recursive | head -3          # must still list files
```
📸 S3 → bucket → Permissions → Bucket policy.
If anything says **Access Denied**: `aws s3api delete-bucket-policy --bucket $BUCKET` and tell Sukrutha.

---

## Live quarantine demo (during the presentation)

Start this **at the beginning of Ruchi's section** so it finishes in time (~4 minutes total):

```bash
D=$(date +%F)
aws s3 cp data/demo/pos_sales_2026-08-30_terminal7_late_batch.csv s3://$BUCKET/landing/pos/
aws s3 cp s3://$BUCKET/landing/pos/pos_sales_2026-08-30_terminal7_late_batch.csv s3://$BUCKET/raw/pos/ingest_date=$D/
aws glue start-job-run --job-name raw-to-staging
# when it SUCCEEDS:
aws glue start-job-run --job-name dq-gate
# when it SUCCEEDS:
aws glue start-crawler --name quality-crawler
```
Then in Athena run QA1 and QA4. Expected: 8 new POS rejects, one per rule (duplicate, quantity, price, date,
customer, category, missing price, region), and **trusted still has 1,601 rows**.

**After the demo**, remove the batch so the data is clean again:
```bash
aws s3 rm s3://$BUCKET/raw/pos/ingest_date=$D/pos_sales_2026-08-30_terminal7_late_batch.csv
aws s3 rm s3://$BUCKET/landing/pos/pos_sales_2026-08-30_terminal7_late_batch.csv
```

Backup: record this whole sequence on Thursday.

## Before logging off
```bash
aws glue stop-crawler --name curated-crawler 2>/dev/null
aws glue update-crawler --name curated-crawler --schedule ""   # optional: remove schedule after grading
```
