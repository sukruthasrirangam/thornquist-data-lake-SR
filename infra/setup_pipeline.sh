#!/bin/bash
# =============================================================================
# setup_pipeline.sh  |  Thornquist Outfitters data lake  (owners: Ruchi & Sukrutha)
#
# Infrastructure as code for AWS Academy Learner Lab: builds the whole pipeline
# from a fresh session with the AWS CLI, in the same order as the README setup.
# CloudFormation is not used because Learner Lab only allows the fixed LabRole;
# every resource below is created against that role.
#
# Usage (CloudShell, us-east-1, from the repo folder):
#     bash infra/setup_pipeline.sh you@example.com
# The email receives Glue job-failure alerts (confirm the SNS subscription email).
# =============================================================================
set -euo pipefail

ALERT_EMAIL="${1:?usage: bash infra/setup_pipeline.sh you@example.com}"
REGION=us-east-1
ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
BUCKET="thornquist-lake-${ACCOUNT}"
ROLE=$(aws iam get-role --role-name LabRole --query Role.Arn --output text)
REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
WORK=$(mktemp -d)
ORIGINAL_BUCKET=thornquist-lake-764541939057
export AWS_DEFAULT_REGION=$REGION

log()  { echo -e "\n=== $*"; }
# create-* calls are allowed to fail with "already exists" so the script can be re-run
ok_if_exists() { "$@" 2>"$WORK/err" || grep -qiE "AlreadyExists|already exists|ResourceConflict|BucketAlreadyOwnedByYou" "$WORK/err" || { cat "$WORK/err"; exit 1; }; }
wait_job() {   # wait_job <job-name> <run-id>
  while true; do
    s=$(aws glue get-job-run --job-name "$1" --run-id "$2" --query JobRun.JobRunState --output text)
    echo "  $1: $s"; case $s in SUCCEEDED) return 0;; FAILED|TIMEOUT|STOPPED|ERROR) exit 1;; esac; sleep 20
  done
}
run_job()   { id=$(aws glue start-job-run --job-name "$1" --query JobRunId --output text); wait_job "$1" "$id"; }
run_crawler() {
  aws glue start-crawler --name "$1"; sleep 15
  while [ "$(aws glue get-crawler --name "$1" --query Crawler.State --output text)" != "READY" ]; do echo "  $1 running"; sleep 15; done
}
# copy of a repo file with the bucket name adjusted to this account
local_copy() { sed "s/${ORIGINAL_BUCKET}/${BUCKET}/g" "$REPO_DIR/$1" > "$WORK/$(basename "$1")"; echo "$WORK/$(basename "$1")"; }

log "1. Bucket $BUCKET and zones"
ok_if_exists aws s3 mb "s3://$BUCKET"
for z in landing/pos landing/ecommerce landing/customers raw staging trusted quarantine quality curated benchmark athena-results scripts; do
  aws s3api put-object --bucket "$BUCKET" --key "$z/" >/dev/null; done

log "2. POS ingestion (scripted upload: landing -> raw/pos/ingest_date=...)"
D=$(date -u +%Y-%m-%d)
for f in "$REPO_DIR"/data/pos_sales_*.csv; do
  n=$(basename "$f")
  aws s3 cp "$f" "s3://$BUCKET/landing/pos/$n"
  aws s3 cp "s3://$BUCKET/landing/pos/$n" "s3://$BUCKET/raw/pos/ingest_date=$D/$n"
done

log "3. E-commerce ingestion (Lambda ecom-ingest + S3 trigger)"
(cd "$REPO_DIR/ingestion" && zip -q "$WORK/lambda.zip" ecom_ingest_lambda.py)
ok_if_exists aws lambda create-function --function-name ecom-ingest --runtime python3.12 --role "$ROLE" \
  --handler ecom_ingest_lambda.lambda_handler --timeout 30 --zip-file "fileb://$WORK/lambda.zip"
aws lambda wait function-active --function-name ecom-ingest
ok_if_exists aws lambda add-permission --function-name ecom-ingest --statement-id s3-landing-ecommerce \
  --action lambda:InvokeFunction --principal s3.amazonaws.com --source-arn "arn:aws:s3:::$BUCKET" --source-account "$ACCOUNT"
LAMBDA_ARN=$(aws lambda get-function --function-name ecom-ingest --query Configuration.FunctionArn --output text)
cat > "$WORK/notify.json" <<EOF
{"LambdaFunctionConfigurations":[{"LambdaFunctionArn":"$LAMBDA_ARN","Events":["s3:ObjectCreated:*"],
 "Filter":{"Key":{"FilterRules":[{"Name":"prefix","Value":"landing/ecommerce/"},{"Name":"suffix","Value":".json"}]}}}]}
EOF
aws s3api put-bucket-notification-configuration --bucket "$BUCKET" --notification-configuration "file://$WORK/notify.json"
aws s3 cp "$REPO_DIR/data/ecommerce_orders.json" "s3://$BUCKET/landing/ecommerce/"
until aws s3 ls "s3://$BUCKET/raw/ecommerce/" --recursive | grep -q jsonl; do echo "  waiting for Lambda output"; sleep 5; done

log "4. Customer ingestion (scheduled Glue Python shell job)"
aws s3 cp "$REPO_DIR/data/customer_extract.csv" "s3://$BUCKET/landing/customers/"
for f in glue/customer_ingest.py glue/raw_to_staging.py glue/dq_gate.py glue/trusted_to_curated.py; do
  aws s3 cp "$(local_copy $f)" "s3://$BUCKET/scripts/"; done
ok_if_exists aws glue create-job --name customer-ingest --role "$ROLE" --max-capacity 0.0625 \
  --command "Name=pythonshell,ScriptLocation=s3://$BUCKET/scripts/customer_ingest.py,PythonVersion=3.9"
ok_if_exists aws glue create-trigger --name customer-ingest-weekly --type SCHEDULED \
  --schedule "cron(0 6 ? * MON *)" --actions JobName=customer-ingest --start-on-creation
run_job customer-ingest

log "5. Glue Spark jobs"
SPARK="--glue-version 4.0 --worker-type G.1X --number-of-workers 2 --timeout 10"
ok_if_exists aws glue create-job --name raw-to-staging --role "$ROLE" $SPARK \
  --command "Name=glueetl,ScriptLocation=s3://$BUCKET/scripts/raw_to_staging.py,PythonVersion=3"
ok_if_exists aws glue create-job --name dq-gate --role "$ROLE" $SPARK \
  --command "Name=glueetl,ScriptLocation=s3://$BUCKET/scripts/dq_gate.py,PythonVersion=3" \
  --default-arguments "{\"--BUCKET\":\"$BUCKET\",\"--RUN_SELF_TEST\":\"true\",\"--enable-continuous-cloudwatch-log\":\"true\"}"
ok_if_exists aws glue create-job --name trusted-to-curated --role "$ROLE" $SPARK \
  --command "Name=glueetl,ScriptLocation=s3://$BUCKET/scripts/trusted_to_curated.py,PythonVersion=3" \
  --default-arguments "{\"--BUCKET\":\"$BUCKET\",\"--enable-continuous-cloudwatch-log\":\"true\"}"

log "6. Failure alerts (EventBridge -> SNS email)"
TOPIC=$(aws sns create-topic --name thornquist-pipeline-alerts --query TopicArn --output text)
aws sns subscribe --topic-arn "$TOPIC" --protocol email --notification-endpoint "$ALERT_EMAIL" >/dev/null
aws sns set-topic-attributes --topic-arn "$TOPIC" --attribute-name Policy --attribute-value \
  "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Principal\":{\"Service\":\"events.amazonaws.com\"},\"Action\":\"sns:Publish\",\"Resource\":\"$TOPIC\"}]}"
aws events put-rule --name glue-job-failure --event-pattern \
  '{"source":["aws.glue"],"detail-type":["Glue Job State Change"],"detail":{"state":["FAILED","TIMEOUT","STOPPED"]}}' >/dev/null
aws events put-targets --rule glue-job-failure --targets "Id"="sns-alert","Arn"="$TOPIC" >/dev/null

log "7. Run the pipeline: raw -> staging -> trusted/quarantine -> curated"
run_job raw-to-staging
run_job dq-gate
run_job trusted-to-curated

log "8. Data Catalog: databases and scheduled crawlers for every zone"
for db in thornquist_raw thornquist_staging thornquist_trusted thornquist_quality thornquist_curated thornquist_benchmark; do
  ok_if_exists aws glue create-database --database-input "Name=$db"; done
crawler() {  # crawler <name> <database> <schedule> <path> [<path> ...]
  local name=$1 db=$2 sched=$3; shift 3
  local targets; targets=$(printf '{"Path":"s3://%s/%s"},' $(for p in "$@"; do echo "$BUCKET" "$p"; done)); targets="{\"S3Targets\":[${targets%,}]}"
  ok_if_exists aws glue create-crawler --name "$name" --role "$ROLE" --database-name "$db" --schedule "$sched" --targets "$targets"
}
crawler raw-crawler      thornquist_raw      "cron(0 7 * * ? *)"  raw/ecommerce/ raw/customers/
crawler staging-crawler  thornquist_staging  "cron(30 7 * * ? *)" staging/sales_lines/ staging/customers/
crawler trusted-crawler  thornquist_trusted  "cron(45 7 * * ? *)" trusted/sales_lines/ trusted/customers/
crawler quality-crawler  thornquist_quality  "cron(45 7 * * ? *)" quarantine/quarantined_sales/ quality/dq_metrics/ quality/rule_self_test/
crawler curated-crawler  thornquist_curated  "cron(0 8 * * ? *)"  curated/sales_fact/ curated/customer_dim/
for c in raw-crawler staging-crawler trusted-crawler quality-crawler curated-crawler; do run_crawler $c; done

log "9. Athena: analyst workgroup, manual raw POS table, benchmark tables"
ok_if_exists aws athena create-work-group --name analyst-wg --configuration \
  "ResultConfiguration={OutputLocation=s3://$BUCKET/athena-results/analyst/},EnforceWorkGroupConfiguration=true,PublishCloudWatchMetricsEnabled=true,BytesScannedCutoffPerQuery=1073741824"
athena() {   # run one SQL statement and wait
  local id; id=$(aws athena start-query-execution --work-group analyst-wg --query-string "$1" --query QueryExecutionId --output text)
  while true; do s=$(aws athena get-query-execution --query-execution-id "$id" --query QueryExecution.Status.State --output text)
    case $s in SUCCEEDED) return 0;; FAILED|CANCELLED) echo "  query failed: ${1:0:80}"; return 1;; esac; sleep 2; done
}
# split the SQL files into statements (skip comments and the SELECT test queries)
for f in sql/01_create_raw_pos_sales.sql sql/30_benchmark.sql; do
  python3 - "$(local_copy $f)" > "$WORK/stmts.txt" <<'PY'
import re, sys
sql = re.sub(r'--[^\n]*', '', open(sys.argv[1]).read())
for s in (x.strip() for x in sql.split(';')):
    if s and not s.upper().startswith('SELECT'):
        print(' '.join(s.split()))
PY
  while IFS= read -r stmt; do athena "$stmt" || true; done < "$WORK/stmts.txt"
done

log "10. Governance: bucket policy restricting landing/raw/staging/quarantine to the pipeline"
aws s3api put-bucket-policy --bucket "$BUCKET" --policy "file://$(local_copy governance/bucket_policy.json)"
aws s3 ls "s3://$BUCKET/raw/" --recursive | head -3 || true

log "Done. Bucket: $BUCKET"
echo "Next: confirm the SNS email; run sql/10_business_queries.sql and sql/20_quality_queries.sql in Athena (workgroup analyst-wg);"
echo "Lake Formation grants: governance/lakeformation_permissions.md (console)."
rm -rf "$WORK"
