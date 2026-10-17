# Governance Package

## Learner Lab constraint
AWS Academy Learner Lab provides one pre-built role (**LabRole**) and blocks IAM role and user creation. Every Lambda function, Glue job and crawler in this project therefore runs as LabRole, which has broad permissions.

We handled this in two layers:

| Layer | What | Deployed? |
|---|---|---|
| Design | Three least-privilege IAM policies (`iam/`), one per pipeline role | No: role creation is blocked. These are the policies we would attach in a production account |
| Enforced | S3 bucket policy (`bucket_policy.json`) | Yes |
| Enforced | Lake Formation grants (`lakeformation_permissions.md`) | Yes, where the lab allows |
| Enforced | Athena workgroup `analyst-wg` with a per-query scan limit | Yes |

## Files
| File | Purpose |
|---|---|
| `iam/ingestion_role_policy.json` | Script, Lambda and customer Glue job: write `landing/` and `raw/` only |
| `iam/etl_role_policy.json` | Glue ETL and DQ jobs: read `raw/`, `staging/`, `trusted/`; write `staging/`, `trusted/`, `quarantine/`, `curated/`; explicit deny on `landing/` |
| `iam/analyst_role_policy.json` | Analyst: Athena in `analyst-wg` only, read `curated/` only |
| `bucket_policy.json` | Blocks everyone except the pipeline (LabRole) and lab admins (voclabs) from internal zones; requires HTTPS |
| `lakeformation_permissions.md` | Table-level and row-level grants on the curated database |
| `raci.md` | Who can read and write each zone, and who owns each activity |

## Design choices
- **Separate role per pipeline stage.** If the ingestion Lambda were compromised, it could not read curated data or rewrite trusted data.
- **Prefix-level S3 resources** (`raw/*`, `curated/*`) instead of the whole bucket.
- **Explicit Deny** on `landing/` for the ETL role, so originals can never be modified after delivery.
- **Analyst never sees raw, staging or quarantine.** These zones contain unvalidated data and customer emails.

Deployed in account `764541939057`, bucket `thornquist-lake-764541939057` (us-east-1).
