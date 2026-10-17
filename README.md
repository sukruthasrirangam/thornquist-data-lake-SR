# Thornquist Outfitters – Governed Data Lake

CCGC 5506 Mid-Term Project 1 · Team: Ruchi, Sukrutha

A four-zone S3 data lake (landing → raw → staging → trusted → curated) built with
AWS Glue, Lambda, Athena and Lake Formation in AWS Academy Learner Lab.

## Repository layout

| Folder | Contents |
|---|---|
| `data/` | Instructor sample data |
| `docs/` | Architecture diagram, zone rules, schema conflicts |
| `ingestion/` | Upload script, Lambda, customer ingest job |
| `glue/` | Glue ETL, data quality and curated jobs |
| `sql/` | Athena business queries |
| `governance/` | IAM policies, bucket policy, Lake Formation, RACI |
| `notebook/` | Dashboard notebook |
| `evidence/` | Screenshots and recordings |

## Setup

(To be completed: step-by-step instructions to rebuild the pipeline from a fresh Learner Lab session.)
