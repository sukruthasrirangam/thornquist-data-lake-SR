# Lake Formation Permission Set

## Principals
Learner Lab cannot create IAM roles, so the analyst and regional manager are **hypothetical principals**. When testing in the lab, LabRole stands in for the analyst.

| Principal | ARN (production design) |
|---|---|
| Analyst | `arn:aws:iam::764541939057:role/ThornquistAnalyst` |
| Regional manager (Ontario) | `arn:aws:iam::764541939057:role/ThornquistOntarioManager` |
| Pipeline (ETL) | `arn:aws:iam::764541939057:role/LabRole` |

## Registered data location
`s3://thornquist-lake-764541939057/curated/` is registered with Lake Formation, so access to curated data is decided by Lake Formation grants rather than S3 permissions alone.

## Grants
| # | Principal | Resource | Permissions | Purpose |
|---|---|---|---|---|
| 1 | Analyst | Database `thornquist_curated` | DESCRIBE | See the database |
| 2 | Analyst | All tables in `thornquist_curated` | SELECT, DESCRIBE | Query curated data |
| 3 | Analyst | `thornquist_raw`, `thornquist_trusted` | none | No access to unvalidated data |
| 4 | Regional manager | Data filter `ontario_only` on `sales_fact` (row filter `sales_region = 'Ontario'`) | SELECT | Row-level security (bonus) |
| 5 | Pipeline (LabRole) | `thornquist_curated` | ALL, data location access | Write curated tables |

## Console steps
1. Lake Formation → if prompted, **Add myself** as administrator.
2. **Data lake locations → Register location** → `s3://thornquist-lake-764541939057/curated/` → role: LabRole.
3. **Data lake permissions → Grant** → principal → database `thornquist_curated` → tables: All tables → SELECT, DESCRIBE.
4. **Data filters → Create** → table `sales_fact` → name `ontario_only` → row filter `sales_region = 'Ontario'`, all columns → then grant SELECT on the filter.

## Fallback
If Learner Lab blocks a step, we screenshot the error (`evidence/`) and keep this document as the intended design. For row-level security, an Athena view `v_sales_ontario` (`WHERE sales_region = 'Ontario'`) demonstrates the same rule.
