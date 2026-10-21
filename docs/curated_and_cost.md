# Curated layer, queries and cost (`glue/trusted_to_curated.py`)

## Curated model

| Table | Grain | Storage | Partitions |
|---|---|---|---|
| `thornquist_curated.sales_fact` | one row per sale line (store + online) | Parquet, Snappy | `year` / `month` / `channel` |
| `thornquist_curated.customer_dim` | one row per customer | Parquet, Snappy | none (400 rows) |

`sales_fact` already carries the customer's loyalty tier as `customer_segment` (`Gold`, `Silver`, `Standard`,
or `Walk-in` for store sales with no customer), so the most common business questions need no join.

### Why Parquet and these partitions
- **Parquet is columnar**: a query that sums `line_amount` reads only that column, not every field as with CSV.
  Athena charges per byte scanned, so this is a direct cost saving.
- **Partitioned by year/month and channel** because almost every business question filters by period and by
  channel. Athena skips whole folders that cannot match (partition pruning).
- **Deliberately few partitions.** The data is small, so more partition columns (store, day, category) would
  create many tiny files, which is slower and costs more. One file per partition is written on purpose.
- **Storage class:** S3 Standard while data is queried weekly. In production, a lifecycle rule would move
  `raw/` to Standard-IA after 30 days and Glacier after 1 year (raw is only reread to reprocess).

## Business queries (`sql/10_business_queries.sql`)

| # | Question | Key result |
|---|---|---|
| Q1 | How do the channels compare? | Online average order **$469.06** vs in-store **$204.73**: **2.3×**, with 2.75 items vs 1.19 |
| Q2 | Which categories drive each region? | Camping leads Alberta (28%); Accessories lead Quebec (30.5%) |
| Q3 | How do loyalty segments shop by channel? | Silver members spend most online ($539/order); Gold members spend *less* online ($347) than Standard ($459) |
| Q4 | Top 10 products | Alpine Hiking Shoe ($20.1k) and Trailrunner Boot ($19.8k) lead; 3 of the top 4 are footwear |
| Q5 | Daily trend | In-store peaks on Friday ($35.9k); online is steady at about $15k/day |
| Q6 | Walk-in blind spot by region | British Columbia 21.6% of store revenue has no customer attached; Atlantic 12.7% |

Also notable: **Atlantic is the only region where online revenue ($22.0k) exceeds in-store ($20.8k)**.

## Benchmark (bonus): CSV vs Parquet (`sql/30_benchmark.sql`)

Same 1,601 rows in three layouts. Fill in from Athena → Query stats.

| Query | CSV scanned | Parquet flat scanned | Parquet partitioned scanned | Saving vs CSV |
|---|---|---|---|---|
| Test 1: online revenue by region | | | | |
| Test 2: total revenue | | | | |
| Test 3: full scan | | | | |

File sizes: CSV **≈ 240 KB**, Parquet **≈ 56 KB** (about 77% smaller before any query runs).

Note: Athena bills a minimum of 10 MB per query, so at this sample size every query costs the same
($0.00005). The saving is in **bytes scanned**, which scales linearly: at one year of data for 10 stores
(≈ 52× this week), Parquet + partitioning keeps every query a fraction of the CSV cost.

## Cost of running the pipeline

| Item | Assumption | Per weekly run |
|---|---|---|
| Glue Spark jobs (raw-to-staging, dq-gate, trusted-to-curated) | 2 DPU × ~1–2 min each, $0.44/DPU-hour | ≈ $0.06–0.09 |
| Glue Python shell (customer-ingest) | 1/16 DPU × 1 min | < $0.01 |
| Crawlers | ~1 min each | ≈ $0.02 |
| Lambda, S3 storage (< 1 MB), Athena (10 MB min/query) | | < $0.01 |
| **Total** | | **≈ $0.10–0.15 per run** |

No always-on resources: nothing costs money between runs (no EMR cluster, no QuickSight subscription,
no running notebook instance).
