# CSV vs Parquet benchmark (bonus)

Run the queries in `sql/30_benchmark.sql` in workgroup `analyst-wg` and copy *Data scanned* and *Run time*
from the **Query stats** tab.

| Test | Table | Data scanned | Run time |
|---|---|---|---|
| 1. Online revenue by region | `thornquist_benchmark.sales_csv` | | |
| | `thornquist_benchmark.sales_parquet_flat` | | |
| | `thornquist_curated.sales_fact` (partitioned) | | |

**Result:** Parquet + partitioning scanned __% less data than CSV for Test 1.
See `docs/curated_and_cost.md` for what this means at production scale.
