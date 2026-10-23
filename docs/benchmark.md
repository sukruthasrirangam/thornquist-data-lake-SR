# CSV vs Parquet benchmark (bonus)

Same question on the same 1,601 curated sales lines stored three ways: online revenue by region
(`WHERE channel = 'online'`). Queries are in `sql/30_benchmark.sql`, run in workgroup `analyst-wg`;
numbers are from Athena's **Query stats** tab (screenshots: Figures 36–38 in `evidence/`).

| Table | Storage | Data scanned | Run time | Less data than CSV |
|---|---|---|---|---|
| `thornquist_benchmark.sales_csv` | CSV text | **224.43 KB** | 936 ms | – |
| `thornquist_benchmark.sales_parquet_flat` | Parquet, one folder | **0.12 KB** | 539 ms | 99.9% |
| `thornquist_curated.sales_fact` | Parquet, partitioned by year/month/channel | **3.03 KB** | 581 ms | **98.6%** |

All three queries return the same revenue per region.

## What it shows
- **Parquet is columnar and compressed.** CSV has to read every column of every row; Parquet reads only
  `region`, `line_amount` and `channel`. That alone cuts the data scanned by more than 98%.
- **Why flat Parquet scanned less than the partitioned table here:** with a single tiny file, Parquet's
  built-in metadata (column statistics in the file footer) lets Athena skip almost all of the data, so
  partitioning has nothing extra to save. The partitioned table reads a few small files (one per
  partition), each with its own footer. Partitioning pays off as data grows: with a year of weekly files
  for 10 stores, a query for one month or one channel skips every other folder entirely.
- **Cost:** Athena charges per byte scanned (with a 10 MB minimum per query, so at this sample size every
  query costs the same). The saving scales with data volume: at production size, a 98% smaller scan is a
  98% smaller query bill.
