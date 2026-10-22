-- =====================================================================
-- Bonus: CSV vs Parquet cost benchmark  (owner: Sukrutha)
-- trusted-to-curated writes the SAME rows in three layouts:
--   benchmark/sales_csv/            CSV
--   benchmark/sales_parquet_flat/   Parquet, no partitions
--   curated/sales_fact/             Parquet, partitioned by year/month/channel
-- Run B1 and B2 once, then run each test query on all three tables and record
-- "Data scanned" and "Run time" from the Query stats tab in docs/benchmark.md.
-- =====================================================================

-- B1. Database for the benchmark tables
CREATE DATABASE IF NOT EXISTS thornquist_benchmark;

-- B2a. CSV table
CREATE EXTERNAL TABLE IF NOT EXISTS thornquist_benchmark.sales_csv (
  transaction_id string, line_number int, source_system string, channel string,
  sale_date date, region string, store_id int, store_name string,
  category string, sku string, product_name string, quantity int,
  unit_price double, line_amount double, payment_method string, cust_num string,
  is_walk_in boolean, customer_segment string, day_of_week string, year int, month int)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION 's3://thornquist-lake-764541939057/benchmark/sales_csv/'
TBLPROPERTIES ('skip.header.line.count'='1');

-- B2b. Parquet, unpartitioned
CREATE EXTERNAL TABLE IF NOT EXISTS thornquist_benchmark.sales_parquet_flat (
  transaction_id string, line_number int, source_system string, channel string,
  sale_date date, region string, store_id int, store_name string,
  category string, sku string, product_name string, quantity int,
  unit_price double, line_amount double, payment_method string, cust_num string,
  is_walk_in boolean, customer_segment string, day_of_week string, year int, month int)
STORED AS PARQUET
LOCATION 's3://thornquist-lake-764541939057/benchmark/sales_parquet_flat/';

-- Test 1: online revenue by region  (partitioned table can skip all in-store files)
SELECT region, ROUND(SUM(line_amount), 2) AS revenue FROM thornquist_benchmark.sales_csv          WHERE channel = 'online' GROUP BY region;
SELECT region, ROUND(SUM(line_amount), 2) AS revenue FROM thornquist_benchmark.sales_parquet_flat WHERE channel = 'online' GROUP BY region;
SELECT region, ROUND(SUM(line_amount), 2) AS revenue FROM thornquist_curated.sales_fact           WHERE channel = 'online' GROUP BY region;
