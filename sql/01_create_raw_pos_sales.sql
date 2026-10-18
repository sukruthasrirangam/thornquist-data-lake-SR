CREATE EXTERNAL TABLE thornquist_raw.pos_sales (
  transaction_id BIGINT, store_id INT, store_name STRING, region STRING,
  sale_date STRING, category STRING, product_name STRING, quantity INT,
  unit_price DOUBLE, payment_method STRING, cust_num STRING
)
PARTITIONED BY (ingest_date STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 's3://thornquist-lake-764541939057/raw/pos/'
TBLPROPERTIES ('skip.header.line.count'='1');

MSCK REPAIR TABLE thornquist_raw.pos_sales;
