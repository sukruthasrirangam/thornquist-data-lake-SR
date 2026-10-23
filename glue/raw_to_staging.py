"""
raw-to-staging  |  Thornquist Outfitters data lake  (owner: Ruchi)
Conforms the three raw sources into one sales model + a clean customer table.

Schema conflicts resolved here (format fixes only):
  1. Date formats      : POS mixes YYYY-MM-DD and DD/MM/YYYY; e-commerce uses ISO-8601 UTC timestamps
  2. Customer key      : POS `cust_num` (text, '0000000' = walk-in) vs e-commerce `cust_ref` (number)
  3. Structure         : e-commerce orders hold a nested `items` array; POS is one row per line
  4. Product taxonomy  : POS has `category`; e-commerce only has a SKU prefix (CAM/ACC/FOO/APP)
  5. Region field      : POS store `region` vs e-commerce `shipping_region`; no channel column in POS
  6. Dirty text        : non-breaking spaces, stray whitespace and Windows line endings (\r)

Duplicates, orphaned customers and range problems are NOT removed here.
They pass through to staging/ so the dq-gate job can detect and quarantine them.
"""
import sys
from pyspark.sql import SparkSession, functions as F

# ---------- Glue / local bootstrap ----------
try:
    from awsglue.utils import getResolvedOptions
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from pyspark.context import SparkContext

    args = getResolvedOptions(sys.argv, ["JOB_NAME"])
    sc = SparkContext.getOrCreate()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)
    BASE = "s3://thornquist-lake-764541939057"
except ImportError:  # local test run: python raw_to_staging.py <base_path>
    spark = SparkSession.builder.master("local[*]").appName("raw-to-staging").getOrCreate()
    job = None
    BASE = sys.argv[1]

RAW = f"{BASE}/raw"
STAGING = f"{BASE}/staging"


def clean_text(col):
    """Remove non-breaking spaces and carriage returns, then trim."""
    return F.trim(F.regexp_replace(F.col(col), "[\\u00A0\\r]", ""))


# ---------- 1. POS (CSV, one row per line item) ----------
pos = (spark.read.option("header", True).csv(f"{RAW}/pos/")
       .withColumn("_source_file", F.input_file_name()))
for c in [c for c in pos.columns if c not in ("ingest_date", "_source_file")]:
    pos = pos.withColumn(c, clean_text(c))

pos_date = (F.when(F.col("sale_date").rlike(r"^\d{4}-\d{2}-\d{2}$"), F.to_date("sale_date", "yyyy-MM-dd"))
             .when(F.col("sale_date").rlike(r"^\d{2}/\d{2}/\d{4}$"), F.to_date("sale_date", "dd/MM/yyyy")))

pos_lines = pos.select(
    F.lit("pos").alias("source_system"),
    F.lit("in-store").alias("channel"),
    F.col("transaction_id").alias("transaction_id"),
    F.lit(1).alias("line_number"),
    pos_date.alias("sale_date"),
    F.lit(None).cast("timestamp").alias("sale_ts"),
    F.col("sale_date").alias("sale_date_original"),
    F.col("region"),
    F.col("store_id").cast("int").alias("store_id"),
    F.col("store_name"),
    F.col("category"),
    F.lit(None).cast("string").alias("sku"),
    F.col("product_name"),
    F.col("quantity").cast("int").alias("quantity"),
    F.col("unit_price").cast("decimal(10,2)").alias("unit_price"),
    F.col("payment_method"),
    F.when(F.col("cust_num") == "0000000", None).otherwise(F.col("cust_num")).alias("cust_num"),
    (F.col("cust_num") == "0000000").alias("is_walk_in"),
    F.col("ingest_date").cast("string").alias("ingest_date"),
    F.col("_source_file"),
)

# ---------- 2. E-commerce (JSON Lines, nested items) ----------
ecom = spark.read.json(f"{RAW}/ecommerce/")
ecom_lines = (ecom
              .select("*", F.posexplode("items").alias("pos_idx", "item"))
              .withColumn("ts", F.to_timestamp("order_timestamp", "yyyy-MM-dd'T'HH:mm:ssX")))

sku_prefix = F.substring(F.col("item.sku"), 1, 3)
category = (F.when(sku_prefix == "CAM", "Camping")
             .when(sku_prefix == "ACC", "Accessories")
             .when(sku_prefix == "FOO", "Footwear")
             .when(sku_prefix == "APP", "Apparel")
             .otherwise("Unknown"))

ecom_lines = ecom_lines.select(
    F.lit("ecommerce").alias("source_system"),
    F.lower(F.trim("channel")).alias("channel"),
    F.col("order_id").cast("string").alias("transaction_id"),
    (F.col("pos_idx") + 1).alias("line_number"),
    F.to_date("ts").alias("sale_date"),
    F.col("ts").alias("sale_ts"),
    F.col("order_timestamp").alias("sale_date_original"),
    F.trim("shipping_region").alias("region"),
    F.lit(None).cast("int").alias("store_id"),
    F.lit(None).cast("string").alias("store_name"),
    category.alias("category"),
    F.trim("item.sku").alias("sku"),
    F.trim(F.regexp_replace("item.product_name", "[\\u00A0\\r]", "")).alias("product_name"),
    F.col("item.qty").cast("int").alias("quantity"),
    F.col("item.unit_price").cast("decimal(10,2)").alias("unit_price"),
    F.lit(None).cast("string").alias("payment_method"),
    F.col("cust_ref").cast("string").alias("cust_num"),
    F.lit(False).alias("is_walk_in"),
    F.col("ingest_date").cast("string").alias("ingest_date"),
    F.col("_source_file"),
)

# ---------- 3. Union into one conformed sales model ----------
sales_lines = (pos_lines.unionByName(ecom_lines)
               .withColumn("line_amount", (F.col("quantity") * F.col("unit_price")).cast("decimal(12,2)"))
               .withColumn("_staged_ts", F.current_timestamp()))

(sales_lines.write.mode("overwrite")
 .partitionBy("source_system")
 .parquet(f"{STAGING}/sales_lines/"))

# ---------- 4. Customers (flat-file extract) ----------
cust = spark.read.option("header", True).csv(f"{RAW}/customers/")
for c in [c for c in cust.columns if c != "ingest_date"]:
    cust = cust.withColumn(c, clean_text(c))

customers = cust.select(
    F.col("cust_num"),
    F.col("first_name"),
    F.col("last_name"),
    F.lower("email").alias("email"),
    F.to_date("signup_date", "yyyy-MM-dd").alias("signup_date"),
    F.col("loyalty_tier"),
    F.col("region").alias("home_region"),
    F.col("ingest_date").cast("string").alias("ingest_date"),
    F.current_timestamp().alias("_staged_ts"),
)
customers.write.mode("overwrite").parquet(f"{STAGING}/customers/")

print(f"raw-to-staging complete: {pos_lines.count()} POS lines, "
      f"{ecom_lines.count()} e-commerce lines, {customers.count()} customers")

if job:
    job.commit()
