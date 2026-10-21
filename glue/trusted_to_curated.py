"""
trusted-to-curated  |  Thornquist Outfitters data lake  (owner: Sukrutha)

Shapes trusted data for analysts:
    curated/sales_fact/     one row per sale line, joined to the customer's loyalty tier
                            Parquet + Snappy, partitioned by year / month / channel
    curated/customer_dim/   one row per customer (duplicate-email flag kept)

Why these partitions: almost every business question filters on time and on channel,
so Athena can skip whole folders. Only a few partition levels, because the data is small
(too many partitions = many tiny files = slower and more expensive queries).

Also writes the same sales data in two other layouts, used only for the cost benchmark:
    benchmark/sales_csv/            plain CSV (what we'd have without the lake)
    benchmark/sales_parquet_flat/   Parquet, no partitions

Glue job settings
    Type Spark | Glue 4.0 | Python 3 | Role LabRole | G.1X x 2 workers | timeout 10 min
    Job parameter:  --BUCKET thornquist-lake-764541939057
Local test:  python trusted_to_curated.py <base_path>
"""
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def run(spark: SparkSession, base: str) -> None:
    # small data: 8 shuffle partitions instead of the default 200 (fewer tiny files, faster, cheaper)
    spark.conf.set("spark.sql.shuffle.partitions", "8")
    sales = spark.read.parquet(f"{base}/trusted/sales_lines/")
    customers = spark.read.parquet(f"{base}/trusted/customers/")

    customer_dim = customers.select(
        "cust_num", "first_name", "last_name", "email", "signup_date",
        "loyalty_tier", "home_region", F.col("dq_warning").alias("data_quality_flag"))

    tiers = customers.select("cust_num", "loyalty_tier")
    sales_fact = (
        sales.join(tiers, "cust_num", "left")
        .select(
            "transaction_id", "line_number", "source_system", "channel",
            "sale_date", "region", "store_id", "store_name",
            "category", "sku", "product_name", "quantity",
            F.col("unit_price").cast("double").alias("unit_price"),
            F.col("line_amount").cast("double").alias("line_amount"),
            "payment_method", "cust_num", "is_walk_in",
            # walk-ins have no customer record, so give them their own segment
            F.when(F.col("is_walk_in"), "Walk-in").otherwise(F.col("loyalty_tier")).alias("customer_segment"),
            F.date_format("sale_date", "EEEE").alias("day_of_week"),
            F.year("sale_date").alias("year"),
            F.month("sale_date").alias("month"),
        )
    )

    (sales_fact.repartition("year", "month", "channel")   # one file per partition, no tiny files
     .write.mode("overwrite").option("compression", "snappy")
     .partitionBy("year", "month", "channel").parquet(f"{base}/curated/sales_fact/"))
    customer_dim.coalesce(1).write.mode("overwrite").parquet(f"{base}/curated/customer_dim/")

    # ---- benchmark copies (same rows, different layouts) ----
    sales_fact.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{base}/benchmark/sales_csv/")
    sales_fact.coalesce(1).write.mode("overwrite").parquet(f"{base}/benchmark/sales_parquet_flat/")

    print(f"trusted-to-curated complete: {sales_fact.count()} sales_fact rows, "
          f"{customer_dim.count()} customers")


if __name__ == "__main__":
    try:
        from awsglue.context import GlueContext
        from awsglue.job import Job
        from awsglue.utils import getResolvedOptions
        from pyspark.context import SparkContext

        args = getResolvedOptions(sys.argv, ["JOB_NAME", "BUCKET"])
        glue_context = GlueContext(SparkContext.getOrCreate())
        job = Job(glue_context)
        job.init(args["JOB_NAME"], args)
        run(glue_context.spark_session, f"s3://{args['BUCKET']}")
        job.commit()
    except ImportError:
        spark = SparkSession.builder.master("local[*]").appName("trusted-to-curated").getOrCreate()
        run(spark, sys.argv[1])
