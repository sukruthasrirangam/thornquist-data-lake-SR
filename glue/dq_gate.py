"""
dq-gate  |  Thornquist Outfitters data lake  (owner: Sukrutha)

Reads the conformed staging zone (written by raw-to-staging) and decides, row by row,
what is trustworthy:

    staging/sales_lines/  --(rules 1-6)-->  trusted/sales_lines/                 rows that pass
                                        \->  quarantine/quarantined_sales/        rows that fail + dq_reason
    staging/customers/    --(rule 7)---->  trusted/customers/                    all rows, dq_warning flag

It also writes two "quality" outputs used in the dashboard and demo:
    quality/dq_metrics/run_date=.../        rows and revenue per rule (the cost of bad data)
    quality/rule_self_test/run_date=.../    one deliberately broken row per rule + PASS/FAIL

Rules
    1 completeness   transaction_id, quantity, unit_price present; cust_num present unless walk-in
    2 validity       sale_date could be parsed (raw text kept in sale_date_original)
    3 uniqueness     source_system + transaction_id + line_number seen only once
    4 referential    cust_num exists in the customer extract (walk-ins are exempt by design)
    5 range          0 < quantity <= 100 and 0 < unit_price <= 5000
    6 domain         category, region and channel are known values
    7 customers      the same email on several cust_num values is FLAGGED, not rejected

Glue job settings
    Type Spark | Glue 4.0 | Python 3 | Role LabRole | G.1X x 2 workers | timeout 10 min
    Job parameters:  --BUCKET thornquist-lake-764541939057   --RUN_SELF_TEST true
Local test:  python dq_gate.py <base_path>
"""
import sys
from datetime import date

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

MAX_QUANTITY = 100
MAX_UNIT_PRICE = 5000
VALID_CATEGORIES = ["Camping", "Accessories", "Footwear", "Apparel"]
VALID_REGIONS = ["Ontario", "Quebec", "Alberta", "British Columbia", "Atlantic"]
VALID_CHANNELS = ["in-store", "online"]
RULE_ORDER = ["missing_required_field", "invalid_date", "duplicate_record", "orphan_customer",
              "quantity_out_of_range", "price_out_of_range", "unknown_category",
              "unknown_region", "unknown_channel"]


# ---------- Rule 7: customers ----------
def flag_customers(customers: DataFrame) -> DataFrame:
    """Keep every customer, but flag emails shared by more than one cust_num."""
    same_email = Window.partitionBy(F.lower(F.trim("email")))
    return (customers
            .withColumn("_n", F.count("*").over(same_email))
            .withColumn("dq_warning", F.when(F.col("_n") > 1, F.lit("duplicate_email")))
            .drop("_n"))


# ---------- Rules 1-6: sales lines ----------
def check_sales(sales: DataFrame, customers: DataFrame) -> DataFrame:
    """Add dq_reason: null when the row passes, otherwise every broken rule joined by ';'."""
    # Rule 3 prep: number the copies of each line; copy 2+ is a duplicate (terminal re-send / webhook retry).
    copies = Window.partitionBy("source_system", "transaction_id", "line_number").orderBy("_source_file")
    sales = sales.withColumn("_copy", F.row_number().over(copies))

    # Rule 4 prep: which cust_num values exist in the customer extract?
    known = customers.select(F.col("cust_num").alias("_known")).distinct()
    sales = sales.join(known, sales.cust_num == known._known, "left")

    walk_in = F.coalesce(F.col("is_walk_in"), F.lit(False))
    checks = [
        # 1 completeness
        F.when(F.col("transaction_id").isNull() | F.col("quantity").isNull()
               | F.col("unit_price").isNull() | (F.col("cust_num").isNull() & ~walk_in),
               "missing_required_field"),
        # 2 validity
        F.when(F.col("sale_date").isNull(), "invalid_date"),
        # 3 uniqueness
        F.when(F.col("_copy") > 1, "duplicate_record"),
        # 4 referential integrity
        F.when(~walk_in & F.col("cust_num").isNotNull() & F.col("_known").isNull(), "orphan_customer"),
        # 5 range
        F.when((F.col("quantity") <= 0) | (F.col("quantity") > MAX_QUANTITY), "quantity_out_of_range"),
        F.when((F.col("unit_price") <= 0) | (F.col("unit_price") > MAX_UNIT_PRICE), "price_out_of_range"),
        # 6 domain
        F.when(~F.col("category").isin(VALID_CATEGORIES), "unknown_category"),
        F.when(~F.col("region").isin(VALID_REGIONS), "unknown_region"),
        F.when(~F.col("channel").isin(VALID_CHANNELS), "unknown_channel"),
    ]
    reasons = F.concat_ws(";", *checks)   # nulls are skipped, so a clean row gives ""
    return (sales.withColumn("dq_reason", F.when(reasons == "", None).otherwise(reasons))
                 .drop("_copy", "_known"))


# ---------- Self-test: prove every rule fires ----------
def build_self_test(sales: DataFrame) -> DataFrame:
    """Copy one clean walk-in sale and break one thing per test case.
    The sample data never triggers some rules (range, domain), so this shows they work."""
    base = (sales.filter(F.col("is_walk_in") & F.col("sale_date").isNotNull()).limit(1)
            .withColumn("_source_file", F.lit("self-test")))

    def case(name, expected, **changes):
        row = base.withColumn("transaction_id", F.lit(f"TEST-{name}"))
        for col, value in changes.items():
            row = row.withColumn(col, value)
        return row.withColumn("test_case", F.lit(name)).withColumn("expected_reason", F.lit(expected))

    cases = [
        case("control_clean_row", None),
        case("missing_price", "missing_required_field", unit_price=F.lit(None).cast("decimal(10,2)")),
        case("impossible_date", "invalid_date",
             sale_date=F.lit(None).cast("date"), sale_date_original=F.lit("31/31/2026")),
        case("negative_quantity", "quantity_out_of_range", quantity=F.lit(-2)),
        case("absurd_price", "price_out_of_range", unit_price=F.lit(99999).cast("decimal(10,2)")),
        case("unknown_customer", "orphan_customer",
             cust_num=F.lit("999999"), is_walk_in=F.lit(False)),
        case("unmapped_sku", "unknown_category", category=F.lit("Unknown")),
        case("bad_region", "unknown_region", region=F.lit("Nowhere")),
        case("duplicate_first_copy", None, transaction_id=F.lit("TEST-dup"), _source_file=F.lit("a")),
        case("duplicate_second_copy", "duplicate_record", transaction_id=F.lit("TEST-dup"),
             _source_file=F.lit("b")),
    ]
    batch = cases[0]
    for c in cases[1:]:
        batch = batch.unionByName(c)
    return batch


# ---------- Metrics: the cost of bad data ----------
def build_metrics(checked: DataFrame) -> DataFrame:
    amount = F.round(F.sum("line_amount").cast("double"), 2)
    by_rule = (checked.filter(F.col("dq_reason").isNotNull())
               .withColumn("metric", F.explode(F.split("dq_reason", ";"))))
    parts = [
        checked.groupBy("source_system").agg(F.lit("rows_checked").alias("metric"),
                                             F.count("*").alias("row_count"), amount.alias("revenue")),
        checked.filter(F.col("dq_reason").isNull()).groupBy("source_system")
               .agg(F.lit("rows_passed").alias("metric"), F.count("*").alias("row_count"),
                    amount.alias("revenue")),
        checked.filter(F.col("dq_reason").isNotNull()).groupBy("source_system")
               .agg(F.lit("rows_quarantined").alias("metric"), F.count("*").alias("row_count"),
                    amount.alias("revenue")),
        by_rule.groupBy("source_system", "metric")
               .agg(F.count("*").alias("row_count"), amount.alias("revenue")),
        # Not an error but a blind spot: valid store sales with no customer attached.
        checked.filter(F.col("dq_reason").isNull() & F.col("is_walk_in")).groupBy("source_system")
               .agg(F.lit("info_walk_in_sales").alias("metric"), F.count("*").alias("row_count"),
                    amount.alias("revenue")),
    ]
    cols = ["source_system", "metric", "row_count", "revenue"]
    out = parts[0].select(cols)
    for p in parts[1:]:
        out = out.unionByName(p.select(cols))
    return out


# ---------- Job ----------
def run(spark: SparkSession, base: str, run_date: str, self_test: bool = True) -> bool:
    # small data: 8 shuffle partitions instead of the default 200 (fewer tiny files, faster, cheaper)
    spark.conf.set("spark.sql.shuffle.partitions", "8")
    sales = spark.read.parquet(f"{base}/staging/sales_lines/")
    customers = flag_customers(spark.read.parquet(f"{base}/staging/customers/"))

    data = (sales.withColumn("test_case", F.lit(None).cast("string"))
                 .withColumn("expected_reason", F.lit(None).cast("string")))
    if self_test:
        data = data.unionByName(build_self_test(sales))

    checked_all = check_sales(data, customers).cache()
    tests = checked_all.filter(F.col("test_case").isNotNull())
    checked = checked_all.filter(F.col("test_case").isNull()).drop("test_case", "expected_reason")

    passed = checked.filter(F.col("dq_reason").isNull()).drop("dq_reason")
    failed = checked.filter(F.col("dq_reason").isNotNull())
    metrics = build_metrics(checked)

    passed.repartition("source_system").write.mode("overwrite").partitionBy("source_system").parquet(f"{base}/trusted/sales_lines/")
    customers.coalesce(1).write.mode("overwrite").parquet(f"{base}/trusted/customers/")
    # one folder per run, so earlier rejects stay as history
    failed.coalesce(1).write.mode("overwrite").parquet(f"{base}/quarantine/quarantined_sales/run_date={run_date}/")
    metrics.coalesce(1).write.mode("overwrite").parquet(f"{base}/quality/dq_metrics/run_date={run_date}/")

    # ---- summary in the job log (CloudWatch) ----
    print(f"===== DQ GATE run_date={run_date} =====")
    print(f"checked={checked.count()} passed={passed.count()} quarantined={failed.count()} "
          f"customers_flagged={customers.filter(F.col('dq_warning').isNotNull()).count()}")
    for r in metrics.orderBy("source_system", "metric").collect():
        print(f"  {r['source_system']:<10} {r['metric']:<24} rows={r['row_count']:<6} "
              f"revenue=${r['revenue'] or 0:,.2f}")

    all_ok = True
    if self_test:
        results = tests.withColumn(
            "test_result",
            F.when(F.coalesce("dq_reason", F.lit("")) == F.coalesce("expected_reason", F.lit("")),
                   "PASS").otherwise("FAIL"))
        (results.select("test_case", "expected_reason", "dq_reason", "test_result")
                .coalesce(1).write.mode("overwrite").parquet(f"{base}/quality/rule_self_test/run_date={run_date}/"))
        print("----- rule self-test -----")
        for r in results.orderBy("test_case").collect():
            print(f"  {r['test_result']}  {r['test_case']:<22} expected={r['expected_reason']} "
                  f"got={r['dq_reason']}")
            all_ok &= r["test_result"] == "PASS"
        if not all_ok:   # fail loudly: EventBridge -> SNS sends the alert email
            raise RuntimeError("DQ rule self-test failed: a rule is not working as designed")
    return all_ok


if __name__ == "__main__":
    try:
        from awsglue.context import GlueContext
        from awsglue.job import Job
        from awsglue.utils import getResolvedOptions
        from pyspark.context import SparkContext

        args = getResolvedOptions(sys.argv, ["JOB_NAME", "BUCKET", "RUN_SELF_TEST"])
        glue_context = GlueContext(SparkContext.getOrCreate())
        job = Job(glue_context)
        job.init(args["JOB_NAME"], args)
        run(glue_context.spark_session, f"s3://{args['BUCKET']}", date.today().isoformat(),
            args["RUN_SELF_TEST"].lower() == "true")
        job.commit()
    except ImportError:   # local test run
        spark = SparkSession.builder.master("local[*]").appName("dq-gate").getOrCreate()
        run(spark, sys.argv[1], date.today().isoformat())
