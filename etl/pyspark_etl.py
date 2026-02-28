import argparse
import logging
from datetime import datetime
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def setup_logging(log_dir: Path) -> logging.Logger:
    log_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    latest_log = log_dir / "pipeline_latest.log"
    run_log = log_dir / f"pipeline_run_{ts}.log"

    logger = logging.getLogger("pyspark_etl")
    logger.setLevel(logging.INFO)
    logger.handlers = []

    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

    console = logging.StreamHandler()
    console.setFormatter(formatter)
    logger.addHandler(console)

    for target in (latest_log, run_log):
        fh = logging.FileHandler(target, mode="w", encoding="utf-8")
        fh.setFormatter(formatter)
        logger.addHandler(fh)

    return logger


def spark_session(app_name: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def normalize_columns(df):
    return df.toDF(*[c.strip().lower() for c in df.columns])


def write_parquet(df, target: Path):
    target.parent.mkdir(parents=True, exist_ok=True)
    df.write.mode("overwrite").parquet(str(target))


def load_bronze(spark: SparkSession, input_dir: Path, lake_dir: Path, logger: logging.Logger):
    logger.info("Loading Bronze layer")
    bronze_root = lake_dir / "bronze"

    source_dirs = [
        input_dir / "source_crm",
        input_dir / "source_erp",
        input_dir / "source_marketing",
    ]

    for source_dir in source_dirs:
        source_name = source_dir.name.replace("source_", "")
        for csv_path in sorted(source_dir.glob("*.csv")):
            table_name = csv_path.stem.lower()
            target = bronze_root / source_name / table_name

            logger.info("Bronze %s -> %s", csv_path, target)
            df = spark.read.option("header", True).option("inferSchema", True).csv(str(csv_path))
            df = normalize_columns(df)
            df = df.withColumn("_ingest_ts", F.current_timestamp()).withColumn("_source_system", F.lit(source_name))
            write_parquet(df, target)


def load_silver(spark: SparkSession, lake_dir: Path, logger: logging.Logger):
    logger.info("Transforming Silver layer")
    silver_root = lake_dir / "silver"
    bronze_root = lake_dir / "bronze"

    crm_cust = spark.read.parquet(str(bronze_root / "crm" / "cust_info"))
    erp_cust = spark.read.parquet(str(bronze_root / "erp" / "cust_info"))

    crm_cust_clean = crm_cust.select(
        F.col("cst_id").cast("int").alias("customer_id"),
        F.trim(F.col("cst_key")).alias("customer_key"),
        F.initcap(F.trim(F.col("cst_firstname"))).alias("first_name"),
        F.initcap(F.trim(F.col("cst_lastname"))).alias("last_name"),
        F.upper(F.trim(F.col("cst_marital_status"))).alias("marital_status"),
        F.upper(F.trim(F.col("cst_gndr"))).alias("gender"),
        F.to_date("cst_create_date").alias("created_date"),
    )

    erp_cust_clean = erp_cust.select(
        F.regexp_extract(F.col("cid"), r"(\\d+)$", 1).cast("int").alias("customer_id"),
        F.to_date("bdate").alias("birth_date"),
        F.initcap(F.trim(F.col("gen"))).alias("erp_gender"),
    )

    customers = (
        crm_cust_clean.alias("c")
        .join(erp_cust_clean.alias("e"), on="customer_id", how="left")
        .withColumn("gender", F.coalesce("c.gender", F.substring(F.col("e.erp_gender"), 1, 1)))
        .drop("erp_gender")
    )
    write_parquet(customers, silver_root / "customers")

    crm_prd = spark.read.parquet(str(bronze_root / "crm" / "prd_info"))
    erp_cat = spark.read.parquet(str(bronze_root / "erp" / "px_cat_info"))

    products = crm_prd.select(
        F.col("prd_id").cast("int").alias("product_id"),
        F.trim("prd_key").alias("product_key"),
        F.trim("prd_nm").alias("product_name"),
        F.col("prd_cost").cast("double").alias("product_cost"),
        F.regexp_replace(F.trim("prd_line"), " ", "").alias("product_line"),
        F.to_date("prd_start_dt").alias("start_date"),
        F.to_date("prd_end_dt").alias("end_date"),
        F.regexp_replace(F.substring(F.col("prd_key"), 1, 5), "-", "_").alias("category_id"),
    )

    categories = erp_cat.select(
        F.trim("id").alias("category_id"),
        F.trim("cat").alias("category"),
        F.trim("subcat").alias("subcategory"),
        F.trim("maintenance").alias("maintenance"),
    )

    products = products.join(categories, on="category_id", how="left")
    write_parquet(products, silver_root / "products")

    sales = spark.read.parquet(str(bronze_root / "crm" / "sales_details"))
    sales = sales.select(
        F.trim("sls_ord_num").alias("order_num"),
        F.trim("sls_prd_key").alias("product_key"),
        F.col("sls_cust_id").cast("int").alias("customer_id"),
        F.to_date(F.col("sls_order_dt").cast("string"), "yyyyMMdd").alias("order_date"),
        F.to_date(F.col("sls_ship_dt").cast("string"), "yyyyMMdd").alias("ship_date"),
        F.to_date(F.col("sls_due_dt").cast("string"), "yyyyMMdd").alias("due_date"),
        F.col("sls_quantity").cast("int").alias("quantity"),
        F.col("sls_price").cast("double").alias("price"),
        F.coalesce(F.col("sls_sales").cast("double"), F.col("sls_quantity") * F.col("sls_price")).alias("sales_amount"),
    )
    write_parquet(sales, silver_root / "sales")

    salesperson = spark.read.parquet(str(bronze_root / "marketing" / "salesperson"))
    salesperson = salesperson.select(
        F.trim("salesperson_id").alias("salesperson_id"),
        F.trim("name").alias("name"),
        F.trim("region").alias("region"),
        F.trim("email").alias("email"),
    )
    write_parquet(salesperson, silver_root / "salesperson")

    discount = spark.read.parquet(str(bronze_root / "marketing" / "discount_info"))
    discount = discount.select(
        F.trim("discount_id").alias("discount_id"),
        F.trim("description").alias("description"),
        F.col("percent").cast("double").alias("discount_percent"),
        F.col("active").cast("boolean").alias("active"),
    )
    write_parquet(discount, silver_root / "discount")

    salesperson_sales = spark.read.parquet(str(bronze_root / "marketing" / "salesperson_sales"))
    salesperson_sales = salesperson_sales.select(
        F.trim("salesperson_id").alias("salesperson_id"),
        F.trim("sls_ord_num").alias("order_num"),
    )
    write_parquet(salesperson_sales, silver_root / "salesperson_sales")

    sales_discount = spark.read.parquet(str(bronze_root / "marketing" / "sales_discount"))
    sales_discount = sales_discount.select(
        F.trim("discount_id").alias("discount_id"),
        F.trim("sls_ord_num").alias("order_num"),
    )
    write_parquet(sales_discount, silver_root / "sales_discount")


def load_gold(spark: SparkSession, lake_dir: Path, logger: logging.Logger):
    logger.info("Building Gold layer")
    silver_root = lake_dir / "silver"
    gold_root = lake_dir / "gold"

    customers = spark.read.parquet(str(silver_root / "customers"))
    products = spark.read.parquet(str(silver_root / "products"))
    salesperson = spark.read.parquet(str(silver_root / "salesperson"))
    discount = spark.read.parquet(str(silver_root / "discount"))
    sales = spark.read.parquet(str(silver_root / "sales"))
    salesperson_sales = spark.read.parquet(str(silver_root / "salesperson_sales"))
    sales_discount = spark.read.parquet(str(silver_root / "sales_discount"))

    w_cust = Window.orderBy("customer_id")
    w_prod = Window.orderBy("product_id")
    w_salesperson = Window.orderBy("salesperson_id")
    w_discount = Window.orderBy("discount_id")

    dim_customers = customers.select(
        F.dense_rank().over(w_cust).alias("customer_key"),
        "customer_id",
        "customer_key",
        "first_name",
        "last_name",
        "marital_status",
        "gender",
        "birth_date",
        "created_date",
    )
    write_parquet(dim_customers, gold_root / "dim_customers")

    dim_products = products.select(
        F.dense_rank().over(w_prod).alias("product_key"),
        "product_id",
        "product_key",
        "product_name",
        "product_cost",
        "product_line",
        "category",
        "subcategory",
        "maintenance",
    )
    write_parquet(dim_products, gold_root / "dim_products")

    dim_salesperson = salesperson.select(
        F.dense_rank().over(w_salesperson).alias("salesperson_key"),
        "salesperson_id",
        "name",
        "region",
        "email",
    )
    write_parquet(dim_salesperson, gold_root / "dim_salesperson")

    dim_discount = discount.select(
        F.dense_rank().over(w_discount).alias("discount_key"),
        "discount_id",
        "description",
        "discount_percent",
        "active",
    )
    write_parquet(dim_discount, gold_root / "dim_discount")

    fact_sales = (
        sales.alias("s")
        .join(dim_customers.alias("dc"), F.col("s.customer_id") == F.col("dc.customer_id"), "left")
        .join(dim_products.alias("dp"), F.col("s.product_key") == F.col("dp.product_key"), "left")
        .join(salesperson_sales.alias("ss"), F.col("s.order_num") == F.col("ss.order_num"), "left")
        .join(dim_salesperson.alias("dsp"), F.col("ss.salesperson_id") == F.col("dsp.salesperson_id"), "left")
        .join(sales_discount.alias("sd"), F.col("s.order_num") == F.col("sd.order_num"), "left")
        .join(dim_discount.alias("dd"), F.col("sd.discount_id") == F.col("dd.discount_id"), "left")
        .select(
            F.col("s.order_num"),
            F.col("dc.customer_key"),
            F.col("dp.product_key"),
            F.col("dsp.salesperson_key"),
            F.col("dd.discount_key"),
            F.col("s.order_date"),
            F.col("s.ship_date"),
            F.col("s.due_date"),
            F.col("s.quantity"),
            F.col("s.price"),
            F.col("s.sales_amount"),
        )
    )
    write_parquet(fact_sales, gold_root / "fact_sales")


def parse_args():
    parser = argparse.ArgumentParser(description="Run PySpark sales ETL pipeline")
    parser.add_argument("--input-dir", default="datasets", help="Input datasets directory")
    parser.add_argument("--lake-dir", default="data_lake", help="Output data lake directory")
    parser.add_argument("--log-dir", default="log", help="Pipeline log directory")
    return parser.parse_args()


def main():
    args = parse_args()
    input_dir = Path(args.input_dir)
    lake_dir = Path(args.lake_dir)
    log_dir = Path(args.log_dir)

    logger = setup_logging(log_dir)
    spark = spark_session("sales-pyspark-etl")

    try:
        logger.info("Starting PySpark ETL pipeline")
        load_bronze(spark, input_dir=input_dir, lake_dir=lake_dir, logger=logger)
        load_silver(spark, lake_dir=lake_dir, logger=logger)
        load_gold(spark, lake_dir=lake_dir, logger=logger)
        logger.info("Pipeline completed successfully")
        logger.info("Data lake output: %s", lake_dir.resolve())
        logger.info("Log folder: %s", log_dir.resolve())
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
