from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from sales_etl.common.io_utils import write_parquet


def run_silver(spark: SparkSession, lake_dir: Path, logger) -> None:
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
