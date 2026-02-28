from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from sales_etl.common.io_utils import add_surrogate_key, write_parquet


def run_gold(spark: SparkSession, lake_dir: Path, logger) -> None:
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

    dim_customers = add_surrogate_key(customers, "customer_sk").select(
        "customer_sk",
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

    dim_products = add_surrogate_key(products, "product_sk").select(
        "product_sk",
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

    dim_salesperson = add_surrogate_key(salesperson, "salesperson_key").select(
        "salesperson_id",
        "salesperson_key",
        "name",
        "region",
        "email",
    )
    write_parquet(dim_salesperson, gold_root / "dim_salesperson")

    dim_discount = add_surrogate_key(discount, "discount_key").select(
        "discount_id",
        "discount_key",
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
            F.col("dc.customer_sk").alias("customer_key"),
            F.col("dp.product_sk").alias("product_key"),
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
