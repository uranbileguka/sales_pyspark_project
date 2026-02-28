from pathlib import Path

from pyspark.sql import SparkSession

from sales_etl.config import PostgresPublishConfig


GOLD_TABLES = [
    "dim_customers",
    "dim_products",
    "dim_salesperson",
    "dim_discount",
    "fact_sales",
]


def _ensure_schema(postgres: PostgresPublishConfig) -> None:
    import psycopg
    from psycopg import sql

    conn = psycopg.connect(
        host=postgres.host,
        port=postgres.port,
        dbname=postgres.database,
        user=postgres.user,
        password=postgres.password,
    )
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL("CREATE SCHEMA IF NOT EXISTS {};").format(
                        sql.Identifier(postgres.schema)
                    )
                )
    finally:
        conn.close()


def run_publish_gold(spark: SparkSession, lake_dir: Path, postgres: PostgresPublishConfig, logger) -> None:
    missing = postgres.missing_required()
    if missing:
        logger.warning("Skipping publish: missing env vars: %s", ", ".join(missing))
        return

    logger.info("Publishing Gold tables to Postgres schema: %s", postgres.schema)
    _ensure_schema(postgres)

    for table in GOLD_TABLES:
        source = lake_dir / "gold" / table
        target = f"{postgres.schema}.{table}"
        logger.info("Publish %s -> %s", source, target)

        df = spark.read.parquet(str(source))
        (
            df.write
            .format("jdbc")
            .option("url", postgres.jdbc_url())
            .option("dbtable", target)
            .option("user", postgres.user)
            .option("password", postgres.password)
            .option("driver", postgres.jdbc_driver)
            .mode("overwrite")
            .save()
        )
