from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from sales_etl.common.io_utils import normalize_columns, write_parquet


def run_bronze(spark: SparkSession, input_dir: Path, lake_dir: Path, logger) -> None:
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
