import argparse
from pathlib import Path

from sales_etl.common.logging_utils import setup_logging
from sales_etl.common.spark_session import create_spark_session
from sales_etl.config import PipelineConfig
from sales_etl.pipeline import run_pipeline


def parse_args():
    parser = argparse.ArgumentParser(description="Run sales PySpark ETL")
    parser.add_argument("--input-dir", default="datasets", help="Input datasets directory")
    parser.add_argument("--lake-dir", default="data_lake", help="Output data lake directory")
    parser.add_argument("--log-dir", default="log", help="Pipeline log directory")
    parser.add_argument("--app-name", default="sales-pyspark-etl", help="Spark application name")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = PipelineConfig(
        input_dir=Path(args.input_dir),
        lake_dir=Path(args.lake_dir),
        log_dir=Path(args.log_dir),
        app_name=args.app_name,
    )

    logger = setup_logging(config.log_dir)
    spark = create_spark_session(config.app_name)

    try:
        run_pipeline(spark, config, logger)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
