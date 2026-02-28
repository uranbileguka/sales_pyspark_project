from sales_etl.config import PipelineConfig
from sales_etl.jobs.bronze import run_bronze
from sales_etl.jobs.gold import run_gold
from sales_etl.jobs.publish import run_publish_gold
from sales_etl.jobs.silver import run_silver


def run_pipeline(spark, config: PipelineConfig, logger) -> None:
    logger.info("Starting pipeline")
    run_bronze(spark, input_dir=config.input_dir, lake_dir=config.lake_dir, logger=logger)
    run_silver(spark, lake_dir=config.lake_dir, logger=logger)
    run_gold(spark, lake_dir=config.lake_dir, logger=logger)

    if config.postgres and config.postgres.enabled:
        run_publish_gold(spark, lake_dir=config.lake_dir, postgres=config.postgres, logger=logger)
    else:
        logger.info("Postgres publish disabled (set PUBLISH_GOLD_TO_POSTGRES=true to enable)")

    logger.info("Pipeline completed")
    logger.info("Data lake output: %s", config.lake_dir.resolve())
    logger.info("Log folder: %s", config.log_dir.resolve())
