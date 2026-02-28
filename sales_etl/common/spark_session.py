from pyspark.sql import SparkSession


def create_spark_session(app_name: str, jars_packages: list[str] | None = None, jars: list[str] | None = None) -> SparkSession:
    builder = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
    )

    if jars_packages:
        builder = builder.config("spark.jars.packages", ",".join(jars_packages))
    if jars:
        builder = builder.config("spark.jars", ",".join(jars))

    return builder.getOrCreate()
