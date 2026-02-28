from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def normalize_columns(df: DataFrame) -> DataFrame:
    return df.toDF(*[col.strip().lower() for col in df.columns])


def write_parquet(df: DataFrame, target: Path) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    df.write.mode("overwrite").parquet(str(target))


def add_surrogate_key(df: DataFrame, key_name: str) -> DataFrame:
    return df.withColumn(key_name, F.monotonically_increasing_id() + F.lit(1))
