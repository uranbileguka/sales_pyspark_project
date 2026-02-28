from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class PipelineConfig:
    input_dir: Path
    lake_dir: Path
    log_dir: Path
    app_name: str = "sales-pyspark-etl"
