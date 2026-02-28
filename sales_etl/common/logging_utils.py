import logging
from datetime import datetime
from pathlib import Path


def setup_logging(log_dir: Path) -> logging.Logger:
    log_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    latest_log = log_dir / "pipeline_latest.log"
    run_log = log_dir / f"pipeline_run_{ts}.log"

    logger = logging.getLogger("sales_etl")
    logger.setLevel(logging.INFO)
    logger.handlers = []

    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    stream = logging.StreamHandler()
    stream.setFormatter(formatter)
    logger.addHandler(stream)

    for log_file in (latest_log, run_log):
        fh = logging.FileHandler(log_file, mode="w", encoding="utf-8")
        fh.setFormatter(formatter)
        logger.addHandler(fh)

    return logger
