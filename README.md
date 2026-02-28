# Sales PySpark ETL Project

This project now runs as a local PySpark ETL pipeline with layered outputs:
- Bronze: raw CSV ingested to Parquet
- Silver: cleaned/standardized entities
- Gold: analytics-ready dimensions and fact table

## Tech Stack
- PySpark
- Python
- Parquet

## Project Structure
```text
.
├── datasets/                # Source CSV files
├── etl/
│   ├── pyspark_etl.py       # Main PySpark ETL pipeline
│   ├── run_pipeline.py      # Wrapper entrypoint
│   └── requirements.txt
├── script/
│   └── run_etl.sh           # Shell runner
├── log/                     # Pipeline logs
├── data_lake/               # Generated Bronze/Silver/Gold parquet outputs
└── scripts/                 # Existing SQL/generator assets (kept in repo)
```

## Setup
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r etl/requirements.txt
```

## Run ETL
Option 1:
```bash
python -m etl.run_pipeline
```

Option 2:
```bash
./script/run_etl.sh
```

## Output
- Data lake output: `data_lake/bronze`, `data_lake/silver`, `data_lake/gold`
- Logs: `log/pipeline_latest.log` and timestamped run logs in `log/`

## Notes
- Input CSV source remains under `datasets/`.
- Existing legacy PostgreSQL SQL files are kept under `scripts/` and are no longer required for this PySpark run path.
