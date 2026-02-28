# Sales PySpark ETL Project

PySpark ETL project using a layered lakehouse-style pipeline:
- Bronze: raw CSV ingestion to Parquet
- Silver: cleaned and standardized entities
- Gold: analytics-ready dimensions and fact table

## Best-Practice Structure

```text
.
├── sales_etl/
│   ├── cli.py                  # CLI entrypoint
│   ├── config.py               # Typed runtime configuration
│   ├── pipeline.py             # Orchestration flow
│   ├── common/
│   │   ├── io_utils.py         # Shared read/write + schema helpers
│   │   ├── logging_utils.py    # File + console logging
│   │   └── spark_session.py    # SparkSession builder
│   └── jobs/
│       ├── bronze.py           # Bronze transforms
│       ├── silver.py           # Silver transforms
│       └── gold.py             # Gold transforms
├── datasets/                   # Source CSVs
├── data_lake/                  # Generated Parquet outputs
├── log/                        # Pipeline logs
├── script/
│   └── run_etl.sh              # Shell runner
├── tests/                      # SQL quality checks kept from previous design
└── requirements.txt
```

## Install

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Run

Preferred:
```bash
python -m sales_etl.cli
```

Shell runner:
```bash
./script/run_etl.sh
```

## Outputs

- Bronze Parquet: `data_lake/bronze/`
- Silver Parquet: `data_lake/silver/`
- Gold Parquet: `data_lake/gold/`
- Logs: `log/pipeline_latest.log` and timestamped logs in `log/`
