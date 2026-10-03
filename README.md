# Voicecall ETL Pipeline

A local ETL project for processing synthetic voice-agent call logs, validating noisy records, enriching accepted calls, and generating reporting-ready outputs for operational analysis.

## Overview

This project demonstrates a practical data pipeline that:

- generates raw call-log data with intentional data quality issues
- validates incoming records and captures rejection reasons
- normalizes timestamps and computes business-relevant features
- loads clean records into SQLite using idempotent upsert logic
- runs SQL-based analytics and exports CSV summaries for reporting

The implementation uses:

- Python for the ETL workflow
- Pandas for validation, cleaning, and transformation
- SQLite for local storage and query-based analysis
- CSV files for reporting outputs

## Key capabilities

- reproducible synthetic data generation using a fixed seed
- detection of missing fields, malformed timestamps, duplicates, and invalid values
- IST-normalized timestamps for downstream analysis
- call duration and feature engineering
- null handling for `amount_promised` using imputation
- transformation-stage deduplication using the latest `start_time`
- idempotent database loading with `call_id` as the unique key
- business analytics exports for operational insights

## Project structure

- `generate.py` — creates the raw call-log dataset
- `pipeline.py` — validates, transforms, and loads clean records into SQLite
- `queries.py` — runs analytics queries and writes CSV reports
- `data/raw_calls.json` — generated raw dataset
- `data/rejected_log.json` — rejected records with reason and contextual metadata
- `database/call_data.db` — SQLite database used for the cleaned data
- `output/` — analytics CSV exports
- `tests/test_pipeline.py` — validation and transformation checks
- `requirements.txt` — project dependencies
- `.gitignore` — excludes local cache and editor files while keeping required outputs

## Setup

Create a virtual environment and install the dependencies:

```bash
python -m venv .venv
source .venv/bin/activate   # Linux/macOS
# or .venv\Scripts\activate  # Windows
pip install -r requirements.txt
```

## Run the pipeline

```bash
python generate.py --output data/raw_calls.json --count 500 --seed 42
python pipeline.py --input data/raw_calls.json --db-path database/call_data.db
python queries.py --db-path database/call_data.db --output-dir output
```

## Data quality and transformation

The source dataset is intentionally noisy and includes:

- missing required fields across a subset of records
- exact duplicate raw records
- malformed timestamps
- invalid values or type mismatches where relevant

This creates a realistic data-cleaning workflow and ensures validation is not skipped during ingestion.

The transformation layer enriches each valid record with:

- normalized timestamps in IST
- `call_duration_seconds`
- `call_hour`
- `call_date`
- `is_weekend`
- `duration_bucket` values (`short`, `medium`, `long`)
- `is_amount_imputed`
- `amount_promised` imputation to `0` when null

Deduplication is handled in two stages:

1. ingestion rejects exact duplicate raw rows
2. transformation keeps the latest record for each `call_id` based on `start_time`

## SQLite loading behavior

The database stores:

- `calls` — cleaned analytical fact table
- `ingestion_log` — metadata for each run, including processing summary and rejection counts

The pipeline uses `CREATE TABLE IF NOT EXISTS` and an upsert strategy on `call_id`, ensuring repeated runs remain idempotent without creating duplicate rows. Each run still records a new ingestion entry while preserving historical data already loaded.

## Analytics outputs

The project exports CSV reports for:

- connect rate by language
- callback request rate by hour
- long-call share and average promised amount
- top 3 agents by total calls and outcome mix
- daily call-volume trend

## Testing

Run the test suite with:

```bash
pytest -q
```

## Design notes

This project was designed around the realities of ETL work: messy source data, explicit validation, and the need for reliable downstream analysis. I separated generation, ingestion, transformation, and reporting into clear stages so each part can be verified and understood independently.

The most important modeling decision was handling duplicates in a meaningful way. Exact duplicate raw records are rejected early because they represent a true source-data problem. However, records with the same `call_id` but different content are still valid candidates for transformation, where the latest `start_time` is kept. This matches business logic more closely than rejecting everything too early and helps preserve the most recent version of a conversation.

I also chose SQLite because it keeps the project lightweight, portable, and easy to run locally without introducing a separate database service. The result is a clean and practical pipeline that reflects the structure and discipline of a production-grade data workflow without becoming unnecessarily complex.

If this were scaled up, I would keep the same core design but move the workload to a more distributed and monitored pipeline. I would likely process larger datasets in batches, add stronger operational logging, and introduce partitioning or incremental loading for the fact table so the system can handle higher volume without slowing down the entire workflow. I would also add better orchestration and alerting around data quality checks, because in a production environment the main concern is not just getting the data in, but also making sure the pipeline remains reliable and observable as volume grows.

## License

This project is intended for personal and professional portfolio use.
