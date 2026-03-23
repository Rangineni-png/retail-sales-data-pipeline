# Retail Sales Data Pipeline (Superstore)

A medium-level batch ETL + analytics warehouse project. The pipeline ingests raw Superstore sales data, cleans it with Python, loads it into PostgreSQL staging, builds a dimensional (star) schema, and runs KPI + data quality SQL checks.

## Tech Stack
- Python (pandas)
- PostgreSQL
- SQL (staging → dimensions + fact)
- Git/GitHub

## Architecture (high level)
Raw CSV → **extract.py** (clean/standardize) → processed CSV → **load.py** (COPY into Postgres staging) → SQL transforms (dims + fact) → KPI queries + quality checks.

## Folder Structure
- `data/raw/` raw dataset (ignored by git)
- `data/processed/` cleaned dataset (ignored by git)
- `src/`
  - `extract.py` → creates `data/processed/superstore_clean.csv`
  - `load.py` → loads staging using Postgres COPY
- `sql/schema/` → table creation
- `sql/transforms/`
  - `01_clean_sales.sql` → build dimensions + fact
  - `02_kpis.sql` → KPI queries
  - `03_data_quality_checks.sql` → validation checks
- `docs/architecture.md` → pipeline notes

## Setup
### 1) Create Python environment
```bash
conda create -n de-pipeline python=3.11 -y
conda activate de-pipeline
pip install -r requirements.txt