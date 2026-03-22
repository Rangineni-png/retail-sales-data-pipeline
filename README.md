# Retail Sales Data Pipeline (Superstore)

Medium-level Data Engineering portfolio project: a batch ETL pipeline that ingests raw Superstore sales data, cleans it with Python, loads it into PostgreSQL, and builds analytics-ready tables + KPI SQL queries.

## Tech Stack
- Python (pandas)
- PostgreSQL
- SQL (staging → dimensional model)
- Git/GitHub

## Repo Structure
- `data/raw/` raw dataset (ignored by git)
- `data/processed/` cleaned outputs (ignored by git)
- `sql/schema/` database schema
- `sql/transforms/` transformations + KPI queries
- `src/` ETL scripts
- `docs/` architecture notes

## Status
Day 1: repo scaffold + environment setup ✅