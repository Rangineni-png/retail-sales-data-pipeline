# Architecture

## Pipeline
1. **Extract (Python / pandas)**
   - Input: `data/raw/superstore.csv`
   - Output: `data/processed/superstore_clean.csv`
   - Standardizes column names (snake_case), casts dates/numerics, removes non-data rows.

2. **Load (Python / psycopg2)**
   - Loads processed CSV into `retail.stg_superstore_orders` using Postgres `COPY`
   - Idempotent load: truncates staging before loading

3. **Transform (SQL)**
   - `01_clean_sales.sql`: builds star schema
     - Dimensions: customer, product, geography, ship mode
     - Fact: `fact_sales` (one row per order line)

4. **Analytics + Quality**
   - `02_kpis.sql`: KPI queries
   - `03_data_quality_checks.sql`: validation checks

## Star Schema
- **fact_sales** joins to:
  - `dim_customer`
  - `dim_product`
  - `dim_geo`
  - `dim_ship_mode`