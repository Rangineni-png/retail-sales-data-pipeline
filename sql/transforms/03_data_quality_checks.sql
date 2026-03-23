-- sql/transforms/03_data_quality_checks.sql
-- Simple but meaningful data quality checks for the pipeline

-- 1) Row count consistency: staging vs fact
SELECT
  (SELECT COUNT(*) FROM retail.stg_superstore_orders) AS stg_rows,
  (SELECT COUNT(*) FROM retail.fact_sales)            AS fact_rows;

-- 2) Uniqueness: row_id should be unique in staging and fact
SELECT
  (SELECT COUNT(*) FROM retail.stg_superstore_orders) -
  (SELECT COUNT(DISTINCT row_id) FROM retail.stg_superstore_orders) AS stg_duplicate_row_id_count,
  (SELECT COUNT(*) FROM retail.fact_sales) -
  (SELECT COUNT(DISTINCT row_id) FROM retail.fact_sales)            AS fact_duplicate_row_id_count;

-- 3) Null checks for required fields
SELECT
  SUM(CASE WHEN order_id   IS NULL THEN 1 ELSE 0 END) AS null_order_id,
  SUM(CASE WHEN order_date IS NULL THEN 1 ELSE 0 END) AS null_order_date
FROM retail.fact_sales;

-- 4) FK checks: ensure fact keys exist (should be 0 if joins worked)
SELECT
  SUM(CASE WHEN customer_key  IS NULL THEN 1 ELSE 0 END) AS null_customer_key,
  SUM(CASE WHEN product_key   IS NULL THEN 1 ELSE 0 END) AS null_product_key,
  SUM(CASE WHEN geo_key       IS NULL THEN 1 ELSE 0 END) AS null_geo_key,
  SUM(CASE WHEN ship_mode_key IS NULL THEN 1 ELSE 0 END) AS null_ship_mode_key
FROM retail.fact_sales;

-- 5) Numeric sanity checks
SELECT
  SUM(CASE WHEN sales < 0 THEN 1 ELSE 0 END)    AS negative_sales_rows,
  SUM(CASE WHEN quantity < 0 THEN 1 ELSE 0 END) AS negative_quantity_rows,
  SUM(CASE WHEN discount < 0 OR discount > 1 THEN 1 ELSE 0 END) AS invalid_discount_rows
FROM retail.fact_sales;

-- 6) Date sanity checks: ship_date should not be before order_date
SELECT
  COUNT(*) AS ship_before_order_rows
FROM retail.fact_sales
WHERE ship_date IS NOT NULL
  AND ship_date < order_date;