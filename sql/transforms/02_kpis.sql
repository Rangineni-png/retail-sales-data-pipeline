-- sql/transforms/02_kpis.sql
-- KPI queries (read-only). Run them in psql to show results.

-- 1) Monthly sales & profit trend
SELECT
  DATE_TRUNC('month', f.order_date)::date AS month,
  ROUND(SUM(f.sales), 2)  AS total_sales,
  ROUND(SUM(f.profit), 2) AS total_profit,
  ROUND(SUM(f.profit) / NULLIF(SUM(f.sales), 0), 4) AS profit_margin
FROM retail.fact_sales f
GROUP BY 1
ORDER BY 1;

-- 2) Sales & profit by region
SELECT
  g.region,
  ROUND(SUM(f.sales), 2)  AS total_sales,
  ROUND(SUM(f.profit), 2) AS total_profit
FROM retail.fact_sales f
JOIN retail.dim_geo g ON f.geo_key = g.geo_key
GROUP BY 1
ORDER BY total_profit DESC;

-- 3) Top 10 products by profit
SELECT
  p.product_name,
  ROUND(SUM(f.profit), 2) AS total_profit,
  ROUND(SUM(f.sales), 2)  AS total_sales
FROM retail.fact_sales f
JOIN retail.dim_product p ON f.product_key = p.product_key
GROUP BY 1
ORDER BY total_profit DESC
LIMIT 10;

-- 4) Category/Sub-category performance
SELECT
  p.category,
  p.sub_category,
  ROUND(SUM(f.sales), 2)  AS total_sales,
  ROUND(SUM(f.profit), 2) AS total_profit
FROM retail.fact_sales f
JOIN retail.dim_product p ON f.product_key = p.product_key
GROUP BY 1, 2
ORDER BY total_profit DESC;

-- 5) Discount impact (bucketed) vs profit margin
SELECT
  CASE
    WHEN f.discount = 0 THEN '0%'
    WHEN f.discount <= 0.10 THEN '0–10%'
    WHEN f.discount <= 0.20 THEN '10–20%'
    WHEN f.discount <= 0.30 THEN '20–30%'
    ELSE '30%+'
  END AS discount_bucket,
  COUNT(*) AS line_items,
  ROUND(SUM(f.sales), 2)  AS total_sales,
  ROUND(SUM(f.profit), 2) AS total_profit,
  ROUND(SUM(f.profit) / NULLIF(SUM(f.sales), 0), 4) AS profit_margin
FROM retail.fact_sales f
GROUP BY 1
ORDER BY
  CASE discount_bucket
    WHEN '0%' THEN 1
    WHEN '0–10%' THEN 2
    WHEN '10–20%' THEN 3
    WHEN '20–30%' THEN 4
    ELSE 5
  END;

-- 6) Shipping mode usage + profitability
SELECT
  sm.ship_mode,
  COUNT(*) AS line_items,
  ROUND(SUM(f.sales), 2)  AS total_sales,
  ROUND(SUM(f.profit), 2) AS total_profit
FROM retail.fact_sales f
JOIN retail.dim_ship_mode sm ON f.ship_mode_key = sm.ship_mode_key
GROUP BY 1
ORDER BY total_profit DESC;

-- 7) Top 10 customers by profit
SELECT
  c.customer_name,
  c.segment,
  ROUND(SUM(f.profit), 2) AS total_profit,
  ROUND(SUM(f.sales), 2)  AS total_sales
FROM retail.fact_sales f
JOIN retail.dim_customer c ON f.customer_key = c.customer_key
GROUP BY 1, 2
ORDER BY total_profit DESC
LIMIT 10;