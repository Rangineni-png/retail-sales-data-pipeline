-- Build dimensions + fact from staging (re-runnable)

BEGIN;

-- Clear existing modeled tables so reruns don't duplicate data
TRUNCATE TABLE retail.fact_sales RESTART IDENTITY;
TRUNCATE TABLE retail.dim_customer RESTART IDENTITY CASCADE;
TRUNCATE TABLE retail.dim_product  RESTART IDENTITY CASCADE;
TRUNCATE TABLE retail.dim_geo      RESTART IDENTITY CASCADE;
TRUNCATE TABLE retail.dim_ship_mode RESTART IDENTITY CASCADE;

-- 1) Create DIM tables from unique values in staging
INSERT INTO retail.dim_customer (customer_id, customer_name, segment)
SELECT DISTINCT customer_id, customer_name, segment
FROM retail.stg_superstore_orders
WHERE customer_id IS NOT NULL;

INSERT INTO retail.dim_product (product_id, category, sub_category, product_name)
SELECT
  product_id,
  MAX(category)      AS category,
  MAX(sub_category)  AS sub_category,
  MAX(product_name)  AS product_name
FROM retail.stg_superstore_orders
WHERE product_id IS NOT NULL
GROUP BY product_id;

INSERT INTO retail.dim_geo (country, state, city, postal_code, region)
SELECT DISTINCT country, state, city, postal_code, region
FROM retail.stg_superstore_orders;

INSERT INTO retail.dim_ship_mode (ship_mode)
SELECT DISTINCT ship_mode
FROM retail.stg_superstore_orders
WHERE ship_mode IS NOT NULL;

-- 2) Create FACT table by joining staging → dims to get keys
INSERT INTO retail.fact_sales (
  row_id, order_id, order_date, ship_date,
  customer_key, product_key, geo_key, ship_mode_key,
  sales, quantity, discount, profit
)
SELECT
  s.row_id, s.order_id, s.order_date, s.ship_date,
  c.customer_key,
  p.product_key,
  g.geo_key,
  sm.ship_mode_key,
  s.sales, s.quantity, s.discount, s.profit
FROM retail.stg_superstore_orders s
LEFT JOIN retail.dim_customer c ON s.customer_id = c.customer_id
LEFT JOIN retail.dim_product  p ON s.product_id  = p.product_id
LEFT JOIN retail.dim_geo g
  ON s.country = g.country
 AND s.state = g.state
 AND s.city = g.city
 AND COALESCE(s.postal_code, '') = COALESCE(g.postal_code, '')
 AND s.region = g.region
LEFT JOIN retail.dim_ship_mode sm ON s.ship_mode = sm.ship_mode;

COMMIT;