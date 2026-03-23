-- sql/schema/01_create_tables.sql
-- Retail Sales Data Pipeline - Superstore
-- Creates a small warehouse model: staging + dims + fact

BEGIN;

-- Optional: keep everything under one schema
CREATE SCHEMA IF NOT EXISTS retail;

-- 1) Staging table (cleaned/typed but still "one row per order line")
DROP TABLE IF EXISTS retail.stg_superstore_orders;
CREATE TABLE retail.stg_superstore_orders (
  row_id            BIGINT PRIMARY KEY,
  order_id          TEXT NOT NULL,
  order_date        DATE NOT NULL,
  ship_date         DATE,
  ship_mode         TEXT,

  customer_id       TEXT,
  customer_name     TEXT,
  segment           TEXT,

  country           TEXT,
  city              TEXT,
  state             TEXT,
  postal_code       TEXT,
  region            TEXT,

  product_id        TEXT,
  category          TEXT,
  sub_category      TEXT,
  product_name      TEXT,

  sales             NUMERIC(12,2),
  quantity          INTEGER,
  discount          NUMERIC(6,4),
  profit            NUMERIC(12,2),

  -- lineage / audit
  source_file       TEXT,
  loaded_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_stg_order_id   ON retail.stg_superstore_orders(order_id);
CREATE INDEX IF NOT EXISTS idx_stg_order_date ON retail.stg_superstore_orders(order_date);
CREATE INDEX IF NOT EXISTS idx_stg_customer   ON retail.stg_superstore_orders(customer_id);
CREATE INDEX IF NOT EXISTS idx_stg_product    ON retail.stg_superstore_orders(product_id);

-- 2) Dimensions
DROP TABLE IF EXISTS retail.dim_customer;
CREATE TABLE retail.dim_customer (
  customer_key      BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  customer_id       TEXT UNIQUE,
  customer_name     TEXT,
  segment           TEXT,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

DROP TABLE IF EXISTS retail.dim_product;
CREATE TABLE retail.dim_product (
  product_key       BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  product_id        TEXT UNIQUE,
  category          TEXT,
  sub_category      TEXT,
  product_name      TEXT,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

DROP TABLE IF EXISTS retail.dim_geo;
CREATE TABLE retail.dim_geo (
  geo_key           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  country           TEXT,
  state             TEXT,
  city              TEXT,
  postal_code       TEXT,
  region            TEXT,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE(country, state, city, postal_code, region)
);

DROP TABLE IF EXISTS retail.dim_ship_mode;
CREATE TABLE retail.dim_ship_mode (
  ship_mode_key     BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  ship_mode         TEXT UNIQUE,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 3) Fact table (line-level sales)
DROP TABLE IF EXISTS retail.fact_sales;
CREATE TABLE retail.fact_sales (
  sales_key         BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

  row_id            BIGINT NOT NULL UNIQUE,
  order_id          TEXT NOT NULL,
  order_date        DATE NOT NULL,
  ship_date         DATE,

  customer_key      BIGINT REFERENCES retail.dim_customer(customer_key),
  product_key       BIGINT REFERENCES retail.dim_product(product_key),
  geo_key           BIGINT REFERENCES retail.dim_geo(geo_key),
  ship_mode_key     BIGINT REFERENCES retail.dim_ship_mode(ship_mode_key),

  sales             NUMERIC(12,2),
  quantity          INTEGER,
  discount          NUMERIC(6,4),
  profit            NUMERIC(12,2),

  loaded_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_fact_order_date ON retail.fact_sales(order_date);
CREATE INDEX IF NOT EXISTS idx_fact_order_id   ON retail.fact_sales(order_id);
CREATE INDEX IF NOT EXISTS idx_fact_customer   ON retail.fact_sales(customer_key);
CREATE INDEX IF NOT EXISTS idx_fact_product    ON retail.fact_sales(product_key);
CREATE INDEX IF NOT EXISTS idx_fact_geo        ON retail.fact_sales(geo_key);

COMMIT;