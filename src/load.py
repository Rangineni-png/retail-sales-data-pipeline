import os
import sys
from pathlib import Path

import psycopg2
from dotenv import load_dotenv


PROCESSED_DEFAULT = "data/processed/superstore_clean.csv"


def get_conn():
    """
    Why: Connect to Postgres to load data.
    How: Read connection settings from .env (environment variables).
    """
    load_dotenv()

    host = os.getenv("DB_HOST", "localhost")
    port = int(os.getenv("DB_PORT", "5432"))
    dbname = os.getenv("DB_NAME", "retail_sales")
    user = os.getenv("DB_USER", "postgres")
    password = os.getenv("DB_PASSWORD", "")

    return psycopg2.connect(
        host=host, port=port, dbname=dbname, user=user, password=password
    )


def load_staging(csv_path: str) -> int:
    """
    Load processed CSV into retail.stg_superstore_orders using COPY.

    Why COPY:
      - fastest bulk load in Postgres (industry standard)
    Why TRUNCATE first:
      - idempotent batch load (re-running won't duplicate rows)
    """
    p = Path(csv_path)
    if not p.exists():
        raise FileNotFoundError(f"Processed CSV not found: {csv_path}")

    copy_sql = """
        COPY retail.stg_superstore_orders (
            row_id, order_id, order_date, ship_date, ship_mode,
            customer_id, customer_name, segment,
            country, city, state, postal_code, region,
            product_id, category, sub_category, product_name,
            sales, quantity, discount, profit
        )
        FROM STDIN WITH (FORMAT CSV, HEADER TRUE, DELIMITER ',', QUOTE '\"');
    """

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE retail.stg_superstore_orders;")

            with p.open("r", encoding="utf-8") as f:
                cur.copy_expert(copy_sql, f)

            # add lineage info (optional but useful)
            cur.execute(
                """
                UPDATE retail.stg_superstore_orders
                SET source_file = %s
                WHERE source_file IS NULL;
                """,
                (p.name,),
            )

            cur.execute("SELECT COUNT(*) FROM retail.stg_superstore_orders;")
            (count,) = cur.fetchone()

    print(f"Loaded {count} rows into retail.stg_superstore_orders from {p}")
    return count


def main() -> int:
    csv_path = sys.argv[1] if len(sys.argv) > 1 else PROCESSED_DEFAULT
    load_staging(csv_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())