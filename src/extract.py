import os
import re
import sys
from datetime import datetime

import pandas as pd

# We’ll read from .env later via config.py, but keeping this runnable immediately.
RAW_PATH_DEFAULT = "data/raw/superstore.csv"
OUT_PATH_DEFAULT = "data/processed/superstore_clean.csv"


def to_snake_case(name: str) -> str:
    """
    Convert column names like 'Order Date' or 'Sub-Category' to 'order_date'/'sub_category'.

    Why:
      - snake_case is the standard in analytics engineering and SQL
      - avoids quoting issues in SQL ("Order Date" needs quotes)
    How:
      - replace non-alphanumeric with underscores, collapse repeats, lowercase
    """
    name = name.strip()
    name = re.sub(r"[^A-Za-z0-9]+", "_", name)
    name = re.sub(r"_+", "_", name)
    return name.strip("_").lower()


def main(raw_path: str = RAW_PATH_DEFAULT, out_path: str = OUT_PATH_DEFAULT) -> int:
    if not os.path.exists(raw_path):
        print(f"[ERROR] Raw file not found: {raw_path}")
        return 1

    # 1) Extract: read raw CSV
    # Why parse as strings first:
    # - avoids pandas guessing wrong types (postal codes losing leading zeros, etc.)
    df = pd.read_csv(raw_path, dtype=str)
    raw_rows = len(df)

    # 2) Standardize column names
    df.columns = [to_snake_case(c) for c in df.columns]

    # Expected columns based on your header row
    expected = {
        "row_id", "order_id", "order_date", "ship_date", "ship_mode",
        "customer_id", "customer_name", "segment",
        "country", "city", "state", "postal_code", "region",
        "product_id", "category", "sub_category", "product_name",
        "sales", "quantity", "discount", "profit",
    }
    missing = expected - set(df.columns)
    if missing:
        print(f"[ERROR] Missing expected columns in dataset: {sorted(missing)}")
        print(f"[DEBUG] Columns found: {list(df.columns)}")
        return 1

    # 3) Clean whitespace for all text columns
    # Why: trailing spaces cause duplicate-looking keys and messy group-bys
    for col in df.columns:
        df[col] = df[col].astype(str).str.strip()

    # 4) Type conversions (Transform-lite in Extract step)
    # Dates: dataset uses formats like 11/8/2017
    # errors='coerce' turns invalid dates into NaT (we will validate later)
    df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")
    df["ship_date"] = pd.to_datetime(df["ship_date"], errors="coerce")

    # Numeric columns: remove commas just in case and convert
    # Why: ensures consistent numeric types for Postgres and KPIs
    def to_numeric(series: pd.Series) -> pd.Series:
        return pd.to_numeric(series.astype(str).str.replace(",", "", regex=False), errors="coerce")

    df["sales"] = to_numeric(df["sales"])
    df["discount"] = to_numeric(df["discount"])
    df["profit"] = to_numeric(df["profit"])

    # Quantity should be integer; keep as nullable Int64 to preserve nulls if any
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce").astype("Int64")

    # Postal code: keep as text to preserve leading zeros and mixed formats
    # Also normalize blanks to NaN
    df["postal_code"] = df["postal_code"].replace({"": pd.NA, "nan": pd.NA, "None": pd.NA})

    # Row ID must be unique and numeric
    # Row ID should be numeric; some datasets include footer/summary rows.
    df["row_id"] = pd.to_numeric(df["row_id"], errors="coerce").astype("Int64")

    rows_before_filter = len(df)
    df = df[df["row_id"].notna()].copy()
    rows_after_filter = len(df)

    dropped_non_numeric = rows_before_filter - rows_after_filter
    if dropped_non_numeric > 0:
        print(f"[WARN] Dropped {dropped_non_numeric} rows with non-numeric row_id (likely footer/summary).")
    else:
        print("[INFO] No non-numeric row_id rows found.")

    # Drop exact duplicate row_id keeping first (safety), but we’ll also validate
    before = len(df)
    df = df.drop_duplicates(subset=["row_id"], keep="first")
    after = len(df)

    # 5) Save processed output
    os.makedirs(os.path.dirname(out_path), exist_ok=True)

    # Save dates as ISO strings for consistent downstream load
    df_out = df.copy()
    df_out["order_date"] = df_out["order_date"].dt.date.astype(str)
    df_out["ship_date"] = df_out["ship_date"].dt.date.astype(str)

    df_out.to_csv(out_path, index=False)

    # 6) Print a useful run summary (helps debugging + shows professionalism)
    print("=== Extract Summary ===")
    print(f"Raw input:       {raw_path}")
    print(f"Processed output:{out_path}")
    print(f"Rows in raw file: {raw_rows}")
    print(f"Rows after filter:{rows_after_filter} (dropped {dropped_non_numeric} non-data rows)")
    print(f"Rows kept final:  {after} (dropped {rows_after_filter - after} duplicate row_id)")
    print("Null counts (key fields):")
    print(df_out[["order_id", "order_date", "customer_id", "product_id"]].isna().sum())

    return 0


if __name__ == "__main__":
    raw_path = sys.argv[1] if len(sys.argv) > 1 else RAW_PATH_DEFAULT
    out_path = sys.argv[2] if len(sys.argv) > 2 else OUT_PATH_DEFAULT
    raise SystemExit(main(raw_path, out_path))