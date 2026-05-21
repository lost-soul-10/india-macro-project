# fetch_wpi_mospi.py
# Reads WPI data from Excel and upserts it into raw_macro_series.

from pathlib import Path
from typing import Any, Optional

import pandas as pd
from dotenv import load_dotenv
from supabase import create_client
import os

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SECRET_KEY = os.getenv("SUPABASE_SECRET_KEY")

if not SUPABASE_URL or not SUPABASE_SECRET_KEY:
    raise ValueError("Missing SUPABASE_URL or SUPABASE_SECRET_KEY in .env")

supabase = create_client(SUPABASE_URL, SUPABASE_SECRET_KEY)

# Hardcoded Excel path.
# Keep this file in the same data folder as cpi_data.xlsx.
WPI_EXCEL_PATH = "data/wpi_data.xlsx"


def safe_float(value: Any) -> Optional[float]:
    if value is None:
        return None

    s = str(value).strip()

    if s == "" or s.lower() in {"nan", "none", "null"}:
        return None

    try:
        return float(s)
    except ValueError:
        return None


def clean_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [
        str(col).strip().lower().replace(" ", "_")
        for col in df.columns
    ]
    return df


def validate_columns(df: pd.DataFrame) -> None:
    required_columns = [
        "year",
        "month",
        "majorgroup",
        "group",
        "index_value",
    ]

    missing = [col for col in required_columns if col not in df.columns]

    if missing:
        raise ValueError(f"Missing required WPI Excel columns: {missing}")


def load_wpi_excel() -> pd.DataFrame:
    path = Path(WPI_EXCEL_PATH)

    if not path.exists():
        raise FileNotFoundError(
            f"WPI Excel file not found at {path}. "
            "Make sure it is saved as data/wpi_data.xlsx."
        )

    df = pd.read_excel(path)
    df = clean_columns(df)
    validate_columns(df)

    df["year"] = pd.to_numeric(df["year"], errors="coerce")
    df["month"] = df["month"].astype(str).str.strip()
    df["majorgroup"] = df["majorgroup"].astype(str).str.strip()
    df["group"] = df["group"].astype(str).str.strip()
    df["index_value"] = pd.to_numeric(df["index_value"], errors="coerce")

    df = df.dropna(subset=["year", "month", "index_value"])
    df["year"] = df["year"].astype(int)

    df["period_date"] = pd.to_datetime(
        df["month"] + " " + df["year"].astype(str),
        format="%B %Y",
        errors="coerce",
    )

    df = df.dropna(subset=["period_date"])

    return df


def transform_rows(df: pd.DataFrame) -> list[dict]:
    rows: list[dict] = []

    df = df[
        df["majorgroup"].str.lower().eq("wholesale price index")
    ].copy()

    if df.empty:
        raise ValueError("No headline WPI rows found in Excel.")

    for _, row in df.iterrows():
        value = safe_float(row.get("index_value"))

        if value is None:
            continue

        period_date = pd.to_datetime(row["period_date"]).date()

        rows.append(
            {
                "series_name": "WPI",
                "source": "excel",
                "period_date": str(period_date),
                "release_date": None,
                "value": value,
                "unit": "index",
                "frequency": "monthly",
            }
        )

    return rows


def dedupe_rows(rows: list[dict]) -> list[dict]:
    deduped = {
        (row["series_name"], row["period_date"]): row
        for row in rows
    }

    return list(deduped.values())


def upsert_rows(rows: list[dict]) -> None:
    if not rows:
        print("No WPI rows found to upsert.")
        return

    final_rows = dedupe_rows(rows)

    result = (
        supabase.table("raw_macro_series")
        .upsert(final_rows, on_conflict="series_name,period_date")
        .execute()
    )

    print(f"Upserted {len(final_rows)} WPI rows into raw_macro_series")
    print(result)


def main() -> None:
    print(f"WPI_EXCEL_PATH = {WPI_EXCEL_PATH}")

    df = load_wpi_excel()

    print("Loaded WPI Excel rows:", len(df))
    print("WPI Excel preview:")
    print(
        df[
            [
                "year",
                "month",
                "majorgroup",
                "group",
                "index_value",
                "period_date",
            ]
        ].head(12)
    )

    rows = transform_rows(df)

    print("Prepared WPI rows:", len(rows))
    upsert_rows(rows)


if __name__ == "__main__":
    main()