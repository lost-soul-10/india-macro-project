# fetch_cpi_excel.py

import os
from pathlib import Path
from typing import Any, Optional

import pandas as pd
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SECRET_KEY = os.getenv("SUPABASE_SECRET_KEY")

if not SUPABASE_URL or not SUPABASE_SECRET_KEY:
    raise ValueError("Missing SUPABASE_URL or SUPABASE_SECRET_KEY in .env")

supabase = create_client(SUPABASE_URL, SUPABASE_SECRET_KEY)

CPI_EXCEL_PATH = os.getenv("CPI_EXCEL_PATH", "data/cpi_data.xlsx")

OLD_CPI_BASE_YEAR = str(os.getenv("OLD_CPI_BASE_YEAR", "2012")).strip()
NEW_CPI_BASE_YEAR = str(os.getenv("NEW_CPI_BASE_YEAR", "2024")).strip()


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
        "baseyear",
        "year",
        "month_code",
        "month",
        "state",
        "sector",
        "group",
        "index",
        "inflation",
    ]

    missing = [col for col in required_columns if col not in df.columns]

    if missing:
        raise ValueError(f"Missing required CPI Excel columns: {missing}")


def load_cpi_excel() -> pd.DataFrame:
    path = Path(CPI_EXCEL_PATH)

    if not path.exists():
        raise FileNotFoundError(
            f"CPI Excel file not found at {path}. "
            "Put it in data/cpi_data.xlsx or update CPI_EXCEL_PATH in .env."
        )

    df = pd.read_excel(path)
    df = clean_columns(df)
    validate_columns(df)

    df["baseyear"] = df["baseyear"].astype(str).str.strip()
    df["state"] = df["state"].astype(str).str.strip()
    df["sector"] = df["sector"].astype(str).str.strip()
    df["group"] = df["group"].astype(str).str.strip()

    df["year"] = pd.to_numeric(df["year"], errors="coerce")
    df["month_code"] = pd.to_numeric(df["month_code"], errors="coerce")
    df["index"] = pd.to_numeric(df["index"], errors="coerce")
    df["inflation"] = pd.to_numeric(df["inflation"], errors="coerce")

    df = df.dropna(subset=["baseyear", "year", "month_code"])
    df["year"] = df["year"].astype(int)
    df["month_code"] = df["month_code"].astype(int)

    df["period_date"] = pd.to_datetime(
        {
            "year": df["year"],
            "month": df["month_code"],
            "day": 1,
        }
    )

    return df


def get_series_config(baseyear: str) -> Optional[dict]:
    baseyear = str(baseyear).strip()

    if baseyear == OLD_CPI_BASE_YEAR:
        return {
            "source": f"mospi_{OLD_CPI_BASE_YEAR}_excel",
            "index_series_name": "CPI_HEADLINE_INDEX_OLD",
            "inflation_series_name": "CPI_HEADLINE_INFLATION_OLD",
        }

    if baseyear == NEW_CPI_BASE_YEAR:
        return {
            "source": f"mospi_{NEW_CPI_BASE_YEAR}_excel",
            "index_series_name": "CPI_HEADLINE_INDEX_NEW",
            "inflation_series_name": "CPI_HEADLINE_INFLATION_NEW",
        }

    return None


def transform_rows(df: pd.DataFrame) -> list[dict]:
    rows: list[dict] = []

    df = df[
        (df["state"] == "All India")
        & (df["sector"] == "Combined")
    ].copy()

    if df.empty:
        raise ValueError("No All India + Combined CPI rows found in Excel.")

    # Keep only headline/general rows.
    # Old base uses "General"; new base uses "CPI (General)".
    df = df[
        df["group"].isin(["General", "CPI (General)"])
    ].copy()

    if df.empty:
        raise ValueError("No General / CPI (General) headline rows found in Excel.")

    for _, r in df.iterrows():
        config = get_series_config(r["baseyear"])

        if config is None:
            print(f"Skipping unsupported baseyear: {r['baseyear']}")
            continue

        period_date = pd.to_datetime(r["period_date"]).date()

        index_value = safe_float(r.get("index"))
        inflation_value = safe_float(r.get("inflation"))

        if index_value is not None:
            rows.append(
                {
                    "series_name": config["index_series_name"],
                    "source": config["source"],
                    "period_date": str(period_date),
                    "release_date": None,
                    "value": index_value,
                    "unit": "index",
                    "frequency": "monthly",
                }
            )

        if inflation_value is not None:
            rows.append(
                {
                    "series_name": config["inflation_series_name"],
                    "source": config["source"],
                    "period_date": str(period_date),
                    "release_date": None,
                    "value": inflation_value,
                    "unit": "percent",
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
        print("No CPI rows found to upsert.")
        return

    final_rows = dedupe_rows(rows)

    result = (
        supabase.table("raw_macro_series")
        .upsert(final_rows, on_conflict="series_name,period_date")
        .execute()
    )

    print(f"Upserted {len(final_rows)} CPI rows into raw_macro_series")
    print(result)


def main() -> None:
    print(f"CPI_EXCEL_PATH = {CPI_EXCEL_PATH}")
    print(f"OLD_CPI_BASE_YEAR = {OLD_CPI_BASE_YEAR}")
    print(f"NEW_CPI_BASE_YEAR = {NEW_CPI_BASE_YEAR}")

    df = load_cpi_excel()

    print("Loaded CPI Excel rows:", len(df))
    print("CPI Excel preview:")
    print(
        df[
            [
                "baseyear",
                "year",
                "month_code",
                "month",
                "state",
                "sector",
                "group",
                "index",
                "inflation",
                "period_date",
            ]
        ].head(12)
    )

    rows = transform_rows(df)
    upsert_rows(rows)


if __name__ == "__main__":
    main()