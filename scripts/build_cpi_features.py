# build_cpi_features.py

import os
from typing import Dict, List, Tuple

import pandas as pd
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SECRET_KEY = os.getenv("SUPABASE_SECRET_KEY")

if not SUPABASE_URL or not SUPABASE_SECRET_KEY:
    raise ValueError("Missing SUPABASE_URL or SUPABASE_SECRET_KEY in .env")

supabase = create_client(SUPABASE_URL, SUPABASE_SECRET_KEY)

RAW_SERIES_NAME = "CPI_HEADLINE_INDEX"
FEATURE_GROUP = "inflation"


def fetch_cpi_index_series() -> pd.DataFrame:
    response = (
        supabase.table("raw_macro_series")
        .select("series_name, period_date, value")
        .eq("series_name", RAW_SERIES_NAME)
        .order("period_date")
        .execute()
    )

    rows = response.data or []
    if not rows:
        raise ValueError("No CPI_HEADLINE_INDEX rows found in raw_macro_series")

    df = pd.DataFrame(rows)
    df["period_date"] = pd.to_datetime(df["period_date"])
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df = df.dropna(subset=["value"]).sort_values("period_date").reset_index(drop=True)

    return df


def compute_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # Month-on-month % change
    df["cpi_mom_change"] = ((df["value"] / df["value"].shift(1)) - 1) * 100
    df["cpi_yoy_change"] = ((df["value"] / df["value"].shift(12)) - 1) * 100

    return df


def build_feature_rows(df: pd.DataFrame) -> List[Dict]:
    rows: List[Dict] = []

    for _, row in df.iterrows():
        as_of_date = row["period_date"].date().isoformat()

        mom_value = row["cpi_mom_change"]
        yoy_value = row["cpi_yoy_change"]

        if pd.notna(mom_value):
            rows.append({
                "as_of_date": as_of_date,
                "feature_name": "cpi_headline_index_mom_change",
                "feature_value": round(float(mom_value), 10),
                "feature_group": FEATURE_GROUP,
            })

        if pd.notna(yoy_value):
            rows.append({
                "as_of_date": as_of_date,
                "feature_name": "cpi_headline_index_yoy_change",
                "feature_value": round(float(yoy_value), 10),
                "feature_group": FEATURE_GROUP,
            })

    return rows


def dedupe_rows(rows: List[Dict]) -> List[Dict]:
    deduped: Dict[Tuple[str, str], Dict] = {
        (row["feature_name"], row["as_of_date"]): row
        for row in rows
    }
    return list(deduped.values())


def upsert_macro_features(rows: List[Dict]) -> None:
    if not rows:
        print("No macro feature rows to upsert.")
        return

    final_rows = dedupe_rows(rows)

    result = (
        supabase.table("macro_features")
        .upsert(final_rows, on_conflict="feature_name,as_of_date")
        .execute()
    )

    print(f"Upserted {len(final_rows)} rows into macro_features")
    print(result)


def main() -> None:
    df = fetch_cpi_index_series()
    df = compute_features(df)
    feature_rows = build_feature_rows(df)
    upsert_macro_features(feature_rows)


if __name__ == "__main__":
    main()
