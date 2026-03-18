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

FEATURE_GROUP = "inflation"


def fetch_raw_cpi_series() -> pd.DataFrame:
    response = (
        supabase.table("raw_macro_series")
        .select("series_name, period_date, value, source")
        .in_("series_name", ["CPI_HEADLINE_INDEX", "CPI_HEADLINE_INFLATION"])
        .order("period_date")
        .execute()
    )

    rows = response.data or []
    if not rows:
        raise ValueError("No CPI rows found in raw_macro_series")

    df = pd.DataFrame(rows)
    df["period_date"] = pd.to_datetime(df["period_date"])
    df["value"] = pd.to_numeric(df["value"], errors="coerce")

    df = df.dropna(subset=["period_date", "value"])

    # CPI base-year methodology may appear as multiple sources for the same month.
    # Deterministically prefer the newer base (e.g., mospi_2024) when available.
    source_priority = {
        "mospi_2024": 2,
        "mospi_2012": 1,
    }
    if "source" in df.columns:
        df["source_priority"] = df["source"].map(source_priority).fillna(0).astype(int)
        df = (
            df.sort_values(["series_name", "period_date", "source_priority"], ascending=[True, True, False])
            .drop_duplicates(subset=["series_name", "period_date"], keep="first")
            .reset_index(drop=True)
        )
        df = df.drop(columns=["source_priority"])
    else:
        df = df.sort_values(["series_name", "period_date"]).reset_index(drop=True)

    return df


def reshape_cpi_data(df: pd.DataFrame) -> pd.DataFrame:
    wide = (
        df.pivot_table(
            index="period_date",
            columns="series_name",
            values="value",
            aggfunc="last"
        )
        .sort_index()
        .reset_index()
    )

    return wide


def compute_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # MoM change from CPI index
    if "CPI_HEADLINE_INDEX" in df.columns:
        df["cpi_headline_index_mom_change"] = (
            (df["CPI_HEADLINE_INDEX"] / df["CPI_HEADLINE_INDEX"].shift(1)) - 1
        ) * 100

    # YoY inflation directly from official inflation series
    if "CPI_HEADLINE_INFLATION" in df.columns:
        df["cpi_headline_index_yoy_change"] = df["CPI_HEADLINE_INFLATION"]

    return df


def build_feature_rows(df: pd.DataFrame) -> List[Dict]:
    rows: List[Dict] = []

    for _, row in df.iterrows():
        as_of_date = row["period_date"].date().isoformat()

        mom_value = row.get("cpi_headline_index_mom_change")
        yoy_value = row.get("cpi_headline_index_yoy_change")

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
    raw_df = fetch_raw_cpi_series()
    wide_df = reshape_cpi_data(raw_df)
    feature_df = compute_features(wide_df)

    print("CPI feature preview:")
    print(feature_df.tail(12))

    feature_rows = build_feature_rows(feature_df)
    upsert_macro_features(feature_rows)


if __name__ == "__main__":
    main()