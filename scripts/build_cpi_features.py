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

CPI_BREAK_DATE = pd.to_datetime(os.getenv("CPI_BREAK_DATE", "2026-01-01"))

RAW_CPI_SERIES = [
    "CPI_HEADLINE_INDEX_OLD",
    "CPI_HEADLINE_INFLATION_OLD",
    "CPI_HEADLINE_INDEX_NEW",
    "CPI_HEADLINE_INFLATION_NEW",
]


def fetch_raw_cpi_series() -> pd.DataFrame:
    response = (
        supabase.table("raw_macro_series")
        .select("series_name, period_date, value, source")
        .in_("series_name", RAW_CPI_SERIES)
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

    df = (
        df.sort_values(["series_name", "period_date"])
        .drop_duplicates(subset=["series_name", "period_date"], keep="last")
        .reset_index(drop=True)
    )

    return df


def reshape_cpi_data(df: pd.DataFrame) -> pd.DataFrame:
    wide = (
        df.pivot_table(
            index="period_date",
            columns="series_name",
            values="value",
            aggfunc="last",
        )
        .sort_index()
        .reset_index()
    )

    return wide


def choose_old_new_value(row: pd.Series, old_col: str, new_col: str):
    period_date = row["period_date"]

    if period_date < CPI_BREAK_DATE:
        return row.get(old_col)

    return row.get(new_col)


def compute_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df["cpi_headline_index_combined"] = df.apply(
        lambda row: choose_old_new_value(
            row,
            "CPI_HEADLINE_INDEX_OLD",
            "CPI_HEADLINE_INDEX_NEW",
        ),
        axis=1,
    )

    df["cpi_headline_inflation_combined"] = df.apply(
        lambda row: choose_old_new_value(
            row,
            "CPI_HEADLINE_INFLATION_OLD",
            "CPI_HEADLINE_INFLATION_NEW",
        ),
        axis=1,
    )

    df["cpi_headline_index_mom_change"] = pd.NA

    pre_break_mask = df["period_date"] < CPI_BREAK_DATE
    post_break_mask = df["period_date"] >= CPI_BREAK_DATE

    if "CPI_HEADLINE_INDEX_OLD" in df.columns:
        old_index = df.loc[pre_break_mask, "CPI_HEADLINE_INDEX_OLD"]

        df.loc[pre_break_mask, "cpi_headline_index_mom_change"] = (
            (old_index / old_index.shift(1)) - 1
        ) * 100

    if "CPI_HEADLINE_INDEX_NEW" in df.columns:
        new_index = df.loc[post_break_mask, "CPI_HEADLINE_INDEX_NEW"]

        df.loc[post_break_mask, "cpi_headline_index_mom_change"] = (
            (new_index / new_index.shift(1)) - 1
        ) * 100

    # Official YoY inflation from Excel.
    # OLD before CPI_BREAK_DATE, NEW from CPI_BREAK_DATE onward.
    df["cpi_headline_index_yoy_change"] = df["cpi_headline_inflation_combined"]

    return df


def build_feature_rows(df: pd.DataFrame) -> List[Dict]:
    rows: List[Dict] = []

    for _, row in df.iterrows():
        as_of_date = row["period_date"].date().isoformat()

        mom_value = row.get("cpi_headline_index_mom_change")
        yoy_value = row.get("cpi_headline_index_yoy_change")

        if pd.notna(mom_value):
            rows.append(
                {
                    "as_of_date": as_of_date,
                    "feature_name": "cpi_headline_index_mom_change",
                    "feature_value": round(float(mom_value), 10),
                    "feature_group": FEATURE_GROUP,
                }
            )

        if pd.notna(yoy_value):
            rows.append(
                {
                    "as_of_date": as_of_date,
                    "feature_name": "cpi_headline_index_yoy_change",
                    "feature_value": round(float(yoy_value), 10),
                    "feature_group": FEATURE_GROUP,
                }
            )

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
    print(f"CPI_BREAK_DATE = {CPI_BREAK_DATE.date()}")

    raw_df = fetch_raw_cpi_series()
    wide_df = reshape_cpi_data(raw_df)
    feature_df = compute_features(wide_df)

    print("CPI feature preview:")

    preview_cols = [
        col
        for col in [
            "period_date",
            "CPI_HEADLINE_INDEX_OLD",
            "CPI_HEADLINE_INFLATION_OLD",
            "CPI_HEADLINE_INDEX_NEW",
            "CPI_HEADLINE_INFLATION_NEW",
            "cpi_headline_index_mom_change",
            "cpi_headline_index_yoy_change",
        ]
        if col in feature_df.columns
    ]

    print(feature_df[preview_cols].tail(18))

    feature_rows = build_feature_rows(feature_df)
    upsert_macro_features(feature_rows)


if __name__ == "__main__":
    main()