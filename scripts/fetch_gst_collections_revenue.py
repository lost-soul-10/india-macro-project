import os
import pandas as pd
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SECRET_KEY = os.getenv("SUPABASE_SECRET_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_SECRET_KEY)

FILE_PATH = "data/gst_collection_revenues.xlsx"


def load_gst_collections():
    df = pd.read_excel(FILE_PATH)

    # keep only the columns you actually have
    df = df[["period_date", "series_name", "value"]].copy()

    # parse date
    df["period_date"] = pd.to_datetime(df["period_date"], errors="coerce")

    # numeric value
    df["value"] = pd.to_numeric(df["value"], errors="coerce")

    # drop bad rows
    df = df.dropna(subset=["period_date", "value"])

    # optional year filter
    df = df[
        (df["period_date"].dt.year >= 2022) &
        (df["period_date"].dt.year <= 2026)
    ]

    df = df.sort_values("period_date").reset_index(drop=True)

    print("Rows loaded:", len(df))
    print(df.head())
    print(df.tail())

    return df


def build_rows(df):
    rows = []

    for _, r in df.iterrows():
        rows.append({
            "series_name": "GST_COLLECTIONS",
            "source": "manual_excel",
            "period_date": str(r["period_date"].date()),
            "release_date": None,
            "value": float(r["value"]),
            "unit": "inr_crore",
            "frequency": "monthly"
        })

    return rows


def upsert_rows(rows):
    if not rows:
        print("No GST rows found")
        return

    result = supabase.table("raw_macro_series").upsert(
        rows,
        on_conflict="series_name,period_date"
    ).execute()

    print("Inserted/updated rows:", len(rows))
    print(result)


def main():
    df = load_gst_collections()
    rows = build_rows(df)
    upsert_rows(rows)


if __name__ == "__main__":
    main()