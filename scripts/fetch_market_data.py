import os
import pandas as pd
from fredapi import Fred
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()

fred = Fred(api_key=os.getenv("FRED_API_KEY"))

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SECRET_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)


def fetch_series(series_id, name, unit="index"):

    try:
        data = fred.get_series(series_id)
    except Exception as e:
        print(f"Failed to fetch {series_id}: {e}")
        return []

    df = pd.DataFrame({
        "date": data.index,
        "value": data.values
    })

    df = df.dropna()

    df["date"] = pd.to_datetime(df["date"])

    # keep only 2022–2026
    df = df[(df["date"].dt.year >= 2022) & (df["date"].dt.year <= 2026)]

    rows = []

    for _, r in df.iterrows():
        rows.append({
            "series_name": name,
            "source": "fred",
            "period_date": str(r["date"].date()),
            "release_date": None,
            "value": float(r["value"]),
            "unit": unit,
            "frequency": "monthly"
        })

    return rows


def upsert(rows):

    if not rows:
        print("No rows to insert")
        return

    result = supabase.table("raw_macro_series").upsert(
        rows,
        on_conflict="series_name,period_date"
    ).execute()

    print("Inserted/updated rows:", len(rows))


def main():

    bond_rows = fetch_series("INDIRLTLT01STM", "INDIA_10Y_YIELD", "percent")
    print("Bond rows:", len(bond_rows))

    fx_rows = fetch_series("CCUSMA02INM618N", "USD_INR", "inr_per_usd")
    print("FX rows:", len(fx_rows))

    oil_rows = fetch_series("WTISPLC", "CRUDE_OIL_WTI", "usd_per_barrel")
    print("Oil rows:", len(oil_rows))

    upsert(bond_rows)
    upsert(fx_rows)
    upsert(oil_rows)


if __name__ == "__main__":
    main()