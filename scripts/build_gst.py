import os
import pandas as pd
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SECRET_KEY = os.getenv("SUPABASE_SECRET_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_SECRET_KEY)


def fetch_gst_series():
    result = (
        supabase.table("raw_macro_series")
        .select("series_name, period_date, value")
        .eq("series_name", "GST_COLLECTIONS")
        .order("period_date")
        .execute()
    )

    df = pd.DataFrame(result.data)

    if df.empty:
        print("No data found for GST_COLLECTIONS")
        return None

    df["period_date"] = pd.to_datetime(df["period_date"])
    df["value"] = pd.to_numeric(df["value"], errors="coerce")

    df = df.dropna(subset=["period_date", "value"])
    df = df.sort_values("period_date").reset_index(drop=True)

    print("GST raw rows loaded:", len(df))
    print(df.tail(12))

    return df


def build_features(df):
    rows = []

    df["gst_collections"] = df["value"]
    df["gst_mom_change"] = df["value"].pct_change() * 100
    df["gst_yoy_change"] = df["value"].pct_change(12) * 100
    df["gst_3m_avg"] = df["value"].rolling(3).mean()
    df["gst_3m_yoy_avg"] = df["gst_yoy_change"].rolling(3).mean()

    feature_cols = [
        "gst_collections",
        "gst_mom_change",
        "gst_yoy_change",
        "gst_3m_avg",
        "gst_3m_yoy_avg"
    ]

    for _, r in df.iterrows():
        as_of_date = r["period_date"].date()

        for feature_name in feature_cols:
            feature_value = r[feature_name]

            if pd.notnull(feature_value):
                rows.append({
                    "as_of_date": str(as_of_date),
                    "feature_name": feature_name,
                    "feature_value": round(float(feature_value), 4),
                    "feature_group": "growth"
                })

    return rows


def upsert_rows(rows):
    if not rows:
        print("No GST feature rows generated")
        return

    result = supabase.table("macro_features").upsert(
        rows,
        on_conflict="as_of_date,feature_name"
    ).execute()

    print("Inserted/updated GST feature rows:", len(rows))
    print(result)


def main():
    df = fetch_gst_series()

    if df is None:
        return

    rows = build_features(df)
    upsert_rows(rows)


if __name__ == "__main__":
    main()