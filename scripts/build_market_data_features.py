import os
import pandas as pd
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SECRET_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)


def fetch_series(series_name):

    result = (
        supabase.table("raw_macro_series")
        .select("series_name, period_date, value")
        .eq("series_name", series_name)
        .order("period_date")
        .execute()
    )

    df = pd.DataFrame(result.data)

    if df.empty:
        print(f"No data found for {series_name}")
        return None

    df["period_date"] = pd.to_datetime(df["period_date"])
    df["value"] = pd.to_numeric(df["value"])

    df = df.sort_values("period_date").reset_index(drop=True)

    return df


def build_features():

    rows = []

    series_map = {
        "INDIA_10Y_YIELD": "financial_conditions",
        "USD_INR": "external",
        "CRUDE_OIL_WTI": "external"
    }

    for series_name, group in series_map.items():

        df = fetch_series(series_name)

        if df is None:
            continue

        if series_name == "INDIA_10Y_YIELD":

            df["bond_yield_10y"] = df["value"]
            df["bond_yield_change"] = df["value"].diff()
            df["bond_yield_3m_avg"] = df["value"].rolling(3).mean()

            feature_cols = [
                "bond_yield_10y",
                "bond_yield_change",
                "bond_yield_3m_avg"
            ]

        elif series_name == "USD_INR":

            df["usd_inr"] = df["value"]
            df["usd_inr_mom_change"] = df["value"].pct_change() * 100
            df["usd_inr_3m_change"] = df["value"].pct_change(3) * 100

            feature_cols = [
                "usd_inr",
                "usd_inr_mom_change",
                "usd_inr_3m_change"
            ]

        elif series_name == "CRUDE_OIL_WTI":

            df["oil_price"] = df["value"]
            df["oil_mom_change"] = df["value"].pct_change() * 100
            df["oil_yoy_change"] = df["value"].pct_change(12) * 100
            df["oil_3m_avg"] = df["value"].rolling(3).mean()

            feature_cols = [
                "oil_price",
                "oil_mom_change",
                "oil_yoy_change",
                "oil_3m_avg"
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
                        "feature_group": group
                    })

    return rows


def main():

    rows = build_features()

    if not rows:
        print("No features generated")
        return

    result = supabase.table("macro_features").upsert(
        rows,
        on_conflict="as_of_date,feature_name"
    ).execute()

    print("Upserted market feature rows:", len(rows))
    print(result)


if __name__ == "__main__":
    main()