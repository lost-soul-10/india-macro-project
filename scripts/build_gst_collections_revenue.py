import os
import pandas as pd
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SECRET_KEY = os.getenv("SUPABASE_SECRET_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_SECRET_KEY)

FILE_PATH = "data/gst_collection_revenue.xlsx"


def load_gst_collections():
    # read Excel file
    df = pd.read_excel(FILE_PATH)

    # keep only first two columns
    df = df.iloc[:, :2]

    # rename columns
    df.columns = ["date", "value"]

    # parse dates
    df["date"] = pd.to_datetime(df["date"], errors="coerce")

    # remove bad date rows
    df = df.dropna(subset=["date"])

    # clean value column
    df["value"] = (
        df["value"]
        .astype(str)
        .str.replace(",", "", regex=False)
        .str.replace("₹", "", regex=False)
        .str.strip()
    )

    # replace blanks / dash values
    df["value"] = df["value"].replace(["-", "", "nan", "None"], pd.NA)

    # convert to numeric
    df["value"] = pd.to_numeric(df["value"], errors="coerce")

    # drop rows where value is missing
    df = df.dropna(subset=["value"])

    # keep only 2022–2026
    df = df[(df["date"].dt.year >= 2022) & (df["date"].dt.year <= 2026)]

    # sort
    df = df.sort_values("date").reset_index(drop=True)

    print("Rows loaded:", len(df))
    print(df)

    return df


def build_rows(df):
    rows = []

    for _, r in df.iterrows():
        rows.append({
            "series_name": "GST_COLLECTIONS",
            "source": "manual_excel",
            "period_date": str(r["date"].date()),
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