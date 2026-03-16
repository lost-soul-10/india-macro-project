import os
import pandas as pd
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SECRET_KEY = os.getenv("SUPABASE_SECRET_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_SECRET_KEY)

FILE_PATH = "data/repo_rate.xlsx"


def load_repo_rate():

    # header row 1: "Effective Date", "Repo"; data rows 2+
    df = pd.read_excel(FILE_PATH)

    # keep only first two columns
    df = df.iloc[:, :2]

    # rename columns
    df.columns = ["date", "value"]

    # let pandas parse dates automatically
    df["date"] = pd.to_datetime(df["date"], errors="coerce")

    # remove rows where date failed
    df = df.dropna(subset=["date"])

    # replace RBI dash values
    df["value"] = df["value"].replace("-", pd.NA)

    # convert repo rate to numeric
    df["value"] = pd.to_numeric(df["value"], errors="coerce")

    # forward fill repo rate
    df["value"] = df["value"].ffill()

    # keep only 2022–2026
    df = df[(df["date"].dt.year >= 2022) & (df["date"].dt.year <= 2026)]

    df = df.sort_values("date").reset_index(drop=True)

    print("Rows loaded:", len(df))
    print(df)

    return df

def build_rows(df):

    rows = []

    for _, r in df.iterrows():
        # skip rows where value is missing after cleaning/ffill
        if pd.isna(r["value"]):
            continue

        rows.append({
            "series_name": "REPO_RATE",
            "source": "rbi_dbie",
            "period_date": str(r["date"].date()),
            "release_date": None,
            "value": float(r["value"]),
            "unit": "percent",
            "frequency": "daily"
        })

    return rows


def upsert_rows(rows):

    if not rows:
        print("No repo rate rows found")
        return

    result = supabase.table("raw_macro_series").upsert(
        rows,
        on_conflict="series_name,period_date"
    ).execute()

    print("Inserted/updated rows:", len(rows))


def main():

    df = load_repo_rate()

    rows = build_rows(df)

    upsert_rows(rows)


if __name__ == "__main__":
    main()
