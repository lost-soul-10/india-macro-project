import os
import pandas as pd
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SECRET_KEY = os.getenv("SUPABASE_SECRET_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_SECRET_KEY)

result = (
    supabase.table("raw_macro_series")
    .select("series_name, period_date, value")
    .eq("series_name", "REPO_RATE")
    .order("period_date")
    .execute()
)

df = pd.DataFrame(result.data)

if df.empty:
    print("No repo rate data found")
    raise SystemExit

df["period_date"] = pd.to_datetime(df["period_date"])
df["value"] = pd.to_numeric(df["value"])

df = df.sort_values("period_date").reset_index(drop=True)

df["repo_rate"] = df["value"]
df["repo_rate_change"] = df["value"].diff()

rows = []

for _, r in df.iterrows():

    as_of_date = r["period_date"].date()

    feature_map = {
        "repo_rate": r["repo_rate"],
        "repo_rate_change": r["repo_rate_change"]
    }

    for feature_name, feature_value in feature_map.items():

        if pd.notnull(feature_value):

            rows.append({
                "as_of_date": str(as_of_date),
                "feature_name": feature_name,
                "feature_value": round(float(feature_value), 4),
                "feature_group": "policy"
            })

result = supabase.table("macro_features").upsert(
    rows,
    on_conflict="as_of_date,feature_name"
).execute()

print("Upserted repo feature rows:", len(rows))
