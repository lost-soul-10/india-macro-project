import os
import pandas as pd
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# Fetch WPI data from raw table
result = (
    supabase.table("raw_macro_series")
    .select("series_name, period_date, value")
    .eq("series_name", "WPI")
    .order("period_date")
    .execute()
)

df = pd.DataFrame(result.data)

if df.empty:
    print("No WPI data found in raw_macro_series")
    raise SystemExit

df["period_date"] = pd.to_datetime(df["period_date"])
df["value"] = pd.to_numeric(df["value"])

# Sort just in case
df = df.sort_values("period_date").reset_index(drop=True)

# Build features
df["wpi_mom_change"] = df["value"].pct_change()
df["wpi_yoy_change"] = df["value"].pct_change(12)
df["wpi_3m_avg"] = df["value"].rolling(3).mean()

rows = []

for _, r in df.iterrows():
    as_of_date = r["period_date"].date()

    feature_map = {
        "wpi_index": r["value"],
        "wpi_mom_change": r["wpi_mom_change"],
        "wpi_yoy_change": r["wpi_yoy_change"],
        "wpi_3m_avg": r["wpi_3m_avg"],
    }

    for feature_name, feature_value in feature_map.items():
        if pd.notnull(feature_value):
            rows.append({
                "as_of_date": str(as_of_date),
                "feature_name": feature_name,
                "feature_value": float(feature_value),
                "feature_group": "inflation"
            })

result = supabase.table("macro_features").upsert(
    rows,
    on_conflict="as_of_date,feature_name"
).execute()

print("Upserted feature rows:", len(rows))
print(result)