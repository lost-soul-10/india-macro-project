import os
import pandas as pd
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SECRET_KEY = os.getenv("SUPABASE_SECRET_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_SECRET_KEY)

# Fetch IIP index data
result = (
    supabase.table("raw_macro_series")
    .select("series_name, period_date, value")
    .eq("series_name", "IIP_INDEX")
    .order("period_date")
    .execute()
)

df = pd.DataFrame(result.data)

if df.empty:
    print("No IIP data found in raw_macro_series")
    raise SystemExit

df["period_date"] = pd.to_datetime(df["period_date"])
df["value"] = pd.to_numeric(df["value"])

df = df.sort_values("period_date").reset_index(drop=True)

# Build features
df["iip_mom_change"] = df["value"].pct_change() * 100
df["iip_yoy_change"] = df["value"].pct_change(12) * 100
df["iip_3m_avg"] = df["value"].rolling(3).mean()

rows = []

for _, r in df.iterrows():
    as_of_date = r["period_date"].date()

    feature_map = {
        "iip_index": round(r["value"], 4) if pd.notnull(r["value"]) else None,
        "iip_mom_change": round(r["iip_mom_change"], 4) if pd.notnull(r["iip_mom_change"]) else None,
        "iip_yoy_change": round(r["iip_yoy_change"], 4) if pd.notnull(r["iip_yoy_change"]) else None,
        "iip_3m_avg": round(r["iip_3m_avg"], 4) if pd.notnull(r["iip_3m_avg"]) else None,
    }

    for feature_name, feature_value in feature_map.items():
        if pd.notnull(feature_value):
            rows.append({
                "as_of_date": str(as_of_date),
                "feature_name": feature_name,
                "feature_value": float(feature_value),
                "feature_group": "industrial"
            })

result = supabase.table("macro_features").upsert(
    rows,
    on_conflict="as_of_date,feature_name"
).execute()

print("Upserted feature rows:", len(rows))
print(result)
