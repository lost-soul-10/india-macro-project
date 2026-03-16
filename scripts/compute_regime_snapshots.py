import os
import pandas as pd
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SECRET_KEY = os.getenv("SUPABASE_SECRET_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_SECRET_KEY)

# -------------------------------------------
# Load macro feature table
# -------------------------------------------

result = (
    supabase.table("macro_features")
    .select("as_of_date,feature_name,feature_value")
    .execute()
)

df = pd.DataFrame(result.data)

if df.empty:
    print("macro_features table is empty")
    raise SystemExit

df["as_of_date"] = pd.to_datetime(df["as_of_date"])
df["feature_value"] = pd.to_numeric(df["feature_value"])

# -------------------------------------------
# Pivot features
# -------------------------------------------

df = df.pivot_table(
    index="as_of_date",
    columns="feature_name",
    values="feature_value"
)

df = df.sort_index()

# Forward fill slower data (GDP / repo etc.)
df = df.ffill()

print("Available columns:")
print(df.columns)

# -------------------------------------------
# Compute macro scores
# -------------------------------------------

if "cpi_headline_index_yoy_change" in df.columns:

    df["inflation_score"] = (
        df["cpi_headline_index_yoy_change"]
        - df["cpi_headline_index_yoy_change"].rolling(3).mean()
    )

if "gdp_growth_real" in df.columns and "gdp_growth_4q_avg" in df.columns:

    df["growth_score"] = (
        df["gdp_growth_real"]
        - df["gdp_growth_4q_avg"]
    )

if "repo_rate" in df.columns and "cpi_headline_index_yoy_change" in df.columns:

    df["real_policy_rate"] = (
        df["repo_rate"]
        - df["cpi_headline_index_yoy_change"]
    )

    df["policy_score"] = -df["real_policy_rate"]

if "oil_mom_change" in df.columns and "usd_inr_mom_change" in df.columns:

    df["external_score"] = (
        -df["oil_mom_change"]
        -df["usd_inr_mom_change"]
    )

df = df.reset_index()

# -------------------------------------------
# Regime classification
# -------------------------------------------

def classify(row):

    g = row.get("growth_score")
    i = row.get("inflation_score")

    if pd.isna(g) or pd.isna(i):
        return None, None

    if g > 0 and i < 0:
        return "Goldilocks Expansion", "Growth accelerating while inflation cools"

    if g > 0 and i > 0:
        return "Overheating Economy", "Growth strong but inflation rising"

    if g < 0 and i > 0:
        return "Stagflation Risk", "Growth slowing while inflation remains high"

    if g < 0 and i < 0:
        return "Slowdown / Disinflation", "Growth slowing and inflation easing"

    return None, None


df[["regime_label", "explanation"]] = df.apply(
    lambda r: pd.Series(classify(r)),
    axis=1
)

# -------------------------------------------
# Build rows for insertion
# -------------------------------------------

rows = []

for _, r in df.iterrows():

    if pd.isna(r["regime_label"]):
        continue

    rows.append({
        "as_of_date": str(r["as_of_date"].date()),
        "growth_score": round(float(r.get("growth_score", 0)), 4),
        "inflation_score": round(float(r.get("inflation_score", 0)), 4),
        "policy_score": round(float(r.get("policy_score", 0)), 4),
        "external_score": round(float(r.get("external_score", 0)), 4),
        "regime_label": r["regime_label"],
        "explanation": r["explanation"]
    })

if len(rows) == 0:
    print("No regime rows generated. Check feature inputs.")
    raise SystemExit

print("Rows prepared:", len(rows))

# -------------------------------------------
# Insert into regime table
# -------------------------------------------

result = supabase.table("regime_snapshots").upsert(
    rows,
    on_conflict="as_of_date"
).execute()

print("Inserted regime snapshots:", len(rows))
