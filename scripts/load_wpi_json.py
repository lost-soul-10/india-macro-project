import json
import os
from datetime import datetime
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# load JSON file
with open("data/wpi_2024.json") as f:
    data = json.load(f)

rows = []

for r in data["data"]:

    if r["majorgroup"] == "Wholesale Price Index":

        period_date = datetime.strptime(
            f"{r['month']} {r['year']}",
            "%B %Y"
        ).date()

        rows.append({
            "series_name": "WPI",
            "source": "mospi",
            "period_date": str(period_date),
            "release_date": None,
            "value": float(r["index_value"]),
            "unit": "index",
            "frequency": "monthly"
        })

result = supabase.table("raw_macro_series").upsert(
    rows,
    on_conflict="series_name,period_date"
).execute()

print("Upserted rows:", len(rows))