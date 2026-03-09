import os
from datetime import datetime
import requests
from dotenv import load_dotenv
from supabase import create_client
from dateutil.relativedelta import relativedelta

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
MOSPI_EMAIL = os.getenv("MOSPI_EMAIL")
MOSPI_PASSWORD = os.getenv("MOSPI_PASSWORD")

if not all([SUPABASE_URL, SUPABASE_KEY, MOSPI_EMAIL, MOSPI_PASSWORD]):
    raise ValueError("Missing one or more environment variables in .env")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

LOGIN_URL = "https://api.mospi.gov.in/api/users/login"
WPI_URL = "https://api.mospi.gov.in/api/wpi/getWpiRecords"


def login_and_get_token() -> str:
    payload = {
        "username": MOSPI_EMAIL,
        "password": MOSPI_PASSWORD,
        "organization": "None",
        "purpose": "View/Download the Data",
        "gender": "Female"
    }

    headers = {
        "Content-Type": "application/json",
        "accept": "*/*"
    }

    response = requests.post(LOGIN_URL, json=payload, headers=headers, timeout=30)
    response.raise_for_status()

    data = response.json()

    if not data.get("statusCode"):
        raise ValueError(f"Login failed: {data}")

    token = data.get("response")
    if not token or not isinstance(token, str):
        raise ValueError(f"Token not found in response: {data}")

    return token


def fetch_wpi_month(token: str, year: int, month_code: int) -> list[dict]:
    headers = {
        "Authorization": token,
        "accept": "*/*"
    }

    params = {
        "year": str(year),
        "month_code": str(month_code),
        "Format": "JSON"
    }

    response = requests.get(WPI_URL, headers=headers, params=params, timeout=60)
    response.raise_for_status()

    data = response.json()
    records = data.get("data", [])

    if not isinstance(records, list):
        raise ValueError(f"Unexpected API response for {year}-{month_code}: {data}")

    return records


def transform_records(records: list[dict]) -> list[dict]:
    rows = []

    for r in records:
        major_group = (r.get("majorgroup") or "").strip()

        if major_group != "Wholesale Price Index":
            continue

        period_date = datetime.strptime(
            f"{r['month']} {int(r['year'])}",
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

    return rows


def get_latest_stored_month():
    result = (
        supabase.table("raw_macro_series")
        .select("period_date")
        .eq("series_name", "WPI")
        .order("period_date", desc=True)
        .limit(1)
        .execute()
    )

    if not result.data:
        return None

    return datetime.strptime(result.data[0]["period_date"], "%Y-%m-%d")


def upsert_rows(rows: list[dict]) -> None:
    if not rows:
        print("No WPI rows found to upsert.")
        return

    # Deduplicate before sending to Supabase
    deduped = {
        (row["series_name"], row["period_date"]): row
        for row in rows
    }

    final_rows = list(deduped.values())

    result = supabase.table("raw_macro_series").upsert(
        final_rows,
        on_conflict="series_name,period_date"
    ).execute()

    print(f"Upserted {len(final_rows)} rows")
    print(result)


def main():
    token = login_and_get_token()

    latest = get_latest_stored_month()

    if latest is None:
        # Initial historical backfill if table is empty
        start_date = datetime(2022, 1, 1)
        print("No WPI data found in Supabase. Starting historical backfill from 2022-01.")
    else:
        # Start from the month after the latest stored one
        start_date = latest + relativedelta(months=1)
        print(f"Latest stored WPI month: {latest.strftime('%Y-%m-%d')}")
        print(f"Fetching from: {start_date.strftime('%Y-%m')}")

    today = datetime.today()
    current = datetime(start_date.year, start_date.month, 1)

    all_rows = []

    while current <= today:
        year = current.year
        month_code = current.month

        print(f"Fetching WPI for {year}-{month_code:02d}")
        records = fetch_wpi_month(token, year, month_code)
        rows = transform_records(records)
        all_rows.extend(rows)

        current += relativedelta(months=1)

    upsert_rows(all_rows)


if __name__ == "__main__":
    main()