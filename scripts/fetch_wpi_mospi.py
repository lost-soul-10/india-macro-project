import os
import time
from datetime import datetime

import requests
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SECRET_KEY = os.getenv("SUPABASE_SECRET_KEY")
MOSPI_EMAIL = os.getenv("MOSPI_EMAIL")
MOSPI_PASSWORD = os.getenv("MOSPI_PASSWORD")

if not all([SUPABASE_URL, SUPABASE_SECRET_KEY, MOSPI_EMAIL, MOSPI_PASSWORD]):
    raise ValueError("Missing one or more environment variables in .env")

supabase = create_client(SUPABASE_URL, SUPABASE_SECRET_KEY)

LOGIN_URL = "https://api.mospi.gov.in/api/users/login"
WPI_URL = "https://api.mospi.gov.in/api/wpi/getWpiRecords"

session = requests.Session()


def login_and_get_token() -> str:
    payload = {
        "username": MOSPI_EMAIL,
        "password": MOSPI_PASSWORD,
        "organization": "None",
        "purpose": "View/Download the Data",
        "gender": "Female",
    }

    headers = {
        "Content-Type": "application/json",
        "accept": "*/*",
    }

    response = session.post(LOGIN_URL, json=payload, headers=headers, timeout=30)
    response.raise_for_status()

    data = response.json()

    if not data.get("statusCode"):
        raise ValueError(f"Login failed: {data}")

    token = data.get("response")
    if not token or not isinstance(token, str):
        raise ValueError(f"Token not found in response: {data}")

    return token


def get_with_retry(url: str, token: str, params: dict) -> requests.Response:
    headers = {
        "Authorization": token,
        "accept": "*/*",
    }

    for attempt in range(5):
        response = session.get(url, headers=headers, params=params, timeout=60)

        if response.status_code == 429:
            wait = min(2 ** attempt, 15)
            print(f"429 hit. Waiting {wait}s and retrying...")
            time.sleep(wait)
            continue

        if response.status_code == 401:
            raise RuntimeError("Unauthorized or token expired. Re-run the script.")

        if response.status_code >= 400:
            print("Request params:", params)
            print("Response text:", response.text)

        response.raise_for_status()
        return response

    raise RuntimeError(f"Too many retries for params={params}")


def fetch_wpi_month(token: str, year: int, month_code: int) -> list[dict]:
    params = {
        "year": str(year),
        "month_code": str(month_code),
        "Format": "JSON",
    }

    response = get_with_retry(WPI_URL, token, params)
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
            "%B %Y",
        ).date()

        rows.append({
            "series_name": "WPI",
            "source": "mospi",
            "period_date": str(period_date),
            "release_date": None,
            "value": float(r["index_value"]),
            "unit": "index",
            "frequency": "monthly",
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

    deduped = {
        (row["series_name"], row["period_date"]): row
        for row in rows
    }

    final_rows = list(deduped.values())

    result = supabase.table("raw_macro_series").upsert(
        final_rows,
        on_conflict="series_name,period_date",
    ).execute()

    print(f"Upserted {len(final_rows)} rows")
    print(result)


def main():
    print("OPENSSL_CONF =", os.getenv("OPENSSL_CONF"))

    token = login_and_get_token()
    latest = get_latest_stored_month()

    if latest is None:
        start_date = datetime(2022, 1, 1)
        print("No WPI data found in Supabase. Starting historical backfill from 2022-01.")
    else:
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
        time.sleep(0.4)

    upsert_rows(all_rows)


if __name__ == "__main__":
    main()