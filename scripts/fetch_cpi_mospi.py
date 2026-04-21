import os
import time
from datetime import datetime
from typing import Any, Optional

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
<<<<<<< HEAD
CPI_URL = "https://api.mospi.gov.in/api/cpi/getCPIData"
=======

# 2024-base endpoint
CPI_2024_URL = "https://api.mospi.gov.in/api/cpi/getCPIData"
>>>>>>> a5c2fd7 (Refactor CPI fetching logic by removing 2012-base endpoint and related functions, focusing on 2024-base CPI data retrieval.)

session = requests.Session()


def safe_float(value: Any) -> Optional[float]:
    if value is None:
        return None

    s = str(value).strip()
    if s == "" or s.lower() == "null":
        return None

    try:
        return float(s)
    except ValueError:
        return None


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


<<<<<<< HEAD
def fetch_cpi_month(token: str, year: int, month_code: int) -> list[dict]:
=======
def pick_2024_headline_row(rows: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """
    For 2024-base API, keep only the headline row:
    - All India
    - Combined
    - division = CPI (General)
    - no group/class/sub_class/item breakdown
    """
    for row in rows:
        if (
            str(row.get("state", "")).strip() == "All India"
            and str(row.get("sector", "")).strip() == "Combined"
            and str(row.get("division", "")).strip() == "CPI (General)"
            and row.get("group") is None
            and row.get("class") is None
            and row.get("sub_class") is None
            and row.get("item") is None
        ):
            return row

    return None


def fetch_2024_month(token: str, year: int, month_code: int) -> Optional[Dict[str, Any]]:
>>>>>>> a5c2fd7 (Refactor CPI fetching logic by removing 2012-base endpoint and related functions, focusing on 2024-base CPI data retrieval.)
    params = {
        "base_year": "2024",
        "year": str(year),
        "month_code": str(month_code),
        "level": "subgroup",
        "state_code": "1",
        "sector_code": "3",
        "division": "0",
        "limit": "100",
        "page": "1",
    }

    response = get_with_retry(CPI_URL, token, params)
    data = response.json()
    records = data.get("data", [])

    if not isinstance(records, list):
        raise ValueError(f"Unexpected API response for {year}-{month_code}: {data}")

    return records


def transform_records(records: list[dict]) -> list[dict]:
    rows = []

    for r in records:
        state = (r.get("state") or "").strip()
        sector = (r.get("sector") or "").strip()

        if state != "All India" or sector != "Combined":
            continue

        period_date = datetime.strptime(
            f"{r['month']} {int(r['year'])}",
            "%B %Y"
        ).date()

        index_value = safe_float(r.get("index"))
        inflation_value = safe_float(r.get("inflation"))

        if index_value is not None:
            rows.append({
                "series_name": "CPI_HEADLINE_INDEX",
                "source": "mospi",
                "period_date": str(period_date),
                "release_date": None,
                "value": index_value,
                "unit": "index",
                "frequency": "monthly"
            })

        if inflation_value is not None:
            rows.append({
                "series_name": "CPI_HEADLINE_INFLATION",
                "source": "mospi",
                "period_date": str(period_date),
                "release_date": None,
                "value": inflation_value,
                "unit": "percent",
                "frequency": "monthly"
            })

        break

    return rows


def get_latest_stored_month():
    result = (
        supabase.table("raw_macro_series")
        .select("period_date")
        .eq("series_name", "CPI_HEADLINE_INDEX")
        .order("period_date", desc=True)
        .limit(1)
        .execute()
    )

    if not result.data:
        return None

    return datetime.strptime(result.data[0]["period_date"], "%Y-%m-%d")


<<<<<<< HEAD
def upsert_rows(rows: list[dict]) -> None:
=======
def build_rows_from_2024(row: Dict[str, Any]) -> List[Dict[str, Any]]:
    year = int(row["year"])
    month = str(row["month"]).strip()
    period_date = parse_period_date(year, month)

    index_value = safe_float(row.get("index"))
    inflation_value = safe_float(row.get("inflation"))

    out: List[Dict[str, Any]] = []

    if index_value is not None:
        out.append({
            "series_name": "CPI_HEADLINE_INDEX",
            "source": "mospi_2024",
            "period_date": period_date,
            "release_date": None,
            "value": index_value,
            "unit": "index",
            "frequency": "monthly",
        })

    if inflation_value is not None:
        out.append({
            "series_name": "CPI_HEADLINE_INFLATION",
            "source": "mospi_2024",
            "period_date": period_date,
            "release_date": None,
            "value": inflation_value,
            "unit": "percent",
            "frequency": "monthly",
        })

    return out


def dedupe_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    deduped: Dict[Tuple[str, str], Dict[str, Any]] = {
        (row["series_name"], row["period_date"]): row
        for row in rows
    }
    return list(deduped.values())


def upsert(rows: List[Dict[str, Any]]) -> None:
>>>>>>> a5c2fd7 (Refactor CPI fetching logic by removing 2012-base endpoint and related functions, focusing on 2024-base CPI data retrieval.)
    if not rows:
        print("No CPI rows found to upsert.")
        return

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
    print("OPENSSL_CONF =", os.getenv("OPENSSL_CONF"))

<<<<<<< HEAD
    token = login_and_get_token()

    latest = get_latest_stored_month()

    if latest is None:
        start_date = datetime(2025, 1, 1)
        print("No CPI data found in Supabase. Starting historical backfill from 2025-01.")
    else:
        start_date = latest + relativedelta(months=1)
        print(f"Latest stored CPI month: {latest.strftime('%Y-%m-%d')}")
        print(f"Fetching from: {start_date.strftime('%Y-%m')}")

    today = datetime.today()
    current = datetime(start_date.year, start_date.month, 1)
=======
    # 2024 base -> use for 2026
    for month_code in range(1, 13):
        print(f"Fetching 2024-base CPI 2026-{month_code:02d}")
        row = fetch_2024_month(token, 2026, month_code)
>>>>>>> a5c2fd7 (Refactor CPI fetching logic by removing 2012-base endpoint and related functions, focusing on 2024-base CPI data retrieval.)

    all_rows = []

    while current <= today:
        year = current.year
        month_code = current.month

        print(f"Fetching CPI for {year}-{month_code:02d}")
        records = fetch_cpi_month(token, year, month_code)
        rows = transform_records(records)
        all_rows.extend(rows)

        current += relativedelta(months=1)
        time.sleep(0.4)

    upsert_rows(all_rows)


if __name__ == "__main__":
    main()
