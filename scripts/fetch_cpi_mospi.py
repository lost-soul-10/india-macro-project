import os
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import requests
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SECRET_KEY = os.getenv("SUPABASE_SECRET_KEY")
MOSPI_EMAIL = os.getenv("MOSPI_EMAIL")
MOSPI_PASSWORD = os.getenv("MOSPI_PASSWORD")

if not SUPABASE_URL or not SUPABASE_SECRET_KEY:
    raise ValueError("Missing SUPABASE_URL or SUPABASE_SECRET_KEY in .env")

if not MOSPI_EMAIL or not MOSPI_PASSWORD:
    raise ValueError("Missing MOSPI_EMAIL or MOSPI_PASSWORD in .env")

supabase = create_client(SUPABASE_URL, SUPABASE_SECRET_KEY)

LOGIN_URL = "https://api.mospi.gov.in/api/users/login"
CPI_2012_URL = "https://api.mospi.gov.in/api/cpi/getCPIIndex"

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


def parse_period_date(year: int, month_name: str) -> str:
    dt = datetime.strptime(f"{month_name.strip()} {year}", "%B %Y").date()
    return str(dt.replace(day=1))


def login() -> str:
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
    if not token:
        raise ValueError(f"Token not found: {data}")

    return token


def get_with_retry(url: str, token: str, params: Dict[str, Any]) -> requests.Response:
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
            raise RuntimeError("Unauthorized / token expired. Re-run the script.")

        response.raise_for_status()
        return response

    raise RuntimeError(f"Too many retries for params={params}")


def fetch_2012_month(token: str, year: int, month_code: int) -> Optional[Dict[str, Any]]:
    """
    Validated 2012-base codes:
    state_code   = 99    -> All India
    sector_code  = 3     -> Combined
    group_code   = 0     -> General
    subgroup_code= 0.99  -> General-Overall
    """
    params = {
        "base_year": "2012",
        "series": "Current",
        "year": str(year),
        "month_code": str(month_code),
        "state_code": "99",
        "group_code": "0",
        "subgroup_code": "0.99",
        "sector_code": "3",
        "Format": "JSON",
        "page": 1,
    }

    response = get_with_retry(CPI_2012_URL, token, params)
    payload = response.json()
    rows = payload.get("data", [])

    if not rows:
        print(f"No 2012-base row for {year}-{month_code:02d}")
        return None

    return rows[0]


def build_rows_from_api(
    row: Dict[str, Any],
    add_inflation: bool = True,
) -> List[Dict[str, Any]]:
    year = int(row["year"])
    month = str(row["month"]).strip()
    period_date = parse_period_date(year, month)

    index_value = safe_float(row.get("index"))
    inflation_value = safe_float(row.get("inflation"))

    out: List[Dict[str, Any]] = []

    if index_value is not None:
        out.append({
            "series_name": "CPI_HEADLINE_INDEX",
            "source": "mospi",
            "period_date": period_date,
            "release_date": None,
            "value": index_value,
            "unit": "index",
            "frequency": "monthly",
        })

    if add_inflation and inflation_value is not None:
        out.append({
            "series_name": "CPI_HEADLINE_INFLATION",
            "source": "mospi",
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
    if not rows:
        print("No CPI rows found to upsert.")
        return

    final_rows = dedupe_rows(rows)

    result = supabase.table("raw_macro_series").upsert(
        final_rows,
        on_conflict="series_name,period_date"
    ).execute()

    print(f"Inserted/updated {len(final_rows)} rows")
    print(result)


def main() -> None:
    token = login()
    all_rows: List[Dict[str, Any]] = []

    # Fetch monthly CPI from 2012-base only
    for year in [2022, 2023, 2024, 2025, 2026]:
        for month_code in range(1, 13):
            print(f"Fetching 2012-base CPI {year}-{month_code:02d}")
            row = fetch_2012_month(token, year, month_code)

            if row:
                all_rows.extend(build_rows_from_api(row, add_inflation=True))

            time.sleep(0.4)

    upsert(all_rows)


if __name__ == "__main__":
    main()
