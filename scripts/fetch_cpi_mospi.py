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

# 2012-base endpoint
CPI_2012_URL = "https://api.mospi.gov.in/api/cpi/getCPIIndex"

# 2024-base endpoint
CPI_2024_URL = "https://api.mospi.gov.in/api/cpi/getCPIData"

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
    2012-base headline CPI:
    - state_code   = 99 -> All India
    - sector_code  = 3  -> Combined
    - group_code   = 0  -> General
    - subgroup_code= 0.99 -> General-Overall
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
        "page": 1,
        "Format": "JSON",
    }

    response = get_with_retry(CPI_2012_URL, token, params)
    payload = response.json()
    rows = payload.get("data", [])

    if not rows:
        print(f"No 2012-base CPI row for {year}-{month_code:02d}")
        return None

    # Usually this query returns the exact row we want, but we still filter defensively
    for row in rows:
        if (
            str(row.get("state", "")).strip() == "All India"
            and str(row.get("sector", "")).strip() == "Combined"
            and str(row.get("group", "")).strip() == "General"
            and str(row.get("subgroup", "")).strip() == "General-Overall"
        ):
            return row

    # fallback
    return rows[0]


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
    params = {
        "base_year": "2024",
        "year": str(year),
        "month_code": str(month_code),
        "state_code": "1",   # All India
        "sector_code": "3",  # Combined
        "limit": "1000",
        "page": "1",
    }

    response = get_with_retry(CPI_2024_URL, token, params)
    payload = response.json()
    rows = payload.get("data", [])

    if not rows:
        print(f"No 2024-base CPI data for {year}-{month_code:02d}")
        return None

    headline_row = pick_2024_headline_row(rows)

    if headline_row is None:
        print(f"No 2024-base headline CPI row found for {year}-{month_code:02d}")
        return None

    return headline_row


def build_rows_from_2012(row: Dict[str, Any]) -> List[Dict[str, Any]]:
    year = int(row["year"])
    month = str(row["month"]).strip()
    period_date = parse_period_date(year, month)

    index_value = safe_float(row.get("index"))
    inflation_value = safe_float(row.get("inflation"))

    out: List[Dict[str, Any]] = []

    if index_value is not None:
        out.append({
            "series_name": "CPI_HEADLINE_INDEX",
            "source": "mospi_2012",
            "period_date": period_date,
            "release_date": None,
            "value": index_value,
            "unit": "index",
            "frequency": "monthly",
        })

    if inflation_value is not None:
        out.append({
            "series_name": "CPI_HEADLINE_INFLATION",
            "source": "mospi_2012",
            "period_date": period_date,
            "release_date": None,
            "value": inflation_value,
            "unit": "percent",
            "frequency": "monthly",
        })

    return out


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

    # 2012 base -> use for 2022 to 2025
    for year in [2022, 2023, 2024, 2025]:
        for month_code in range(1, 13):
            print(f"Fetching 2012-base CPI {year}-{month_code:02d}")
            row = fetch_2012_month(token, year, month_code)

            if row:
                all_rows.extend(build_rows_from_2012(row))

            time.sleep(0.4)

    # 2024 base -> use for 2026
    for month_code in range(1, 13):
        print(f"Fetching 2024-base CPI 2026-{month_code:02d}")
        row = fetch_2024_month(token, 2026, month_code)

        if row:
            all_rows.extend(build_rows_from_2024(row))

        time.sleep(0.4)

    print("Total CPI rows prepared:", len(all_rows))
    upsert(all_rows)


if __name__ == "__main__":
    main()