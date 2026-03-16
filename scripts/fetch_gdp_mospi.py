import os
import time
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
GDP_URL = "https://api.mospi.gov.in/api/nas/getNASData"

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


def fiscal_quarter_to_period_date(fy: str, quarter: str) -> str:
    """
    Example:
    2024-25 Q1 -> 2024-06-30
    2024-25 Q2 -> 2024-09-30
    2024-25 Q3 -> 2024-12-31
    2024-25 Q4 -> 2025-03-31
    """
    fy = fy.strip()
    start_year = int(fy.split("-")[0])
    end_part = fy.split("-")[1]
    end_year = int("20" + end_part) if len(end_part) == 2 else int(end_part)

    quarter = quarter.strip().upper()

    if quarter == "Q1":
        return f"{start_year}-06-30"
    if quarter == "Q2":
        return f"{start_year}-09-30"
    if quarter == "Q3":
        return f"{start_year}-12-31"
    if quarter == "Q4":
        return f"{end_year}-03-31"

    raise ValueError(f"Unknown quarter: {quarter}")


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


def fetch_all_pages(token: str) -> List[Dict[str, Any]]:
    all_rows: List[Dict[str, Any]] = []
    page = 1

    while True:
        params = {
            "base_year": "2011-12",
            "series": "Current",
            "frequency_code": "02",
            "indicator_code": "22",  # GDP Growth Rate
            "year": "2022-23,2023-24,2024-25,2025-26",
            "quarterly_code": "1,2,3,4",
            "Format": "JSON",
            "page": str(page),
        }

        response = get_with_retry(GDP_URL, token, params)
        payload = response.json()

        rows = payload.get("data", [])
        meta = payload.get("meta_data", {}) or {}

        if not isinstance(rows, list):
            raise ValueError(f"Unexpected response shape: {payload}")

        all_rows.extend(rows)

        total_pages = int(meta.get("totalPages", 1))
        print(f"Fetched page {page}/{total_pages}, rows={len(rows)}, total={len(all_rows)}")

        if page >= total_pages or not rows:
            break

        page += 1
        time.sleep(0.3)

    return all_rows


def filter_quarterly_gdp_growth(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out = []

    for row in rows:
        if (
            str(row.get("indicator", "")).strip() == "GDP Growth Rate"
            and str(row.get("frequency", "")).strip() == "Quarterly"
            and str(row.get("series", "")).strip() == "Current"
            and row.get("quarter") is not None
        ):
            out.append(row)

    return out


def build_rows_from_api(row: Dict[str, Any]) -> List[Dict[str, Any]]:
    fy = str(row["year"]).strip()
    quarter = str(row["quarter"]).strip()
    period_date = fiscal_quarter_to_period_date(fy, quarter)

    nominal = safe_float(row.get("current_price"))
    real = safe_float(row.get("constant_price"))

    out: List[Dict[str, Any]] = []

    if nominal is not None:
        out.append({
            "series_name": "GDP_GROWTH_NOMINAL_QUARTERLY",
            "source": "mospi",
            "period_date": period_date,
            "release_date": None,
            "value": nominal,
            "unit": "percent",
            "frequency": "quarterly",
        })

    if real is not None:
        out.append({
            "series_name": "GDP_GROWTH_REAL_QUARTERLY",
            "source": "mospi",
            "period_date": period_date,
            "release_date": None,
            "value": real,
            "unit": "percent",
            "frequency": "quarterly",
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
        print("No quarterly GDP rows found to upsert.")
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
    raw_rows = fetch_all_pages(token)
    quarterly_rows = filter_quarterly_gdp_growth(raw_rows)

    all_rows: List[Dict[str, Any]] = []
    for row in quarterly_rows:
        all_rows.extend(build_rows_from_api(row))

    upsert(all_rows)


if __name__ == "__main__":
    main()
