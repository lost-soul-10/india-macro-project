import os
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

import requests
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
IIP_URL = "https://api.mospi.gov.in/api/iip/getIIPMonthly"

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
        raise ValueError(f"MOSPI login failed: {data}")

    token = data.get("response")
    if not token or not isinstance(token, str):
        raise ValueError(f"Token not found in response: {data}")

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
            raise RuntimeError("Unauthorized or token expired. Re-run the script.")

        if response.status_code >= 400:
            print("Request params:", params)
            print("Response text:", response.text)

        response.raise_for_status()
        return response

    raise RuntimeError(f"Too many retries for params={params}")


def fetch_pages(token: str) -> List[Dict[str, Any]]:
    page = 1
    all_rows: List[Dict[str, Any]] = []

    while True:
        params = {
            "base_year": "2011-12",
            "year": "2022,2023,2024,2025,2026",
            "month_code": "1,2,3,4,5,6,7,8,9,10,11,12",
            "type": "All",
            "Format": "JSON",
            "limit": "100",
            "page": str(page),
        }

        response = get_with_retry(IIP_URL, token, params)
        payload = response.json()

        rows = payload.get("data", [])
        meta = payload.get("meta_data", {})

        if not isinstance(rows, list):
            raise ValueError(f"Unexpected IIP API response on page {page}: {payload}")

        all_rows.extend(rows)

        total_pages = int(meta.get("totalPages", 1))
        print(f"Fetched page {page}/{total_pages}")

        if page >= total_pages:
            break

        page += 1
        time.sleep(0.3)

    return all_rows


def filter_headline_iip(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out = []

    for r in rows:
        if (
            str(r.get("type", "")).strip() == "General"
            and str(r.get("category", "")).strip() == "General"
            and str(r.get("sub_category", "")).strip() == ""
        ):
            out.append(r)

    return out


def build_rows(row: Dict[str, Any]) -> List[Dict[str, Any]]:
    year = int(row["year"])
    month = str(row["month"]).strip()
    period_date = parse_period_date(year, month)

    index_val = safe_float(row.get("index"))
    growth_val = safe_float(row.get("growth_rate"))

    rows = []

    if index_val is not None:
        rows.append({
            "series_name": "IIP_INDEX",
            "source": "mospi",
            "period_date": period_date,
            "release_date": None,
            "value": index_val,
            "unit": "index",
            "frequency": "monthly",
        })

    if growth_val is not None:
        rows.append({
            "series_name": "IIP_GROWTH_RATE",
            "source": "mospi",
            "period_date": period_date,
            "release_date": None,
            "value": growth_val,
            "unit": "percent",
            "frequency": "monthly",
        })

    return rows


def dedupe(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    m = {(r["series_name"], r["period_date"]): r for r in rows}
    return list(m.values())


def upsert(rows: List[Dict[str, Any]]):
    if not rows:
        print("No IIP rows found to upsert.")
        return

    final_rows = dedupe(rows)

    res = supabase.table("raw_macro_series").upsert(
        final_rows,
        on_conflict="series_name,period_date",
    ).execute()

    print(f"Inserted {len(final_rows)} rows")
    print(res)


def main():
    print("OPENSSL_CONF =", os.getenv("OPENSSL_CONF"))

    token = login()
    raw_rows = fetch_pages(token)
    headline_rows = filter_headline_iip(raw_rows)

    all_rows: List[Dict[str, Any]] = []

    for r in headline_rows:
        all_rows.extend(build_rows(r))

    upsert(all_rows)


if __name__ == "__main__":
    main()