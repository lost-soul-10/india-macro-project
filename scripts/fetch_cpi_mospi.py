import os
import ssl
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import requests
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from supabase import create_client
from urllib3.poolmanager import PoolManager

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
CPI_2024_URL = "https://api.mospi.gov.in/api/cpi/getCPIData"


class LegacyTLSAdapter(HTTPAdapter):
    def init_poolmanager(self, connections, maxsize, block=False, **pool_kwargs):
        ctx = ssl.create_default_context()

        if hasattr(ssl, "OP_LEGACY_SERVER_CONNECT"):
            ctx.options |= ssl.OP_LEGACY_SERVER_CONNECT
        else:
            raise RuntimeError(
                "This Python/OpenSSL build does not support OP_LEGACY_SERVER_CONNECT"
            )

        self.poolmanager = PoolManager(
            num_pools=connections,
            maxsize=maxsize,
            block=block,
            ssl_context=ctx,
            **pool_kwargs,
        )


def build_session() -> requests.Session:
    s = requests.Session()
    s.mount("https://", LegacyTLSAdapter())
    return s


# IMPORTANT: create the patched session only once, and do NOT overwrite it later
session = build_session()


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
    print("Login response received.")

    if not data.get("statusCode"):
        raise ValueError(f"Login failed: {data}")

    token = data.get("response")
    if not token:
        raise ValueError(f"Token not found in login response: {data}")

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

        response.raise_for_status()
        return response

    raise RuntimeError(f"Too many retries for params={params}")


def is_blank(value: Any) -> bool:
    return value is None or str(value).strip() == ""


def pick_2024_headline_row(rows: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """
    For 2024-base API, try to keep only the headline row:
    - All India
    - Combined
    - division = CPI (General)
    - no lower-level breakdown fields populated
    """

    for row in rows:
        state = str(row.get("state", "")).strip().lower()
        sector = str(row.get("sector", "")).strip().lower()
        division = str(row.get("division", "")).strip().lower()

        if (
            state in {"all india", "all-india"}
            and sector == "combined"
            and division == "cpi (general)"
            and is_blank(row.get("group"))
            and is_blank(row.get("class"))
            and is_blank(row.get("sub_class"))
            and is_blank(row.get("item"))
        ):
            return row

    # fallback: if the exact top-line row shape changes, still try the most likely match
    for row in rows:
        state = str(row.get("state", "")).strip().lower()
        sector = str(row.get("sector", "")).strip().lower()
        division = str(row.get("division", "")).strip().lower()

        if (
            state in {"all india", "all-india"}
            and sector == "combined"
            and division == "cpi (general)"
        ):
            return row

    return None


def fetch_2024_month(token: str, year: int, month_code: int) -> Optional[Dict[str, Any]]:
    params = {
        "base_year": "2024",
        "year": str(year),
        "month_code": str(month_code),
        "state_code": "1",   # verify after first successful response
        "sector_code": "3",  # Combined
        "limit": "1000",
        "page": "1",
    }

    response = get_with_retry(CPI_2024_URL, token, params)
    payload = response.json()
    rows = payload.get("data", [])

    if not rows:
        print(f"No 2024-base CPI data for {year}-{month_code:02d}")
        print("Payload keys:", list(payload.keys()))
        print("Payload preview:", str(payload)[:1000])
        return None

    headline_row = pick_2024_headline_row(rows)

    if headline_row is None:
        print(f"No 2024-base headline CPI row found for {year}-{month_code:02d}")
        print("First 3 rows preview:")
        for i, row in enumerate(rows[:3], start=1):
            print(f"Row {i}: {row}")
        return None

    return headline_row


def build_rows_from_2024(row: Dict[str, Any]) -> List[Dict[str, Any]]:
    year = int(row["year"])
    month = str(row["month"]).strip()
    period_date = parse_period_date(year, month)

    index_value = safe_float(row.get("index"))
    inflation_value = safe_float(row.get("inflation"))

    out: List[Dict[str, Any]] = []

    if index_value is not None:
        out.append(
            {
                "series_name": "CPI_HEADLINE_INDEX",
                "source": "mospi_2024",
                "period_date": period_date,
                "release_date": None,
                "value": index_value,
                "unit": "index",
                "frequency": "monthly",
            }
        )

    if inflation_value is not None:
        out.append(
            {
                "series_name": "CPI_HEADLINE_INFLATION",
                "source": "mospi_2024",
                "period_date": period_date,
                "release_date": None,
                "value": inflation_value,
                "unit": "percent",
                "frequency": "monthly",
            }
        )

    return out


def dedupe_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    deduped: Dict[Tuple[str, str], Dict[str, Any]] = {
        (row["series_name"], row["period_date"]): row for row in rows
    }
    return list(deduped.values())


def upsert(rows: List[Dict[str, Any]]) -> None:
    if not rows:
        print("No CPI rows found to upsert.")
        return

    final_rows = dedupe_rows(rows)

    result = (
        supabase.table("raw_macro_series")
        .upsert(final_rows, on_conflict="series_name,period_date")
        .execute()
    )

    print(f"Inserted/updated {len(final_rows)} rows")
    print(result)


def main() -> None:
    print("Logging into MOSPI...")
    token = login()
    print("MOSPI login successful.")

    all_rows: List[Dict[str, Any]] = []

    for month_code in range(1, 13):
        print(f"Fetching 2024-base CPI 2026-{month_code:02d}")
        row = fetch_2024_month(token, 2026, month_code)

        if row:
            built = build_rows_from_2024(row)
            all_rows.extend(built)
            print(f"Prepared {len(built)} rows for 2026-{month_code:02d}")

        time.sleep(0.4)

    print("Total CPI rows prepared:", len(all_rows))
    upsert(all_rows)


if __name__ == "__main__":
    main()
