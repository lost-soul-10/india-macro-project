from __future__ import annotations

import calendar
import hashlib
import html as html_lib
import os
import re
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

import feedparser
import requests
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SECRET_KEY = os.getenv("SUPABASE_SECRET_KEY")

if not SUPABASE_URL or not SUPABASE_SECRET_KEY:
    raise ValueError("Missing SUPABASE_URL or SUPABASE_SECRET_KEY in .env")

supabase = create_client(SUPABASE_URL, SUPABASE_SECRET_KEY)

REQUEST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome Safari"
    ),
    "Accept": "application/rss+xml, application/xml;q=0.9, */*;q=0.8",
    "Accept-Language": "en-IN,en;q=0.9",
}

# -----------------------------------
# RSS feeds: India domestic + RBI official
# -----------------------------------
RSS_FEEDS = [
    # LiveMint — economy and policy sections
    ("LiveMint Economy",            "https://www.livemint.com/rss/economy"),
    ("LiveMint Politics & Policy",  "https://www.livemint.com/rss/politics"),
    ("LiveMint Opinion",            "https://www.livemint.com/rss/opinion"),

    # Economic Times — economy sub-sections
    ("ET Economy",                  "https://economictimes.indiatimes.com/news/economy/rssfeeds/1373380680.cms"),
    ("ET Economy Policy",           "https://economictimes.indiatimes.com/news/economy/policy/rssfeeds/1052732854.cms"),

    # RBI official feeds — primary source
    ("RBI Press Releases",          "https://www.rbi.org.in/pressreleases_rss.xml"),
    ("RBI Speeches",                "https://www.rbi.org.in/speeches_rss.xml"),
    ("RBI Publications",            "https://www.rbi.org.in/Publication_rss.xml"),
]

# -----------------------------------
# Macro theme keywords — filtering + classification
# -----------------------------------
MACRO_THEME_KEYWORDS: dict[str, list[str]] = {
    "rbi_policy": [
        "rbi", "reserve bank of india", "repo rate", "reverse repo", "crr", "slr",
        "mpc", "monetary policy committee", "rate hike", "rate cut", "liquidity",
        "policy rate", "hawkish", "dovish", "open market operations", "omo",
        "standing deposit facility", "sdf", "marginal standing facility", "msf",
        "monetary policy", "interest rate",
    ],
    "inflation": [
        "inflation", "cpi", "consumer price index", "wpi", "wholesale price index",
        "core inflation", "food inflation", "headline inflation", "price pressures",
        "disinflation", "deflation", "price rise", "retail inflation",
    ],
    "gdp_growth": [
        "gdp", "gross domestic product", "economic growth", "gva", "gross value added",
        "iip", "index of industrial production", "industrial output", "industrial production",
        "pmi", "purchasing managers index", "manufacturing pmi", "services pmi",
        "economic slowdown", "recession", "growth forecast", "growth outlook",
    ],
    "fiscal": [
        "fiscal deficit", "budget deficit", "government borrowing", "government spending",
        "capital expenditure", "capex", "revenue deficit", "union budget", "fiscal policy",
        "government debt", "sovereign debt", "disinvestment", "tax collection",
        "direct tax", "indirect tax", "income tax", "corporate tax", "gst",
        "gst collection", "fiscal consolidation",
    ],
    "currency_fx": [
        "rupee", "usd/inr", "dollar index", "dxy", "forex", "foreign exchange",
        "currency depreciation", "currency appreciation", "exchange rate",
        "rbi intervention", "forex reserves", "foreign exchange reserves",
        "capital flows", "fii flows", "fpi flows",
    ],
    "oil_commodities": [
        "crude oil", "brent crude", "wti", "crude prices", "oil prices", "opec",
        "opec+", "energy prices", "natural gas", "commodity prices",
        "gold prices", "gold", "silver", "copper", "base metals",
    ],
    "trade": [
        "trade deficit", "current account deficit", "cad", "trade balance",
        "exports", "imports", "merchandise exports", "services exports",
        "trade data", "balance of payments", "bop", "remittances",
        "foreign investment", "fdi", "foreign direct investment",
    ],
}

# Flat list for quick filter pass
ALL_MACRO_KEYWORDS: list[str] = [kw for kws in MACRO_THEME_KEYWORDS.values() for kw in kws]

# -----------------------------------
# Hard exclusions
# -----------------------------------
EXCLUDE_KEYWORDS = [
    "crypto", "bitcoin", "ethereum",
    "box office", "movie", "film", "actor", "actress", "celebrity",
    "ipl", "icc", "t20", "match", "cricket",
    "travel", "tourism", "weekend getaway",
    "neet", "jee", "exam", "cutoff", "admission",
    "stock split", "bonus shares", "rights issue", "ipo listing",
    "nifty it", "nifty bank", "nifty auto", "small cap", "mid cap", "CPI(M)", "party worker", "adivasis", "AI agent","digital addiction","CAPF", "Devina Mehra", 
]

# -----------------------------------
# Topic → driver + news_type mappings
# -----------------------------------
TOPIC_TO_DRIVER: dict[str, str] = {
    "rbi_policy":      "policy",
    "inflation":       "inflation",
    "gdp_growth":      "growth",
    "fiscal":          "policy",
    "currency_fx":     "external",
    "oil_commodities": "external",
    "trade":           "external",
}

TOPIC_TO_NEWS_TYPE: dict[str, str] = {
    "rbi_policy":      "policy",
    "inflation":       "macro",
    "gdp_growth":      "macro",
    "fiscal":          "macro",
    "currency_fx":     "markets",
    "oil_commodities": "commodities",
    "trade":           "macro",
}


# -----------------------------------
# Utility functions
# -----------------------------------
def clean_text(raw: str) -> str:
    if not raw:
        return ""
    text = html_lib.unescape(raw)
    text = re.sub(r"<[^>]+>", " ", text)
    text = " ".join(text.split())
    return text.strip()


def normalize_title(title: str) -> str:
    t = (title or "").lower().strip()
    t = re.sub(r"\s+", " ", t)
    t = re.sub(r"[^\w\s]", "", t)
    return t


def make_article_id(url: str) -> str:
    return hashlib.sha256(url.strip().encode("utf-8")).hexdigest()


def make_duplicate_group_key(title: str) -> str:
    return hashlib.sha256(normalize_title(title).encode("utf-8")).hexdigest()


def parse_published(entry) -> str:
    if getattr(entry, "published", None):
        try:
            dt = parsedate_to_datetime(entry.published)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc).isoformat()
        except Exception:
            pass

    if getattr(entry, "updated", None):
        try:
            dt = parsedate_to_datetime(entry.updated)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc).isoformat()
        except Exception:
            pass

    if getattr(entry, "published_parsed", None):
        try:
            ts = calendar.timegm(entry.published_parsed)
            return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
        except Exception:
            pass

    return datetime.now(timezone.utc).isoformat()


# -----------------------------------
# Filter
# -----------------------------------
def passes_macro_filter(text: str) -> bool:
    t = (text or "").lower()
    if any(x in t for x in EXCLUDE_KEYWORDS):
        return False
    return any(k in t for k in ALL_MACRO_KEYWORDS)


# -----------------------------------
# Classification
# -----------------------------------
def detect_topic(text: str) -> str | None:
    t = text.lower()
    for theme, keywords in MACRO_THEME_KEYWORDS.items():
        if any(k in t for k in keywords):
            return theme
    return None


def detect_market_scope(text: str) -> str:
    t = text.lower()
    if any(k in t for k in [
        "india", "indian", "rbi", "rupee", "nifty", "sensex",
        "bse", "nse", "mospi", "finmin", "nirmala", "shaktikanta",
    ]):
        return "india"
    if any(k in t for k in [
        "u.s.", "us ", "federal reserve", "fed", "nasdaq", "s&p 500",
        "wall street", "dow jones",
    ]):
        return "us"
    return "global"


def classify_article(title: str, summary: str) -> dict:
    full_text = f"{title} {summary}".strip()
    topic = detect_topic(full_text)
    market_scope = detect_market_scope(full_text)
    driver_tag = TOPIC_TO_DRIVER.get(topic, "macro") if topic else "macro"
    news_type = TOPIC_TO_NEWS_TYPE.get(topic, "macro") if topic else "macro"
    tags = sorted({news_type, driver_tag, topic} - {None})

    return {
        "news_type": news_type,
        "topic": topic,
        "market_scope": market_scope,
        "entity_type": None,
        "entity_name": None,
        "driver_tag": driver_tag,
        "is_macro_relevant": True,
        "tags": tags,
    }


# -----------------------------------
# Feed fetching
# -----------------------------------
def fetch_feed(source_name: str, url: str) -> list[dict]:
    try:
        response = requests.get(url, headers=REQUEST_HEADERS, timeout=20)
        response.raise_for_status()
        feed = feedparser.parse(response.content)
    except Exception as e:
        print(f"  ✗ Failed to fetch {source_name}: {e}")
        return []

    articles = []
    for entry in feed.entries[:60]:
        title = clean_text((entry.get("title") or "").strip())
        summary = clean_text(entry.get("summary") or entry.get("description") or "")
        link = (entry.get("link") or "").strip()

        if not title or not link:
            continue

        full_text = f"{title} {summary}"
        if not passes_macro_filter(full_text):
            continue

        published_at = parse_published(entry)
        article_id = make_article_id(link)
        duplicate_group_key = make_duplicate_group_key(title)
        classification = classify_article(title, summary)

        articles.append({
            "article_id": article_id,
            "published_at": published_at,
            "source": source_name,
            "source_type": "rss",
            "title": title,
            "summary": summary,
            "url": link,
            "news_type": classification["news_type"],
            "topic": classification["topic"],
            "market_scope": classification["market_scope"],
            "entity_type": classification["entity_type"],
            "entity_name": classification["entity_name"],
            "driver_tag": classification["driver_tag"],
            "is_macro_relevant": classification["is_macro_relevant"],
            "language": "en",
            "tags": classification["tags"],
            "duplicate_group_key": duplicate_group_key,
        })

    return articles


# -----------------------------------
# Upsert
# -----------------------------------
def upsert_articles(articles: list[dict]) -> None:
    if not articles:
        print("No articles to upsert.")
        return

    deduped = {a["article_id"]: a for a in articles}
    final_articles = list(deduped.values())
    print(f"\nUnique articles to upsert: {len(final_articles)}")

    chunk_size = 200
    for i in range(0, len(final_articles), chunk_size):
        chunk = final_articles[i:i + chunk_size]
        supabase.table("news_articles").upsert(chunk, on_conflict="article_id").execute()
        print(f"  Upserted chunk {i // chunk_size + 1} ({len(chunk)} rows)")


# -----------------------------------
# Main
# -----------------------------------
def main():
    all_articles = []

    for source_name, url in RSS_FEEDS:
        print(f"Fetching {source_name} ...")
        articles = fetch_feed(source_name, url)
        print(f"  → {len(articles)} macro articles")
        all_articles.extend(articles)

    upsert_articles(all_articles)
    print("\nDone.")


if __name__ == "__main__":
    main()