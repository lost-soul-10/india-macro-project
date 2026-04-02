from __future__ import annotations

import calendar
import hashlib
import html as html_lib
import os
import re
from datetime import datetime, timedelta, timezone
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

MAX_ARTICLE_AGE_DAYS = 30

# -----------------------------------
# RSS feeds: India domestic + RBI official
# -----------------------------------
RSS_FEEDS = [
    ("LiveMint Economy",           "https://www.livemint.com/rss/economy"),
    ("LiveMint Politics & Policy", "https://www.livemint.com/rss/politics"),
    ("LiveMint Opinion",           "https://www.livemint.com/rss/opinion"),
    ("ET Economy",                 "https://economictimes.indiatimes.com/news/economy/rssfeeds/1373380680.cms"),
    ("ET Economy Policy",          "https://economictimes.indiatimes.com/news/economy/policy/rssfeeds/1052732854.cms"),
    ("RBI Press Releases",         "https://www.rbi.org.in/pressreleases_rss.xml"),
    ("RBI Speeches",               "https://www.rbi.org.in/speeches_rss.xml"),
    ("RBI Publications",           "https://www.rbi.org.in/Publication_rss.xml"),
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
        "monetary policy", "interest rate", "money market operations",
    ],
    "inflation": [
        "inflation", "cpi", "consumer price index", "wpi", "wholesale price index",
        "core inflation", "food inflation", "headline inflation", "price pressures",
        "disinflation", "deflation", "price rise", "retail inflation", "imported inflation",
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
        "gst collection", "fiscal consolidation", "tax administration",
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
        "gold prices", "gold", "silver", "copper", "base metals", "lpg",
    ],
    "trade": [
        "trade deficit", "current account deficit", "cad", "trade balance",
        "exports", "imports", "merchandise exports", "services exports",
        "trade data", "balance of payments", "bop", "remittances",
        "foreign investment", "fdi", "foreign direct investment",
    ],
}

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
    "nifty it", "nifty bank", "nifty auto", "small cap", "mid cap",
    "cpi(m)", "party worker", "adivasis", "ai agent", "digital addiction",
    "capf", "devina mehra",
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

TOPIC_TO_LINKED_FEATURES: dict[str, list[str]] = {
    "rbi_policy": ["repo_rate", "real_policy_rate"],
    "inflation": ["headline_cpi_inflation", "wpi_inflation"],
    "gdp_growth": ["gdp_growth", "iip_growth", "gst_growth"],
    "fiscal": ["gst_growth"],
    "currency_fx": ["usd_inr_change"],
    "oil_commodities": ["oil_price_change", "usd_inr_change"],
    "trade": ["usd_inr_change"],
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


def is_recent_enough(published_at: str, max_age_days: int = MAX_ARTICLE_AGE_DAYS) -> bool:
    try:
        published_dt = datetime.fromisoformat(published_at.replace("Z", "+00:00"))
        cutoff = datetime.now(timezone.utc) - timedelta(days=max_age_days)
        return published_dt >= cutoff
    except Exception:
        return False

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
        "gst", "lok sabha", "rajya sabha",
    ]):
        return "india"
    if any(k in t for k in [
        "u.s.", "us ", "federal reserve", "fed", "nasdaq", "s&p 500",
        "wall street", "dow jones",
    ]):
        return "us"
    return "global"


def detect_entity_type(text: str, topic: str | None) -> str | None:
    t = text.lower()

    if "rbi" in t or "reserve bank of india" in t:
        return "institution"
    if "government" in t or "finmin" in t or "cbdt" in t:
        return "institution"
    if topic == "currency_fx":
        return "asset"
    if topic == "oil_commodities":
        return "commodity"
    if topic in {"inflation", "gdp_growth", "fiscal", "trade"}:
        return "indicator"

    return None


def detect_entity_name(text: str, topic: str | None) -> str | None:
    t = text.lower()

    explicit_entities = [
        ("reserve bank of india", "Reserve Bank of India"),
        ("rbi", "RBI"),
        ("brent crude", "Brent crude"),
        ("wti", "WTI crude"),
        ("crude oil", "Crude oil"),
        ("oil prices", "Oil prices"),
        ("rupee", "Indian rupee"),
        ("usd/inr", "USD/INR"),
        ("gst", "GST"),
        ("consumer price index", "CPI"),
        ("cpi", "CPI"),
        ("wholesale price index", "WPI"),
        ("wpi", "WPI"),
        ("repo rate", "Repo rate"),
        ("money market", "Money market"),
    ]

    for needle, label in explicit_entities:
        if needle in t:
            return label

    topic_defaults = {
        "inflation": "Inflation",
        "gdp_growth": "Growth",
        "fiscal": "Fiscal conditions",
        "trade": "Trade balance",
    }
    return topic_defaults.get(topic)


def classify_article(title: str, summary: str) -> dict:
    full_text = f"{title} {summary}".strip()
    topic = detect_topic(full_text)
    market_scope = detect_market_scope(full_text)
    driver_tag = TOPIC_TO_DRIVER.get(topic, "macro") if topic else "macro"
    news_type = TOPIC_TO_NEWS_TYPE.get(topic, "macro") if topic else "macro"

    return {
        "news_type": news_type,
        "topic": topic,
        "market_scope": market_scope,
        "entity_type": detect_entity_type(full_text, topic),
        "entity_name": detect_entity_name(full_text, topic),
        "driver_tag": driver_tag,
        "is_macro_relevant": True,
        "tags": [],
    }

# -----------------------------------
# Enrichment helpers
# -----------------------------------
def extract_mentioned_values(text: str) -> list[str]:
    if not text:
        return []

    patterns = [
        r"\$\d+(?:\.\d+)?(?:\s?(?:per barrel|a barrel|barrel))?",
        r"₹\s?\d+(?:\.\d+)?(?:\s?(?:crore|lakh crore|trillion|billion))?",
        r"rs\.?\s?\d+(?:\.\d+)?(?:\s?(?:crore|lakh crore|trillion|billion))?",
        r"\d+(?:\.\d+)?\s?%",
        r"\d+(?:\.\d+)?\s?(?:basis points|bps)",
        r"\d+(?:\.\d+)?\s?(?:trillion|billion|million|crore|lakh crore)",
        r"\b\d+(?:\.\d+)?\s?(?:mbpd|bpd)\b",
    ]

    matches: list[str] = []
    for pattern in patterns:
        matches.extend(re.findall(pattern, text, flags=re.IGNORECASE))

    clean_matches: list[str] = []
    seen: set[str] = set()
    for m in matches:
        value = " ".join(m.split()).strip()
        key = value.lower()
        if value and key not in seen:
            seen.add(key)
            clean_matches.append(value)

    return clean_matches[:8]


def detect_direction(text: str) -> str | None:
    t = text.lower()

    up_words = ["rose", "rises", "up", "gained", "surged", "jumped", "climbed", "higher", "increased", "accelerated"]
    down_words = ["fell", "falls", "down", "declined", "dropped", "slipped", "lower", "decreased", "eased", "softened"]
    stable_words = ["unchanged", "steady", "stable", "retained", "maintained", "kept"]

    if any(w in t for w in up_words):
        return "up"
    if any(w in t for w in down_words):
        return "down"
    if any(w in t for w in stable_words):
        return "stable"
    return None


def build_macro_implication(topic: str | None, driver_tag: str, direction_tag: str | None) -> str:
    implication_map = {
        "rbi_policy": {
            "up": "This may signal tighter liquidity or a firmer policy stance.",
            "down": "This may indicate easier liquidity or a more supportive policy stance.",
            "stable": "This may suggest policy continuity and a steady liquidity backdrop.",
            None: "This may matter for RBI policy expectations and liquidity conditions.",
        },
        "inflation": {
            "up": "This may add to price pressures and influence RBI policy expectations.",
            "down": "This may indicate easing price pressures and some relief for inflation expectations.",
            "stable": "This may suggest inflation conditions are broadly steady for now.",
            None: "This may matter for price pressures and inflation expectations.",
        },
        "gdp_growth": {
            "up": "This may point to stronger activity and firmer growth momentum.",
            "down": "This may suggest softer activity or slowing growth momentum.",
            "stable": "This may suggest growth conditions are broadly steady.",
            None: "This may matter for the near-term growth outlook.",
        },
        "fiscal": {
            "up": "This may support revenue strength or government fiscal capacity.",
            "down": "This may reflect weaker fiscal momentum or a softer revenue backdrop.",
            "stable": "This may suggest fiscal conditions are relatively steady.",
            None: "This may matter for fiscal conditions and public-sector activity.",
        },
        "currency_fx": {
            "up": "This may increase imported inflation or external stress if it reflects rupee weakness or dollar strength.",
            "down": "This may ease some imported inflation or external pressure if it reflects rupee stability or a softer dollar.",
            "stable": "This may suggest a relatively stable external currency backdrop.",
            None: "This may matter for imported inflation, capital flows, and external stress.",
        },
        "oil_commodities": {
            "up": "This may raise imported inflation risks and worsen India's external pressure.",
            "down": "This may reduce imported inflation risks and ease some external pressure.",
            "stable": "This may suggest the external commodity backdrop is relatively steady.",
            None: "This may matter for imported inflation and India's external position.",
        },
        "trade": {
            "up": "This may affect the current account and external balance depending on whether exports or imports are driving the move.",
            "down": "This may affect the current account and external balance depending on which trade component is weakening.",
            "stable": "This may suggest trade conditions are broadly steady.",
            None: "This may matter for India's external balance and current account dynamics.",
        },
        None: {
            "up": "This may matter for the macro backdrop.",
            "down": "This may matter for the macro backdrop.",
            "stable": "This may matter for the macro backdrop.",
            None: "This may matter for the macro backdrop.",
        },
    }

    topic_map = implication_map.get(topic, implication_map[None])
    return topic_map.get(direction_tag, topic_map[None])


def build_enriched_summary(title: str, raw_summary: str, classification: dict) -> tuple[str, list[str], str | None, str, list[str]]:
    base_text = f"{title}. {raw_summary}".strip()
    base_text = re.sub(r"\s+", " ", base_text)

    mentioned_values = extract_mentioned_values(base_text)
    direction_tag = detect_direction(base_text)
    linked_features = TOPIC_TO_LINKED_FEATURES.get(classification.get("topic"), [])
    macro_implication = build_macro_implication(
        classification.get("topic"),
        classification.get("driver_tag", "macro"),
        direction_tag,
    )

    parts = [base_text]

    if mentioned_values:
        parts.append(f"Key figures mentioned: {', '.join(mentioned_values)}.")

    if direction_tag == "up":
        parts.append("The article indicates the relevant variable or theme is moving higher.")
    elif direction_tag == "down":
        parts.append("The article indicates the relevant variable or theme is moving lower.")
    elif direction_tag == "stable":
        parts.append("The article indicates conditions are broadly steady or unchanged.")

    if linked_features:
        parts.append(f"Relevant dashboard features: {', '.join(linked_features)}.")

    if macro_implication:
        parts.append(macro_implication)

    enriched_summary = " ".join(parts).strip()
    return enriched_summary, mentioned_values, direction_tag, macro_implication, linked_features


def build_tags(classification: dict, title: str, raw_summary: str) -> list[str]:
    t = f"{title} {raw_summary}".lower()
    tags = {
        classification.get("news_type"),
        classification.get("driver_tag"),
        classification.get("topic"),
        classification.get("market_scope"),
        classification.get("entity_name"),
        detect_direction(t),
    }

    if "brent" in t:
        tags.add("brent")
    if "wti" in t:
        tags.add("wti")
    if "crude" in t or "oil" in t:
        tags.add("oil")
    if "rupee" in t or "usd/inr" in t:
        tags.add("rupee")
    if "gst" in t:
        tags.add("gst")
    if "repo" in t:
        tags.add("repo")
    if "inflation" in t or "cpi" in t or "wpi" in t:
        tags.add("inflation")

    return sorted(tag for tag in tags if tag)

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

    articles: list[dict] = []

    for entry in feed.entries[:60]:
        title = clean_text((entry.get("title") or "").strip())
        raw_summary = clean_text(entry.get("summary") or entry.get("description") or "")
        link = (entry.get("link") or "").strip()

        if not title or not link:
            continue

        full_text = f"{title} {raw_summary}"
        if not passes_macro_filter(full_text):
            continue

        published_at = parse_published(entry)
        if not is_recent_enough(published_at):
            continue

        article_id = make_article_id(link)
        duplicate_group_key = make_duplicate_group_key(title)

        classification = classify_article(title, raw_summary)
        (
            enriched_summary,
            mentioned_values,
            direction_tag,
            macro_implication,
            linked_features,
        ) = build_enriched_summary(title, raw_summary, classification)
        tags = build_tags(classification, title, raw_summary)

        articles.append({
            "article_id": article_id,
            "published_at": published_at,
            "source": source_name,
            "source_type": "rss",
            "title": title,
            "raw_summary": raw_summary,
            "summary": enriched_summary,
            "summary_enriched": enriched_summary,
            "url": link,
            "news_type": classification["news_type"],
            "topic": classification["topic"],
            "market_scope": classification["market_scope"],
            "entity_type": classification["entity_type"],
            "entity_name": classification["entity_name"],
            "driver_tag": classification["driver_tag"],
            "is_macro_relevant": classification["is_macro_relevant"],
            "language": "en",
            "tags": tags,
            "mentioned_values": mentioned_values,
            "direction_tag": direction_tag,
            "macro_implication": macro_implication,
            "linked_features": linked_features,
            "duplicate_group_key": duplicate_group_key,
            "created_at": datetime.now(timezone.utc).isoformat(),
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
# Optional cleanup: remove old rows from database too
# -----------------------------------
def delete_old_articles(days: int = MAX_ARTICLE_AGE_DAYS) -> None:
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
    try:
        supabase.table("news_articles").delete().lt("published_at", cutoff).execute()
        print(f"Deleted articles older than {days} days (before {cutoff})")
    except Exception as e:
        print(f"Warning: failed to delete old articles: {e}")

# -----------------------------------
# Main
# -----------------------------------
def main():
    all_articles: list[dict] = []

    for source_name, url in RSS_FEEDS:
        print(f"Fetching {source_name} ...")
        articles = fetch_feed(source_name, url)
        print(f"  → {len(articles)} macro articles within last {MAX_ARTICLE_AGE_DAYS} days")
        all_articles.extend(articles)

    upsert_articles(all_articles)
    delete_old_articles(MAX_ARTICLE_AGE_DAYS)
    print("\nDone.")

if __name__ == "__main__":
    main()