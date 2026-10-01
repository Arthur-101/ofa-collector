# news_fetcher.py — RSS news fetcher for OFA Cloud Collector
#
# Fetches recent NIFTY-relevant headlines from ET Markets and Moneycontrol.
# Stores raw headlines in Supabase news_raw table.
# Returns list of headline strings for LLM trading thesis context.

import logging
import requests
import xml.etree.ElementTree as ET
from datetime import datetime, timezone, timedelta

import db
import db_supabase

logger = logging.getLogger(__name__)

# ── RSS feed sources ───────────────────────────────────────────────────────────
RSS_FEEDS = [
    {
        "name": "ET Markets",
        "url": "https://economictimes.indiatimes.com/markets/rssfeeds/1977021501.cms",
    },
    {
        "name": "Moneycontrol",
        "url": "https://www.moneycontrol.com/rss/marketoutlook.xml",
    },
]

RELEVANT_KEYWORDS = [
    "nifty", "sensex", "market", "index", "option", "derivative",
    "fii", "dii", "institutional", "expiry", "volatility", "iv",
    "bull", "bear", "rally", "fall", "crash", "breakout",
    "rbi", "fed", "rate", "inflation", "gdp", "earnings",
]

MAX_AGE_HOURS = 6
MAX_HEADLINES = 8


def _parse_rss(feed_url: str, feed_name: str) -> list[dict]:
    try:
        resp = requests.get(feed_url, timeout=10, headers={
            "User-Agent": "Mozilla/5.0 (compatible; OFA-Cloud-Collector/1.0)"
        })
        resp.raise_for_status()
    except Exception as e:
        logger.warning("RSS fetch failed for %s: %s", feed_name, e)
        return []

    articles = []
    try:
        root = ET.fromstring(resp.content)
        items = root.findall(".//item")

        for item in items:
            title = item.findtext("title", "").strip()
            pub_date_str = item.findtext("pubDate", "").strip()
            link = item.findtext("link", "").strip()

            if not title:
                continue

            pub_dt = None
            for fmt in [
                "%a, %d %b %Y %H:%M:%S %z",
                "%a, %d %b %Y %H:%M:%S GMT",
            ]:
                try:
                    pub_dt = datetime.strptime(pub_date_str, fmt)
                    if pub_dt.tzinfo is None:
                        pub_dt = pub_dt.replace(tzinfo=timezone.utc)
                    break
                except ValueError:
                    continue

            articles.append({
                "headline": title,
                "source": feed_name,
                "url": link,
                "published_at": pub_dt,
            })
    except ET.ParseError as e:
        logger.warning("RSS parse error for %s: %s", feed_name, e)

    return articles


def _is_relevant(headline: str) -> bool:
    hl_lower = headline.lower()
    return any(kw in hl_lower for kw in RELEVANT_KEYWORDS)


def _is_recent(pub_dt: datetime | None) -> bool:
    if pub_dt is None:
        return True
    cutoff = datetime.now(timezone.utc) - timedelta(hours=MAX_AGE_HOURS)
    return pub_dt >= cutoff


def fetch_news(symbol: str) -> list[str]:
    """
    Fetch and filter recent relevant news headlines.
    Stores raw headlines in Supabase news_raw table.
    Returns list of headline strings for LLM context (max MAX_HEADLINES).
    """
    all_articles = []
    for feed in RSS_FEEDS:
        articles = _parse_rss(feed["url"], feed["name"])
        all_articles.extend(articles)

    filtered = [
        a for a in all_articles
        if _is_relevant(a["headline"]) and _is_recent(a["published_at"])
    ]

    seen = set()
    deduped = []
    for a in filtered:
        if a["headline"] not in seen:
            seen.add(a["headline"])
            deduped.append(a)

    deduped.sort(
        key=lambda x: x["published_at"] or datetime.min.replace(tzinfo=timezone.utc),
        reverse=True
    )

    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    # Format for database insertion
    db_records = [
        {
            "fetched_at":   now_utc,
            "symbol":       symbol,
            "headline":     a["headline"],
            "source":       a["source"],
            "url":          a["url"],
            "published_at": a["published_at"].strftime("%Y-%m-%d %H:%M:%S") if a["published_at"] else None,
        }
        for a in deduped
    ]

    if db_records:
        # Save to local SQLite
        try:
            db.insert_news_local(db_records, symbol)
        except Exception as e:
            logger.warning("Local SQLite news insert failed: %s", e)

        # Save to Supabase
        try:
            db_supabase.insert_news_headlines_supabase(db_records)
        except Exception as e:
            logger.warning("Supabase news insert failed: %s", e)

    headlines = [a["headline"] for a in deduped[:MAX_HEADLINES]]
    logger.info("[%s] News fetched: %d total → %d relevant → %d returned for LLM",
                symbol, len(all_articles), len(deduped), len(headlines))
    return headlines
