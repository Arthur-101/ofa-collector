# db_supabase.py — Supabase PostgreSQL operations for ofa-collector
#
# Replaces the old local-only SQLite temp DB approach.
# All options chain data now flows directly to Supabase cloud.
# Uses idempotent upsert (ignore_duplicates=True) for zero duplicates.

import os
import logging
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

_client: Client | None = None


def get_supabase() -> Client:
    """Lazy-initialise and return the Supabase client singleton."""
    global _client
    if _client is None:
        if not SUPABASE_URL or not SUPABASE_KEY:
            raise RuntimeError(
                "SUPABASE_URL and SUPABASE_KEY must be set in environment"
            )
        _client = create_client(SUPABASE_URL, SUPABASE_KEY)
        logger.info("Supabase client initialised for %s", SUPABASE_URL)
    return _client


def insert_options_rows_supabase(rows: list[dict]) -> None:
    """
    Batch upsert options chain rows to Supabase.
    Uses ignore_duplicates=True — rows matching the UNIQUE constraint
    (timestamp, symbol, expiry, strike, option_type) are silently skipped.
    """
    if not rows:
        return
    client = get_supabase()
    try:
        client.table("options_chain").upsert(
            rows,
            on_conflict="timestamp,symbol,expiry,strike,option_type",
            ignore_duplicates=True,
        ).execute()
        logger.info("Supabase: %d rows upserted to options_chain", len(rows))
    except Exception as e:
        logger.error("Supabase upsert failed: %s", e)
        raise


def insert_signal_supabase(signal: dict) -> None:
    """
    Insert a single signal row to Supabase.
    Uses upsert with ignore_duplicates to prevent duplicates.
    """
    client = get_supabase()
    try:
        client.table("signals").upsert(
            [signal],
            on_conflict="symbol,strike,option_type,signal_type",
            ignore_duplicates=True,
        ).execute()
        logger.info(
            "Supabase: signal inserted — %s %s %.0f %s",
            signal.get("signal_type", ""),
            signal.get("symbol", ""),
            signal.get("strike", 0),
            signal.get("option_type", ""),
        )
    except Exception as e:
        logger.error("Supabase signal insert failed: %s", e)
        raise


def get_latest_oi_snapshot_supabase(symbol: str) -> dict[tuple, int]:
    """
    Returns {(strike, option_type, expiry): oi} for the most recent
    timestamp in Supabase for the given symbol.
    Used by flush_to_db() to compute oi_change.
    """
    client = get_supabase()
    try:
        # Get the most recent timestamp
        latest_res = (
            client.table("options_chain")
            .select("timestamp")
            .eq("symbol", symbol)
            .order("timestamp", desc=True)
            .limit(1)
            .execute()
        )
        if not latest_res.data:
            return {}

        latest_ts = latest_res.data[0]["timestamp"]

        # Get all rows for that timestamp
        res = (
            client.table("options_chain")
            .select("strike,option_type,expiry,oi")
            .eq("symbol", symbol)
            .eq("timestamp", latest_ts)
            .execute()
        )
        return {
            (float(r["strike"]), r["option_type"], r["expiry"]): r["oi"]
            for r in (res.data or [])
        }
    except Exception as e:
        logger.error("Supabase get_latest_oi_snapshot failed: %s", e)
        return {}


def get_rows_for_date_supabase(date_str: str) -> list[dict]:
    """
    Returns all options_chain rows for a given date from Supabase.
    Used by the exporter for the daily CSV dump to GitHub.
    """
    client = get_supabase()
    try:
        all_rows = []
        offset = 0
        batch_size = 1000
        while True:
            res = (
                client.table("options_chain")
                .select("*")
                .gte("timestamp", f"{date_str}T00:00:00")
                .lt("timestamp", f"{date_str}T23:59:59.999999")
                .order("timestamp", desc=False)
                .range(offset, offset + batch_size - 1)
                .execute()
            )
            batch = res.data or []
            all_rows.extend(batch)
            if len(batch) < batch_size:
                break
            offset += batch_size
        return all_rows
    except Exception as e:
        logger.error("Supabase get_rows_for_date failed: %s", e)
        return []


def get_row_count_supabase() -> int:
    """Return total row count of options_chain in Supabase."""
    client = get_supabase()
    try:
        res = (
            client.table("options_chain")
            .select("id", count="exact")
            .execute()
        )
        return res.count or 0
    except Exception as e:
        logger.error("Supabase row count failed: %s", e)
        return 0


def purge_old_data_supabase() -> None:
    """
    Call the Supabase SQL function to purge options_chain and news_raw
    rows older than 7 days. Keeps database under ~90 MB forever.
    """
    client = get_supabase()
    try:
        client.rpc("purge_old_options_data").execute()
        logger.info("Supabase: 7-day retention purge executed successfully")
    except Exception as e:
        logger.error("Supabase purge failed: %s", e)
        raise


def heartbeat() -> bool:
    """
    Lightweight query to keep Supabase project alive.
    Prevents the 7-day inactivity auto-pause on free tier.
    Called from /health endpoint.
    """
    try:
        client = get_supabase()
        client.table("signals").select("id").limit(1).execute()
        return True
    except Exception as e:
        logger.warning("Supabase heartbeat failed: %s", e)
        return False
