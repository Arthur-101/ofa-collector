# db_supabase.py — Supabase PostgreSQL operations for ofa-collector
#
# Replaces the old local-only SQLite temp DB approach.
# All options chain data now flows directly to Supabase cloud.
# Uses idempotent upsert (ignore_duplicates=True) for zero duplicates.

import os
import json
import base64
import logging
from pathlib import Path
from datetime import datetime, timezone
from supabase import create_client, Client
from dotenv import load_dotenv
import numpy as np

load_dotenv(Path(__file__).parent / ".env")
load_dotenv()
logger = logging.getLogger(__name__)

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
SUPABASE_KEY = SUPABASE_SERVICE_ROLE_KEY or os.getenv("SUPABASE_KEY")

_client: Client | None = None

def _is_anon_jwt(token: str | None) -> bool:
    """Check if JWT token has role 'anon'."""
    if not token:
        return True
    try:
        parts = token.split(".")
        if len(parts) >= 2:
            padded = parts[1] + "=" * ((4 - len(parts[1]) % 4) % 4)
            payload = json.loads(base64.urlsafe_b64decode(padded))
            return payload.get("role") == "anon"
    except Exception:
        pass
    return False

def verify_credentials() -> None:
    """
    Ensure the collector is configured with SUPABASE_SERVICE_ROLE_KEY.
    Fails loudly at startup if only anon key is present.
    """
    if not SUPABASE_URL:
        raise RuntimeError("CRITICAL: SUPABASE_URL must be set in environment")
    if not SUPABASE_SERVICE_ROLE_KEY:
        raise RuntimeError(
            "CRITICAL STARTUP ERROR: SUPABASE_SERVICE_ROLE_KEY is missing from environment! "
            "The ofa-collector backend requires the service_role key to bypass RLS policies "
            "on 'trades' and 'strike_daily_stats'. Set SUPABASE_SERVICE_ROLE_KEY in .env and Render env vars."
        )
    if _is_anon_jwt(SUPABASE_SERVICE_ROLE_KEY):
        raise RuntimeError(
            "CRITICAL STARTUP ERROR: SUPABASE_SERVICE_ROLE_KEY has role 'anon'! "
            "You provided the public anon key instead of the secret service_role key. "
            "Get the service_role key from Supabase Dashboard -> Settings -> API -> Project API keys."
        )

ALLOWED_OPTIONS_COLS = {
    "timestamp", "symbol", "expiry", "strike", "option_type",
    "oi", "oi_change", "volume", "iv", "last_price", "spot_price"
}


def get_supabase() -> Client:
    """Lazy-initialise and return the Supabase client singleton."""
    global _client
    if _client is None:
        if not SUPABASE_URL or not SUPABASE_KEY:
            raise RuntimeError(
                "SUPABASE_URL and SUPABASE_KEY must be set in environment"
            )
        _client = create_client(SUPABASE_URL, SUPABASE_KEY)
        role = "anon" if _is_anon_jwt(SUPABASE_KEY) else "service_role"
        logger.info("Supabase client initialised for %s (role: %s)", SUPABASE_URL, role)
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
    clean_rows = [{k: v for k, v in r.items() if k in ALLOWED_OPTIONS_COLS} for r in rows]
    try:
        client.table("options_chain").upsert(
            clean_rows,
            on_conflict="timestamp,symbol,expiry,strike,option_type",
            ignore_duplicates=True,
        ).execute()
        logger.info("Supabase: %d rows upserted to options_chain", len(clean_rows))
    except Exception as e:
        logger.error("Supabase upsert failed: %s", e)
        raise


def insert_signal_supabase(signal: dict) -> int | None:
    """
    Insert a single signal row to Supabase.
    Returns the generated signal ID on success, or None on failure.
    """
    client = get_supabase()
    try:
        res = client.table("signals").insert([signal]).execute()
        sig_id = res.data[0]["id"] if res.data else None
        logger.info(
            "Supabase: signal inserted (id=%s) — %s %s %.0f %s",
            sig_id,
            signal.get("signal_type", ""),
            signal.get("symbol", ""),
            signal.get("strike", 0),
            signal.get("option_type", ""),
        )
        return sig_id
    except Exception as e:
        logger.error("Supabase signal insert failed: %s", e)
        return None


def update_signal_thesis_supabase(signal_id: int, thesis_data: dict) -> bool:
    """
    Update a signal row in Supabase with LLM thesis output.
    """
    if not signal_id or not thesis_data:
        return False
    client = get_supabase()
    try:
        client.table("signals").update({
            "llm_thesis": thesis_data.get("thesis"),
            "llm_bias": thesis_data.get("bias"),
            "llm_confidence": thesis_data.get("confidence"),
            "thesis_at": thesis_data.get("thesis_at") or datetime.now(timezone.utc).isoformat(),
        }).eq("id", signal_id).execute()
        logger.info("Supabase: signal %d updated with LLM thesis (%s, conf %s)",
                    signal_id, thesis_data.get("bias"), thesis_data.get("confidence"))
        return True
    except Exception as e:
        logger.error("Supabase signal thesis update failed for id=%s: %s", signal_id, e)
        return False


def signal_already_fired_today_supabase(symbol: str, strike: float, option_type: str,
                                        signal_type: str, date_str: str) -> bool:
    """
    Check if this signal type on this strike has already fired today in Supabase.
    Prevents duplicate signals across flushes.
    """
    client = get_supabase()
    try:
        res = (
            client.table("signals")
            .select("id")
            .eq("symbol", symbol)
            .eq("strike", strike)
            .eq("option_type", option_type)
            .eq("signal_type", signal_type)
            .gte("fired_at", f"{date_str}T00:00:00")
            .lt("fired_at", f"{date_str}T23:59:59.999999")
            .limit(1)
            .execute()
        )
        return bool(res.data)
    except Exception as e:
        logger.warning("Supabase signal_already_fired_today check error: %s", e)
        return False


def insert_market_context_supabase(ctx: dict) -> None:
    """
    Upsert a 5-minute market context snapshot to Supabase.
    Uses on_conflict="symbol,timestamp" so updates to the same flush are safe.
    """
    if not ctx:
        return
    client = get_supabase()
    try:
        client.table("market_context").upsert(
            [ctx],
            on_conflict="symbol,timestamp",
        ).execute()
        logger.info(
            "Supabase: market_context upserted for %s at %s (spot=%.2f PCR=%.2f stance=%s)",
            ctx.get("symbol"), ctx.get("timestamp"), ctx.get("spot_price", 0),
            ctx.get("pcr") or 0.0, ctx.get("ai_stance", "N/A")
        )
    except Exception as e:
        logger.error("Supabase market_context upsert failed: %s", e)
        raise


def get_latest_market_context_supabase(symbol: str) -> dict | None:
    """
    Fetch the most recent market_context row for a symbol from Supabase.
    """
    client = get_supabase()
    try:
        res = (
            client.table("market_context")
            .select("*")
            .eq("symbol", symbol)
            .order("timestamp", desc=True)
            .limit(1)
            .execute()
        )
        return res.data[0] if res.data else None
    except Exception as e:
        logger.error("Supabase get_latest_market_context failed: %s", e)
        return None


def insert_news_headlines_supabase(headlines: list[dict]) -> None:
    """
    Insert raw news headlines into news_raw table in Supabase.
    """
    if not headlines:
        return
    client = get_supabase()
    try:
        client.table("news_raw").insert(headlines).execute()
        logger.info("Supabase: %d headlines inserted to news_raw", len(headlines))
    except Exception as e:
        logger.error("Supabase news_raw insert failed: %s", e)


def get_spot_series_supabase(symbol: str, date_str: str) -> list[float]:
    """
    Fetch ordered spot price series for date_str from Supabase.
    """
    client = get_supabase()
    try:
        res = (
            client.table("options_chain")
            .select("timestamp,spot_price")
            .eq("symbol", symbol)
            .gte("timestamp", f"{date_str}T00:00:00")
            .lte("timestamp", f"{date_str}T23:59:59")
            .order("timestamp", desc=False)
            .limit(1000)
            .execute()
        )
        if not res.data:
            return []
        by_ts = {}
        for r in res.data:
            ts = r.get("timestamp")
            sp = r.get("spot_price")
            if ts and sp and sp > 0:
                by_ts[ts] = float(sp)
        return [by_ts[ts] for ts in sorted(by_ts.keys())]
    except Exception as e:
        logger.warning("Supabase get_spot_series error: %s", e)
        return []


def get_intraday_oi_changes_supabase(symbol: str, date_str: str) -> list[dict]:
    """
    Fetch non-null oi_change rows for date_str from Supabase.
    """
    client = get_supabase()
    try:
        res = (
            client.table("options_chain")
            .select("strike,option_type,expiry,oi_change,volume,iv,spot_price,timestamp")
            .eq("symbol", symbol)
            .gte("timestamp", f"{date_str}T00:00:00")
            .lte("timestamp", f"{date_str}T23:59:59")
            .not_.is_("oi_change", "null")
            .order("timestamp", desc=False)
            .limit(1000)
            .execute()
        )
        return res.data or []
    except Exception as e:
        logger.warning("Supabase get_intraday_oi_changes error: %s", e)
        return []




def get_latest_snapshot_supabase(symbol: str) -> dict[tuple, dict]:
    """
    Returns {(strike, option_type, expiry): {"oi": oi, "last_price": last_price, "volume": volume}}
    for the most recent timestamp in Supabase for the given symbol.
    """
    client = get_supabase()
    try:
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
        res = (
            client.table("options_chain")
            .select("strike,option_type,expiry,oi,last_price,volume")
            .eq("symbol", symbol)
            .eq("timestamp", latest_ts)
            .execute()
        )
        return {
            (float(r["strike"]), r["option_type"], r["expiry"]): {
                "oi": r.get("oi"),
                "last_price": r.get("last_price"),
                "volume": r.get("volume"),
            }
            for r in (res.data or [])
        }
    except Exception as e:
        logger.error("Supabase get_latest_snapshot failed: %s", e)
        return {}


def get_latest_oi_snapshot_supabase(symbol: str) -> dict[tuple, int]:
    """
    Returns {(strike, option_type, expiry): oi} for the most recent
    timestamp in Supabase for the given symbol.
    """
    snap = get_latest_snapshot_supabase(symbol)
    return {k: (v.get("oi") if isinstance(v, dict) else v) for k, v in snap.items()}


def get_prior_snapshot_supabase(symbol: str, before_timestamp: str) -> dict[tuple, dict]:
    """
    Returns {(strike, option_type, expiry): {"oi": oi, "last_price": last_price, "volume": volume}}
    for the snapshot immediately preceding before_timestamp.
    """
    client = get_supabase()
    try:
        prior_res = (
            client.table("options_chain")
            .select("timestamp")
            .eq("symbol", symbol)
            .lt("timestamp", before_timestamp)
            .order("timestamp", desc=True)
            .limit(1)
            .execute()
        )
        if not prior_res.data:
            return {}

        prior_ts = prior_res.data[0]["timestamp"]
        res = (
            client.table("options_chain")
            .select("strike,option_type,expiry,oi,last_price,volume")
            .eq("symbol", symbol)
            .eq("timestamp", prior_ts)
            .execute()
        )
        return {
            (float(r["strike"]), r["option_type"], r["expiry"]): {
                "oi": r.get("oi"),
                "last_price": r.get("last_price"),
                "volume": r.get("volume"),
            }
            for r in (res.data or [])
        }
    except Exception as e:
        logger.error("Supabase get_prior_snapshot failed: %s", e)
        return {}


def insert_trade_supabase(trade: dict) -> int | None:
    """
    Insert a trade execution row into trades table.
    """
    if not trade:
        return None
    if _is_anon_jwt(SUPABASE_KEY):
        raise RuntimeError(
            "CRITICAL: Cannot write to 'trades' using 'anon' key (violates RLS). "
            "SUPABASE_SERVICE_ROLE_KEY is required in environment."
        )
    client = get_supabase()
    allowed_cols = {
        "signal_id", "source", "strike", "option_type", "expiry",
        "bias_used", "entry_at", "entry_premium", "exit_premium",
        "exit_reason", "target_pct", "stop_pct", "net_pnl_pct",
        "mfe", "mae", "resolved_at", "stale_ltp", "thesis_at", "is_exploratory"
    }
    clean = {k: v for k, v in trade.items() if k in allowed_cols}
    try:
        res = client.table("trades").insert([clean]).execute()
        return res.data[0]["id"] if res.data else None
    except Exception as e:
        logger.error("Supabase insert_trade failed: %s", e)
        return None


def update_trade_supabase(trade_id: int, updates: dict) -> bool:
    """
    Update a trade row in trades table by id.
    """
    if not trade_id or not updates:
        return False
    if _is_anon_jwt(SUPABASE_KEY):
        raise RuntimeError(
            "CRITICAL: Cannot update 'trades' using 'anon' key (violates RLS). "
            "SUPABASE_SERVICE_ROLE_KEY is required in environment."
        )
    client = get_supabase()
    allowed_cols = {
        "exit_premium", "exit_reason", "net_pnl_pct",
        "mfe", "mae", "resolved_at", "stale_ltp"
    }
    clean = {k: v for k, v in updates.items() if k in allowed_cols}
    try:
        client.table("trades").update(clean).eq("id", trade_id).execute()
        return True
    except Exception as e:
        logger.error("Supabase update_trade failed for id=%s: %s", trade_id, e)
        return False


def insert_strike_daily_stats_supabase(rows: list[dict]) -> int:
    """
    Batch upsert or insert rows into strike_daily_stats table.
    """
    if not rows:
        return 0
    if _is_anon_jwt(SUPABASE_KEY):
        raise RuntimeError(
            "CRITICAL: Cannot write to 'strike_daily_stats' using 'anon' key (violates RLS). "
            "SUPABASE_SERVICE_ROLE_KEY is required in environment."
        )
    client = get_supabase()
    allowed_cols = {
        "trade_date", "strike_offset", "option_type",
        "time_bucket", "dte", "volume", "volume_delta", "oi_change"
    }
    clean_rows = [{k: v for k, v in r.items() if k in allowed_cols} for r in rows]
    total = 0
    batch_size = 500
    for i in range(0, len(clean_rows), batch_size):
        chunk = clean_rows[i:i + batch_size]
        try:
            client.table("strike_daily_stats").upsert(
                chunk,
                on_conflict="trade_date,strike_offset,option_type,time_bucket",
                ignore_duplicates=False,
            ).execute()
            total += len(chunk)
        except Exception as e:
            logger.debug("Upsert not permitted or conflict target mismatch, falling back to insert: %s", e)
            try:
                client.table("strike_daily_stats").insert(chunk).execute()
                total += len(chunk)
            except Exception as e2:
                logger.error("Supabase insert_strike_daily_stats failed: %s", e2)
    return total


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
        client.rpc("purge_old_data").execute()
        logger.info("Supabase: 7-day retention purge completed via RPC")
    except Exception as e:
        logger.debug("Supabase purge_old_data RPC call: %s, falling back to direct delete", e)
        try:
            from datetime import timedelta
            cutoff = (datetime.now(timezone.utc) - timedelta(days=7)).isoformat()
            client.table("options_chain").delete().lt("timestamp", cutoff).execute()
            logger.info("Supabase: direct 7-day retention purge completed for options_chain (cutoff: %s)", cutoff)
        except Exception as e2:
            logger.error("Supabase direct purge failed: %s", e2)


def count_trading_days_supabase(symbol: str) -> int:
    """
    Count distinct trading dates in options_chain from Supabase.
    Used by anomaly detector to determine BOOTSTRAP vs FULL mode.
    """
    client = get_supabase()
    try:
        res = (
            client.table("options_chain")
            .select("timestamp")
            .eq("symbol", symbol)
            .order("timestamp", desc=True)
            .limit(1000)
            .execute()
        )
        if not res.data:
            return 0
        dates = {r["timestamp"][:10] for r in res.data if r.get("timestamp")}
        return len(dates)
    except Exception as e:
        logger.warning("Supabase count_trading_days error: %s", e)
        return 0


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
