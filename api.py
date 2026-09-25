# api.py — FastAPI REST endpoints for ofa-collector
#
# Endpoints:
#   GET /health          → liveness check + Supabase heartbeat
#   GET /data?date=      → all rows for a given date (default: today UTC)
#   GET /status          → row count + WebSocket connection state

import logging
from datetime import datetime, timezone
from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse
from db import get_rows_for_date, get_row_count
from db_supabase import heartbeat as supabase_heartbeat, get_row_count_supabase
import ws_feed

logger = logging.getLogger(__name__)

app = FastAPI(title="OFA Collector API", version="2.0.0")


@app.get("/health")
def health():
    """
    Liveness check — confirms the service is alive.
    Also performs a Supabase heartbeat query to prevent
    the 7-day inactivity auto-pause on the free tier.
    """
    sb_alive = supabase_heartbeat()
    return {
        "status": "ok",
        "time_utc": datetime.now(timezone.utc).isoformat(),
        "ws_connected": ws_feed.is_connected(),
        "tick_count": ws_feed.tick_count(),
        "supabase_alive": sb_alive,
    }


@app.get("/status")
def status():
    """Returns DB row count (local + Supabase) and feed state."""
    return {
        "db_rows_local": get_row_count(),
        "db_rows_supabase": get_row_count_supabase(),
        "ws_connected": ws_feed.is_connected(),
        "tick_count": ws_feed.tick_count(),
        "time_utc": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/data")
def get_data(date: str = Query(default=None, description="Date in YYYY-MM-DD format (UTC). Defaults to today.")):
    """
    Returns all options_chain rows for the given date as JSON.

    Example:
        GET /data                      → today's rows
        GET /data?date=2026-03-28      → specific date rows

    Each row contains:
        id, timestamp, symbol, expiry, strike, option_type,
        oi, oi_change, volume, iv, last_price, spot_price
    """
    if date is None:
        date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    # Basic validation
    try:
        datetime.strptime(date, "%Y-%m-%d")
    except ValueError:
        return JSONResponse(
            status_code=400,
            content={"error": f"Invalid date format: '{date}'. Use YYYY-MM-DD."}
        )

    rows = get_rows_for_date(date)
    return {
        "date": date,
        "count": len(rows),
        "rows": rows,
    }
