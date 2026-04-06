# ws_feed.py — Angel One WebSocket V2 feed
#
# Architecture:
#   - SmartWebSocketV2 runs in a background thread, receives live ticks
#   - Each tick updates an in-memory store (latest snapshot per token)
#   - Every 5 min, the scheduler calls flush_to_db() which snapshots
#     the in-memory store and writes to SQLite
#
# SnapQuote mode gives us: LTP, open, high, low, close, volume, OI
# IV is computed from LTP using Black-Scholes (no paid feed needed)

import os
import math
import time
import logging
import threading
import pyotp
from datetime import datetime, timezone, date, timedelta
from dotenv import load_dotenv
from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2

from db import get_latest_oi_snapshot, insert_options_rows
from angel_fetcher import fetch_and_store as _angel_fetch_and_store

# Import internal helpers directly by loading the module
import angel_fetcher as _af

load_dotenv()
logger = logging.getLogger(__name__)

API_KEY     = os.getenv("ANGEL_API_KEY")
CLIENT_ID   = os.getenv("ANGEL_CLIENT_ID")
PASSWORD    = os.getenv("ANGEL_PASSWORD")
TOTP_SECRET = os.getenv("ANGEL_TOTP_SECRET")

# ── In-memory tick store ──────────────────────────────────────────────────────
# {token_str: {ltp, oi, volume, ...}}  — updated on every tick
_tick_store: dict[str, dict] = {}
_tick_lock  = threading.Lock()

# ── Token → scrip mapping (set at startup) ───────────────────────────────────
_token_map: dict[str, dict] = {}   # {token_str: scrip_dict}

# ── WebSocket state ───────────────────────────────────────────────────────────
_sws: SmartWebSocketV2 | None = None
_ws_thread: threading.Thread | None = None
_connected = False


# ── Start the WebSocket feed ──────────────────────────────────────────────────

def start_feed(symbols: list[str]) -> None:
    """
    Login, load instruments, subscribe to WebSocket SnapQuote feed.
    Runs the WebSocket in a background daemon thread.
    Call once at startup.
    """
    global _sws, _ws_thread, _token_map, _connected

    # Login
    client = _af._login()
    auth_token  = client.access_token
    feed_token  = client.getfeedToken()

    # Get spot for each symbol to determine ATM range
    all_scrips = []
    for sym in symbols:
        spot = _af._get_spot(client, sym)
        if not spot:
            logger.error("Could not get spot for %s", sym)
            continue
        scrips = _af._load_instruments(sym, spot)
        all_scrips.extend(scrips)
        logger.info("%s: %d scrips loaded (spot ₹%.2f)", sym, len(scrips), spot)

    if not all_scrips:
        raise RuntimeError("No scrips loaded — cannot start WebSocket feed")

    # Build token map
    _token_map = {s["token"]: s for s in all_scrips}
    logger.info("Total tokens to subscribe: %d", len(_token_map))

    # Build subscription token list (NFO = exchangeType 2)
    token_list = [{"exchangeType": 2, "tokens": list(_token_map.keys())}]

    # Initialise WebSocket
    _sws = SmartWebSocketV2(
        auth_token  = auth_token,
        api_key     = API_KEY,
        client_code = CLIENT_ID,
        feed_token  = feed_token,
    )

    # Wire callbacks
    _sws.on_open  = lambda wsapp: _on_open(wsapp, token_list)
    _sws.on_data  = _on_data
    _sws.on_error = _on_error
    _sws.on_close = _on_close

    # Run in background thread
    _ws_thread = threading.Thread(
        target=_sws.connect,
        daemon=True,
        name="ws-feed"
    )
    _ws_thread.start()
    logger.info("WebSocket feed thread started")


# ── WebSocket callbacks ───────────────────────────────────────────────────────

def _on_open(wsapp, token_list: list[dict]) -> None:
    global _connected
    _connected = True
    logger.info("WebSocket connected — subscribing %d tokens in SnapQuote mode",
                sum(len(t["tokens"]) for t in token_list))
    _sws.subscribe(
        correlation_id = "ofa00001",
        mode           = SmartWebSocketV2.SNAP_QUOTE,   # mode 3 — includes OI + volume
        token_list     = token_list,
    )


def _on_data(wsapp, tick: dict) -> None:
    """
    Called on every incoming tick. Updates the in-memory store.
    SnapQuote field names (confirmed from raw tick inspection):
        token                    — integer, must str() to match _token_map
        last_traded_price        — in paise, divide by 100
        open_interest            — in contracts ✅
        volume_trade_for_the_day — total day volume ✅
        open_price_of_the_day, high_price_of_the_day, low_price_of_the_day
        closed_price             — previous close, in paise
    """
    token = str(tick.get("token", ""))   # WebSocket sends int, map has str keys
    if not token or token not in _token_map:
        return

    with _tick_lock:
        _tick_store[token] = {
            "ltp":    tick.get("last_traded_price", 0) / 100,
            "oi":     tick.get("open_interest", 0),
            "volume": tick.get("volume_trade_for_the_day", 0),
            "open":   tick.get("open_price_of_the_day", 0) / 100,
            "high":   tick.get("high_price_of_the_day", 0) / 100,
            "low":    tick.get("low_price_of_the_day", 0) / 100,
            "ts":     datetime.now(timezone.utc).isoformat(),
        }


def _on_error(wsapp, error) -> None:
    global _connected
    _connected = False
    logger.error("WebSocket error: %s", error)


def _on_close(wsapp, close_status_code=None, close_msg=None) -> None:
    global _connected
    _connected = False
    logger.warning("WebSocket closed — code=%s msg=%s", close_status_code, close_msg)

    # Auto-reconnect after 60 seconds
    import threading
    from config import SYMBOLS
    def _reconnect():
        import time
        time.sleep(60)
        logger.info("Attempting manual reconnect...")
        try:
            start_feed(SYMBOLS)
        except Exception as e:
            logger.error("Reconnect failed: %s", e)
    threading.Thread(target=_reconnect, daemon=True, name="ws-reconnect").start()

# ── 5-min flush to SQLite ─────────────────────────────────────────────────────

def flush_to_db(symbol: str) -> list[dict]:
    """
    Called by the scheduler every 5 min.
    Takes a snapshot of the tick store and writes rows to SQLite.
    Returns the list of rows written.
    """
    if not _tick_store:
        logger.warning("flush_to_db: tick store is empty — WebSocket may not have data yet")
        return []

    # Get current spot
    try:
        client = _af._get_client()
        spot = _af._get_spot(client, symbol)
        if not spot:  # session expired, re-login
            logger.warning("Spot fetch failed, re-logging in...")
            client = _af._login()
            spot = _af._get_spot(client, symbol)
    except Exception as e:
        logger.error("Could not fetch spot price: %s", e)
        spot = None

    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    prev_oi   = get_latest_oi_snapshot(symbol)

    rows = []
    with _tick_lock:
        snapshot = dict(_tick_store)  # copy while holding lock

    for token, tick in snapshot.items():
        scrip = _token_map.get(token)
        if not scrip or scrip.get("name") != symbol:
            continue

        oi     = tick.get("oi")     or 0
        volume = tick.get("volume") or 0
        ltp    = tick.get("ltp")
        iv     = _compute_iv(
            ltp    = ltp,
            spot   = spot,
            strike = scrip["strike"],
            expiry = scrip["expiry"],
            opt    = scrip["option_type"],
        )

        key       = (scrip["strike"], scrip["option_type"], scrip["expiry"])
        prev      = prev_oi.get(key)
        oi_change = (oi - prev) if (oi is not None and prev is not None) else None

        rows.append({
            "timestamp":   timestamp,
            "symbol":      symbol,
            "expiry":      scrip["expiry"],
            "strike":      scrip["strike"],
            "option_type": scrip["option_type"],
            "oi":          oi,
            "oi_change":   oi_change,
            "volume":      volume,
            "iv":          iv,
            "last_price":  ltp,
            "spot_price":  spot,
        })

    if rows:
        insert_options_rows(rows)
        logger.info("[%s] %s — %d rows flushed to DB (spot ₹%.2f)",
                    timestamp, symbol, len(rows), spot or 0)
    return rows


# ── Black-Scholes IV (Newton-Raphson) ────────────────────────────────────────

def _compute_iv(
    ltp:    float | None,
    spot:   float | None,
    strike: float,
    expiry: str,
    opt:    str,
    r:      float = 0.065,   # Indian risk-free rate ~6.5%
) -> float | None:
    """
    Estimate IV from option price using Newton-Raphson on Black-Scholes.
    Returns IV as a percentage (e.g. 18.5 means 18.5%), or None if it fails.
    """
    if not ltp or not spot or ltp <= 0 or spot <= 0:
        return None
    try:
        today = date.today()
        exp   = date.fromisoformat(expiry)
        T     = (exp - today).days / 365.0
        if T <= 0:
            return None

        S, K = spot, strike
        is_call = (opt == "CE")

        # Newton-Raphson — start at 30% vol
        sigma = 0.30
        for _ in range(50):
            price = _bs_price(S, K, T, r, sigma, is_call)
            vega  = _bs_vega(S, K, T, r, sigma)
            if vega < 1e-8:
                break
            sigma = sigma - (price - ltp) / vega
            if sigma <= 0:
                return None
            if abs(price - ltp) < 0.01:
                break

        iv_pct = round(sigma * 100, 2)
        return iv_pct if 1 < iv_pct < 300 else None

    except Exception:
        return None


def _bs_price(S, K, T, r, sigma, is_call: bool) -> float:
    d1 = (math.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * math.sqrt(T))
    d2 = d1 - sigma * math.sqrt(T)
    if is_call:
        return S * _norm_cdf(d1) - K * math.exp(-r * T) * _norm_cdf(d2)
    else:
        return K * math.exp(-r * T) * _norm_cdf(-d2) - S * _norm_cdf(-d1)


def _bs_vega(S, K, T, r, sigma) -> float:
    d1 = (math.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * math.sqrt(T))
    return S * math.sqrt(T) * _norm_pdf(d1)


def _norm_cdf(x: float) -> float:
    return (1.0 + math.erf(x / math.sqrt(2.0))) / 2.0


def _norm_pdf(x: float) -> float:
    return math.exp(-0.5 * x ** 2) / math.sqrt(2 * math.pi)


# ── Feed health check ─────────────────────────────────────────────────────────

def is_connected() -> bool:
    return _connected

def tick_count() -> int:
    return len(_tick_store)