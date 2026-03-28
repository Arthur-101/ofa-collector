# angel_fetcher.py — Angel One SmartAPI options chain fetcher
#
# Token source: Angel One instrument master JSON (daily download)
#   → fetch_instruments.py writes nifty_instruments.json
#   → This file is refreshed once at startup and cached in memory
#
# Data per poll cycle:
#   - ltpData        → LTP, open, high, low, close per token (free tier ✅)
#   - getCandleData  → 5-min OHLCV volume per token (free tier ✅)
#   - OI / IV        → None (requires paid market data feed)

import os
import re
import json
import time
import logging
import requests
import pyotp
from datetime import date, timedelta, datetime, timezone
from dotenv import load_dotenv
from SmartApi import SmartConnect

from db import get_latest_oi_snapshot, insert_options_rows

load_dotenv()
logger = logging.getLogger(__name__)

API_KEY     = os.getenv("ANGEL_API_KEY")
CLIENT_ID   = os.getenv("ANGEL_CLIENT_ID")
PASSWORD    = os.getenv("ANGEL_PASSWORD")
TOTP_SECRET = os.getenv("ANGEL_TOTP_SECRET")

MIN_DTE    = 2     # skip expiries expiring within 2 calendar days
STRIKE_PCT = 0.10  # keep strikes within ±10% of spot
NUM_EXPIRIES = 2   # how many active expiries to track

INSTRUMENT_URL = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"
INSTRUMENT_CACHE = "nifty_instruments.json"

SYMBOL_RE = re.compile(
    r"^(?P<n>NIFTY|BANKNIFTY)"
    r"(?P<dd>\d{2})(?P<mon>[A-Z]{3})(?P<yy>\d{2})"
    r"(?P<strike>\d+)"
    r"(?P<otype>CE|PE)$"
)
MONTH_MAP = {
    "JAN":"01","FEB":"02","MAR":"03","APR":"04","MAY":"05","JUN":"06",
    "JUL":"07","AUG":"08","SEP":"09","OCT":"10","NOV":"11","DEC":"12"
}

# ── Session ───────────────────────────────────────────────────────────────────
_client: SmartConnect | None = None

def _get_client() -> SmartConnect:
    global _client
    if _client is None:
        _client = _login()
    return _client

def _login() -> SmartConnect:
    global _client
    logger.info("Logging in to Angel One …")
    totp = pyotp.TOTP(TOTP_SECRET).now()
    obj  = SmartConnect(api_key=API_KEY)
    resp = obj.generateSession(CLIENT_ID, PASSWORD, totp)
    if resp.get("status") is False:
        raise RuntimeError(f"Login failed: {resp.get('message')}")
    _client = obj
    logger.info("Angel One login successful.")
    return obj

def _reset_client() -> None:
    global _client
    _client = None

# ── Instrument master ─────────────────────────────────────────────────────────
_instruments_cache: list[dict] | None = None
_instruments_date:  date | None       = None

def _load_instruments(symbol: str, spot: float) -> list[dict]:
    """
    Load the Angel One instrument master (download fresh once per day).
    Returns filtered list for `symbol` near spot price.
    """
    global _instruments_cache, _instruments_date

    today = date.today()

    # Re-download if cache is stale (new trading day)
    if _instruments_cache is None or _instruments_date != today:
        logger.info("Downloading instrument master from Angel One …")
        try:
            r    = requests.get(INSTRUMENT_URL, timeout=30)
            r.raise_for_status()
            data = r.json()
            logger.info("Instrument master downloaded: %d instruments", len(data))
        except Exception as e:
            logger.warning("Download failed, falling back to local cache: %s", e)
            if os.path.exists(INSTRUMENT_CACHE):
                with open(INSTRUMENT_CACHE) as f:
                    return json.load(f)
            raise RuntimeError("No instrument cache available") from e

        # Parse and store all NIFTY/BANKNIFTY options
        parsed = []
        for item in data:
            if item.get("exch_seg") != "NFO":
                continue
            if item.get("instrumenttype") != "OPTIDX":
                continue
            name = item.get("name", "")
            if name not in ("NIFTY", "BANKNIFTY"):
                continue
            sym = item.get("symbol", "")
            m   = SYMBOL_RE.match(sym)
            if not m:
                continue
            expiry_date = date(
                2000 + int(m.group("yy")),
                int(MONTH_MAP[m.group("mon")]),
                int(m.group("dd"))
            )
            parsed.append({
                "token":       item["token"],
                "symbol":      sym,
                "name":        name,
                "expiry":      expiry_date.isoformat(),
                "expiry_date": expiry_date,
                "strike":      float(item["strike"]) / 100,
                "option_type": m.group("otype"),
            })

        _instruments_cache = parsed
        _instruments_date  = today

        # Write local cache for offline fallback
        with open(INSTRUMENT_CACHE, "w") as f:
            json.dump(
                [{k: v for k, v in p.items() if k != "expiry_date"} for p in parsed],
                f, indent=2
            )

    # Filter to requested symbol, active expiries, ATM strikes
    min_dte = today + timedelta(days=MIN_DTE)
    lo, hi  = spot * (1 - STRIKE_PCT), spot * (1 + STRIKE_PCT)

    filtered = [
        p for p in _instruments_cache
        if p["name"] == symbol
        and p["expiry_date"] > min_dte
        and lo <= p["strike"] <= hi
    ]

    expiries = sorted(set(p["expiry"] for p in filtered))
    near     = set(expiries[:NUM_EXPIRIES])
    return [p for p in filtered if p["expiry"] in near]


# ── Public entry point ────────────────────────────────────────────────────────
def fetch_and_store(symbol: str) -> list[dict]:
    try:
        client = _get_client()
    except Exception as e:
        logger.error("Login failed: %s", e)
        return []

    # Step 1: spot price
    spot = _get_spot(client, symbol)
    if not spot:
        logger.error("Could not get spot price for %s", symbol)
        return []
    logger.info("%s spot: ₹%.2f", symbol, spot)

    # Step 2: load instruments with correct tokens
    scrips = _load_instruments(symbol, spot)
    logger.info("Active scrips (from instrument master): %d", len(scrips))
    if not scrips:
        return []

    # Step 3: fetch LTP for all scrips (includes day volume)
    ltp_data = _fetch_all_ltp(client, scrips)
    logger.info("LTP fetched: %d/%d", len(ltp_data), len(scrips))

    # Step 4: build + store rows
    prev_oi   = get_latest_oi_snapshot(symbol)
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    rows      = _build_rows(scrips, ltp_data, symbol, spot, prev_oi, timestamp)

    if rows:
        insert_options_rows(rows)
        logger.info("[%s] %s — %d rows stored (spot ₹%.2f)",
                    timestamp, symbol, len(rows), spot)
    else:
        logger.warning("[%s] %s — 0 rows built", timestamp, symbol)

    return rows


# ── Spot price ────────────────────────────────────────────────────────────────
_SPOT_TOKENS = {
    "NIFTY":     ("NSE", "Nifty 50",   "99926000"),
    "BANKNIFTY": ("NSE", "Nifty Bank", "99926009"),
}

def _get_spot(client: SmartConnect, symbol: str) -> float | None:
    info = _SPOT_TOKENS.get(symbol)
    if not info:
        return None
    try:
        resp = client.ltpData(info[0], info[1], info[2])
        if resp.get("status"):
            return float(resp["data"]["ltp"])
    except Exception as e:
        logger.warning("Spot fetch failed: %s", e)
    return None


# ── LTP for all scrips ────────────────────────────────────────────────────────
def _fetch_all_ltp(client: SmartConnect, scrips: list[dict]) -> dict:
    """
    Returns {token: data_dict} with ltp, open, high, low, close, volume.
    ltpData response includes day's total traded volume — no getCandleData needed.
    Uses exponential backoff on rate limit errors (AB1019).
    """
    result  = {}
    delay   = 0.35   # start at 350ms — safely under Angel One's ~3 req/sec limit

    for s in scrips:
        retries = 0
        while retries < 3:
            try:
                resp = client.ltpData("NFO", s["symbol"], s["token"])
                if resp.get("status") and resp.get("data"):
                    result[s["token"]] = resp["data"]
                    break
                elif resp.get("errorcode") == "AB1019":
                    # Rate limited — back off and retry
                    wait = delay * (2 ** retries)
                    logger.warning("Rate limited on %s — waiting %.1fs", s["symbol"], wait)
                    time.sleep(wait)
                    retries += 1
                else:
                    logger.debug("ltpData failed for %s: %s", s["symbol"], resp.get("message"))
                    break
            except Exception as e:
                logger.debug("ltpData exception for %s: %s", s["symbol"], e)
                break
        time.sleep(delay)

    return result


# ── Build rows ────────────────────────────────────────────────────────────────
def _build_rows(
    scrips:      list[dict],
    ltp_data:    dict,
    symbol:      str,
    spot_price:  float,
    prev_oi:     dict,
    timestamp:   str,
) -> list[dict]:
    rows = []
    for s in scrips:
        token = s["token"]
        ltp   = ltp_data.get(token, {})

        last_price = _safe_float(ltp.get("ltp"))
        # ltpData returns day total volume directly
        volume     = _safe_int(ltp.get("tradedvolume") or ltp.get("volume"))

        rows.append({
            "timestamp":   timestamp,
            "symbol":      symbol,
            "expiry":      s["expiry"],
            "strike":      s["strike"],
            "option_type": s["option_type"],
            "oi":          None,        # requires paid feed
            "oi_change":   None,        # requires paid feed
            "volume":      volume,      # day total from ltpData ✅
            "iv":          None,        # requires paid feed
            "last_price":  last_price,  # ltpData ✅
            "spot_price":  spot_price,  # index ltpData ✅
        })
    return rows


# ── Type helpers ──────────────────────────────────────────────────────────────
def _safe_float(val) -> float | None:
    try:    return float(val)
    except: return None

def _safe_int(val) -> int | None:
    try:    return int(val)
    except: return None