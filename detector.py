# detector.py — Options Flow Anomaly Detection Engine for OFA Cloud Collector
#
# Real-world signal logic:
#   OI_BUILDUP  : oi_change > 0 + volume confirms → new positions being built
#   OI_UNWIND   : oi_change < 0 + volume confirms → positions being closed
#   VOLUME_SPIKE: high volume but flat OI → intraday speculation / day traders
#   IV_SPIKE    : IV significantly above session average → someone paying premium
#
# Modes:
#   BOOTSTRAP (<20 trading days): intraday percentile rank thresholds
#   FULL      (>=20 trading days): rolling average thresholds

import logging
from collections import defaultdict
from datetime import datetime, timezone, date
import numpy as np

import db
import db_supabase

logger = logging.getLogger(__name__)

# ── Thresholds ─────────────────────────────────────────────────────────────────
BOOTSTRAP_OI_PERCENTILE     = 90    # top 10% of today's oi_change values
BOOTSTRAP_VOLUME_PERCENTILE = 95    # top 5% of today's volume values
BOOTSTRAP_IV_PERCENTILE     = 90    # top 10% of today's IV values

FULL_OI_MULTIPLIER     = 2.0   # oi_change > 2x average
FULL_VOLUME_ZSCORE     = 3.0   # volume > mean + 3σ
FULL_IV_MULTIPLIER     = 1.5   # iv > 1.5x session average

DEEP_ITM_PCT            = 0.10  # >10% away from spot = deep ITM
MIN_DTE                 = 0     # 0 = analyze today's expiry contracts
MIN_OI                  = 1000  # skip strikes with negligible OI
MIN_VOLUME              = 500   # skip strikes with negligible volume
VOLUME_OI_CONFIRM_RATIO = 0.01  # volume > 1% of OI = real flow
FULL_MODE_MIN_DAYS      = 20
BOOTSTRAP_STRENGTH_SCALE = 5.0


def _pre_filter(rows: list[dict], spot: float) -> list[dict]:
    """
    Remove rows that shouldn't be analysed:
    1. Restrict strictly to nearest active weekly expiry (matches market context walls)
    2. oi_change IS NULL (first flush of day)
    3. Deep ITM (>10% from spot)
    4. Expiry in the past (< today)
    5. Negligible OI or volume
    """
    today = date.today()
    today_str = today.isoformat()

    # Restrict detector strictly to nearest active weekly contract (no mixing with monthly/far-dated)
    expiries = sorted(list(set([r["expiry"] for r in rows if r.get("expiry")])))
    valid_expiries = [e for e in expiries if e >= today_str]
    target_expiry = valid_expiries[0] if valid_expiries else (expiries[0] if expiries else None)

    filtered = []
    for r in rows:
        if target_expiry and r.get("expiry") != target_expiry:
            continue

        if r.get("oi_change") is None:
            continue

        strike = float(r.get("strike", 0))
        if spot and abs(strike - spot) / spot > DEEP_ITM_PCT:
            continue

        try:
            exp = date.fromisoformat(r["expiry"])
            if (exp - today).days < 0:
                continue
        except (ValueError, TypeError):
            pass

        if (r.get("oi") or 0) < MIN_OI:
            continue

        filtered.append(r)

    return filtered


NOISE_PRICE_CHANGE_PCT   = 0.02  # 2.0% minimum relative premium move to filter theta/noise


def _percentile_strength(value: float, population: list[float]) -> float:
    if not population:
        return 1.0
    pct = float(np.mean([v <= value for v in population])) * 100.0
    strength = 1.0 + (pct / 100.0) * (BOOTSTRAP_STRENGTH_SCALE - 1.0)
    return round(float(strength), 2)


def _build_signal(fired_at: str, symbol: str, row: dict, signal_type: str,
                  strength: float, bias: str, mode: str) -> dict:
    exp_str = row.get("expiry")
    dte = 0
    if exp_str:
        try:
            exp_date = date.fromisoformat(str(exp_str)[:10])
            fired_date = datetime.fromisoformat(fired_at.replace("Z", "+00:00")).date()
            dte = max(0, (exp_date - fired_date).days)
        except Exception:
            dte = 0
    is_expiry_day = (dte == 0)
    entry_prem = float(row.get("last_price") or 0)

    return {
        "fired_at":        fired_at,
        "symbol":          symbol,
        "expiry":          row.get("expiry"),
        "strike":          float(row.get("strike", 0)),
        "option_type":     row.get("option_type"),
        "signal_type":     signal_type,
        "signal_strength": strength,
        "oi_change":       row.get("oi_change"),
        "volume":          row.get("volume"),
        "iv":              row.get("iv"),
        "spot_price":      row.get("spot_price"),
        "bias":            bias,
        "mode":            mode,
        "entry_premium":   round(entry_prem, 2) if entry_prem > 0 else None,
        "dte":             dte,
        "is_expiry_day":   is_expiry_day,
        "target_pct":      0.30,
        "stop_pct":        -0.20,
        "is_exploratory":  False,
        "logic_version":   "v2_quadrant",
        "llm_thesis":      None,
        "llm_bias":        None,
        "llm_confidence":  None,
        "news_summary_id": None,
    }


def _detect_oi_flow(rows, symbol, spot, today, now_utc, mode, intraday_rows, prior_snapshot_map=None):
    signals = []
    all_changes = [
        abs(r["oi_change"]) for r in intraday_rows
        if r.get("oi_change") is not None and r["oi_change"] != 0
    ]
    if len(all_changes) < 5:
        # If insufficient intraday flushes yet, fallback to percentiles of current rows
        all_changes = [
            abs(r["oi_change"]) for r in rows
            if r.get("oi_change") is not None and r["oi_change"] != 0
        ]
        if len(all_changes) < 5:
            return []

    threshold = float(np.percentile(all_changes, BOOTSTRAP_OI_PERCENTILE))

    for r in rows:
        oi_change = r.get("oi_change")
        volume = r.get("volume") or 0
        oi = r.get("oi") or 0
        opt_type = r.get("option_type")
        strike = float(r.get("strike") or 0)
        expiry = r.get("expiry")

        if not oi_change or oi_change == 0:
            continue

        volume_confirms = volume >= max(MIN_VOLUME, oi * VOLUME_OI_CONFIRM_RATIO)
        abs_change = abs(oi_change)

        if abs_change < threshold or not volume_confirms:
            continue

        # Optional volume delta confirmation if available
        vol_delta = r.get("volume_delta")
        if vol_delta is not None and vol_delta <= 0:
            continue

        strength = _percentile_strength(abs_change, all_changes)

        # 4-Quadrant Bias Engine:
        # Determine relative price change over the 5-min window
        price_change_pct = r.get("price_change_pct")
        if price_change_pct is None:
            prev_price = r.get("prev_price")
            if prev_price is None and prior_snapshot_map:
                prev_data = prior_snapshot_map.get((strike, opt_type, expiry))
                if prev_data:
                    prev_price = prev_data.get("last_price") if isinstance(prev_data, dict) else prev_data
            curr_ltp = float(r.get("last_price") or 0)
            if prev_price and prev_price > 0 and curr_ltp > 0:
                price_change_pct = (curr_ltp - prev_price) / prev_price

        # Quadrant classification with noise threshold
        if price_change_pct is None or abs(price_change_pct) < NOISE_PRICE_CHANGE_PCT:
            # Noise / flat move -> non-directional
            bias = "NEUTRAL"
            signal_type = "OI_BUILDUP" if oi_change > 0 else "OI_UNWIND"
        elif oi_change > 0:
            signal_type = "OI_BUILDUP"
            if price_change_pct >= NOISE_PRICE_CHANGE_PCT:
                # Long Buildup: Call Buying (Bullish), Put Buying (Bearish)
                bias = "BULLISH" if opt_type == "CE" else "BEARISH"
            else:
                # Short Buildup: Call Writing (Bearish), Put Writing (Bullish)
                bias = "BEARISH" if opt_type == "CE" else "BULLISH"
        else:  # oi_change < 0
            signal_type = "OI_UNWIND"
            if price_change_pct >= NOISE_PRICE_CHANGE_PCT:
                # Short Covering: Call Short Covering (Bullish), Put Short Covering (Bearish)
                bias = "BULLISH" if opt_type == "CE" else "BEARISH"
            else:
                # Long Unwinding: Call Long Unwinding (Bearish), Put Long Unwinding (Bullish)
                bias = "BEARISH" if opt_type == "CE" else "BULLISH"

        signals.append(_build_signal(now_utc, symbol, r, signal_type, strength, bias, mode))

    return signals


def _detect_volume_spike(rows, symbol, spot, today, now_utc, mode, intraday_rows):
    signals = []
    all_volumes = [
        r["volume"] for r in intraday_rows
        if r.get("volume") is not None and r["volume"] > 0
    ]
    if len(all_volumes) < 5:
        all_volumes = [
            r["volume"] for r in rows
            if r.get("volume") is not None and r["volume"] > 0
        ]
        if len(all_volumes) < 5:
            return []

    threshold = float(np.percentile(all_volumes, BOOTSTRAP_VOLUME_PERCENTILE))

    for r in rows:
        volume = r.get("volume") or 0
        oi_change = r.get("oi_change") or 0

        if volume < MIN_VOLUME:
            continue

        # If OI change is large, it's an OI_BUILDUP, not a pure volume spike
        if abs(oi_change) > volume * 0.3:
            continue

        if volume < threshold:
            continue

        strength = _percentile_strength(volume, all_volumes)
        signals.append(_build_signal(now_utc, symbol, r, "VOLUME_SPIKE", strength, "NEUTRAL", mode))

    return signals


def _detect_iv_spike(rows, symbol, spot, today, now_utc, intraday_rows):
    signals = []
    iv_rows = [r for r in rows if r.get("iv") is not None and r["iv"] > 0]
    if len(iv_rows) < 8:
        return []

    iv_by_group = defaultdict(list)
    for r in iv_rows:
        key = (r.get("expiry"), r.get("option_type"))
        iv_by_group[key].append(r["iv"])

    intra_iv = [r["iv"] for r in intraday_rows if r.get("iv") and r["iv"] > 0]

    for r in rows:
        iv = r.get("iv")
        if not iv or iv <= 0:
            continue

        key = (r.get("expiry"), r.get("option_type"))
        group_ivs = iv_by_group.get(key, [])
        if len(group_ivs) < 4:
            continue

        group_mean = float(np.mean(group_ivs))
        if group_mean <= 0:
            continue

        iv_ratio = iv / group_mean
        if iv_ratio < FULL_IV_MULTIPLIER:
            continue

        # Meaningful IV spikes are usually near ATM
        strike = float(r.get("strike", 0))
        if spot and abs(strike - spot) / spot > 0.05:
            continue

        strength = _percentile_strength(iv, intra_iv) if intra_iv else round(float(iv_ratio), 2)
        bias = "BULLISH" if r.get("option_type") == "CE" else "BEARISH"

        signals.append(_build_signal(now_utc, symbol, r, "IV_SPIKE", strength, bias, "BOOTSTRAP"))

    return signals


def _remove_offsetting_signals(signals: list[dict]) -> list[dict]:
    buildup_strikes = defaultdict(list)
    for sig in signals:
        if sig["signal_type"] == "OI_BUILDUP":
            key = (sig["strike"], sig["expiry"])
            buildup_strikes[key].append(sig)

    offsetting_keys = {k for k, v in buildup_strikes.items() if len(v) >= 2}
    if offsetting_keys:
        for k in offsetting_keys:
            logger.info("Removing offsetting CE+PE buildup at strike=%.0f expiry=%s (spread)", k[0], k[1])

    return [s for s in signals if not (s["signal_type"] == "OI_BUILDUP" and (s["strike"], s["expiry"]) in offsetting_keys)]


# ── Main Entry Point ───────────────────────────────────────────────────────────

def run_detection(symbol: str, current_rows: list[dict] | None = None,
                  prior_snapshot_map: dict | None = None) -> list[dict]:
    """
    Run full anomaly detection pipeline for a symbol.
    Persists new signals into both Supabase and local SQLite.
    Returns list of newly fired signal dicts (with 'id' set).
    """
    today = date.today().isoformat()
    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    # Mode selection based on Supabase trading days
    try:
        trading_days = db_supabase.count_trading_days_supabase(symbol)
    except Exception:
        trading_days = 1
    mode = "FULL" if trading_days >= FULL_MODE_MIN_DAYS else "BOOTSTRAP"

    if not current_rows:
        current_rows = db.get_today_snapshot(symbol, today)

    if not current_rows:
        logger.warning("[%s] No rows for today — skipping detection", symbol)
        return []

    spot = next((r["spot_price"] for r in current_rows if r.get("spot_price")), None)
    if not spot:
        logger.warning("[%s] No spot price available — skipping detection", symbol)
        return []

    filtered = _pre_filter(current_rows, spot)
    logger.info("[%s] Detection run — mode=%s, %d filtered rows (from %d total)",
                symbol, mode, len(filtered), len(current_rows))

    if not filtered:
        return []

    intraday_rows = db.get_intraday_oi_changes(symbol, today)

    if prior_snapshot_map is None:
        try:
            prior_snapshot_map = db_supabase.get_latest_snapshot_supabase(symbol)
        except Exception:
            prior_snapshot_map = {}

    # Detect anomalies
    candidates = []
    candidates += _detect_oi_flow(filtered, symbol, spot, today, now_utc, mode, intraday_rows, prior_snapshot_map)
    candidates += _detect_volume_spike(filtered, symbol, spot, today, now_utc, mode, intraday_rows)
    candidates += _detect_iv_spike(filtered, symbol, spot, today, now_utc, intraday_rows)

    # Filter offsetting spreads
    candidates = _remove_offsetting_signals(candidates)

    inserted_signals = []
    for sig in candidates:
        # Check duplicate in Supabase and local SQLite
        already_fired_cloud = db_supabase.signal_already_fired_today_supabase(
            symbol, sig["strike"], sig["option_type"], sig["signal_type"], today
        )
        already_fired_local = db.signal_already_fired_today_local(
            symbol, sig["strike"], sig["option_type"], sig["signal_type"], today
        )

        if already_fired_cloud or already_fired_local:
            logger.debug("Skipping duplicate signal: %s %s %.0f %s",
                         sig["signal_type"], symbol, sig["strike"], sig["option_type"])
            continue

        # Insert into local SQLite
        local_id = db.insert_signal_local(sig)

        # Insert into Supabase
        cloud_id = db_supabase.insert_signal_supabase(sig)

        # Store primary ID (cloud_id preferred for Supabase updates, local_id fallback)
        sig["id"] = cloud_id or local_id
        sig["local_id"] = local_id
        sig["cloud_id"] = cloud_id

        inserted_signals.append(sig)

    logger.info("[%s] %d new signals fired and stored", symbol, len(inserted_signals))
    return inserted_signals
