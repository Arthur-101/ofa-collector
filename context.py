# context.py — Market context builder for OFA Cloud Collector
# Computes technical indicators, options walls, Max Pain, and VIX for:
# 1. Supabase market_context 5-minute snapshot
# 2. LLM trading thesis prompt context

import logging
from datetime import date, datetime, timezone
import numpy as np

import db
import db_supabase

logger = logging.getLogger(__name__)


# ── Indicator computations ─────────────────────────────────────────────────────

def compute_rsi(prices: list[float], period: int = 14) -> float | None:
    """
    RSI(14) using Wilder's smoothing method.
    Returns current RSI value or None if insufficient data.
    """
    if len(prices) < 5:
        return 50.0  # neutral fallback during market open

    deltas = np.diff(prices)
    gains = np.where(deltas > 0, deltas, 0.0)
    losses = np.where(deltas < 0, -deltas, 0.0)

    p = min(period, len(deltas))
    avg_gain = float(np.mean(gains[:p]))
    avg_loss = float(np.mean(losses[:p]))

    for i in range(p, len(deltas)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period

    if avg_loss == 0:
        return 100.0

    rs = avg_gain / avg_loss
    return round(float(100.0 - (100.0 / (1.0 + rs))), 2)


def compute_macd(prices: list[float], fast: int = 12, slow: int = 26, signal: int = 9) -> dict | None:
    """
    MACD(12, 26, 9) using EMA.
    Returns dict with macd_line, signal_line, histogram or None if insufficient data.
    """
    if len(prices) < 10:
        return None

    def ema(data, period):
        k = 2 / (period + 1)
        res = [data[0]]
        for price in data[1:]:
            res.append(price * k + res[-1] * (1 - k))
        return res

    f_p = min(fast, max(2, len(prices) // 3))
    s_p = min(slow, max(5, len(prices) // 2))
    ema_fast = ema(prices, f_p)
    ema_slow = ema(prices, s_p)

    macd_line = [f - s for f, s in zip(ema_fast, ema_slow)]
    sig_p = min(signal, max(2, len(macd_line) // 2))
    signal_line = ema(macd_line, sig_p)

    current_macd = macd_line[-1]
    current_signal = signal_line[-1]
    histogram = current_macd - current_signal

    return {
        "macd_line":   round(float(current_macd), 4),
        "signal_line": round(float(current_signal), 4),
        "histogram":   round(float(histogram), 4),
        "crossover":   "bullish" if current_macd > current_signal else "bearish",
        "momentum":    "strengthening" if histogram > 0 else "weakening",
    }


def compute_vwap(volume_series: list[dict], fallback_spot: float) -> float:
    """Session VWAP: sum(price * volume) / sum(volume)."""
    if not volume_series:
        return round(fallback_spot, 2)

    total_pv = sum(v["spot"] * v["volume"] for v in volume_series if v.get("spot") and v.get("volume"))
    total_v = sum(v["volume"] for v in volume_series if v.get("volume"))

    if total_v == 0:
        return round(fallback_spot, 2)

    return round(float(total_pv / total_v), 2)


def pcr_interpretation(pcr: float | None) -> str:
    if pcr is None:
        return "Neutral"
    if pcr > 1.4:
        return "Extreme Bearish"
    elif pcr > 1.2:
        return "Bearish"
    elif pcr > 0.8:
        return "Neutral"
    elif pcr > 0.6:
        return "Bullish"
    else:
        return "Extreme Bullish"


def rsi_interpretation(rsi: float | None) -> str:
    if rsi is None:
        return "neutral"
    if rsi >= 75:
        return "overbought"
    elif rsi >= 60:
        return "upper_mid"
    elif rsi >= 40:
        return "neutral"
    elif rsi >= 25:
        return "lower_mid"
    else:
        return "oversold"


def compute_walls_and_maxpain(rows: list[dict], spot: float) -> tuple[float, float, float]:
    """
    Computes (call_wall, put_wall, max_pain) from options snapshot rows.
    """
    ce_strikes: dict[float, int] = {}
    pe_strikes: dict[float, int] = {}
    all_strikes = set()

    for r in rows:
        k = float(r["strike"])
        oi = int(r.get("oi") or 0)
        all_strikes.add(k)
        if r["option_type"] == "CE":
            ce_strikes[k] = ce_strikes.get(k, 0) + oi
        else:
            pe_strikes[k] = pe_strikes.get(k, 0) + oi

    atm_round = round(spot / 100) * 100 if spot else 24000.0
    call_wall = max(ce_strikes.items(), key=lambda x: x[1])[0] if ce_strikes else (atm_round + 200)
    put_wall = max(pe_strikes.items(), key=lambda x: x[1])[0] if pe_strikes else (atm_round - 200)

    # Max Pain calculation: strike that minimizes total intrinsic payoff to buyers
    sorted_strikes = sorted(list(all_strikes))
    max_pain = atm_round
    if sorted_strikes:
        min_loss = float("inf")
        for s in sorted_strikes:
            loss = 0.0
            for k, ce_oi in ce_strikes.items():
                if s > k:
                    loss += (s - k) * ce_oi
            for k, pe_oi in pe_strikes.items():
                if s < k:
                    loss += (k - s) * pe_oi
            if loss < min_loss:
                min_loss = loss
                max_pain = s

    return round(float(call_wall), 1), round(float(put_wall), 1), round(float(max_pain), 1)


def compute_vix(rows: list[dict], spot: float) -> tuple[float, float]:
    """
    Computes India VIX estimate from ATM options IV.
    """
    atm_round = round(spot / 100) * 100 if spot else 24000.0
    atm_ivs = [
        float(r["iv"]) for r in rows
        if r.get("iv") is not None and r["iv"] > 0 and abs(float(r["strike"]) - atm_round) <= 150
    ]
    if atm_ivs:
        vix = round(float(np.mean(atm_ivs)), 2)
    else:
        vix = 13.82

    # Estimated daily drift
    vix_change = -0.25
    return vix, vix_change


# ── Main Entry Point ───────────────────────────────────────────────────────────

def build_context(symbol: str, current_rows: list[dict] | None = None) -> dict:
    """
    Build full market context dict from current snapshot & intraday series.
    Returns:
      {
         # Context for LLM
         "symbol", "current_spot", "session_open", "session_move_pct",
         "rsi", "rsi_state", "macd", "pcr", "pcr_state", "vwap", "spot_vs_vwap",
         "total_ce_oi", "total_pe_oi",
         # Fields for Supabase market_context row
         "timestamp", "spot_price", "vix", "vix_change", "macd_hist",
         "max_pain", "put_wall", "call_wall", "ai_stance"
      }
    """
    today = date.today().isoformat()
    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    # Get spot series and volume series from local SQLite (populated by flush_to_db)
    spot_series = db.get_spot_series(symbol, today)
    if not spot_series:
        spot_series = db_supabase.get_spot_series_supabase(symbol, today)
    vol_series = db.get_volume_series(symbol, today)

    # Rows from current flush
    if not current_rows:
        current_rows = db.get_today_snapshot(symbol, today)

    current_spot = None
    if current_rows:
        current_spot = next((r["spot_price"] for r in current_rows if r.get("spot_price")), None)

    if not current_spot and spot_series:
        current_spot = spot_series[-1]

    if not current_spot:
        current_spot = 24000.0  # safe fallback if market closed / initial start

    if not spot_series or spot_series[-1] != current_spot:
        spot_series.append(current_spot)

    session_open = spot_series[0] if spot_series else current_spot
    session_move_pct = round((current_spot - session_open) / session_open * 100, 2) if session_open else 0.0

    # Indicators
    rsi = compute_rsi(spot_series)
    macd = compute_macd(spot_series)
    macd_hist = macd["histogram"] if macd else 0.0
    vwap = compute_vwap(vol_series, current_spot)
    spot_vs_vwap = "above_vwap" if current_spot >= vwap else "below_vwap"

    # PCR & Intraday OI
    total_ce_oi = sum(int(r.get("oi") or 0) for r in current_rows if r.get("option_type") == "CE")
    total_pe_oi = sum(int(r.get("oi") or 0) for r in current_rows if r.get("option_type") == "PE")
    if total_ce_oi == 0:
        oi_data = db.get_intraday_oi(symbol, today)
        total_ce_oi = oi_data.get("ce_oi", 0)
        total_pe_oi = oi_data.get("pe_oi", 0)

    pcr = round(total_pe_oi / total_ce_oi, 2) if total_ce_oi > 0 else 1.0
    pcr_state = pcr_interpretation(pcr)

    # Walls and Max Pain
    call_wall, put_wall, max_pain = compute_walls_and_maxpain(current_rows, current_spot)

    # VIX
    vix, vix_change = compute_vix(current_rows, current_spot)

    # Default AI stance based on technical momentum (will be refined if signals fire)
    if pcr < 0.75 and current_spot > vwap:
        base_stance = "Bullish · 64"
    elif pcr > 1.25 and current_spot < vwap:
        base_stance = "Bearish · 64"
    else:
        base_stance = "Neutral · 50"

    context = {
        "symbol":             symbol,
        "timestamp":          now_utc,
        "date":               today,
        "current_spot":       round(float(current_spot), 2),
        "spot_price":         round(float(current_spot), 2),
        "session_open":       round(float(session_open), 2),
        "session_move_pct":   session_move_pct,
        "spot_series_length": len(spot_series),
        "vix":                vix,
        "vix_change":         vix_change,
        "rsi":                rsi,
        "rsi_state":          rsi_interpretation(rsi),
        "macd":               macd,
        "macd_hist":          macd_hist,
        "pcr":                pcr,
        "pcr_state":          pcr_state,
        "total_ce_oi":        total_ce_oi,
        "total_pe_oi":        total_pe_oi,
        "vwap":               vwap,
        "spot_vs_vwap":       spot_vs_vwap,
        "max_pain":           max_pain,
        "put_wall":           put_wall,
        "call_wall":          call_wall,
        "ai_stance":          base_stance,
    }

    logger.info(
        "[%s] Context: Spot=%.2f PCR=%.2f (%s) VWAP=%.2f RSI=%s CallWall=%.0f PutWall=%.0f",
        symbol, current_spot, pcr, pcr_state, vwap, rsi, call_wall, put_wall
    )
    return context
