# cloud_pipeline.py — Master Autonomous Cloud Analysis Pipeline for Render
#
# Runs every 5 minutes immediately after flush_to_db():
# 1. Calculates Market Context (Spot, VIX, PCR, VWAP, RSI, MACD, Walls, Max Pain)
# 2. Runs Anomaly Detection (OI Buildup, OI Unwind, Volume Spike, IV Spike)
# 3. Fetches Live News (ET Markets, Moneycontrol)
# 4. Generates AI Thesis via OpenRouter 7-Model Fallback Chain
# 5. Persists enriched outputs directly into Supabase (market_context, signals, news_raw)

import logging
from datetime import datetime, timezone
import db_supabase
from context import build_context
from detector import run_detection
from news_fetcher import fetch_news
from llm_engine import generate_theses

logger = logging.getLogger(__name__)


def compute_ai_stance(signals: list[dict], base_stance: str) -> str:
    """
    Computes unified AI Stance (e.g. "Bullish · 68") combining
    quantitative signal strength and qualitative LLM conviction stars.
    """
    if not signals:
        return base_stance

    # Pick the most confident thesis from current signals
    thesis_signals = [s for s in signals if s.get("llm_bias") and s.get("llm_confidence")]
    if not thesis_signals:
        return base_stance

    top_sig = max(thesis_signals, key=lambda s: (s.get("llm_confidence") or 0, s.get("signal_strength") or 0))
    bias = (top_sig.get("llm_bias") or "NEUTRAL").capitalize()
    confidence = top_sig.get("llm_confidence") or 3
    strength = top_sig.get("signal_strength") or 3.0

    # Score from 0 to 100
    raw_score = int(round((strength * 12.0) + (confidence * 8.0)))
    score = max(35, min(95, raw_score))

    return f"{bias} · {score}"


def run_cloud_analysis(symbol: str = "NIFTY", current_rows: list[dict] | None = None) -> dict:
    """
    Execute the complete 5-minute cloud intelligence cycle.
    Called by scheduler._poll_job() after flush_to_db().
    """
    start_time = datetime.now(timezone.utc)
    logger.info("══════════════════════════════════════════════════════════")
    logger.info("🚀 Starting Cloud Intelligence Cycle for %s", symbol)
    logger.info("══════════════════════════════════════════════════════════")

    # 1. Build Market Context
    ctx = build_context(symbol, current_rows)

    # 2. Run Anomaly Detection
    new_signals = run_detection(symbol, current_rows)
    logger.info("[%s] %d new anomaly signals flagged", symbol, len(new_signals))

    theses_count = 0
    if new_signals:
        # 3. Fetch News
        headlines = fetch_news(symbol)

        # 4. Generate AI Theses
        theses_count = generate_theses(new_signals, ctx, headlines)
        logger.info("[%s] %d AI theses generated", symbol, theses_count)

        # 5. Update AI Stance in context
        ctx["ai_stance"] = compute_ai_stance(new_signals, ctx.get("ai_stance", "Neutral · 50"))

    # 6. Persist 1-row Market Context to Supabase
    context_row = {
        "timestamp":   ctx["timestamp"],
        "symbol":      symbol,
        "spot_price":  ctx["spot_price"],
        "vix":         ctx.get("vix"),
        "vix_change":  ctx.get("vix_change"),
        "pcr":         ctx.get("pcr"),
        "pcr_state":   ctx.get("pcr_state"),
        "vwap":        ctx.get("vwap"),
        "rsi":         ctx.get("rsi"),
        "macd_hist":   ctx.get("macd_hist"),
        "max_pain":    ctx.get("max_pain"),
        "put_wall":    ctx.get("put_wall"),
        "call_wall":   ctx.get("call_wall"),
        "ai_stance":   ctx.get("ai_stance"),
    }

    try:
        db_supabase.insert_market_context_supabase(context_row)
        logger.info("✅ Supabase: market_context snapshot successfully saved")
    except Exception as e:
        logger.error("❌ Failed to save market_context to Supabase: %s", e)

    # 7. Real-Time Signal Resolution (Monitor open targets, stops, MFE/MAE)
    try:
        from resolver import resolve_active_signals
        resolve_active_signals(symbol, current_rows, ctx["timestamp"])
    except Exception as e:
        logger.error("❌ Signal resolution failed: %s", e)

    duration = (datetime.now(timezone.utc) - start_time).total_seconds()
    logger.info(
        "✨ Cloud cycle completed in %.2fs — Spot: ₹%.2f | PCR: %.2f | Stance: %s | New Signals: %d | Theses: %d",
        duration, ctx["spot_price"], ctx.get("pcr") or 0, ctx.get("ai_stance"), len(new_signals), theses_count
    )

    return {
        "status": "success",
        "symbol": symbol,
        "duration_seconds": duration,
        "spot_price": ctx["spot_price"],
        "pcr": ctx.get("pcr"),
        "ai_stance": ctx.get("ai_stance"),
        "new_signals_count": len(new_signals),
        "theses_count": theses_count,
    }
