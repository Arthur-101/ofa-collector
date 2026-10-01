# llm_engine.py — LLM thesis generation for OFA Cloud Collector
#
# Takes a signal + market context + news headlines.
# Returns structured thesis: text, bias, confidence (1-5 stars).
# Writes results back to signals table in Supabase and local SQLite.

import os
import json
import logging
import time
from pathlib import Path
import requests
from dotenv import load_dotenv

import db
import db_supabase

load_dotenv(Path(__file__).parent / ".env")
load_dotenv()
logger = logging.getLogger(__name__)

# ── LLM Configuration ─────────────────────────────────────────────────────────
LLM_API_KEY = os.getenv("LLM_API_KEY") or os.getenv("OPENROUTER_API_KEY")
LLM_API_URL = os.getenv("LLM_API_URL") or os.getenv("OPENROUTER_API_URL", "https://openrouter.ai/api/v1/chat/completions")

_models_raw = os.getenv("LLM_MODELS", "")
LLM_MODELS = [m.strip() for m in _models_raw.split(",") if m.strip()]
if not LLM_MODELS:
    LLM_MODELS = [
        "stealth/space-bunny-alpha",
        "inclusionai/ling-3.0-flash-fin:free",
        "nvidia/nemotron-3.5-lightning:free",
        "nvidia/nemotron-3-ultra-550b-a55b:free",
        "dots-studio/dots-3-note-preview:free",
        "z-ai/glm-5.3-flash",
        "deepseek/deepseek-v4.1-flash",
    ]

_RETRYABLE_STATUS_CODES = {400, 403, 404, 429, 500, 502, 503, 504}
MAX_CALLS_PER_CYCLE = 10
DELAY_BETWEEN_CALLS = 1.0

# ── System prompt ─────────────────────────────────────────────────────────────
SYSTEM_PROMPT = """You are an institutional options flow analyst specializing in NSE NIFTY index options.

Your role is to interpret unusual options flow signals and generate concise, actionable trading theses.

You understand:
- OI_BUILDUP means new positions are being opened (directional intent)
- OI_UNWIND means positions are being closed (potential reversal or target achieved)
- VOLUME_SPIKE with flat OI means intraday speculation (less conviction)
- IV_SPIKE means elevated premium payment, often before an expected move
- PCR > 1.2 indicates defensive/bearish market positioning
- PCR < 0.8 indicates aggressive/bullish positioning
- RSI > 70 means market is extended (signals may be late-stage)
- RSI < 30 means market is oversold (signals may be counter-trend bounce)
- MACD crossover direction confirms or contradicts the flow signal

Rules:
1. Never make categorical buy/sell recommendations
2. Express uncertainty when context is ambiguous or contradictory
3. Consider whether flow is institutional (large OI, specific expiry) or retail (scattered strikes)
4. A CE buildup during high PCR environment means smart money is going against the crowd
5. Always reason about WHO is likely making this trade and WHY

Output ONLY valid JSON. No markdown, no explanation outside the JSON.

JSON format:
{
  "thesis": "2-3 sentence analytical thesis explaining the signal in market context",
  "bias": "BULLISH" | "BEARISH" | "NEUTRAL",
  "confidence": 1-5,
  "reasoning_notes": "brief note on key contradictions or confirmations that drove your assessment"
}

Confidence scale:
1 = very uncertain, contradictory signals
2 = weak signal, limited confirmation
3 = moderate conviction, some confirmation
4 = strong signal, multiple confirmations
5 = very high conviction, clear institutional intent with confirming context"""


def _build_prompt(signal: dict, context: dict, headlines: list[str]) -> str:
    macd = context.get("macd")
    macd_str = "insufficient data"
    if macd:
        macd_str = (
            f"MACD={macd['macd_line']} Signal={macd['signal_line']} "
            f"Histogram={macd['histogram']} ({macd['crossover']} crossover, "
            f"momentum {macd['momentum']})"
        )

    news_str = "No recent relevant news available."
    if headlines:
        news_str = "\n".join(f"- {h}" for h in headlines)

    prompt = f"""Analyse this NIFTY options flow signal and generate a thesis.

=== SIGNAL ===
Type: {signal['signal_type']}
Strike: {signal['strike']} {signal['option_type']}
Expiry: {signal['expiry']}
OI Change: {signal.get('oi_change', 'N/A')} contracts
Volume: {signal.get('volume', 'N/A')}
IV: {signal.get('iv', 'N/A')}%
Signal Strength: {signal.get('signal_strength', 'N/A')}/5
Detection Mode: {signal.get('mode', 'N/A')}

=== MARKET CONTEXT ===
NIFTY Spot: {context.get('current_spot', 'N/A')}
Session Open: {context.get('session_open', 'N/A')}
Session Move: {context.get('session_move_pct', 'N/A')}%
Spot vs VWAP: {context.get('spot_vs_vwap', 'N/A')}
VWAP: {context.get('vwap', 'N/A')}

RSI(14): {context.get('rsi', 'N/A')} → {context.get('rsi_state', 'N/A')}
MACD: {macd_str}

PCR: {context.get('pcr', 'N/A')} → {context.get('pcr_state', 'N/A')}
Total CE OI: {context.get('total_ce_oi', 'N/A')}
Total PE OI: {context.get('total_pe_oi', 'N/A')}
Max Pain: {context.get('max_pain', 'N/A')} | Put Wall: {context.get('put_wall', 'N/A')} | Call Wall: {context.get('call_wall', 'N/A')}

=== RECENT NEWS ===
{news_str}

Generate your thesis JSON now:"""
    return prompt


def _call_single_model(prompt: str, model: str) -> dict:
    headers = {
        "Authorization": f"Bearer {LLM_API_KEY}",
        "HTTP-Referer": "https://github.com/Arthur-101/Option-Flow-Analyzer",
        "X-Title": "Options Flow Analyzer Cloud",
        "Content-Type": "application/json"
    }

    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": prompt}
        ],
        "temperature": 0.3,
        "max_tokens": 1024,
        "response_format": {"type": "json_object"}
    }

    resp = requests.post(LLM_API_URL, headers=headers, json=payload, timeout=25)
    resp.raise_for_status()
    data = resp.json()

    text = data["choices"][0]["message"]["content"].strip()
    if "```" in text:
        text = text.split("```")[1]
        if text.startswith("json"):
            text = text[4:]
        text = text.strip()

    start = text.find("{")
    end = text.rfind("}") + 1
    if start != -1 and end > start:
        text = text[start:end]

    return json.loads(text)


def _call_llm(prompt: str) -> dict | None:
    if not LLM_API_KEY:
        logger.error("LLM_API_KEY not configured in environment")
        return None

    total = len(LLM_MODELS)
    for idx, model in enumerate(LLM_MODELS, 1):
        try:
            logger.info("[Model %d/%d] Trying %s...", idx, total, model)
            result = _call_single_model(prompt, model)
            logger.info("[Model %d/%d] ✅ %s responded successfully", idx, total, model)
            return result
        except requests.HTTPError as e:
            status = e.response.status_code if e.response is not None else 0
            err_msg = ""
            if e.response is not None:
                try:
                    err_msg = e.response.json().get("error", {}).get("message", "")[:200]
                except Exception:
                    err_msg = e.response.text[:200]

            if status == 401:
                logger.error("[Model %d/%d] ❌ HTTP 401 Unauthorized — check LLM_API_KEY", idx, total)
                return None
            elif status in _RETRYABLE_STATUS_CODES:
                logger.warning("[Model %d/%d] ⚠️ %s HTTP %d: %s (falling back)", idx, total, model, status, err_msg)
            else:
                logger.warning("[Model %d/%d] ⚠️ %s HTTP %d: %s", idx, total, model, status, err_msg)
        except (requests.ConnectionError, requests.Timeout) as e:
            logger.warning("[Model %d/%d] ⚠️ %s timed out or connection error: %s", idx, total, model, e)
        except json.JSONDecodeError as e:
            logger.warning("[Model %d/%d] ⚠️ %s returned non-JSON text: %s", idx, total, model, e)
        except Exception as e:
            logger.error("[Model %d/%d] ❌ %s error: %s", idx, total, model, e)

    logger.error("All %d models in fallback chain failed", total)
    return None


def generate_theses(signals: list[dict], context: dict, headlines: list[str]) -> int:
    """
    Generate LLM theses for a list of signals.
    Writes results directly to signals in Supabase and local SQLite.
    Returns count of successful theses generated.
    """
    if not signals:
        return 0

    if not LLM_API_KEY:
        logger.warning("LLM_API_KEY not set — skipping LLM thesis generation")
        return 0

    sorted_signals = sorted(
        signals,
        key=lambda s: s.get("signal_strength", 0),
        reverse=True
    )[:MAX_CALLS_PER_CYCLE]

    generated = 0
    for i, signal in enumerate(sorted_signals):
        cloud_id = signal.get("cloud_id") or signal.get("id")
        local_id = signal.get("local_id") or signal.get("id")

        try:
            prompt = _build_prompt(signal, context, headlines)
            result = _call_llm(prompt)

            if result:
                # Update Supabase
                if cloud_id:
                    db_supabase.update_signal_thesis_supabase(cloud_id, result)

                # Update local SQLite
                if local_id:
                    try:
                        db.update_signal_thesis_local(local_id, result)
                    except Exception as e:
                        logger.warning("Local thesis update failed: %s", e)

                signal["llm_thesis"] = result.get("thesis")
                signal["llm_bias"] = result.get("bias")
                signal["llm_confidence"] = result.get("confidence")
                generated += 1

                logger.info(
                    "Thesis: %s %.0f %s → %s (★%s): %s",
                    signal["symbol"], signal.get("strike", 0), signal.get("option_type", ""),
                    result.get("bias"), result.get("confidence", 0),
                    (result.get("thesis") or "")[:80] + "..."
                )

            if i < len(sorted_signals) - 1:
                time.sleep(DELAY_BETWEEN_CALLS)

        except Exception as e:
            logger.error("Thesis generation failed for signal %s: %s", cloud_id, e)

    return generated
