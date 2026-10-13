# backfill_trades.py — Recompute Quadrant Bias and Backfill Trades Table
#
# Rules:
# 1. Strict t+1 entry: LTP from first snapshot strictly after fired_at (gt).
# 2. Strict expiry matching: Filters strictly by expiry == sig['expiry'] (no cross-expiry fallback).
# 3. 4-Quadrant bias re-derivation from underlying options_chain price & OI changes.
# 4. Entry filters: ₹10 minimum premium, 14:40 IST cutoff.
# 5. Stale LTP detection: Checks for zero volume change.
# 6. Auditable population into `trades` table tagged with source='RULES', is_exploratory=TRUE.

import sys
import logging
from datetime import datetime, timezone
import pytz

import db_supabase

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("backfill_trades")
IST = pytz.timezone("Asia/Kolkata")

TARGET_PCT = 0.30       # +30% profit target
STOP_PCT   = -0.20      # -20% stop loss
COST_PCT   = 0.005      # 0.5% transaction cost & slippage
MIN_ENTRY_PREMIUM = 10.0 # Minimum entry premium ₹10
LATE_ENTRY_CUTOFF_HOUR_UTC = 9
LATE_ENTRY_CUTOFF_MINUTE_UTC = 10  # 14:40 IST = 09:10 UTC
TIME_EXIT_HOUR_UTC = 9
TIME_EXIT_MINUTE_UTC = 50          # 15:20 IST = 09:50 UTC
NOISE_PRICE_CHANGE_PCT = 0.02


def _rederive_quadrant_bias(sig: dict, client) -> tuple[str, str]:
    """
    Recompute bias using 4-Quadrant Engine from raw options_chain rows.
    Returns (signal_type, bias).
    """
    oi_change = sig.get("oi_change") or 0
    opt_type = sig.get("option_type")
    strike = float(sig.get("strike") or 0)
    expiry = sig.get("expiry")
    fired_at = sig.get("fired_at")

    if not expiry or not opt_type or not strike or not fired_at:
        return sig.get("signal_type", "OI_BUILDUP"), sig.get("bias", "NEUTRAL")

    # Fetch snapshot at fired_at and previous snapshot
    try:
        res = (
            client.table("options_chain")
            .select("timestamp,last_price,volume,oi")
            .eq("strike", strike)
            .eq("option_type", opt_type)
            .eq("expiry", expiry)
            .lte("timestamp", fired_at)
            .order("timestamp", desc=True)
            .limit(2)
            .execute()
        )
        ticks = res.data or []
        if len(ticks) >= 2:
            curr_ltp = float(ticks[0].get("last_price") or 0)
            prev_ltp = float(ticks[1].get("last_price") or 0)
            if prev_ltp > 0 and curr_ltp > 0:
                pct_change = (curr_ltp - prev_ltp) / prev_ltp
                if abs(pct_change) < NOISE_PRICE_CHANGE_PCT:
                    return ("OI_BUILDUP" if oi_change > 0 else "OI_UNWIND"), "NEUTRAL"
                elif oi_change > 0:
                    if pct_change >= NOISE_PRICE_CHANGE_PCT:
                        return "OI_BUILDUP", ("BULLISH" if opt_type == "CE" else "BEARISH")
                    else:
                        return "OI_BUILDUP", ("BEARISH" if opt_type == "CE" else "BULLISH")
                else: # oi_change < 0
                    if pct_change >= NOISE_PRICE_CHANGE_PCT:
                        return "OI_UNWIND", ("BULLISH" if opt_type == "CE" else "BEARISH")
                    else:
                        return "OI_UNWIND", ("BEARISH" if opt_type == "CE" else "BULLISH")
    except Exception as e:
        logger.debug("Failed to rederive bias for signal %s: %s", sig.get("id"), e)

    return sig.get("signal_type", "OI_BUILDUP"), sig.get("bias", "NEUTRAL")


def backfill_all_signals(dry_run: bool = False):
    logger.info("==================================================")
    logger.info("🚀 Starting historical signals backfill into trades (dry_run=%s)", dry_run)
    logger.info("==================================================")

    client = db_supabase.get_supabase()

    # Fetch all historical signals
    try:
        res = client.table("signals").select("*").order("fired_at", desc=False).execute()
        signals = res.data or []
    except Exception as e:
        logger.error("Failed to fetch signals: %s", e)
        return

    logger.info("Found %d historical signals to process", len(signals))

    trades_created = 0
    skipped_count = 0

    for sig in signals:
        sig_id = sig["id"]
        fired_at = sig["fired_at"]
        symbol = sig.get("symbol", "NIFTY")
        sig_exp = sig.get("expiry")
        sig_strike = float(sig.get("strike") or 0)
        sig_type = sig.get("option_type")
        sig_date = fired_at[:10]

        if not sig_exp:
            continue

        # Re-derive bias with 4-Quadrant rule
        signal_type, bias = _rederive_quadrant_bias(sig, client)

        # Check late entry
        fired_dt = datetime.fromisoformat(fired_at.replace("Z", "+00:00"))
        is_late = (fired_dt.hour, fired_dt.minute) > (LATE_ENTRY_CUTOFF_HOUR_UTC, LATE_ENTRY_CUTOFF_MINUTE_UTC)

        # If neutral bias -> non-directional
        if bias == "NEUTRAL":
            trade_row = {
                "signal_id": sig_id,
                "source": "RULES",
                "strike": sig_strike,
                "option_type": sig_type,
                "expiry": sig_exp,
                "bias_used": bias,
                "entry_at": fired_at,
                "entry_premium": 0.0,
                "exit_premium": 0.0,
                "exit_reason": "NON_DIRECTIONAL",
                "target_pct": TARGET_PCT,
                "stop_pct": STOP_PCT,
                "net_pnl_pct": 0.0,
                "mfe": 0.0,
                "mae": 0.0,
                "resolved_at": fired_at,
                "stale_ltp": False,
                "is_exploratory": True,
            }
            if not dry_run:
                db_supabase.insert_trade_supabase(trade_row)
            trades_created += 1
            continue

        # Fetch ticks strictly after fired_at on that same day, strictly matching target expiry
        target_type = "CE" if bias == "BULLISH" else "PE"
        target_strike = sig_strike if sig_type == target_type else round(float(sig.get("spot_price") or 22500) / 50.0) * 50.0

        try:
            tick_res = (
                client.table("options_chain")
                .select("timestamp,last_price,volume")
                .eq("symbol", symbol)
                .eq("expiry", sig_exp)
                .eq("strike", target_strike)
                .eq("option_type", target_type)
                .gt("timestamp", fired_at)
                .lte("timestamp", f"{sig_date}T23:59:59")
                .order("timestamp", desc=False)
                .limit(100)
                .execute()
            )
            ticks = tick_res.data or []
        except Exception as e:
            logger.debug("Failed to fetch tick history for signal %s: %s", sig_id, e)
            continue

        if not ticks:
            skipped_count += 1
            continue

        # Strict t+1 entry tick
        entry_tick = ticks[0]
        entry_at = entry_tick["timestamp"]
        entry_ltp = float(entry_tick.get("last_price") or 0)
        entry_vol = int(entry_tick.get("volume") or 0)

        # Check volume confirmation / stale LTP
        # Check volume at fired_at snapshot
        stale_ltp = False
        try:
            prior_res = (
                client.table("options_chain")
                .select("volume")
                .eq("expiry", sig_exp)
                .eq("strike", target_strike)
                .eq("option_type", target_type)
                .lte("timestamp", fired_at)
                .order("timestamp", desc=True)
                .limit(1)
                .execute()
            )
            if prior_res.data:
                prior_vol = int(prior_res.data[0].get("volume") or 0)
                if entry_vol <= prior_vol:
                    stale_ltp = True
        except Exception:
            pass

        # Apply entry filters
        if is_late:
            trade_row = {
                "signal_id": sig_id,
                "source": "RULES",
                "strike": target_strike,
                "option_type": target_type,
                "expiry": sig_exp,
                "bias_used": bias,
                "entry_at": entry_at,
                "entry_premium": round(entry_ltp, 2),
                "exit_premium": round(entry_ltp, 2),
                "exit_reason": "SKIPPED_LATE_ENTRY",
                "target_pct": TARGET_PCT,
                "stop_pct": STOP_PCT,
                "net_pnl_pct": 0.0,
                "mfe": 0.0,
                "mae": 0.0,
                "resolved_at": entry_at,
                "stale_ltp": stale_ltp,
                "is_exploratory": True,
            }
            if not dry_run:
                db_supabase.insert_trade_supabase(trade_row)
            trades_created += 1
            continue

        if entry_ltp < MIN_ENTRY_PREMIUM:
            trade_row = {
                "signal_id": sig_id,
                "source": "RULES",
                "strike": target_strike,
                "option_type": target_type,
                "expiry": sig_exp,
                "bias_used": bias,
                "entry_at": entry_at,
                "entry_premium": round(entry_ltp, 2),
                "exit_premium": round(entry_ltp, 2),
                "exit_reason": "SKIPPED_LOW_PREMIUM",
                "target_pct": TARGET_PCT,
                "stop_pct": STOP_PCT,
                "net_pnl_pct": 0.0,
                "mfe": 0.0,
                "mae": 0.0,
                "resolved_at": entry_at,
                "stale_ltp": stale_ltp,
                "is_exploratory": True,
            }
            if not dry_run:
                db_supabase.insert_trade_supabase(trade_row)
            trades_created += 1
            continue

        # Simulate subsequent ticks to resolution
        mfe = 0.0
        mae = 0.0
        exit_price = entry_ltp
        exit_reason = "TIME_EXIT"
        resolved_at = ticks[-1]["timestamp"]

        for tk in ticks[1:]:
            ltp = float(tk.get("last_price") or 0)
            if ltp <= 0:
                continue
            pct_chg = (ltp - entry_ltp) / entry_ltp
            mfe = max(mfe, pct_chg)
            mae = min(mae, pct_chg)

            tk_dt = datetime.fromisoformat(tk["timestamp"].replace("Z", "+00:00"))
            is_time_exit = (tk_dt.hour > TIME_EXIT_HOUR_UTC or
                            (tk_dt.hour == TIME_EXIT_HOUR_UTC and tk_dt.minute >= TIME_EXIT_MINUTE_UTC))

            # Conservative tie-breaker: stop loss takes precedence
            if pct_chg <= STOP_PCT:
                exit_price = ltp
                exit_reason = "STOP_HIT"
                resolved_at = tk["timestamp"]
                break
            elif pct_chg >= TARGET_PCT:
                exit_price = ltp
                exit_reason = "TARGET_HIT"
                resolved_at = tk["timestamp"]
                break
            elif is_time_exit:
                exit_price = ltp
                exit_reason = "TIME_EXIT"
                resolved_at = tk["timestamp"]
                break

        gross_pnl = (exit_price - entry_ltp) / entry_ltp
        net_pnl = round((gross_pnl - COST_PCT) * 100, 2)

        trade_row = {
            "signal_id": sig_id,
            "source": "RULES",
            "strike": target_strike,
            "option_type": target_type,
            "expiry": sig_exp,
            "bias_used": bias,
            "entry_at": entry_at,
            "entry_premium": round(entry_ltp, 2),
            "exit_premium": round(exit_price, 2),
            "exit_reason": exit_reason,
            "target_pct": TARGET_PCT,
            "stop_pct": STOP_PCT,
            "net_pnl_pct": net_pnl,
            "mfe": round(mfe * 100, 2),
            "mae": round(mae * 100, 2),
            "resolved_at": resolved_at,
            "stale_ltp": stale_ltp,
            "is_exploratory": True,
        }

        if not dry_run:
            db_supabase.insert_trade_supabase(trade_row)

        trades_created += 1

    logger.info("==================================================")
    logger.info("✅ Backfill Complete: %d trades created (%d skipped without ticks)",
                trades_created, skipped_count)
    logger.info("==================================================")


if __name__ == "__main__":
    dry_run = "--dry-run" in sys.argv
    backfill_all_signals(dry_run=dry_run)
