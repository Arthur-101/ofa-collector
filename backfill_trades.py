# backfill_trades.py — Recompute Quadrant Bias and Backfill Trades Table
#
# Rules:
# 1. Strict t+1 entry: LTP from first snapshot strictly after fired_at (gt).
# 2. Strict expiry matching: Filters strictly by expiry == sig['expiry'] (no cross-expiry fallback).
# 3. 4-Quadrant bias re-derivation from underlying options_chain price & OI changes.
# 4. Entry filters: ₹10 minimum premium, 14:40 IST cutoff.
# 5. Stale LTP detection: Checks for zero volume change.
# 6. Auditable population into `trades` table for both RULES and LLM sources, tagged with is_exploratory=TRUE.

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


def _simulate_and_insert_trade(client, sig_id: int, source: str, target_strike: float,
                               target_type: str, sig_exp: str, bias_used: str,
                               fired_at: str, is_late: bool, dry_run: bool,
                               thesis_at: str | None = None) -> bool:
    """Simulate a single trade from historical ticks and insert into trades table."""
    sig_date = fired_at[:10]
    search_start = (thesis_at if (source == "LLM" and thesis_at) else fired_at)

    if bias_used == "NEUTRAL":
        trade_row = {
            "signal_id": sig_id,
            "source": source,
            "symbol": "NIFTY",
            "strike": target_strike,
            "option_type": target_type,
            "expiry": sig_exp,
            "bias_used": bias_used,
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
            "thesis_at": thesis_at,
        }
        if not dry_run:
            db_supabase.insert_trade_supabase(trade_row)
        return True

    try:
        tick_res = (
            client.table("options_chain")
            .select("timestamp,last_price,volume")
            .eq("symbol", "NIFTY")
            .eq("expiry", sig_exp)
            .eq("strike", target_strike)
            .eq("option_type", target_type)
            .gt("timestamp", search_start)
            .lte("timestamp", f"{sig_date}T23:59:59")
            .order("timestamp", desc=False)
            .limit(100)
            .execute()
        )
        ticks = tick_res.data or []
    except Exception as e:
        logger.debug("Failed to fetch tick history for signal %s: %s", sig_id, e)
        return False

    if not ticks:
        exit_rsn = "SKIPPED_LATE_ENTRY" if is_late else "SKIPPED_NO_DATA"
        trade_row = {
            "signal_id": sig_id,
            "source": source,
            "symbol": "NIFTY",
            "strike": target_strike,
            "option_type": target_type,
            "expiry": sig_exp,
            "bias_used": bias_used,
            "entry_at": fired_at,
            "entry_premium": 0.0,
            "exit_premium": 0.0,
            "exit_reason": exit_rsn,
            "target_pct": TARGET_PCT,
            "stop_pct": STOP_PCT,
            "net_pnl_pct": 0.0,
            "mfe": 0.0,
            "mae": 0.0,
            "resolved_at": fired_at,
            "stale_ltp": False,
            "is_exploratory": True,
            "thesis_at": thesis_at,
        }
        if not dry_run:
            db_supabase.insert_trade_supabase(trade_row)
        return True

    entry_tick = ticks[0]
    entry_at = entry_tick["timestamp"]
    entry_ltp = float(entry_tick.get("last_price") or 0)
    entry_vol = int(entry_tick.get("volume") or 0)

    # Check volume confirmation / stale LTP
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

    if is_late:
        trade_row = {
            "signal_id": sig_id,
            "source": source,
            "symbol": "NIFTY",
            "strike": target_strike,
            "option_type": target_type,
            "expiry": sig_exp,
            "bias_used": bias_used,
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
            "thesis_at": thesis_at,
        }
        if not dry_run:
            db_supabase.insert_trade_supabase(trade_row)
        return True

    if entry_ltp < MIN_ENTRY_PREMIUM:
        trade_row = {
            "signal_id": sig_id,
            "source": source,
            "symbol": "NIFTY",
            "strike": target_strike,
            "option_type": target_type,
            "expiry": sig_exp,
            "bias_used": bias_used,
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
            "thesis_at": thesis_at,
        }
        if not dry_run:
            db_supabase.insert_trade_supabase(trade_row)
        return True

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
        "source": source,
        "symbol": "NIFTY",
        "strike": target_strike,
        "option_type": target_type,
        "expiry": sig_exp,
        "bias_used": bias_used,
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
        "thesis_at": thesis_at,
    }
    if not dry_run:
        db_supabase.insert_trade_supabase(trade_row)
    return True


def backfill_all_signals(dry_run: bool = False):
    logger.info("==================================================")
    logger.info("🚀 Starting historical signals backfill into trades (dry_run=%s)", dry_run)
    logger.info("==================================================")

    client = db_supabase.get_supabase()

    try:
        res = client.table("signals").select("*").order("fired_at", desc=False).execute()
        signals = res.data or []
    except Exception as e:
        logger.error("Failed to fetch signals: %s", e)
        return

    logger.info("Found %d historical signals to process", len(signals))

    rules_trades_count = 0
    llm_trades_count = 0
    skipped_count = 0

    for sig in signals:
        sig_id = sig["id"]
        fired_at = sig["fired_at"]
        sig_exp = sig.get("expiry")
        sig_strike = float(sig.get("strike") or 0)
        sig_type = sig.get("option_type")

        if not sig_exp:
            continue

        # Re-derive bias with 4-Quadrant rule
        signal_type, bias = _rederive_quadrant_bias(sig, client)

        fired_dt = datetime.fromisoformat(fired_at.replace("Z", "+00:00"))
        is_late = (fired_dt.hour, fired_dt.minute) > (LATE_ENTRY_CUTOFF_HOUR_UTC, LATE_ENTRY_CUTOFF_MINUTE_UTC)

        # 1. RULES Trade
        target_type = "CE" if bias == "BULLISH" else "PE"
        target_strike = sig_strike if sig_type == target_type else round(float(sig.get("spot_price") or 22500) / 50.0) * 50.0

        ok_rules = _simulate_and_insert_trade(
            client=client,
            sig_id=sig_id,
            source="RULES",
            target_strike=target_strike if bias != "NEUTRAL" else sig_strike,
            target_type=target_type if bias != "NEUTRAL" else sig_type,
            sig_exp=sig_exp,
            bias_used=bias,
            fired_at=fired_at,
            is_late=is_late,
            dry_run=dry_run
        )
        if ok_rules:
            rules_trades_count += 1
        else:
            skipped_count += 1

        # 2. LLM Trade (if llm_bias exists)
        llm_b = sig.get("llm_bias")
        if llm_b:
            llm_bias = str(llm_b).upper()
            llm_type = "CE" if llm_bias == "BULLISH" else "PE"
            llm_strike = sig_strike if sig_type == llm_type else round(float(sig.get("spot_price") or 22500) / 50.0) * 50.0
            ok_llm = _simulate_and_insert_trade(
                client=client,
                sig_id=sig_id,
                source="LLM",
                target_strike=llm_strike if llm_bias != "NEUTRAL" else sig_strike,
                target_type=llm_type if llm_bias != "NEUTRAL" else sig_type,
                sig_exp=sig_exp,
                bias_used=llm_bias,
                fired_at=fired_at,
                is_late=is_late,
                dry_run=dry_run,
                thesis_at=sig.get("thesis_at")
            )
            if ok_llm:
                llm_trades_count += 1

    logger.info("==================================================")
    logger.info("✅ Backfill Complete:")
    logger.info("   RULES trades: %d", rules_trades_count)
    logger.info("   LLM trades:   %d", llm_trades_count)
    logger.info("   Total trades: %d (skipped without ticks: %d)",
                rules_trades_count + llm_trades_count, skipped_count)
    logger.info("==================================================")


if __name__ == "__main__":
    dry_run = "--dry-run" in sys.argv
    backfill_all_signals(dry_run=dry_run)
