# resolver.py — Live Signal Execution Resolver for ofa-collector
# Evaluates open signals against incoming 5-minute options chain ticks in real-time.
#
# Rules:
# 1. Strict t+1 Entry: Signal fires at t, entry_premium is recorded from the t+1 snapshot.
# 2. Strict Expiry Match: NEVER falls back across expiries (prevents Signal 274 contamination).
# 3. Dedicated `trades` execution table: Audits RULES vs LLM sources independently.
# 4. Entry filters:
#    - Premium >= ₹10 (under ₹10 marked SKIPPED_LOW_PREMIUM)
#    - Fired <= 14:40 IST (after 14:40 IST marked SKIPPED_LATE_ENTRY)
#    - Stale LTP check (flagged if no volume change in interval)
# 5. Fixed-rule exits:
#    - Target: +30%
#    - Stop Loss: -20% (tied bars prioritize stop loss first for conservative realism)
#    - Time Exit: Same-day 15:20 IST (09:50 UTC)
#    - Costs: 0.5% flat deduction for slippage/taxes

import logging
from datetime import datetime, timezone
import db_supabase

logger = logging.getLogger(__name__)

TARGET_PCT = 0.30       # +30% profit target
STOP_PCT   = -0.20      # -20% stop loss
COST_PCT   = 0.005      # 0.5% transaction cost & slippage
MIN_ENTRY_PREMIUM = 10.0 # Minimum entry premium ₹10
LATE_ENTRY_CUTOFF_HOUR_UTC = 9
LATE_ENTRY_CUTOFF_MINUTE_UTC = 10  # 14:40 IST = 09:10 UTC
TIME_EXIT_HOUR_UTC = 9
TIME_EXIT_MINUTE_UTC = 50          # 15:20 IST = 09:50 UTC


def _find_matching_option(current_rows: list[dict], target_strike: float,
                          target_type: str, expiry: str | None = None) -> dict | None:
    """Find specific strike & option_type strictly within target expiry."""
    candidates = [
        r for r in current_rows
        if abs(float(r.get("strike") or 0) - target_strike) < 0.1
        and r.get("option_type") == target_type
    ]
    if expiry:
        exp_match = [r for r in candidates if r.get("expiry") == expiry]
        return exp_match[0] if exp_match else None
    return candidates[0] if candidates else None


def _find_atm_option(current_rows: list[dict], spot: float,
                     target_type: str, expiry: str | None = None) -> dict | None:
    """Find closest At-The-Money strike strictly within target expiry."""
    type_rows = [r for r in current_rows if r.get("option_type") == target_type and r.get("strike")]
    if expiry:
        type_rows = [r for r in type_rows if r.get("expiry") == expiry]
    if not type_rows:
        return None
    return min(type_rows, key=lambda r: abs(float(r["strike"]) - spot))


def _is_fired_late(fired_at_str: str) -> bool:
    """Check if signal was fired after 14:40 IST (09:10 UTC)."""
    try:
        fired_dt = datetime.fromisoformat(fired_at_str.replace("Z", "+00:00"))
        return (fired_dt.hour, fired_dt.minute) > (LATE_ENTRY_CUTOFF_HOUR_UTC, LATE_ENTRY_CUTOFF_MINUTE_UTC)
    except Exception:
        return False


def fill_pending_entries(symbol: str, current_rows: list[dict], now_utc: str) -> None:
    """
    For signals that fired in the PREVIOUS flush without an entry_premium:
    1. Evaluates strict t+1 entry from current snapshot.
    2. Applies entry filters (SKIPPED_LOW_PREMIUM, SKIPPED_LATE_ENTRY).
    3. Writes trade execution records to `trades` for RULES and LLM sources.
    4. Updates signals row for backward compatibility.
    """
    if not current_rows:
        return

    client = db_supabase.get_supabase()
    spot = next((float(r["spot_price"]) for r in current_rows if r.get("spot_price")), 22500.0)

    try:
        res = (
            client.table("signals")
            .select("*")
            .eq("symbol", symbol)
            .is_("entry_premium", "null")
            .lt("fired_at", now_utc)
            .execute()
        )
        pending = res.data or []
    except Exception as e:
        logger.warning("Failed to fetch pending entry signals: %s", e)
        return

    for sig in pending:
        sig_id = sig["id"]
        bias = (sig.get("bias") or "NEUTRAL").upper()
        sig_strike = float(sig.get("strike") or spot)
        sig_type = sig.get("option_type") or "CE"
        sig_exp = sig.get("expiry")
        is_late = _is_fired_late(sig.get("fired_at", ""))

        # 1. RULES TRADE
        if bias == "NEUTRAL":
            try:
                client.table("signals").update({
                    "entry_premium": 0.0,
                    "exit_premium": 0.0,
                    "exit_reason": "NON_DIRECTIONAL",
                    "net_pnl_pct": 0.0,
                    "resolved_at": now_utc,
                }).eq("id", sig_id).execute()
                logger.info("Signal %s: Neutral bias marked NON_DIRECTIONAL", sig_id)
            except Exception as e:
                logger.error("Failed to update neutral signal %s: %s", sig_id, e)
        else:
            # Directional RULES trade
            if bias == "BULLISH":
                row_opt = _find_matching_option(current_rows, sig_strike, "CE", sig_exp) if sig_type == "CE" else _find_atm_option(current_rows, spot, "CE", sig_exp)
            else:
                row_opt = _find_matching_option(current_rows, sig_strike, "PE", sig_exp) if sig_type == "PE" else _find_atm_option(current_rows, spot, "PE", sig_exp)

            if row_opt:
                entry_ltp = float(row_opt.get("last_price") or 0)
                vol_delta = row_opt.get("volume_delta")
                is_stale = (vol_delta is not None and vol_delta <= 0)

                if is_late:
                    skip_reason = "SKIPPED_LATE_ENTRY"
                    db_supabase.insert_trade_supabase({
                        "signal_id": sig_id,
                        "source": "RULES",
                        "strike": float(row_opt.get("strike", 0)),
                        "option_type": row_opt.get("option_type"),
                        "expiry": row_opt.get("expiry"),
                        "bias_used": bias,
                        "entry_at": now_utc,
                        "entry_premium": round(entry_ltp, 2),
                        "exit_premium": round(entry_ltp, 2),
                        "exit_reason": skip_reason,
                        "target_pct": TARGET_PCT,
                        "stop_pct": STOP_PCT,
                        "net_pnl_pct": 0.0,
                        "mfe": 0.0,
                        "mae": 0.0,
                        "resolved_at": now_utc,
                        "stale_ltp": is_stale,
                        "is_exploratory": False,
                    })
                    try:
                        client.table("signals").update({
                            "entry_premium": round(entry_ltp, 2),
                            "exit_premium": round(entry_ltp, 2),
                            "exit_reason": skip_reason,
                            "net_pnl_pct": 0.0,
                            "resolved_at": now_utc,
                        }).eq("id", sig_id).execute()
                    except Exception:
                        pass
                    logger.info("Signal %s: Entry skipped (fired after 14:40 IST)", sig_id)

                elif entry_ltp < MIN_ENTRY_PREMIUM:
                    skip_reason = "SKIPPED_LOW_PREMIUM"
                    db_supabase.insert_trade_supabase({
                        "signal_id": sig_id,
                        "source": "RULES",
                        "strike": float(row_opt.get("strike", 0)),
                        "option_type": row_opt.get("option_type"),
                        "expiry": row_opt.get("expiry"),
                        "bias_used": bias,
                        "entry_at": now_utc,
                        "entry_premium": round(entry_ltp, 2),
                        "exit_premium": round(entry_ltp, 2),
                        "exit_reason": skip_reason,
                        "target_pct": TARGET_PCT,
                        "stop_pct": STOP_PCT,
                        "net_pnl_pct": 0.0,
                        "mfe": 0.0,
                        "mae": 0.0,
                        "resolved_at": now_utc,
                        "stale_ltp": is_stale,
                        "is_exploratory": False,
                    })
                    try:
                        client.table("signals").update({
                            "entry_premium": round(entry_ltp, 2),
                            "exit_premium": round(entry_ltp, 2),
                            "exit_reason": skip_reason,
                            "net_pnl_pct": 0.0,
                            "resolved_at": now_utc,
                        }).eq("id", sig_id).execute()
                    except Exception:
                        pass
                    logger.info("Signal %s: Entry skipped (premium ₹%.2f < ₹10)", sig_id, entry_ltp)

                elif entry_ltp > 0:
                    # Valid active RULES trade
                    db_supabase.insert_trade_supabase({
                        "signal_id": sig_id,
                        "source": "RULES",
                        "strike": float(row_opt.get("strike", 0)),
                        "option_type": row_opt.get("option_type"),
                        "expiry": row_opt.get("expiry"),
                        "bias_used": bias,
                        "entry_at": now_utc,
                        "entry_premium": round(entry_ltp, 2),
                        "target_pct": TARGET_PCT,
                        "stop_pct": STOP_PCT,
                        "stale_ltp": is_stale,
                        "is_exploratory": False,
                    })
                    try:
                        client.table("signals").update({
                            "entry_premium": round(entry_ltp, 2),
                            "target_pct": TARGET_PCT,
                            "stop_pct": STOP_PCT,
                        }).eq("id", sig_id).execute()
                    except Exception:
                        pass
                    logger.info("🎯 Signal %s [RULES]: t+1 entry filled at ₹%.2f (%s %s)",
                                sig_id, entry_ltp, row_opt.get("strike"), row_opt.get("option_type"))

        # 2. LLM TRADE (Evaluated if LLM thesis has populated)
        llm_bias = (sig.get("llm_bias") or "").upper()
        if llm_bias in ("BULLISH", "BEARISH"):
            if llm_bias == "BULLISH":
                llm_opt = _find_matching_option(current_rows, sig_strike, "CE", sig_exp) if sig_type == "CE" else _find_atm_option(current_rows, spot, "CE", sig_exp)
            else:
                llm_opt = _find_matching_option(current_rows, sig_strike, "PE", sig_exp) if sig_type == "PE" else _find_atm_option(current_rows, spot, "PE", sig_exp)

            if llm_opt:
                llm_entry_ltp = float(llm_opt.get("last_price") or 0)
                llm_vol_delta = llm_opt.get("volume_delta")
                llm_stale = (llm_vol_delta is not None and llm_vol_delta <= 0)

                if is_late or llm_entry_ltp < MIN_ENTRY_PREMIUM:
                    skip_reason = "SKIPPED_LATE_ENTRY" if is_late else "SKIPPED_LOW_PREMIUM"
                    db_supabase.insert_trade_supabase({
                        "signal_id": sig_id,
                        "source": "LLM",
                        "strike": float(llm_opt.get("strike", 0)),
                        "option_type": llm_opt.get("option_type"),
                        "expiry": llm_opt.get("expiry"),
                        "bias_used": llm_bias,
                        "entry_at": now_utc,
                        "entry_premium": round(llm_entry_ltp, 2),
                        "exit_premium": round(llm_entry_ltp, 2),
                        "exit_reason": skip_reason,
                        "target_pct": TARGET_PCT,
                        "stop_pct": STOP_PCT,
                        "net_pnl_pct": 0.0,
                        "mfe": 0.0,
                        "mae": 0.0,
                        "resolved_at": now_utc,
                        "stale_ltp": llm_stale,
                        "is_exploratory": False,
                        "thesis_at": sig.get("thesis_at"),
                    })
                elif llm_entry_ltp > 0:
                    db_supabase.insert_trade_supabase({
                        "signal_id": sig_id,
                        "source": "LLM",
                        "strike": float(llm_opt.get("strike", 0)),
                        "option_type": llm_opt.get("option_type"),
                        "expiry": llm_opt.get("expiry"),
                        "bias_used": llm_bias,
                        "entry_at": now_utc,
                        "entry_premium": round(llm_entry_ltp, 2),
                        "target_pct": TARGET_PCT,
                        "stop_pct": STOP_PCT,
                        "stale_ltp": llm_stale,
                        "is_exploratory": False,
                        "thesis_at": sig.get("thesis_at"),
                    })
                    logger.info("🤖 Signal %s [LLM]: Entry filled at ₹%.2f (%s %s %s)",
                                sig_id, llm_entry_ltp, llm_bias, llm_opt.get("strike"), llm_opt.get("option_type"))


def resolve_active_signals(symbol: str, current_rows: list[dict], now_utc: str) -> None:
    """
    Checks all open trades in `trades` table against current snapshot.
    Evaluates:
    - Target Hit (+30%)
    - Stop Loss (-20%)
    - Time Exit (15:20 IST)
    - Updates MFE and MAE live.
    - Syncs RULES outcomes back to signals table.
    """
    if not current_rows:
        return

    # 1. Fill any pending t+1 entries first
    fill_pending_entries(symbol, current_rows, now_utc)

    client = db_supabase.get_supabase()

    # 2. Fetch open trades from trades table
    try:
        res = (
            client.table("trades")
            .select("*")
            .is_("resolved_at", "null")
            .not_.is_("entry_premium", "null")
            .gt("entry_premium", 0)
            .execute()
        )
        open_trades = res.data or []
    except Exception as e:
        logger.warning("Failed to fetch open trades: %s", e)
        open_trades = []

    now_dt = datetime.now(timezone.utc)
    is_time_exit = (now_dt.hour > TIME_EXIT_HOUR_UTC or
                    (now_dt.hour == TIME_EXIT_HOUR_UTC and now_dt.minute >= TIME_EXIT_MINUTE_UTC))

    if open_trades:
        logger.info("🔍 Resolving %d open trades against current snapshot...", len(open_trades))

    for tr in open_trades:
        trade_id = tr["id"]
        sig_id = tr["signal_id"]
        source = tr.get("source", "RULES")
        entry_price = float(tr["entry_premium"])
        tr_strike = float(tr.get("strike", 0))
        tr_type = tr.get("option_type")
        tr_exp = tr.get("expiry")

        # Strict lookup: strike, option_type, AND expiry
        row_opt = _find_matching_option(current_rows, tr_strike, tr_type, tr_exp)
        if not row_opt:
            continue

        current_ltp = float(row_opt.get("last_price") or 0)
        if current_ltp <= 0:
            continue

        pct_change = (current_ltp - entry_price) / entry_price

        # Running MFE / MAE
        curr_mfe = float(tr.get("mfe") or 0.0) / 100.0
        curr_mae = float(tr.get("mae") or 0.0) / 100.0
        new_mfe = max(curr_mfe, pct_change)
        new_mae = min(curr_mae, pct_change)

        exit_reason = None
        exit_price = current_ltp

        target_pct = float(tr.get("target_pct") or TARGET_PCT)
        stop_pct = float(tr.get("stop_pct") or STOP_PCT)

        # Conservative tie-breaker: Stop loss takes priority if both breached in same 5-min snapshot
        if pct_change <= stop_pct:
            exit_reason = "STOP_HIT"
            exit_price = current_ltp
        elif pct_change >= target_pct:
            exit_reason = "TARGET_HIT"
            exit_price = current_ltp
        elif is_time_exit:
            exit_reason = "TIME_EXIT"
            exit_price = current_ltp

        if exit_reason:
            gross_pnl = (exit_price - entry_price) / entry_price
            net_pnl = round((gross_pnl - COST_PCT) * 100, 2)
            outcome_correct = (net_pnl > 0)

            # Update trades table
            db_supabase.update_trade_supabase(trade_id, {
                "exit_premium": round(exit_price, 2),
                "exit_reason": exit_reason,
                "net_pnl_pct": net_pnl,
                "mfe": round(new_mfe * 100, 2),
                "mae": round(new_mae * 100, 2),
                "resolved_at": now_utc,
            })

            # For RULES trade: also sync resolution back to signals table
            if source == "RULES":
                try:
                    client.table("signals").update({
                        "exit_premium": round(exit_price, 2),
                        "exit_reason": exit_reason,
                        "net_pnl_pct": net_pnl,
                        "outcome_correct": outcome_correct,
                        "mfe": round(new_mfe * 100, 2),
                        "mae": round(new_mae * 100, 2),
                        "resolved_at": now_utc,
                    }).eq("id", sig_id).execute()
                except Exception as e:
                    logger.error("Failed to sync resolution to signal %s: %s", sig_id, e)

            logger.info("🏁 Trade %s [%s] RESOLVED: %s at ₹%.2f (Entry: ₹%.2f | Net PnL: %+.2f%% | MFE: +%.1f%% | MAE: %.1f%%)",
                        trade_id, source, exit_reason, exit_price, entry_price, net_pnl, new_mfe * 100, new_mae * 100)
        else:
            # Running trade — update MFE/MAE
            db_supabase.update_trade_supabase(trade_id, {
                "mfe": round(new_mfe * 100, 2),
                "mae": round(new_mae * 100, 2),
            })
            if source == "RULES":
                try:
                    client.table("signals").update({
                        "mfe": round(new_mfe * 100, 2),
                        "mae": round(new_mae * 100, 2),
                    }).eq("id", sig_id).execute()
                except Exception:
                    pass

