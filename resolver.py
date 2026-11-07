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
MAX_MONEYNESS_PCT = 0.015  # reject entries more than 1.5% away from spot
MAX_ENTRY_DELAY_MIN = 15   # reject entries more than 15 min after the signal
LATE_ENTRY_CUTOFF_HOUR_UTC = 9
LATE_ENTRY_CUTOFF_MINUTE_UTC = 10  # 14:40 IST = 09:10 UTC
TIME_EXIT_HOUR_UTC = 9
TIME_EXIT_MINUTE_UTC = 50          # 15:20 IST = 09:50 UTC


def _entry_delay_minutes(fired_at_str: str, now_utc: str) -> float | None:
    """Minutes between signal fire time and the current flush."""
    try:
        fired = datetime.fromisoformat(fired_at_str.replace("Z", "+00:00"))
        now = datetime.fromisoformat(now_utc.replace("Z", "+00:00"))
        return (now - fired).total_seconds() / 60.0
    except Exception:
        return None


def _moneyness_pct(strike, spot) -> float | None:
    """Absolute distance of the strike from spot, as a fraction of spot."""
    try:
        spot = float(spot)
        if spot <= 0:
            return None
        return abs(float(strike) - spot) / spot
    except Exception:
        return None


def _write_skip_trade(client, sig: dict, source: str, reason: str, now_utc: str,
                      symbol: str, bias: str | None = None,
                      row_opt: dict | None = None, entry_ltp: float = 0.0) -> None:
    """Record a skipped entry with zero PnL so it never becomes a fake loss."""
    sig_id = sig["id"]
    strike = float(row_opt.get("strike", 0)) if row_opt else float(sig.get("strike") or 0)
    opt_type = row_opt.get("option_type") if row_opt else sig.get("option_type")
    expiry = row_opt.get("expiry") if row_opt else sig.get("expiry")
    entry = round(float(entry_ltp or 0), 2)

    db_supabase.insert_trade_supabase({
        "signal_id": sig_id,
        "source": source,
        "symbol": symbol,
        "strike": strike,
        "option_type": opt_type,
        "expiry": expiry,
        "bias_used": bias or (sig.get("bias") or "NEUTRAL").upper(),
        "entry_at": now_utc,
        "entry_premium": entry,
        "exit_premium": entry,
        "exit_reason": reason,
        "target_pct": TARGET_PCT,
        "stop_pct": STOP_PCT,
        "net_pnl_pct": 0.0,
        "mfe": 0.0,
        "mae": 0.0,
        "resolved_at": now_utc,
        "stale_ltp": reason == "SKIPPED_STALE_LTP",
        "is_exploratory": False,
        "thesis_at": sig.get("thesis_at"),
    })

    # Only the RULES source owns the signal row fields.
    if source == "RULES":
        try:
            client.table("signals").update({
                "entry_premium": entry,
                "exit_premium": entry,
                "exit_reason": reason,
                "net_pnl_pct": 0.0,
                "outcome_correct": None,
                "resolved_at": now_utc,
            }).eq("id", sig_id).execute()
        except Exception:
            pass


def _resolve_skipped_trade(client, tr: dict, reason: str, now_utc: str) -> None:
    """Close an open trade with zero PnL when no trustworthy quote exists."""
    entry = round(float(tr.get("entry_premium") or 0), 2)
    db_supabase.update_trade_supabase(tr["id"], {
        "exit_premium": entry,
        "exit_reason": reason,
        "net_pnl_pct": 0.0,
        "resolved_at": now_utc,
    })
    if tr.get("source") == "RULES":
        try:
            client.table("signals").update({
                "exit_premium": entry,
                "exit_reason": reason,
                "net_pnl_pct": 0.0,
                "outcome_correct": None,
                "resolved_at": now_utc,
            }).eq("id", tr["signal_id"]).execute()
        except Exception:
            pass


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
    spot = next((float(r["spot_price"]) for r in current_rows if r.get("spot_price")), None)

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

    if not pending:
        return

    # Idempotency: never insert a second trade for the same signal+source.
    existing: set[tuple] = set()
    try:
        tr = (
            client.table("trades")
            .select("signal_id,source")
            .in_("signal_id", [s["id"] for s in pending])
            .execute()
        )
        existing = {(r["signal_id"], r.get("source")) for r in (tr.data or [])}
    except Exception as e:
        logger.warning("Failed to fetch existing trades: %s", e)

    for sig in pending:
        sig_id = sig["id"]
        bias = (sig.get("bias") or "NEUTRAL").upper()
        sig_strike = float(sig.get("strike") or spot or 0)
        sig_type = sig.get("option_type") or "CE"
        sig_exp = sig.get("expiry")
        is_late = _is_fired_late(sig.get("fired_at", ""))
        delay_min = _entry_delay_minutes(sig.get("fired_at", ""), now_utc)
        is_stale_signal = delay_min is not None and delay_min > MAX_ENTRY_DELAY_MIN

        # A signal past the entry cap is no longer tradable. Skip both sources.
        if is_stale_signal:
            if (sig_id, "RULES") not in existing:
                _write_skip_trade(client, sig, "RULES", "SKIPPED_STALE_SIGNAL", now_utc, symbol, bias)
                existing.add((sig_id, "RULES"))
            llm_bias = (sig.get("llm_bias") or "").upper()
            if llm_bias in ("BULLISH", "BEARISH") and (sig_id, "LLM") not in existing:
                _write_skip_trade(client, sig, "LLM", "SKIPPED_STALE_SIGNAL", now_utc, symbol, llm_bias)
                existing.add((sig_id, "LLM"))
            logger.info("Signal %s: entry skipped (%.0f min stale)", sig_id, delay_min or 0)
            continue

        # 1. RULES TRADE
        if bias == "NEUTRAL":
            if (sig_id, "RULES") not in existing:
                db_supabase.insert_trade_supabase({
                    "signal_id": sig_id,
                    "source": "RULES",
                    "symbol": symbol,
                    "strike": sig_strike,
                    "option_type": sig_type,
                    "expiry": sig_exp,
                    "bias_used": bias,
                    "entry_at": now_utc,
                    "entry_premium": 0.0,
                    "exit_premium": 0.0,
                    "exit_reason": "NON_DIRECTIONAL",
                    "target_pct": TARGET_PCT,
                    "stop_pct": STOP_PCT,
                    "net_pnl_pct": 0.0,
                    "mfe": 0.0,
                    "mae": 0.0,
                    "resolved_at": now_utc,
                    "stale_ltp": False,
                    "is_exploratory": False,
                })
                existing.add((sig_id, "RULES"))
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

            if not row_opt:
                continue

            entry_ltp = float(row_opt.get("last_price") or 0)
            vol_delta = row_opt.get("volume_delta")
            is_stale = (vol_delta is not None and vol_delta <= 0)
            moneyness = _moneyness_pct(row_opt.get("strike"), spot)

            skip_reason = None
            if moneyness is not None and moneyness > MAX_MONEYNESS_PCT:
                skip_reason = "SKIPPED_MONEYNESS"
            elif is_stale:
                skip_reason = "SKIPPED_STALE_LTP"
            elif is_late:
                skip_reason = "SKIPPED_LATE_ENTRY"
            elif entry_ltp < MIN_ENTRY_PREMIUM:
                skip_reason = "SKIPPED_LOW_PREMIUM"
            elif entry_ltp <= 0:
                skip_reason = "SKIPPED_NO_DATA"

            if (sig_id, "RULES") not in existing:
                if skip_reason:
                    _write_skip_trade(client, sig, "RULES", skip_reason, now_utc, symbol, bias, row_opt, entry_ltp)
                else:
                    db_supabase.insert_trade_supabase({
                        "signal_id": sig_id,
                        "source": "RULES",
                        "symbol": symbol,
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
                existing.add((sig_id, "RULES"))

            if skip_reason:
                logger.info("Signal %s: RULES entry skipped (%s)", sig_id, skip_reason)
            else:
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
        if llm_bias not in ("BULLISH", "BEARISH"):
            continue
        if (sig_id, "LLM") in existing:
            continue

        if llm_bias == "BULLISH":
            llm_opt = _find_matching_option(current_rows, sig_strike, "CE", sig_exp) if sig_type == "CE" else _find_atm_option(current_rows, spot, "CE", sig_exp)
        else:
            llm_opt = _find_matching_option(current_rows, sig_strike, "PE", sig_exp) if sig_type == "PE" else _find_atm_option(current_rows, spot, "PE", sig_exp)

        if not llm_opt:
            continue

        llm_entry_ltp = float(llm_opt.get("last_price") or 0)
        llm_vol_delta = llm_opt.get("volume_delta")
        llm_stale = (llm_vol_delta is not None and llm_vol_delta <= 0)
        llm_moneyness = _moneyness_pct(llm_opt.get("strike"), spot)

        skip_reason = None
        if llm_moneyness is not None and llm_moneyness > MAX_MONEYNESS_PCT:
            skip_reason = "SKIPPED_MONEYNESS"
        elif llm_stale:
            skip_reason = "SKIPPED_STALE_LTP"
        elif is_late:
            skip_reason = "SKIPPED_LATE_ENTRY"
        elif llm_entry_ltp < MIN_ENTRY_PREMIUM:
            skip_reason = "SKIPPED_LOW_PREMIUM"
        elif llm_entry_ltp <= 0:
            skip_reason = "SKIPPED_NO_DATA"

        if skip_reason:
            _write_skip_trade(client, sig, "LLM", skip_reason, now_utc, symbol, llm_bias, llm_opt, llm_entry_ltp)
            logger.info("Signal %s: LLM entry skipped (%s)", sig_id, skip_reason)
        else:
            db_supabase.insert_trade_supabase({
                "signal_id": sig_id,
                "source": "LLM",
                "symbol": symbol,
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
            if is_time_exit:
                _resolve_skipped_trade(client, tr, "SKIPPED_NO_DATA", now_utc)
                logger.info("🏁 Trade %s [%s] closed SKIPPED_NO_DATA (no matching quote)", trade_id, source)
            continue

        current_ltp = float(row_opt.get("last_price") or 0)
        if current_ltp <= 0:
            if is_time_exit:
                _resolve_skipped_trade(client, tr, "SKIPPED_NO_DATA", now_utc)
                logger.info("🏁 Trade %s [%s] closed SKIPPED_NO_DATA (zero LTP)", trade_id, source)
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
        vol_delta = row_opt.get("volume_delta")
        quote_stale = (vol_delta is not None and vol_delta <= 0)

        # Conservative tie-breaker: Stop loss takes priority if both breached in same 5-min snapshot
        if pct_change <= stop_pct:
            exit_reason = "STOP_HIT"
            exit_price = current_ltp
        elif pct_change >= target_pct:
            exit_reason = "TARGET_HIT"
            exit_price = current_ltp
        elif is_time_exit:
            # A time exit at the entry price with no volume is a frozen quote,
            # not a flat trade. Never book it as a guaranteed -0.5% loss.
            if quote_stale and abs(current_ltp - entry_price) < 0.01:
                exit_reason = "SKIPPED_STALE_EXIT"
            else:
                exit_reason = "TIME_EXIT"
            exit_price = current_ltp

        if exit_reason:
            is_skipped_exit = exit_reason.startswith("SKIPPED_")
            if is_skipped_exit:
                net_pnl = 0.0
                outcome_correct = None
            else:
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

