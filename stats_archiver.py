# stats_archiver.py — Daily Strike-Level Rolling Stats Archiver for OFA Cloud Collector
#
# Generates normalized rolling baseline stats into `strike_daily_stats` table:
# (trade_date, strike_offset, option_type, time_bucket, dte, volume, volume_delta, oi_change)
#
# Rules:
# 1. Strictly nearest active weekly contract (min expiry >= trade_date).
# 2. strike_offset is centered on ATM: (strike - ATM_strike) in 50-pt increments.
# 3. time_bucket is in IST ("HH:MM" e.g. "09:15" to "15:30").
# 4. volume_delta is non-negative incremental 5-min volume.

import logging
from datetime import datetime, date, timezone
from collections import defaultdict
import pytz
import numpy as np

import db_supabase

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")


def archive_daily_strike_stats(trade_date: str, dry_run: bool = False) -> int:
    """
    Extracts, normalizes, and stores 5-minute strike intervals for trade_date
    into strike_daily_stats.
    """
    logger.info("📊 Starting daily strike stats archiving for %s...", trade_date)
    client = db_supabase.get_supabase()

    # 1. Fetch all options_chain rows for trade_date
    all_rows = []
    offset = 0
    batch_size = 1000
    while True:
        try:
            res = (
                client.table("options_chain")
                .select("timestamp,expiry,strike,option_type,oi,oi_change,volume,spot_price")
                .gte("timestamp", f"{trade_date}T00:00:00")
                .lte("timestamp", f"{trade_date}T23:59:59.999999")
                .order("timestamp", desc=False)
                .range(offset, offset + batch_size - 1)
                .execute()
            )
            batch = res.data or []
            all_rows.extend(batch)
            if len(batch) < batch_size:
                break
            offset += batch_size
        except Exception as e:
            logger.error("Failed to fetch options_chain rows for %s: %s", trade_date, e)
            break

    if not all_rows:
        logger.warning("No options_chain rows found for %s", trade_date)
        return 0

    logger.info("Found %d total options_chain rows for %s", len(all_rows), trade_date)

    # 2. Determine nearest active weekly expiry (min expiry >= trade_date)
    expiries = {r["expiry"] for r in all_rows if r.get("expiry")}
    valid_expiries = sorted([e for e in expiries if e >= trade_date])
    if not valid_expiries:
        logger.warning("No active expiry >= %s found", trade_date)
        return 0

    target_expiry = valid_expiries[0]
    trade_d = date.fromisoformat(trade_date)
    exp_d = date.fromisoformat(target_expiry)
    dte = max(0, (exp_d - trade_d).days)
    logger.info("Selected nearest weekly expiry: %s (DTE: %d)", target_expiry, dte)

    # 3. Filter strictly to target expiry
    expiry_rows = [r for r in all_rows if r.get("expiry") == target_expiry]
    if not expiry_rows:
        return 0

    # 4. Group rows by snapshot timestamp
    by_timestamp = defaultdict(list)
    for r in expiry_rows:
        ts = r.get("timestamp")
        if ts:
            by_timestamp[ts].append(r)

    sorted_timestamps = sorted(by_timestamp.keys())
    logger.info("Processing %d 5-minute intervals for %s", len(sorted_timestamps), trade_date)

    # 5. Process each interval chronologically to compute volume_delta
    prev_volumes = {}
    prev_ois = {}
    stat_rows = []

    for ts in sorted_timestamps:
        interval_rows = by_timestamp[ts]

        # Convert UTC timestamp to IST time bucket HH:MM
        try:
            utc_dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
            ist_dt = utc_dt.astimezone(IST)
            time_bucket = ist_dt.strftime("%H:%M")
        except Exception:
            time_bucket = ts[11:16]

        # Determine spot and ATM strike for this interval
        spots = [float(r["spot_price"]) for r in interval_rows if r.get("spot_price") and float(r["spot_price"]) > 0]
        if not spots:
            continue
        spot = float(np.median(spots))
        atm_strike = round(spot / 50.0) * 50.0

        for r in interval_rows:
            strike = float(r.get("strike") or 0)
            opt_type = r.get("option_type")
            if not opt_type or strike <= 0:
                continue

            strike_offset = int(round(strike - atm_strike))
            # Keep reasonable strike offsets (within +/- 1500 points of ATM)
            if abs(strike_offset) > 1500:
                continue

            vol = int(r.get("volume") or 0)
            oi = int(r.get("oi") or 0)
            key = (strike, opt_type)

            prev_v = prev_volumes.get(key)
            vol_delta = (vol - prev_v) if prev_v is not None else 0
            if vol_delta < 0:
                vol_delta = 0  # guard against feed restarts / counter resets
            prev_volumes[key] = vol

            prev_oi_val = prev_ois.get(key)
            oi_change = (oi - prev_oi_val) if prev_oi_val is not None else (r.get("oi_change") or 0)
            prev_ois[key] = oi

            stat_rows.append({
                "trade_date": trade_date,
                "strike_offset": strike_offset,
                "option_type": opt_type,
                "time_bucket": time_bucket,
                "dte": dte,
                "volume": vol,
                "volume_delta": vol_delta,
                "oi_change": oi_change,
            })

    logger.info("Generated %d strike_daily_stats records for %s", len(stat_rows), trade_date)

    if dry_run:
        return len(stat_rows)

    # 6. Insert into Supabase
    inserted = db_supabase.insert_strike_daily_stats_supabase(stat_rows)
    logger.info("✅ Archived %d strike_daily_stats rows to Supabase for %s", inserted, trade_date)
    return inserted


def seed_all_historical_stats(dry_run: bool = False) -> dict:
    """
    Seed strike_daily_stats across all trading dates stored in options_chain.
    """
    client = db_supabase.get_supabase()
    # Discover distinct trade dates
    dates_found = set()
    offset = 0
    batch_size = 5000
    while True:
        try:
            res = client.table("options_chain").select("timestamp").order("timestamp", desc=False).range(offset, offset + batch_size - 1).execute()
            batch = res.data or []
            for r in batch:
                ts = r.get("timestamp")
                if ts:
                    dates_found.add(ts[:10])
            if len(batch) < batch_size:
                break
            offset += batch_size
        except Exception as e:
            logger.error("Error discovering dates: %s", e)
            break

    sorted_dates = sorted(list(dates_found))
    logger.info("Discovered %d historical trading dates: %s", len(sorted_dates), sorted_dates)

    results = {}
    for d in sorted_dates:
        count = archive_daily_strike_stats(d, dry_run=dry_run)
        results[d] = count

    return results
