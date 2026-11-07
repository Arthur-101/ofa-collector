# OFA Collector — Issues & Solutions

> Symptom → cause → fix/status.

## Open

### 1. Option feed serves expired/frozen contracts (P0)
- **Symptom:** Since the 2026-10-15 expiry, 185/185 resolved trades have `entry_premium == exit_premium`, `stale_ltp=True`, `MFE=MAE=0`; 54 book exactly −0.50%.
- **Cause:** `start_feed()` subscribes once at startup and never reloads on rollover. `MIN_DTE=2` skipped the nearest weekly expiry. `flush_to_db()` re-wrote the last tick snapshot every 5 minutes with no age check. On 2026-11-02 the chain still contained the expired `2026-10-27` series; 23000 CE had only 2 distinct LTPs all day while spot had 75.
- **Fix (in code):** `MIN_DTE=0`; reload instruments + resubscribe before the open and on an empty trading-day flush; skip ticks older than 3 minutes; skip expired expiries; compute `oi_change` only against a same-day prior snapshot; expose `last_tick_age_sec` in `/health` and `/status`.

### 2. Resolver books stale entries as −0.50% (P0)
- **Symptom:** Every post-rollover time exit is a guaranteed −0.50% (cost only), so the backtest reads 0% win rate.
- **Cause:** `stale_ltp` was recorded but never enforced. The 15:20 IST time exit closed at the same frozen LTP used for entry. Deep-ITM strikes (e.g. 24,200 PE with spot ~23,000) never trade.
- **Fix (in code):** `SKIPPED_STALE_LTP`, `SKIPPED_NO_DATA`, `SKIPPED_MONEYNESS` (>1.5% from spot), and `SKIPPED_STALE_SIGNAL` (>15 min entry delay) all write zero PnL. A stale exit is skipped, never booked as a flat loss.

### 3. Retention purge paused
- **Symptom:** Supabase storage grows without bound.
- **Cause:** Purge disabled on 2026-10-12 for signal backfill (commit `d545978`).
- **Fix:** Confirm backfill complete, choose a retention window, re-enable the purge job, monitor row counts.

### 4. Only 3 trades evaluated (superseded)
- **Symptom:** Backtest reports 0% win rate on 3 trades.
- **Cause:** The 7-day window only covered the broken post-rollover feed (see #1/#2), not a lack of resolution.
- **Fix:** Fixed by #1/#2 plus the 30-day backtest default. Do not surface win rate below ~30 clean trades.

### 5. Stale or missing `entry_premium`
- **Symptom:** Trades could resolve against a missing entry price.
- **Cause:** Detector previously defaulted entry premium.
- **Fix:** `entry_premium` initialised to `None` (commit `9d668c3`); resolution now strict t+1. Audit remaining nulls.

### 6. No alerting
- **Symptom:** A dead WebSocket or failed 5-minute cycle is invisible until someone checks.
- **Cause:** Only `/health` and logs exist.
- **Fix:** Add external uptime check on `/health` (now exposes `last_tick_age_sec` / `feed_stale`) and a failure notifier for `run_cloud_analysis` exceptions.

### 7. Unpinned dependencies
- **Symptom:** Render builds can drift or break.
- **Cause:** `requirements.txt` mostly unpinned.
- **Fix:** Pin versions and test a clean install.

### 8. Local mirror divergence
- **Symptom:** Collector SQLite is wiped daily; local `ofa/options_flow.db` ends 2026-10-01.
- **Cause:** By design — collector SQLite is a temp buffer, Supabase is the record.
- **Fix:** Document clearly; do not treat collector SQLite as history.

## Resolved

### Anon key silently used
- **Symptom:** Writes failed or were restricted.
- **Fix:** `verify_credentials()` fails loudly unless a service_role key is present (commit `12831da`).

### Wrong expiry in detector
- **Symptom:** Signals referenced a non-weekly or far expiry.
- **Fix:** Restrict to nearest active weekly expiry (commits `38ddf6d`, `218440a`).

### 0DTE missed
- **Symptom:** Same-day expiry signals never fired.
- **Fix:** `MIN_DTE=0` (commit `12831da`).

### Missing `symbol` on backfilled trades
- **Symptom:** Backfilled audit trades lacked symbol context.
- **Fix:** Symbol handling added; 327 trades backfilled (commit `41ca84c`).

## Graph audit note

Graphify reports 343 dangling-endpoint edges and 15 collapsed edge pairs across the corpus. These are AST resolution artifacts, not runtime faults. Noted so the graph warning is not read as a collector bug.
