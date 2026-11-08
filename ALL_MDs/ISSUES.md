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

### 9. VOLUME_SPIKE could never be scored (fixed)
- **Symptom:** 181 of 574 signals since 14 Oct were `VOLUME_SPIKE`, all excluded as `NON_DIRECTIONAL`. Zero measurable trades from a third of the feed.
- **Cause:** `_detect_volume_spike` hardcoded `bias="NEUTRAL"` — "high volume, flat OI" has no direction, so the resolver always excluded it. It was noise by construction, not by performance.
- **Fix (in code):** detector and its call removed 2026-11-08; the unused `BOOTSTRAP_VOLUME_PERCENTILE` / `FULL_VOLUME_ZSCORE` constants and the `llm_engine` prompt line went with it. Historical rows keep the label so the frontend filter still matches them. Covered by `test_flush_cap.py`.

### 10. One flush could emit 26 signals (fixed)
- **Symptom:** A single detection run fired up to 26 signals at the same timestamp (14 Oct 09:25, 21 Oct 15:35 IST); 31 of 148 flushes emitted 5 or more.
- **Cause:** No cap — every strike that cleared its threshold was inserted.
- **Fix (in code):** `_cap_per_flush` keeps the top `MAX_SIGNALS_PER_RUN` (8) by `signal_strength`. `test_flush_cap.py` asserts the cap, the ordering and that smaller flushes pass through untouched.

### 11. BEARISH signals underperform (open — no fix)
- **Symptom:** On the 155 deduped clean trades, `BEARISH`/PE is 23/60 (38.3%) against `BULLISH`/CE 59/95 (62.1%).
- **Cause:** Unknown. The sample is two sessions (14–15 Oct) in one direction of the market, so this is equally consistent with a two-day up-move as with a real bias edge.
- **Decision:** **do not** add a regime filter. Filtering on 60 trades from one market move is curve fitting; it would have looked good in hindsight and taught us nothing. Re-check when there are clean trades across both directions.

### 12. IV_SPIKE is unmeasurable so far (open)
- **Symptom:** Only 3 of 72 `IV_SPIKE` signals produced a clean trade (1 win).
- **Cause:** 69 of its 82 trades fell in the 16 Oct–2 Nov frozen-feed window and were excluded as `stale_ltp`. That is the outage, not necessarily the rule.
- **Decision:** leave the rule alone until it has fired on a live session. Do not retire a rule on n=3.

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
