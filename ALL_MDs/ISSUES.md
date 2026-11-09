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

### 6. No alerting (fixed — keep-alive was already there, the status code was not)
- **Symptom:** A dead WebSocket or failed 5-minute cycle is invisible until someone checks.
- **Cause:** `/health` returned `200 OK` and `"status": "ok"` unconditionally; `feed_stale` was only a body field. The keep-alive cron (`OFA Collector Keepalive`, cron-job.org, every 10 min on `/health`) was therefore green all through 2026-11-09 while the feed collected **zero rows for the whole session**. It kept the service awake but could not report that it was broken.
- **Fix (in code):** `/health` now returns **503** with `status: "feed_stale"` when ticks are older than `MAX_TICK_AGE_SECONDS` (180s) **and** the market is open, or **503** `status: "supabase_down"` when the Supabase heartbeat fails. `_market_open_now()` gates on Mon–Fri **09:20–15:35 IST** (09:20 not 09:15 — the pre-open refresh fires at 09:10 and the socket needs a moment) and skips weekends and `NSE_HOLIDAYS`, so a stale feed after hours or on a holiday does not raise a false alarm. Render's `healthCheckPath` is empty, so a 503 cannot trigger a restart. Covered by `test_health_alert.py`.
- **Verified:** `Wed 11 Nov 10:00` open · `09:12` closed · `15:34` open · `16:00` closed · holiday `10 Nov` closed · `Sat 14 Nov` closed.
- **Alert chain (external, cron-job.org job `8512926`):** `*/10 * * * *` on `/health`, channel `Email · arthur14202000@gmail.com`, per-job trigger *"execution of the cronjob fails"* now **enabled** (it was off, so a 503 would have gone nowhere) with `Notify after: 1 failure` and `Notify via: all notification channels`. First real test is the next trading session, 2026-11-11.

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
- **Known limitation:** `signal_strength` is a percentile of raw magnitude (ΔOI / IV), not of expected return, so the cap systematically keeps the biggest anomalies — which may also be the noisiest. Reviewed with Gemini 2026-11-08: keep the cap for execution sanity, but do not treat the kept 8 as the best 8 by edge until strength is validated against outcomes.

### 11. BEARISH signals underperform (open — no fix)
- **Symptom:** On the 155 deduped clean trades, `BEARISH`/PE is 23/60 (38.3%) against `BULLISH`/CE 59/95 (62.1%).
- **Cause:** Unknown. The sample is two sessions (14–15 Oct) in one direction of the market, so this is equally consistent with a two-day up-move as with a real bias edge.
- **Decision:** **do not** add a regime filter. Filtering on 60 trades from one market move is curve fitting; it would have looked good in hindsight and taught us nothing. Re-check when there are clean trades across both directions.

### 12. IV_SPIKE is unmeasurable so far (open)
- **Symptom:** Only 3 of 72 `IV_SPIKE` signals produced a clean trade (1 win).
- **Cause:** 69 of its 82 trades fell in the 16 Oct–2 Nov frozen-feed window and were excluded as `stale_ltp`. That is the outage, not necessarily the rule.
- **Decision:** leave the rule alone until it has fired on a live session. Do not retire a rule on n=3.

### 13. Collector reused yesterday's Angel One session — a full trading day lost (P0, fixed)
- **Symptom:** Monday 2026-11-09 was a normal trading day (checked against the NSE calendar) and produced **zero** rows: `options_chain` and `market_context` still ended at 2026-11-06T10:25, `signals` at 2026-11-02. `/health` reported `ws_connected=false`, `tick_count=392` frozen at Sunday 15:26 UTC, `last_tick_age_sec` ~96,000.
- **Cause:** `_get_client()` cached the SmartConnect object for the life of the process and **`_reset_client()` had no callers anywhere**. Angel One invalidates sessions daily, so once the process survived the night every `ltpData` call failed → `_get_spot()` returned None → empty scrip map → `refresh_feed` logged `no scrips loaded — keeping existing subscription` and returned. The 09:10 pre-open refresh and every 5-minute retry failed the same way, so the socket was never resubscribed.
- **Fix (in code):** `_get_client()` re-logs in when the calendar date changes; `_login()` records the date and `_reset_client()` clears both. `refresh_feed()` now retries once with a fresh session when the map comes back empty, instead of giving up for the cycle. Covered by `test_session_refresh.py`.

### 14. Muhurat session treated as a holiday (open, minor)
- **Symptom:** NSE held a special Muhurat session on Sunday 2026-11-08 (14:20–15:30 IST) and it was skipped.
- **Cause:** `holidays.py` lists `2026-11-08` as "Diwali Laxmi Pujan", which is correct for the normal session but the exchange still trades one hour that evening.
- **Fix:** not worth special-casing a date that occurs once a year; note it if intraday coverage ever matters for a Diwali sample.

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
