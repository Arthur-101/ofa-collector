# OFA Collector — Completed Work

> Newest first. Dates from git history.

## 2026-11-07

- **Feed freshness + rollover fix (P0)** — `MIN_DTE=0` so the nearest weekly (including 0DTE) is collected; `refresh_feed()` reloads the instrument master and resubscribes; pre-open refresh job at 09:10 IST; empty trading-day flush triggers a refresh with a 10-minute cooldown.
- **Stale-tick guard** — `flush_to_db()` drops ticks older than 3 minutes, skips expired expiries, and computes `oi_change`/`volume_delta` only against a same-day prior snapshot. `get_latest_snapshot_with_ts_supabase()` added to support the day check.
- **Feed health exposed** — `/health` and `/status` return `last_tick_age_sec` and `feed_stale`.
- **Resolver skip paths (P0)** — `SKIPPED_STALE_LTP`, `SKIPPED_NO_DATA`, `SKIPPED_MONEYNESS` (>1.5% from spot), `SKIPPED_STALE_SIGNAL` (>15 min entry delay), `SKIPPED_STALE_EXIT`. All write zero PnL and `outcome_correct=None`, so a frozen quote can never become a flat −0.50% loss.
- **Duplicate-trade guard** — the resolver never inserts a second trade for the same `signal_id` + `source`.
- **Self-check** — `test_freshness_guards.py` covers the tick-age, entry-delay, and moneyness helpers.

## 2026-10-13

- **Strict t+1 resolution** — `entry_premium` initialised to `None` so unresolved trades are not silently marked. Commit `9d668c3`.
- **Trade audit backfill** — `symbol` column handling added; 327 audit trades backfilled. Commit `41ca84c`.
- **Nearest weekly expiry + trading-day DTE** — detector restricted to the nearest weekly expiry; DTE computed on trading days. Commit `38ddf6d`.
- **Fail-loud credentials, 0DTE, discrete strike stats, auditable trades** — refuses to start without service_role; `MIN_DTE=0`; discrete strike stats; backfill. Commit `12831da`.
- **RULES + LLM audit** — both trade sources audited with exploratory tag; strike offset limited to 31 strikes. Commit `257fa78`.
- **Detector v2** — quadrant engine, audit trades table, rolling strike stats. Commit `b0892eb`.

## 2026-10-12

- **Real-time resolution engine** — targets, stops, MFE/MAE. Commit `e975352`.
- **Execution fields** — `entry_premium`, `dte`, target/stop auto-populated on future signals. Commit `ed46496`.
- **Weekly-expiry wall/max-pain filter**. Commit `218440a`.
- **Retention purge paused** — temporarily disabled for signal backfill. Commit `d545978`. ⚠️ still paused.

## 2026-10-01

- **NSE holiday calendar + zero-volume safeguard**. Commit `5cd4fe4`.
- **Autonomous cloud pipeline** — anomaly detection, market context snapshot, LLM thesis pipeline on Render. Commit `8cbb741`.

## Earlier

- WebSocket feed, scheduler, exporter, Supabase migration, news fetcher.
- `stats_archiver.py` for the 20-day rolling strike baseline.

## Verified on 2026-11-07

- Supabase serving live data; latest session 2026-11-06.
- Cloud snapshot: spot 25,030.35, PCR 0.64, RSI 85.73, max pain 22,900, put wall 22,000, call wall 23,000, stance Bullish · 64.
- Backtest: 3 evaluated trades, 0% win rate, avg −0.50%.
