# OFA Collector — Project Details

> The collector is the always-on cloud producer. It owns live market capture and cloud-side intelligence; `ofa/` owns the serving layer.

## Startup sequence (`main.py`)

1. `verify_credentials()` — fail loudly unless a Supabase `service_role` key is present.
2. `init_db()` — create local SQLite tables.
3. `start_feed(SYMBOLS)` — Angel One SmartAPI WebSocket in a background thread.
4. Wait 20s for the feed to stabilise.
5. `start_scheduler()` — APScheduler in a background thread.
6. `uvicorn.run(app)` — FastAPI blocks the main thread.

## Scheduled jobs (`scheduler.py`, IST, Mon–Fri)

| Job | Time | Action |
|---|---|---|
| `reset_job` | 09:00 | Wipe temp SQLite for the new session |
| `poll_job` | every 5 min, 09:15–15:30 | Flush WS ticks to DB, then `run_cloud_analysis` |
| `export_job` | 15:31 | Export the day's CSV to GitHub |
| `archive_daily_strike_stats_job` | 15:31 | Archive 5-min strike stats for the 20-day rolling baseline |
| `shutdown_job` | 16:00 | `os._exit(0)` — container stops so billing stops (see Deployment) |

All jobs skip NSE holidays via `holidays.py`.

## The 5-minute cloud cycle (`cloud_pipeline.py`)

`run_cloud_analysis(symbol, rows)`:

1. Detect anomalies / signals (`detector.py`, v2 quadrant engine).
2. Insert signals into Supabase (dedupe via `signal_already_fired_today_supabase`).
3. Compute and insert a `market_context` snapshot (spot, PCR, RSI, max pain, walls, AI stance).
4. Run LLM thesis generation (`llm_engine.py`) for new signals.
5. Resolve open trades (`resolver.py`) — target/stop, MFE/MAE, outcome.

## Module map

| Module | Responsibility |
|---|---|
| `main.py` | Process orchestration |
| `config.py` | Symbols, intervals, env |
| `ws_feed.py` | SmartAPI WebSocket, tick buffer, `flush_to_db` |
| `angel_fetcher.py` | Instrument master fetch |
| `db.py` | Local SQLite schema/queries |
| `db_supabase.py` | All Supabase reads/writes (23 functions) |
| `detector.py` | Signal detection (v2 quadrant engine) |
| `cloud_pipeline.py` | 5-minute analysis orchestration |
| `context.py` | Market context metrics |
| `llm_engine.py` | OpenRouter 7-model fallback chain |
| `news_fetcher.py` | News ingestion |
| `resolver.py` | Trade entry/exit resolution, MFE/MAE |
| `stats_archiver.py` | Daily strike stats, 20-day rolling baseline |
| `exporter.py` | End-of-day CSV export |
| `api.py` | `/health`, `/data`, `/status` |
| `backfill_trades.py` | Backfill audit trades |
| `seed_strike_stats.py` | Seed historical strike stats |

## Supabase tables used

- `options_chain` — raw 5-minute chain snapshots.
- `market_context` — per-symbol snapshot (spot, PCR, RSI, VIX, max pain, walls, stance).
- `signals` — detected signals with execution fields and thesis.
- `news` — headlines and summaries.
- `trades` — auditable RULES and LLM trades; source of truth for backtest.
- `daily_strike_stats` — 20-day rolling strike baseline.

## Detector rules

- Restricted to the **nearest active weekly expiry**.
- `MIN_DTE=0` so 0DTE is captured.
- Strike offset limited to ±31 strikes.
- `entry_premium` initialised to `None` so t+1 resolution stays strict.
- Audits both `RULES` and `LLM` trades with an exploratory tag.
- Discrete strike stats from `daily_strike_stats`.

## Deployment

Railway, US West, service `web` → `https://ofa-collector.up.railway.app`.

```
Procfile:        web: python main.py
Cron:            15 3 * * 1-5   (03:15 UTC = 08:45 IST, Mon–Fri)
```

No Dockerfile. Railway supplies `PORT` (`config.py:45`, falls back to 8000
locally). The live build runs **Python 3.13.18** — `nixpacks.toml` pins 3.11 but
is not taking effect, so treat it as legacy rather than as the real build spec.

**Market hours only, on the Free plan.** Railway bills by uptime, not by replica
limits: 24/7 costs ~$2.14/mo against a $1 grant, while 08:45–16:00 IST on
weekdays measured ~$0.77/mo, which fits permanently. That is why
`shutdown_job` must keep exiting at 16:00 — a container that never exits bills
all night and all weekend.

Two consequences worth knowing:

- **`/health` returns 502 outside 09:00–16:00 IST.** No container is running, so
  nothing answers. This is correct, not an outage. The projects-list badge
  "1/1 service online" is project-level and does not mean a container is up.
- **Redeploys cost egress** ($0.05/GB), and egress was 42% of the first month's
  bill. Deploy rarely.

Railway cron services have **no restart policy**. A run that fails to exit makes
the next day's run be skipped silently, so `cron-job.org` pings `/health` at
09:15 and 09:25 IST on weekdays as the detector.

## Retention

`db_supabase.purge_old_data_supabase()` deletes `options_chain` older than
`RETENTION_DAYS = 45` calendar days (~30 trading days) after every export.
`signals`, `trades` and `strike_daily_stats` are never purged. Neither is
`news_raw` — it is 3.6 MB over 38 days (~34 MB/year), small enough not to
bother.

45 days, not 60, is measured: after `VACUUM` + `REINDEX` the table is 204 B/row
heap + 120 B/row index ≈ 9.3 MB per 29k-row trading day, so 30 trading days is
~280 MB of the 500 MB free tier. 60 days would reach ~435 MB, leaving no room
for autovacuum. `options_chain` also has non-default autovacuum factors
(`0.02`) because a daily 29k-row delete is too small a fraction of a large
table for the defaults to fire promptly.

`options_chain` cannot be backfilled — Angel One's historical API returns OHLCV
only, with no OI/IV — so a purged day is gone for good.
