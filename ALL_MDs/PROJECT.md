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
| `shutdown_job` | 16:00 | **Commented out** — service idles after close |

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

```
Procfile:        web: python main.py
nixpacks.toml:   python311 + gcc, NIXPACKS_PYTHON_VERSION=3.11
```

No Dockerfile. Render supplies `PORT`; `config.py` reads it.
