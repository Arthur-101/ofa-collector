# ofa-collector

Lightweight Railway-deployed data collector for the Options Flow Analyzer project.

Runs during Indian market hours (9:15 AM – 3:30 PM IST), collects NIFTY options chain data every 5 minutes via Angel One WebSocket, and exposes it via a REST API.

## Architecture

```
9:00 AM IST  → DB wiped (fresh slate)
9:15 AM IST  → WebSocket connects, data collection begins
Every 5 min  → flush_to_db() → writes to temp SQLite
[Any time]   → GET /data?date=today → returns all rows so far
3:31 PM IST  → CSV exported → pushed to Arthur-101/ofa-data GitHub repo
4:00 PM IST  → Scheduler stops
```

## API Endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /health` | Liveness check — WS state, tick count |
| `GET /status` | DB row count + feed state |
| `GET /data?date=YYYY-MM-DD` | All rows for a date (default: today) |

## Files

| File | Purpose |
|------|---------|
| `main.py` | Entry point — starts WS + scheduler + FastAPI |
| `ws_feed.py` | Angel One WebSocket feed + flush logic |
| `angel_fetcher.py` | Login + instrument master |
| `db.py` | SQLite setup with daily reset |
| `scheduler.py` | APScheduler — reset, poll, export, shutdown |
| `api.py` | FastAPI endpoints |
| `exporter.py` | CSV export → push to ofa-data repo |
| `config.py` | All constants |
| `Procfile` | Railway process definition |

## Environment Variables (set in Railway)

```
ANGEL_API_KEY=
ANGEL_CLIENT_ID=
ANGEL_PASSWORD=
ANGEL_TOTP_SECRET=
GITHUB_TOKEN=         ← Personal Access Token with repo write access
```

## Local Catch-up

Run from the main OFA project:
```bash
python catchup.py
```
This hits the Railway API and imports all missed rows into the local `options_flow.db`.

## Deploy to Railway

1. Push this repo to GitHub (`Arthur-101/ofa-collector`)
2. New project on Railway → Deploy from GitHub repo
3. Set all env vars in Railway dashboard
4. Railway auto-detects `Procfile` and runs `python main.py`
