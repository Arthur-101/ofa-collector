# OFA Collector — Documentation Index

> Scope: `ofa_collector/` — the cloud data collector deployed on Render. It captures the live options chain, detects anomalies, writes market context, generates LLM theses, and resolves trades.
> Last verified: 2026-11-07.

## Files

| File | What it tracks |
|---|---|
| [PROJECT.md](PROJECT.md) | Pipeline, modules, deployment, Supabase schema usage |
| [DONE.md](DONE.md) | Completed work, newest first |
| [TODO.md](TODO.md) | Open work, prioritized |
| [PHASES.md](PHASES.md) | Roadmap phases and status |
| [ISSUES.md](ISSUES.md) | Known issues, root causes, fixes |

Related docs:
- Backend: [../../ofa/ALL_MDs/README.md](../../ofa/ALL_MDs/README.md)
- Frontend: [../../ofa_frontend/ALL_MDs/README.md](../../ofa_frontend/ALL_MDs/README.md)

## Current status

- **Deployment:** Render web service, `Procfile` → `python main.py`; Python 3.11 via `nixpacks.toml`.
- **Runtime:** Angel One SmartAPI WebSocket → in-memory ticks → 5-minute flush → SQLite + Supabase → cloud analysis.
- **Cloud:** Supabase is the system of record for `options_chain`, `market_context`, `signals`, `news`, `trades`, `daily_strike_stats`.
- **Latest cloud session:** 2026-11-06 — spot 25,030.35, PCR 0.64, RSI 85.73, max pain 22,900, put wall 22,000, call wall 23,000, AI stance Bullish · 64.
- **Repo:** `https://github.com/Arthur-101/ofa-collector.git`, branch `main`, last commit 2026-10-13.

## Run

```powershell
cd ofa_collector
python main.py
# endpoints: /health, /status, /data?date=YYYY-MM-DD
```

Requires `.env` with Angel One credentials and a Supabase **service_role** key. Startup fails loudly if the key is anon or missing.
