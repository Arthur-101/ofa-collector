# OFA Collector — Phases

> Status: ✅ done · 🟡 in progress · ⬜ not started

| Phase | Goal | Status | Notes |
|---|---|---|---|
| **0. Live capture** | SmartAPI WebSocket, SQLite buffer, scheduled flush | ✅ | 5-minute flush; holiday-aware |
| **1. Cloud sync** | Push chain + context to Supabase | ✅ | Service-role key enforced |
| **2. Detection v1** | Basic anomaly/signal detection | ✅ | Superseded by v2 |
| **3. Cloud intelligence** | Autonomous anomaly detection, context snapshot, LLM thesis on Render | ✅ | Shipped 2026-10-01 |
| **4. Detection v2** | Quadrant engine, discrete strike stats, audit trades | ✅ | Shipped 2026-10-13 |
| **5. Outcome resolution** | Target/stop, MFE/MAE, strict t+1 entry | 🟡 | Engine shipped; only 3 trades evaluated |
| **6. Data lifecycle** | Retention purge, archiving, storage bounds | 🟡 | Archiving active; **purge paused** |
| **7. Reliability** | Alerting, tests, runbooks, pinned deps | ⬜ | No alerting or tests |

## Phase gates

- Phase 5 completes when every eligible signal resolves and the evaluated sample is ≥ 30 trades.
- Phase 6 completes when purge is re-enabled with a documented cutoff and a verified backfill.
- Phase 7 starts after Phase 5 and 6 are stable; otherwise alerts will fire on known-bad data.

## Deployment cadence

Render auto-deploys `main`. Keep `main` deployable: `python main.py` must start with valid credentials, and `requirements.txt` must cover every import.
