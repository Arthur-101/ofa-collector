# OFA Collector — To Do

> Prioritized. Move finished items to [DONE.md](DONE.md).

## P0 — data integrity

- [x] **Deploy the feed freshness/rollover fix.** Done 2026-11-07: pushed `4d0d266`, manually deployed on Render (service `srv-darjc617lnhs73dkoku0`, `Deploy succeeded|Live`, 1m 35s). Live `/health` returns the new fields (`last_tick_age_sec`, `feed_stale`), WS connected with 388 scrips. Still needs one live trading session to confirm ticks are fresh during market hours.
- [ ] **Verify the resolver skip paths.** Confirm `SKIPPED_STALE_LTP`, `SKIPPED_NO_DATA`, `SKIPPED_MONEYNESS`, `SKIPPED_STALE_SIGNAL` appear instead of flat −0.50% time exits.
- [ ] **Re-enable retention purge.** Paused on 2026-10-12 for backfill (`d545978`). Storage grows every session. Define a cutoff, verify backfill is complete, then restore the purge.
- [ ] **Grow the clean trade sample.** Re-run the backtest after the feed fix; confirm the resolver processes every eligible signal and that stale/ITM entries are excluded.
- [ ] **Validate outcome fields.** `outcome_correct` / `outcome_7d` are unpopulated in the local mirror; confirm cloud writes are consistent.

## P1 — reliability

- [ ] **Alerting.** `/health` now reports `last_tick_age_sec` and `feed_stale`; add an external uptime check and error notifications.
- [ ] **Idempotency audit.** Verify `signal_already_fired_today_supabase` covers restarts and duplicate 5-minute windows.
- [ ] **Holiday coverage.** Keep `holidays.py` current for the next calendar year; add a test for special sessions (muhurat, half-days).
- [ ] **Backfill scripts.** `backfill_trades.py` and `seed_strike_stats.py` need runbooks (inputs, expected counts, rollback).

## P2 — quality

- [ ] **Tests.** Add tests for detector rules, resolver boundaries (target hit vs stop same bar), stale-tick guard, and expiry selection.
- [ ] **LLM cascade observability.** Log which of the 7 models answered, latency, and fallback depth.
- [ ] **News summaries.** Confirm `news_fetcher.py` writes summaries, not just headlines.
- [ ] **Pin dependencies.** `requirements.txt` is mostly unpinned; pin for reproducible Render builds.

## Explicitly out of scope

- Multi-symbol expansion beyond NIFTY until single-symbol resolution is trustworthy.
- Moving off Supabase free tier.
