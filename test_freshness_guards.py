# test_freshness_guards.py — self-check for the stale-tick and resolver guards
#
# Run: python test_freshness_guards.py
# No network, no DB. Covers the pure helpers that decide whether a quote is
# trustworthy and whether a signal is still tradable.

from datetime import datetime, timedelta, timezone

import resolver
import ws_feed


def test_tick_age_guard():
    now = datetime.now(timezone.utc)

    fresh = ws_feed._parse_tick_ts((now - timedelta(seconds=30)).isoformat())
    stale = ws_feed._parse_tick_ts((now - timedelta(seconds=600)).isoformat())
    assert fresh is not None and stale is not None
    assert (now - fresh).total_seconds() < ws_feed.MAX_TICK_AGE_SECONDS
    assert (now - stale).total_seconds() > ws_feed.MAX_TICK_AGE_SECONDS

    # 'Z' suffix and naive timestamps must both parse.
    assert ws_feed._parse_tick_ts("2026-11-07T09:30:00Z") is not None
    assert ws_feed._parse_tick_ts("2026-11-07T09:30:00").tzinfo is not None
    assert ws_feed._parse_tick_ts(None) is None
    assert ws_feed._parse_tick_ts("not-a-time") is None


def test_entry_delay_guard():
    fired = "2026-11-07T04:00:00+00:00"
    assert resolver._entry_delay_minutes(fired, "2026-11-07T04:05:00+00:00") == 5.0
    assert resolver._entry_delay_minutes(fired, "2026-11-07T04:20:00+00:00") > resolver.MAX_ENTRY_DELAY_MIN
    assert resolver._entry_delay_minutes("bad", "2026-11-07T04:05:00+00:00") is None


def test_moneyness_guard():
    spot = 23000.0
    assert resolver._moneyness_pct(23000, spot) == 0.0
    assert resolver._moneyness_pct(23200, spot) < resolver.MAX_MONEYNESS_PCT
    # 24,200 PE against 23,000 spot is 5.2% away — the illiquid deep-ITM case.
    assert resolver._moneyness_pct(24200, spot) > resolver.MAX_MONEYNESS_PCT
    assert resolver._moneyness_pct(23000, 0) is None
    assert resolver._moneyness_pct(None, spot) is None


if __name__ == "__main__":
    test_tick_age_guard()
    test_entry_delay_guard()
    test_moneyness_guard()
    print("OK: freshness + resolver guards")
