"""Self-check for the /health alert. Run: python test_health_alert.py

Regression guard: on 2026-11-09 the feed was dead and collected zero rows all
day, but /health returned 200 OK on every keep-alive ping because feed_stale
was only a body field. A dead feed during market hours must fail the check.
"""

import api
import ws_feed


def _setup(tick_age, market_open, sb_alive=True):
    ws_feed.last_tick_age_seconds = lambda: tick_age
    api._market_open_now = lambda: market_open
    api.supabase_heartbeat = lambda: sb_alive
    r = api.health()
    return r.status_code, r.body and __import__("json").loads(r.body)["status"]


def test_stale_feed_during_market_hours_alerts():
    assert _setup(900, True) == (503, "feed_stale"), "a dead feed in market hours must fail"


def test_stale_feed_when_market_closed_is_fine():
    assert _setup(900, False) == (200, "ok"), "stale after hours or on a holiday is normal"


def test_fresh_feed_is_healthy():
    assert _setup(30, True) == (200, "ok")


def test_dead_supabase_alerts():
    assert _setup(30, True, sb_alive=False) == (503, "supabase_down")


if __name__ == "__main__":
    test_stale_feed_during_market_hours_alerts()
    test_stale_feed_when_market_closed_is_fine()
    test_fresh_feed_is_healthy()
    test_dead_supabase_alerts()
    print("health alert ok: stale-in-hours 503, stale-off-hours 200, fresh 200, no-supabase 503")
