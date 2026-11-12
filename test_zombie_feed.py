"""Self-check for zombie WebSocket recovery and null-spot flushes.
Run: python test_zombie_feed.py

Regression guard: on 2026-11-12 the socket stayed "connected" while delivering
no ticks for 15+ minutes. start_feed refused to replace it because _connected
was still True, and flush_to_db wrote 401 chain rows with spot_price=None.
"""

from datetime import datetime, timezone

import ws_feed


class AliveThread:
    def is_alive(self):
        return True


def test_fresh_feed_is_not_replaced():
    ws_feed._ws_thread = AliveThread()
    ws_feed._connected = True
    ws_feed.last_tick_age_seconds = lambda: 10
    ws_feed._af._login = lambda: (_ for _ in ()).throw(
        AssertionError("must not replace a feed that is still ticking")
    )
    ws_feed.start_feed(["NIFTY"])  # guard returns before any login


def test_silent_feed_is_replaced():
    ws_feed._ws_thread = AliveThread()
    ws_feed._connected = True
    ws_feed.last_tick_age_seconds = lambda: ws_feed.MAX_TICK_AGE_SECONDS + 1
    logins = []

    def fake_login():
        logins.append(1)
        raise RuntimeError("stop here")

    ws_feed._af._login = fake_login
    try:
        ws_feed.start_feed(["NIFTY"])
    except RuntimeError:
        pass
    assert logins, "a silent feed must be replaced (login attempted)"


def test_no_spot_skips_flush():
    import holidays

    holidays.is_market_holiday = lambda: (False, "")
    ws_feed._tick_store = {
        "1": {
            "ltp": 100.0,
            "oi": 1,
            "volume": 1000,
            "ts": datetime.now(timezone.utc).isoformat(),
        }
    }
    ws_feed._token_map = {
        "1": {"name": "NIFTY", "expiry": "2099-01-01", "strike": 25000.0, "option_type": "CE"}
    }
    ws_feed._af._get_client = lambda: object()
    ws_feed._af._get_spot = lambda client, sym: None
    ws_feed._af._login = lambda: object()
    written = []
    ws_feed.insert_options_rows = lambda rows: written.append(rows)

    out = ws_feed.flush_to_db("NIFTY")
    assert out == [] and not written, "a flush without spot must not persist rows"


if __name__ == "__main__":
    test_fresh_feed_is_not_replaced()
    test_silent_feed_is_replaced()
    test_no_spot_skips_flush()
    print("zombie feed ok: fresh kept, silent replaced, null-spot flush skipped")
