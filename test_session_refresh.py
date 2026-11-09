"""Self-check for the daily Angel One session refresh. Run: python test_session_refresh.py

Regression guard: on 2026-11-09 the collector kept Sunday's expired session all
day, so no spot price resolved, no scrips loaded and the feed captured nothing.
"""

from datetime import date
import angel_fetcher as af

calls = []


def fake_login():
    calls.append(1)
    af._client = object()
    af._client_date = date.today()
    return af._client


def test_same_day_reuses_session():
    af._login = fake_login
    af._reset_client()
    af._get_client()
    assert len(calls) == 1, calls
    af._get_client()
    assert len(calls) == 1, "a second call on the same day must reuse the session"


def test_new_day_forces_login():
    af._client_date = date(2020, 1, 1)
    af._get_client()
    assert len(calls) == 2, "a stale session date must trigger a fresh login"


def test_reset_clears_date():
    af._reset_client()
    assert af._client is None and af._client_date is None
    af._get_client()
    assert len(calls) == 3


if __name__ == "__main__":
    test_same_day_reuses_session()
    test_new_day_forces_login()
    test_reset_clears_date()
    print("session refresh ok: same-day reuse, new-day login, reset clears both")
