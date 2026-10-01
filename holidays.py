# holidays.py — NSE Trading Holiday Calendar & Market State Checker
#
# Provides official NSE holiday dates and active market check.
# Prevents collector from wiping DB, polling empty feeds, or inserting
# flat zero-volume data on market holidays (e.g. Oct 2 Gandhi Jayanti).

import logging
from datetime import datetime, date
import pytz

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")

# Official NSE trading holidays (Equity & F&O segments)
NSE_HOLIDAYS = {
    # 2025
    "2025-01-26": "Republic Day",
    "2025-02-26": "Mahashivratri",
    "2025-03-14": "Holi",
    "2025-03-31": "Id-Ul-Fitr",
    "2025-04-10": "Mahavir Jayanti",
    "2025-04-14": "Dr. Baba Saheb Ambedkar Jayanti",
    "2025-04-18": "Good Friday",
    "2025-05-01": "Maharashtra Day",
    "2025-06-07": "Bakri Id",
    "2025-07-06": "Muharram",
    "2025-08-15": "Independence Day",
    "2025-08-27": "Ganesh Chaturthi",
    "2025-10-02": "Mahatma Gandhi Jayanti",
    "2025-10-21": "Dussehra",
    "2025-10-22": "Diwali Balipratipada",
    "2025-11-05": "Prakash Gurpurb Sri Guru Nanak Dev",
    "2025-12-25": "Christmas",

    # 2026
    "2026-01-26": "Republic Day",
    "2026-02-17": "Mahashivratri",
    "2026-03-03": "Holi",
    "2026-03-20": "Id-Ul-Fitr",
    "2026-03-27": "Ram Navami",
    "2026-04-03": "Good Friday",
    "2026-04-14": "Dr. Baba Saheb Ambedkar Jayanti",
    "2026-05-01": "Maharashtra Day",
    "2026-05-27": "Bakri Id",
    "2026-06-25": "Muharram",
    "2026-08-15": "Independence Day",
    "2026-10-02": "Mahatma Gandhi Jayanti",
    "2026-10-20": "Dussehra",
    "2026-11-08": "Diwali Laxmi Pujan",
    "2026-11-10": "Diwali Balipratipada",
    "2026-11-24": "Gurunanak Jayanti",
    "2026-12-25": "Christmas",
}


def is_market_holiday(date_str: str | None = None) -> tuple[bool, str]:
    """
    Checks if the given date (or current IST date) is an NSE holiday or weekend.
    Returns (is_holiday: bool, reason: str).
    """
    if not date_str:
        now_ist = datetime.now(IST)
        date_str = now_ist.strftime("%Y-%m-%d")
        weekday = now_ist.weekday()
    else:
        dt = date.fromisoformat(date_str)
        weekday = dt.weekday()

    if weekday == 5:
        return True, "Saturday (Weekend)"
    if weekday == 6:
        return True, "Sunday (Weekend)"

    if date_str in NSE_HOLIDAYS:
        return True, f"NSE Holiday: {NSE_HOLIDAYS[date_str]}"

    return False, "Normal Trading Day"
