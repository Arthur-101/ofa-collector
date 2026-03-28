# config.py — ofa-collector configuration

# Symbols to monitor
SYMBOLS = ["NIFTY"]

# Polling interval (minutes)
POLL_INTERVAL_MINUTES = 5

# SQLite database path (temp, resets daily)
DB_PATH = "options_flow.db"

# Market hours (IST)
MARKET_OPEN_HOUR = 9
MARKET_OPEN_MINUTE = 15
MARKET_CLOSE_HOUR = 15
MARKET_CLOSE_MINUTE = 30

# Scheduler jobs (IST)
RESET_HOUR = 9
RESET_MINUTE = 0

EXPORT_HOUR = 15
EXPORT_MINUTE = 31

SHUTDOWN_HOUR = 16
SHUTDOWN_MINUTE = 0

# Strike range around ATM (number of strikes each side)
STRIKE_RANGE = 15

# Expiry: skip contracts with DTE <= this
MIN_DTE = 2

# Max expiries to track per symbol
MAX_EXPIRIES = 2

# GitHub repo for CSV export
GITHUB_DATA_REPO = "Arthur-101/ofa-data"
GITHUB_DATA_BRANCH = "main"
GITHUB_DATA_FOLDER = "data"

# FastAPI — Railway injects PORT env var dynamically, fallback to 8000 locally
import os
API_HOST = "0.0.0.0"
API_PORT = int(os.environ.get("PORT", 8000))