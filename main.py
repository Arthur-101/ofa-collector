# main.py — ofa-collector entry point
#
# Startup sequence:
#   1. Init DB (create tables if needed)
#   2. Start WebSocket feed (Angel One SmartAPI)
#   3. Start APScheduler (reset, poll, export, shutdown jobs)
#   4. Start FastAPI via uvicorn (serves /health, /data, /status)
#
# FastAPI runs in the main thread (blocks).
# WebSocket feed and scheduler run in background threads.

import logging
import time
import threading
import uvicorn
from db import init_db
from ws_feed import start_feed
from scheduler import start_scheduler
from api import app
from config import SYMBOLS, API_HOST, API_PORT

# ── Logging setup ─────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    handlers=[
        logging.StreamHandler(),           # stdout → Railway logs
    ]
)
logger = logging.getLogger(__name__)


def main():
    logger.info("=" * 60)
    logger.info("OFA Collector starting up")
    logger.info("=" * 60)

    # 1. Init DB
    logger.info("Step 1: Initialising database...")
    init_db()

    # 2. Start WebSocket feed in background thread
    logger.info("Step 2: Starting WebSocket feed for %s...", SYMBOLS)
    try:
        start_feed(SYMBOLS)
    except Exception as e:
        logger.error("Failed to start WebSocket feed: %s", e)
        raise

    # Wait for WebSocket to connect and receive initial ticks
    logger.info("Waiting 20s for WebSocket to stabilise...")
    time.sleep(20)

    # 3. Start scheduler in background
    logger.info("Step 3: Starting scheduler...")
    start_scheduler()

    # 4. Start FastAPI (blocks — runs in main thread)
    logger.info("Step 4: Starting FastAPI on %s:%d", API_HOST, API_PORT)
    logger.info("  /health  → liveness check")
    logger.info("  /status  → DB row count + WS state")
    logger.info("  /data    → options chain rows by date")
    logger.info("=" * 60)

    uvicorn.run(
        app,
        host=API_HOST,
        port=API_PORT,
        log_level="warning",   # suppress uvicorn access logs in Railway
    )


if __name__ == "__main__":
    main()
