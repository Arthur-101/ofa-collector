# scheduler.py — APScheduler setup for ofa-collector
#
# Jobs:
#   reset_job   — 9:00 AM IST (Mon-Fri): wipe temp DB
#   poll_job    — every 5 min 9:15–15:30 IST (Mon-Fri): flush WS ticks to DB
#   export_job  — 3:31 PM IST (Mon-Fri): export CSV to GitHub
#   shutdown_job— 4:00 PM IST (Mon-Fri): graceful stop

import logging
import sys
import pytz
from apscheduler.schedulers.background import BackgroundScheduler
from config import (
    SYMBOLS, POLL_INTERVAL_MINUTES,
    RESET_HOUR, RESET_MINUTE,
    EXPORT_HOUR, EXPORT_MINUTE,
    SHUTDOWN_HOUR, SHUTDOWN_MINUTE,
)

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")

_scheduler: BackgroundScheduler | None = None


def _reset_job():
    from db import reset_db
    logger.info("⏰ reset_job fired — wiping DB for new trading day")
    reset_db()


def _poll_job():
    from ws_feed import flush_to_db
    for sym in SYMBOLS:
        try:
            rows = flush_to_db(sym)
            logger.info("poll_job: %d rows flushed for %s", len(rows), sym)
        except Exception as e:
            logger.error("poll_job error for %s: %s", sym, e)


def _export_job():
    from exporter import export_today
    logger.info("⏰ export_job fired — exporting today's data to GitHub")
    try:
        export_today()
    except Exception as e:
        logger.error("export_job error: %s", e)


def _shutdown_job():
    logger.info("⏰ shutdown_job fired — market closed, stopping scheduler")
    if _scheduler:
        _scheduler.shutdown(wait=False)
    logger.info("Scheduler stopped. App idling until next reset_job.")


def start_scheduler() -> BackgroundScheduler:
    global _scheduler

    _scheduler = BackgroundScheduler(timezone=IST)

    # 9:00 AM IST — wipe DB
    _scheduler.add_job(
        _reset_job,
        trigger="cron",
        day_of_week="mon-fri",
        hour=RESET_HOUR,
        minute=RESET_MINUTE,
        id="reset_job",
        name="Daily DB reset",
        misfire_grace_time=120,
    )

    # Every 5 min, 9:15–15:30 IST — flush ticks to DB
    _scheduler.add_job(
        _poll_job,
        trigger="cron",
        day_of_week="mon-fri",
        hour="9-15",
        minute=f"{RESET_MINUTE}/{POLL_INTERVAL_MINUTES}",
        id="poll_job",
        name="5-min flush to DB",
        misfire_grace_time=60,
    )

    # 3:31 PM IST — export to GitHub
    _scheduler.add_job(
        _export_job,
        trigger="cron",
        day_of_week="mon-fri",
        hour=EXPORT_HOUR,
        minute=EXPORT_MINUTE,
        id="export_job",
        name="End-of-day CSV export",
        misfire_grace_time=300,
    )

    # 4:00 PM IST — graceful shutdown
    _scheduler.add_job(
        _shutdown_job,
        trigger="cron",
        day_of_week="mon-fri",
        hour=SHUTDOWN_HOUR,
        minute=SHUTDOWN_MINUTE,
        id="shutdown_job",
        name="Post-market shutdown",
        misfire_grace_time=300,
    )

    _scheduler.start()
    logger.info("Scheduler started with %d jobs", len(_scheduler.get_jobs()))

    for job in _scheduler.get_jobs():
        logger.info("  → %s | next run: %s", job.name, job.next_run_time)

    return _scheduler
