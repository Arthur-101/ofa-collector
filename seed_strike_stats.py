# seed_strike_stats.py — Standalone Seeder for strike_daily_stats
# Runs across all stored dates in options_chain to build the 20-day rolling baseline
# before re-enabling the 7-day retention purge.

import sys
import logging
from datetime import datetime

import db_supabase
from stats_archiver import seed_all_historical_stats

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("seed_strike_stats")


def main():
    dry_run = "--dry-run" in sys.argv
    logger.info("==================================================")
    logger.info("🚀 Starting strike_daily_stats historical seeder (dry_run=%s)", dry_run)
    logger.info("==================================================")

    results = seed_all_historical_stats(dry_run=dry_run)

    logger.info("==================================================")
    logger.info("📊 Seeding Summary:")
    total = 0
    for d, count in results.items():
        logger.info("   Date %s: %d records", d, count)
        total += count
    logger.info("Total records: %d", total)
    logger.info("==================================================")


if __name__ == "__main__":
    main()
