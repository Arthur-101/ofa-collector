# db.py — SQLite setup for ofa-collector (temp daily DB)

import sqlite3
import logging
from config import DB_PATH

logger = logging.getLogger(__name__)


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout = 5000")
    return conn


def init_db() -> None:
    """Create tables if they don't exist."""
    conn = get_conn()
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS options_chain (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp   TEXT NOT NULL,
            symbol      TEXT NOT NULL,
            expiry      TEXT NOT NULL,
            strike      REAL NOT NULL,
            option_type TEXT NOT NULL,
            oi          INTEGER,
            oi_change   INTEGER,
            volume      INTEGER,
            iv          REAL,
            last_price  REAL,
            spot_price  REAL
        )
    """)

    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_ts_sym
        ON options_chain (timestamp, symbol)
    """)

    conn.commit()
    conn.close()
    logger.info("DB initialised at %s", DB_PATH)


def reset_db() -> None:
    """Wipe all rows from options_chain. Called at 9:00 AM daily."""
    conn = get_conn()
    deleted = conn.execute("DELETE FROM options_chain").rowcount
    conn.commit()
    conn.close()
    logger.info("DB reset — %d rows deleted", deleted)


def insert_options_rows(rows: list[dict]) -> None:
    if not rows:
        return
    conn = get_conn()
    conn.executemany("""
        INSERT INTO options_chain
            (timestamp, symbol, expiry, strike, option_type,
             oi, oi_change, volume, iv, last_price, spot_price)
        VALUES
            (:timestamp, :symbol, :expiry, :strike, :option_type,
             :oi, :oi_change, :volume, :iv, :last_price, :spot_price)
    """, rows)
    conn.commit()
    conn.close()


def get_latest_oi_snapshot(symbol: str) -> dict:
    """
    Returns {(strike, option_type, expiry): oi} for the most recent
    timestamp in the DB for the given symbol.
    """
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT strike, option_type, expiry, oi
        FROM options_chain
        WHERE symbol = ?
          AND timestamp = (
              SELECT MAX(timestamp) FROM options_chain WHERE symbol = ?
          )
    """, (symbol, symbol))
    rows = cursor.fetchall()
    conn.close()
    return {(r[0], r[1], r[2]): r[3] for r in rows}


def get_rows_for_date(date_str: str) -> list[dict]:
    """
    Returns all rows for a given date (YYYY-MM-DD) as list of dicts.
    Used by the API and exporter.
    """
    conn = get_conn()
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute("""
        SELECT * FROM options_chain
        WHERE DATE(timestamp) = ?
        ORDER BY timestamp ASC
    """, (date_str,))
    rows = [dict(r) for r in cursor.fetchall()]
    conn.close()
    return rows


def get_row_count() -> int:
    conn = get_conn()
    count = conn.execute("SELECT COUNT(*) FROM options_chain").fetchone()[0]
    conn.close()
    return count
