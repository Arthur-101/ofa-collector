# db.py — SQLite setup for ofa-collector (temp daily DB)

import sqlite3
import logging
from config import DB_PATH

logger = logging.getLogger(__name__)


_db_initialized = False

def get_conn() -> sqlite3.Connection:
    global _db_initialized
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout = 5000")
    if not _db_initialized:
        _db_initialized = True
        _run_init_schema(conn)
    return conn


def _run_init_schema(conn: sqlite3.Connection) -> None:
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

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS signals (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            fired_at         TEXT NOT NULL,
            symbol           TEXT NOT NULL,
            expiry           TEXT,
            strike           REAL,
            option_type      TEXT,
            signal_type      TEXT NOT NULL,
            signal_strength  REAL,
            oi_change        INTEGER,
            volume           INTEGER,
            iv               REAL,
            spot_price       REAL,
            bias             TEXT,
            mode             TEXT,
            llm_thesis       TEXT,
            llm_bias         TEXT,
            llm_confidence   INTEGER,
            news_summary_id  INTEGER,
            logic_version    TEXT,
            is_exploratory   INTEGER DEFAULT 0
        )
    """)

    try:
        cursor.execute("ALTER TABLE signals ADD COLUMN logic_version TEXT")
    except Exception:
        pass
    try:
        cursor.execute("ALTER TABLE signals ADD COLUMN is_exploratory INTEGER DEFAULT 0")
    except Exception:
        pass

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS news_raw (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            fetched_at   TEXT NOT NULL,
            symbol       TEXT NOT NULL,
            headline     TEXT NOT NULL,
            source       TEXT,
            url          TEXT,
            published_at TEXT
        )
    """)

    conn.commit()
    logger.info("DB tables verified at %s", DB_PATH)


def init_db() -> None:
    """Create tables if they don't exist."""
    conn = get_conn()
    conn.close()


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


def get_today_snapshot(symbol: str, date_str: str) -> list[dict]:
    """
    Returns the latest flush rows for today — one row per strike/expiry/type.
    """
    conn = get_conn()
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute("""
        SELECT *
        FROM options_chain
        WHERE symbol = ?
          AND DATE(timestamp) = ?
          AND timestamp = (
              SELECT MAX(timestamp)
              FROM options_chain
              WHERE symbol = ? AND DATE(timestamp) = ?
          )
    """, (symbol, date_str, symbol, date_str))
    rows = [dict(r) for r in cursor.fetchall()]
    conn.close()
    return rows


def get_intraday_oi_changes(symbol: str, date_str: str) -> list[dict]:
    """
    Returns all non-NULL oi_change rows for today.
    Used to compute intraday percentile rank in bootstrap mode.
    """
    conn = get_conn()
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute("""
        SELECT strike, option_type, expiry, oi_change, volume, iv, spot_price, timestamp
        FROM options_chain
        WHERE symbol = ?
          AND DATE(timestamp) = ?
          AND oi_change IS NOT NULL
        ORDER BY timestamp ASC
    """, (symbol, date_str))
    rows = [dict(r) for r in cursor.fetchall()]
    conn.close()
    return rows


def get_spot_series(symbol: str, date_str: str) -> list[float]:
    """
    Returns ordered list of spot_price values for today's session.
    One value per flush timestamp (5-min closes).
    """
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT AVG(spot_price) as spot, timestamp
        FROM options_chain
        WHERE symbol = ?
          AND DATE(timestamp) = ?
          AND spot_price IS NOT NULL
          AND spot_price > 0
        GROUP BY timestamp
        ORDER BY timestamp ASC
    """, (symbol, date_str))
    rows = cursor.fetchall()
    conn.close()
    return [r[0] for r in rows if r[0] is not None]


def get_intraday_oi(symbol: str, date_str: str) -> dict:
    """
    Returns total CE OI and PE OI for the latest flush today.
    """
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT
            SUM(CASE WHEN option_type = 'CE' THEN oi ELSE 0 END) as total_ce_oi,
            SUM(CASE WHEN option_type = 'PE' THEN oi ELSE 0 END) as total_pe_oi
        FROM options_chain
        WHERE symbol = ?
          AND DATE(timestamp) = ?
          AND timestamp = (
              SELECT MAX(timestamp) FROM options_chain
              WHERE symbol = ? AND DATE(timestamp) = ?
          )
    """, (symbol, date_str, symbol, date_str))
    result = cursor.fetchone()
    conn.close()
    if not result:
        return {"ce_oi": 0, "pe_oi": 0}
    return {
        "ce_oi": result[0] or 0,
        "pe_oi": result[1] or 0,
    }


def get_volume_series(symbol: str, date_str: str) -> list[dict]:
    """
    Returns volume and spot per timestamp for VWAP computation.
    """
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT
            timestamp,
            AVG(spot_price) as spot,
            SUM(volume) as total_volume
        FROM options_chain
        WHERE symbol = ?
          AND DATE(timestamp) = ?
          AND spot_price IS NOT NULL
          AND volume IS NOT NULL
        GROUP BY timestamp
        ORDER BY timestamp ASC
    """, (symbol, date_str))
    rows = cursor.fetchall()
    conn.close()
    return [{"spot": r[1], "volume": r[2]} for r in rows if r[1] and r[2]]


def insert_signal_local(signal: dict) -> int:
    """Insert signal into local SQLite DB."""
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO signals
            (fired_at, symbol, expiry, strike, option_type,
             signal_type, signal_strength, oi_change, volume,
             iv, spot_price, bias, mode,
             llm_thesis, llm_bias, llm_confidence, news_summary_id,
             logic_version, is_exploratory)
        VALUES
            (:fired_at, :symbol, :expiry, :strike, :option_type,
             :signal_type, :signal_strength, :oi_change, :volume,
             :iv, :spot_price, :bias, :mode,
             :llm_thesis, :llm_bias, :llm_confidence, :news_summary_id,
             :logic_version, :is_exploratory)
    """, signal)
    conn.commit()
    sig_id = cursor.lastrowid
    conn.close()
    return sig_id


def update_signal_thesis_local(signal_id: int, result: dict) -> None:
    """Update local signal row with LLM output."""
    conn = get_conn()
    conn.execute("""
        UPDATE signals
        SET llm_thesis     = ?,
            llm_bias       = ?,
            llm_confidence = ?
        WHERE id = ?
    """, (
        result.get("thesis"),
        result.get("bias"),
        result.get("confidence"),
        signal_id,
    ))
    conn.commit()
    conn.close()


def insert_news_local(articles: list[dict], symbol: str) -> None:
    """Store fetched headlines in local SQLite news_raw table."""
    if not articles:
        return
    conn = get_conn()
    conn.executemany("""
        INSERT INTO news_raw (fetched_at, symbol, headline, source, url, published_at)
        VALUES (:fetched_at, :symbol, :headline, :source, :url, :published_at)
    """, articles)
    conn.commit()
    conn.close()


def signal_already_fired_today_local(symbol: str, strike: float,
                                      option_type: str, signal_type: str,
                                      date_str: str) -> bool:
    """Check if signal already exists in local SQLite for today."""
    conn = get_conn()
    result = conn.execute("""
        SELECT COUNT(*)
        FROM signals
        WHERE symbol = ?
          AND strike = ?
          AND option_type = ?
          AND signal_type = ?
          AND DATE(fired_at) = ?
    """, (symbol, strike, option_type, signal_type, date_str)).fetchone()
    conn.close()
    return (result[0] > 0) if result else False

