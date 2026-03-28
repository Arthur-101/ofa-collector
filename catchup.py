# catchup.py — Import today's data from Railway into local options_flow.db
#
# Run this after waking up to get all rows collected since 9:15 AM.
# Usage:
#   python catchup.py                    → imports today's data
#   python catchup.py --date 2026-03-28  → imports a specific date

import argparse
import sqlite3
import requests
import pandas as pd
from datetime import date, datetime

# ── Config ────────────────────────────────────────────────────────────────────
RAILWAY_URL = "https://YOUR-APP.railway.app"   # ← update after Railway deploy
LOCAL_DB    = "options_flow.db"

# ── Args ──────────────────────────────────────────────────────────────────────
parser = argparse.ArgumentParser(description="Catch up local DB from Railway")
parser.add_argument("--date", default=str(date.today()), help="Date YYYY-MM-DD (default: today)")
args = parser.parse_args()
target_date = args.date


def main():
    print(f"Fetching data for {target_date} from Railway...")

    # 1. Check Railway is alive
    try:
        health = requests.get(f"{RAILWAY_URL}/health", timeout=10).json()
        print(f"Railway status: {health['status']} | WS connected: {health['ws_connected']} | Ticks: {health['tick_count']}")
    except Exception as e:
        print(f"❌ Could not reach Railway: {e}")
        return

    # 2. Fetch rows for target date
    try:
        res = requests.get(f"{RAILWAY_URL}/data", params={"date": target_date}, timeout=30)
        res.raise_for_status()
        data = res.json()
    except Exception as e:
        print(f"❌ Failed to fetch data: {e}")
        return

    count = data["count"]
    if count == 0:
        print(f"⚠️  No rows found for {target_date} on Railway")
        return

    print(f"✅ Got {count} rows from Railway")

    # 3. Load into DataFrame
    df = pd.DataFrame(data["rows"])
    df = df.drop(columns=["id"], errors="ignore")  # drop Railway's auto-id

    # 4. Insert into local DB (skip duplicates by timestamp+strike+option_type)
    conn = sqlite3.connect(LOCAL_DB)

    # Get existing timestamps for this date to avoid duplication
    existing = pd.read_sql(
        f"SELECT timestamp, strike, option_type, expiry FROM options_chain WHERE DATE(timestamp) = '{target_date}'",
        conn
    )

    if not existing.empty:
        merge_key = ["timestamp", "strike", "option_type", "expiry"]
        df = df.merge(existing[merge_key], on=merge_key, how="left", indicator=True)
        df = df[df["_merge"] == "left_only"].drop(columns=["_merge"])
        print(f"After dedup: {len(df)} new rows to insert")

    if df.empty:
        print("ℹ️  All rows already exist in local DB — nothing to import")
        conn.close()
        return

    df.to_sql("options_chain", conn, if_exists="append", index=False)
    conn.close()

    print(f"✅ Imported {len(df)} rows into local options_flow.db")
    print(f"   Date: {target_date}")
    print(f"   Time range: {df['timestamp'].min()} → {df['timestamp'].max()}")


if __name__ == "__main__":
    main()
