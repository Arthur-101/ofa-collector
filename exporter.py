# exporter.py — End-of-day CSV export to GitHub (ofa-data repo)
#
# Called by scheduler at 3:31 PM IST.
# Exports today's rows as a CSV and pushes to Arthur-101/ofa-data.

import os
import csv
import logging
import io
import base64
from datetime import datetime, timezone
import requests
from db import get_rows_for_date
from config import GITHUB_DATA_REPO, GITHUB_DATA_BRANCH, GITHUB_DATA_FOLDER

logger = logging.getLogger(__name__)

GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
GITHUB_API_BASE = "https://api.github.com"

COLUMNS = [
    "id", "timestamp", "symbol", "expiry", "strike", "option_type",
    "oi", "oi_change", "volume", "iv", "last_price", "spot_price"
]


def _rows_to_csv_bytes(rows: list[dict]) -> bytes:
    """Convert list of row dicts to CSV bytes."""
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=COLUMNS, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(rows)
    return buf.getvalue().encode("utf-8")


def _get_file_sha(path: str) -> str | None:
    """Get SHA of existing file in GitHub repo (needed for updates)."""
    url = f"{GITHUB_API_BASE}/repos/{GITHUB_DATA_REPO}/contents/{path}"
    headers = {
        "Authorization": f"token {GITHUB_TOKEN}",
        "Accept": "application/vnd.github.v3+json",
    }
    res = requests.get(url, headers=headers, timeout=15)
    if res.status_code == 200:
        return res.json().get("sha")
    return None


def _push_to_github(filename: str, csv_bytes: bytes, date_str: str) -> bool:
    """
    Push CSV file to ofa-data GitHub repo.
    Creates or updates the file.
    """
    if not GITHUB_TOKEN:
        logger.error("GITHUB_TOKEN not set — cannot push to GitHub")
        return False

    path = f"{GITHUB_DATA_FOLDER}/{filename}"
    url = f"{GITHUB_API_BASE}/repos/{GITHUB_DATA_REPO}/contents/{path}"
    headers = {
        "Authorization": f"token {GITHUB_TOKEN}",
        "Accept": "application/vnd.github.v3+json",
        "Content-Type": "application/json",
    }

    payload = {
        "message": f"Add options data for {date_str}",
        "content": base64.b64encode(csv_bytes).decode("utf-8"),
        "branch": GITHUB_DATA_BRANCH,
    }

    # Check if file already exists (get SHA for update)
    existing_sha = _get_file_sha(path)
    if existing_sha:
        payload["sha"] = existing_sha
        logger.info("Updating existing file %s (sha: %s...)", path, existing_sha[:8])
    else:
        logger.info("Creating new file %s", path)

    res = requests.put(url, headers=headers, json=payload, timeout=30)

    if res.status_code in (200, 201):
        logger.info("✅ Successfully pushed %s to GitHub (%d bytes)", filename, len(csv_bytes))
        return True
    else:
        logger.error("❌ GitHub push failed: %d — %s", res.status_code, res.text[:200])
        return False


def export_today() -> bool:
    """
    Main export function — called by scheduler at 3:31 PM IST.
    Fetches today's rows, writes CSV, pushes to GitHub.
    """
    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    filename = f"{date_str}.csv"

    logger.info("Starting end-of-day export for %s", date_str)

    rows = get_rows_for_date(date_str)

    if not rows:
        logger.warning("No rows found for %s — skipping export", date_str)
        return False

    logger.info("Exporting %d rows for %s", len(rows), date_str)

    csv_bytes = _rows_to_csv_bytes(rows)
    success = _push_to_github(filename, csv_bytes, date_str)

    if success:
        logger.info("Export complete: %s (%d rows, %d bytes)", filename, len(rows), len(csv_bytes))
    else:
        logger.error("Export failed for %s", date_str)

    return success
