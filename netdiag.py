"""Startup diagnostic: which address does this host egress from, and can it reach the broker?

2026-11-12: Render's egress to Angel One began timing out mid-session with no
change on our side, and the only evidence was a traceback. This logs the egress
address and a TCP result per host, so the next occurrence names the address and
the host that refused instead of leaving it to inference.

Runs in a background thread: it must never delay the port binding.
"""

import logging
import socket
import time

import requests

logger = logging.getLogger(__name__)

HOSTS = [
    ("apiconnect.angelone.in", 443),      # login + REST
    ("smartapisocket.angelone.in", 443),  # tick stream
    ("bmskvnptwervpefxbrge.supabase.co", 443),  # control: known-good egress
]


def _public_ip() -> str:
    try:
        return requests.get("https://api.ipify.org", timeout=8).text.strip()
    except Exception as e:
        return f"unavailable ({e.__class__.__name__})"


def _tcp(host: str, port: int, timeout: float = 7) -> str:
    t0 = time.monotonic()
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return f"ok {int((time.monotonic() - t0) * 1000)}ms"
    except Exception as e:
        return f"{e.__class__.__name__} after {time.monotonic() - t0:.1f}s"


def report() -> None:
    logger.info("netdiag: egress ip = %s", _public_ip())
    for host, port in HOSTS:
        try:
            addr = socket.gethostbyname(host)
        except Exception as e:
            addr = f"dns {e.__class__.__name__}"
        logger.info("netdiag: %s:%d (%s) -> %s", host, port, addr, _tcp(host, port))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    report()
