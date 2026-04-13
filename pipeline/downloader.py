"""
Polite HTTP downloader.
- Adds a User-Agent header identifying the bot.
- Waits REQUEST_DELAY seconds between requests.
- Skips files that already exist locally (unless force=True).
- Returns the local Path on success, None on failure.
"""

import time
import hashlib
import logging
from pathlib import Path

import requests

from config import RAW_DIR, REQUEST_DELAY, REQUEST_TIMEOUT, USER_AGENT

logger = logging.getLogger(__name__)

_last_request_time: float = 0.0


def _polite_wait() -> None:
    global _last_request_time
    elapsed = time.monotonic() - _last_request_time
    wait = REQUEST_DELAY - elapsed
    if wait > 0:
        time.sleep(wait)
    _last_request_time = time.monotonic()


def _pdf_id(url: str) -> str:
    """Stable filename derived from URL hash."""
    return hashlib.sha256(url.encode()).hexdigest()[:16]


def download_pdf(
    url: str,
    source: str,
    filename: str | None = None,
    force: bool = False,
) -> Path | None:
    """
    Download a PDF from `url` into data/raw/<source>/.

    Args:
        url:      Direct URL to the PDF.
        source:   Source key (used as subdirectory name).
        filename: Override the local filename. Defaults to a hash-based name.
        force:    Re-download even if the file already exists.

    Returns:
        Local Path if successful, None otherwise.
    """
    dest_dir = RAW_DIR / source
    dest_dir.mkdir(parents=True, exist_ok=True)

    fname = filename or f"{_pdf_id(url)}.pdf"
    dest  = dest_dir / fname

    if dest.exists() and not force:
        logger.debug("Already downloaded, skipping: %s", dest)
        return dest

    _polite_wait()

    try:
        logger.info("Downloading: %s", url)
        resp = requests.get(
            url,
            headers={"User-Agent": USER_AGENT},
            timeout=REQUEST_TIMEOUT,
            stream=True,
        )
        resp.raise_for_status()

        with dest.open("wb") as fh:
            for chunk in resp.iter_content(chunk_size=65536):
                fh.write(chunk)

        logger.info("Saved: %s (%d bytes)", dest, dest.stat().st_size)
        return dest

    except requests.RequestException as exc:
        logger.warning("Failed to download %s: %s", url, exc)
        return None


def fetch_html(url: str) -> str | None:
    """
    Fetch a page as text (for scraper use). Applies the same polite delay.
    Returns HTML string or None on failure.
    """
    _polite_wait()
    try:
        resp = requests.get(
            url,
            headers={"User-Agent": USER_AGENT},
            timeout=REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        return resp.text
    except requests.RequestException as exc:
        logger.warning("Failed to fetch %s: %s", url, exc)
        return None
