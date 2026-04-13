"""
Scraper for ECDPM (European Centre for Development Policy Management).
https://ecdpm.org/publications/

The listing page is rendered client-side via Algolia search — credentials are
embedded in the page HTML. This scraper queries the Algolia API directly for
structured metadata, then visits each publication page to find the PDF link.

Usage:
    .venv/bin/python scrapers/ecdpm.py               # scrape all
    .venv/bin/python scrapers/ecdpm.py --limit 20    # test with 20 pubs
    .venv/bin/python scrapers/ecdpm.py --dry-run     # metadata only, no downloads
"""

import argparse
import hashlib
import json
import logging
import sys
import time
from datetime import date, datetime, timezone, timedelta
from pathlib import Path

# Ensure project root is on sys.path when running as a script
sys.path.insert(0, str(Path(__file__).parent.parent))

import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

from config import SOURCES, USER_AGENT, REQUEST_DELAY, REQUEST_TIMEOUT
from pipeline.db import init_db, upsert_publication, publication_exists, get_db
from pipeline.downloader import download_pdf

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# ── Algolia config (credentials embedded in ecdpm.org/publications/ page HTML) ─
ALGOLIA_APP_ID  = "MRV08QTWZA"
ALGOLIA_API_KEY = "08ceddfe107c940c82a5e74281e21692"
ALGOLIA_INDEX   = "publications_en_GB"
ALGOLIA_URL     = (
    f"https://{ALGOLIA_APP_ID}-dsn.algolia.net"
    f"/1/indexes/{ALGOLIA_INDEX}/query"
)
ALGOLIA_HEADERS = {
    "X-Algolia-Application-Id": ALGOLIA_APP_ID,
    "X-Algolia-API-Key":        ALGOLIA_API_KEY,
    "Content-Type":             "application/json",
}
ALGOLIA_PAGE_SIZE = 50   # max hits per Algolia request

SOURCE_KEY = "ecdpm"
BASE_URL   = SOURCES[SOURCE_KEY]["base_url"]   # https://ecdpm.org

_last_request: float = 0.0


def _wait() -> None:
    """Polite delay between all HTTP requests."""
    global _last_request
    elapsed = time.monotonic() - _last_request
    pause = REQUEST_DELAY - elapsed
    if pause > 0:
        time.sleep(pause)
    _last_request = time.monotonic()


# ── Algolia helpers ───────────────────────────────────────────────────────────

def algolia_page(page: int) -> dict:
    """Fetch one page of results from the Algolia index."""
    _wait()
    payload = {
        "query":        "",
        "hitsPerPage":  ALGOLIA_PAGE_SIZE,
        "page":         page,
    }
    resp = requests.post(
        ALGOLIA_URL,
        headers=ALGOLIA_HEADERS,
        json=payload,
        timeout=REQUEST_TIMEOUT,
    )
    resp.raise_for_status()
    return resp.json()


def all_hits(limit: int | None = None, cutoff: date | None = None):
    """
    Generator yielding hits from the Algolia index (newest first).

    Stops early when:
      - `limit` publications have been yielded, OR
      - `cutoff` is set and the hit's date is older than the cutoff
        (safe because Algolia returns results sorted by date descending).
    """
    first    = algolia_page(0)
    nb_pages = first.get("nbPages", 1)
    nb_hits  = first.get("nbHits", 0)
    logger.info("Algolia index: %d publications across %d pages", nb_hits, nb_pages)

    count = 0
    done  = False

    for page_num in range(nb_pages):
        if done:
            break
        data = first if page_num == 0 else algolia_page(page_num)
        for hit in data.get("hits", []):
            if limit and count >= limit:
                done = True
                break
            pub_date = _parse_date_obj(hit)
            if cutoff and pub_date and pub_date < cutoff:
                logger.info(
                    "Reached cutoff (%s) at '%s' (%s) — stopping pagination",
                    cutoff, hit.get("title", "")[:60], pub_date,
                )
                done = True
                break
            yield hit
            count += 1


# ── Publication page helpers ──────────────────────────────────────────────────

def _pub_id(page_url: str) -> str:
    return hashlib.sha256(page_url.encode()).hexdigest()[:16]


def _parse_date(hit: dict) -> str | None:
    """
    Best-effort ISO date string from Algolia hit.
    `publication` field is a human date like "10 April 2026"; `year` is int.
    """
    raw = hit.get("publication", "")
    if raw and isinstance(raw, str):
        try:
            dt = datetime.strptime(raw.strip(), "%d %B %Y")
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            pass
    year = hit.get("year")
    if year:
        return str(year)
    return None


def _parse_date_obj(hit: dict) -> date | None:
    """Return a date object for cutoff comparison, or None if unparseable."""
    raw = hit.get("publication", "")
    if raw and isinstance(raw, str):
        try:
            return datetime.strptime(raw.strip(), "%d %B %Y").date()
        except ValueError:
            pass
    year = hit.get("year")
    if year:
        try:
            return date(int(year), 1, 1)
        except (ValueError, TypeError):
            pass
    return None


def find_pdf_url(page_url: str) -> str | None:
    """
    Fetch a publication landing page and return a PDF/download link, or None.

    Priority order:
      1. ECDPM's own /download_file/ links (most reliable)
      2. Any .pdf link on the ecdpm.org domain
      3. Nothing — commentaries without dedicated PDFs return None
    """
    _wait()
    try:
        resp = requests.get(
            page_url,
            headers={"User-Agent": USER_AGENT},
            timeout=REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
    except requests.RequestException as exc:
        logger.warning("Could not fetch %s: %s", page_url, exc)
        return None

    soup = BeautifulSoup(resp.text, "html.parser")
    links = soup.find_all("a", href=True)

    # Pass 1: ECDPM download_file links
    for a in links:
        href = a["href"]
        if "/download_file/" in href:
            return BASE_URL + href if href.startswith("/") else href

    # Pass 2: .pdf links hosted on ecdpm.org
    for a in links:
        href = a["href"]
        if href.lower().endswith(".pdf") and "ecdpm.org" in href:
            return href

    return None


# ── Main scrape logic ─────────────────────────────────────────────────────────

def scrape(limit: int | None = None, dry_run: bool = False) -> None:
    init_db()

    cutoff = date.today().replace(year=date.today().year - 5)
    logger.info("Date cutoff: %s (5 years ago)", cutoff)

    skipped_exists   = 0
    skipped_no_pdf   = 0
    skipped_too_old  = 0
    downloaded       = 0
    failed_download  = 0
    stored           = 0

    hits = list(tqdm(all_hits(limit=limit, cutoff=cutoff), desc="Fetching index", unit="pub"))
    logger.info("Processing %d publications (cutoff: %s)", len(hits), cutoff)

    for i, hit in enumerate(hits, 1):
        path     = hit.get("path", "")
        page_url = BASE_URL + path if path.startswith("/") else path
        pub_id   = _pub_id(page_url)

        title    = hit.get("title", "").strip()
        pub_type = hit.get("publication_type", "")
        year     = _parse_date(hit)
        authors  = json.dumps(
            [a.get("name", "") for a in (hit.get("authors") or [])]
        )

        # Belt-and-suspenders date check (all_hits stops pagination at cutoff,
        # but a hit with a parseable date could still slip through if undated
        # hits appear before the cutoff boundary on a page)
        pub_date = _parse_date_obj(hit)
        if pub_date and pub_date < cutoff:
            logger.debug("  Skipping (too old: %s)", pub_date)
            skipped_too_old += 1
            continue

        logger.info(
            "[%d/%d] %s | %s | %s",
            i, len(hits), pub_type, year or "n/d", title[:70],
        )

        # Skip if already fully processed (has a pdf_url in db)
        if publication_exists(pub_id):
            existing = get_db()["publications"].get(pub_id)
            if existing and existing.get("pdf_url"):
                logger.debug("  Already in db with PDF — skipping")
                skipped_exists += 1
                continue

        # Fetch publication page to find PDF URL
        pdf_url = find_pdf_url(page_url)

        if not pdf_url:
            logger.info("  No PDF found — storing metadata only")
            skipped_no_pdf += 1
            upsert_publication({
                "id":          pub_id,
                "source":      SOURCE_KEY,
                "title":       title,
                "authors":     authors,
                "date":        year,
                "url":         page_url,
                "pdf_url":     None,
                "local_path":  None,
                "language":    None,
            })
            stored += 1
            continue

        local_path = None
        if not dry_run:
            # Derive a clean filename from the URL path slug
            slug = path.rstrip("/").split("/")[-1][:80]
            filename = f"{slug}.pdf"
            local = download_pdf(pdf_url, SOURCE_KEY, filename=filename)
            if local:
                local_path = str(local)
                downloaded += 1
                logger.info("  Downloaded: %s", local_path)
            else:
                failed_download += 1
                logger.warning("  Download failed: %s", pdf_url)
        else:
            logger.info("  [dry-run] PDF: %s", pdf_url)

        upsert_publication({
            "id":          pub_id,
            "source":      SOURCE_KEY,
            "title":       title,
            "authors":     authors,
            "date":        year,
            "url":         page_url,
            "pdf_url":     pdf_url,
            "local_path":  local_path,
            "language":    None,
        })
        stored += 1

    # ── Summary ───────────────────────────────────────────────────────────────
    db = get_db()
    total_in_db = 0
    date_range  = ("n/a", "n/a")

    if "publications" in db.table_names():
        rows = list(db.execute(
            "SELECT COUNT(*), MIN(date), MAX(date) FROM publications WHERE source = ?",
            [SOURCE_KEY],
        ).fetchall())
        if rows and rows[0][0]:
            total_in_db = rows[0][0]
            date_range  = (rows[0][1] or "n/a", rows[0][2] or "n/a")

    print()
    print("=" * 60)
    print("ECDPM scrape complete")
    print(f"  Cutoff date            : {cutoff} (5 years ago)")
    print(f"  Publications fetched   : {len(hits)}")
    print(f"  Skipped (too old)      : {skipped_too_old}")
    print(f"  Stored / updated       : {stored}")
    print(f"  Skipped (already in db): {skipped_exists}")
    print(f"  No PDF found           : {skipped_no_pdf}")
    if not dry_run:
        print(f"  PDFs downloaded        : {downloaded}")
        print(f"  Download failures      : {failed_download}")
    print(f"  Total ECDPM in db      : {total_in_db}")
    print(f"  Date range collected   : {date_range[0]}  →  {date_range[1]}")
    print("=" * 60)


# ── CLI entry point ───────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Scrape ECDPM publications archive"
    )
    parser.add_argument(
        "--limit", type=int, default=None, metavar="N",
        help="Cap at N publications (for testing)",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Scrape metadata and find PDFs but skip downloads",
    )
    args = parser.parse_args()
    scrape(limit=args.limit, dry_run=args.dry_run)
