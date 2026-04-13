"""
Scraper for SAIIA (South African Institute of International Affairs).
https://saiia.org.za/publications/

Uses the WordPress REST API (wp-json/wp/v2/publication) with an `after` date
filter to retrieve only the last 5 years of publications. Paginates via the
`Link: rel="next"` header. Visits each publication page to find the PDF URL
(wp-content/uploads/*.pdf) and author names (/people/ links).

Usage:
    .venv/bin/python scrapers/saiia.py               # scrape last 5 years
    .venv/bin/python scrapers/saiia.py --limit 20    # test with 20 pubs
    .venv/bin/python scrapers/saiia.py --dry-run     # metadata only, no downloads
"""

import argparse
import hashlib
import json
import logging
import re
import sys
import time
from datetime import date, datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import requests
from bs4 import BeautifulSoup

from config import SOURCES, USER_AGENT, REQUEST_DELAY, REQUEST_TIMEOUT
from pipeline.db import init_db, upsert_publication, publication_exists, get_db
from pipeline.downloader import download_pdf

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

SOURCE_KEY = "saiia"
BASE_URL   = SOURCES[SOURCE_KEY]["base_url"]   # https://saiia.org.za
REST_BASE  = f"{BASE_URL}/wp-json/wp/v2"

_last_request: float = 0.0


def _wait() -> None:
    global _last_request
    elapsed = time.monotonic() - _last_request
    pause = REQUEST_DELAY - elapsed
    if pause > 0:
        time.sleep(pause)
    _last_request = time.monotonic()


def _get(url: str, **kwargs) -> requests.Response:
    _wait()
    resp = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=REQUEST_TIMEOUT, **kwargs)
    resp.raise_for_status()
    return resp


# ── Publication-type term lookup ──────────────────────────────────────────────

def fetch_pub_types() -> dict[int, str]:
    """Return a mapping of term_id → type name."""
    resp = _get(f"{REST_BASE}/publication-type?per_page=50")
    return {t["id"]: t["name"] for t in resp.json()}


# ── REST API pagination ───────────────────────────────────────────────────────

def iter_publications(after_iso: str, limit: int | None = None):
    """
    Yield publication records from the REST API, newest first.
    Stops when there are no more pages or `limit` is reached.
    `after_iso` is an ISO-8601 datetime string used to filter by date.
    """
    url = (
        f"{REST_BASE}/publication"
        f"?per_page=100"
        f"&orderby=date&order=desc"
        f"&after={after_iso}"
        f"&_fields=id,title,date,link,slug,publication-type"
    )
    count = 0
    page_num = 0

    while url:
        page_num += 1
        logger.info("Fetching page %d: %s", page_num, url)
        try:
            resp = _get(url)
        except requests.RequestException as exc:
            logger.error("REST API error: %s", exc)
            break

        total = resp.headers.get("X-WP-Total", "?")
        if page_num == 1:
            logger.info("REST API: %s publications in date window", total)

        items = resp.json()
        if not items:
            break

        for item in items:
            if limit and count >= limit:
                return
            yield item
            count += 1

        # Follow Link: rel="next" header for next page
        link_header = resp.headers.get("Link", "")
        next_url = _parse_next_link(link_header)
        url = next_url


def _parse_next_link(link_header: str) -> str | None:
    """Extract the rel="next" URL from a Link header."""
    for part in link_header.split(","):
        if 'rel="next"' in part:
            m = re.search(r'<([^>]+)>', part)
            if m:
                return m.group(1)
    return None


# ── Individual publication page parsing ──────────────────────────────────────

def parse_pub_page(page_url: str) -> tuple[str | None, list[str]]:
    """
    Fetch a publication landing page.
    Returns (pdf_url, author_names).
    pdf_url is None if no PDF is found.
    """
    try:
        resp = _get(page_url)
    except requests.RequestException as exc:
        logger.warning("Could not fetch %s: %s", page_url, exc)
        return None, []

    soup = BeautifulSoup(resp.text, "html.parser")

    # PDF: look for wp-content/uploads links on saiia.org.za ending in .pdf
    pdf_url = None
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if (
            "wp-content/uploads" in href
            and href.lower().endswith(".pdf")
            and "saiia.org.za" in href
        ):
            pdf_url = href
            break

    # Authors: links to /people/ pages
    authors = []
    seen = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "/people/" in href:
            name = a.get_text(strip=True)
            if name and name not in seen:
                authors.append(name)
                seen.add(name)

    return pdf_url, authors


# ── Helpers ───────────────────────────────────────────────────────────────────

def _pub_id(page_url: str) -> str:
    return hashlib.sha256(page_url.encode()).hexdigest()[:16]


def _format_date(iso_str: str) -> str:
    """'2026-04-07T12:46:38' → '2026-04-07'"""
    return iso_str[:10]


# ── Main scrape logic ─────────────────────────────────────────────────────────

def scrape(limit: int | None = None, dry_run: bool = False) -> None:
    init_db()

    cutoff    = date.today().replace(year=date.today().year - 5)
    after_iso = cutoff.isoformat() + "T00:00:00"
    logger.info("Date cutoff: %s (5 years ago)", cutoff)

    pub_types = fetch_pub_types()
    logger.info("Loaded %d publication-type terms", len(pub_types))

    skipped_exists  = 0
    skipped_no_pdf  = 0
    downloaded      = 0
    failed_download = 0
    stored          = 0

    for i, item in enumerate(iter_publications(after_iso, limit=limit), 1):
        page_url = item["link"]
        pub_id   = _pub_id(page_url)
        title    = item["title"]["rendered"].strip()
        pub_date = _format_date(item["date"])
        type_ids = item.get("publication-type") or []
        pub_type = ", ".join(pub_types.get(tid, str(tid)) for tid in type_ids) or "Unknown"

        logger.info("[%d] %s | %s | %s", i, pub_type, pub_date, title[:70])

        # Skip if already in db with a pdf_url
        if publication_exists(pub_id):
            try:
                existing = get_db()["publications"].get(pub_id)
                if existing and existing.get("pdf_url"):
                    logger.debug("  Already in db with PDF — skipping")
                    skipped_exists += 1
                    continue
            except Exception:
                pass

        # Visit publication page for PDF + authors
        pdf_url, authors = parse_pub_page(page_url)

        if not pdf_url:
            logger.info("  No PDF found — storing metadata only")
            skipped_no_pdf += 1
            upsert_publication({
                "id":          pub_id,
                "source":      SOURCE_KEY,
                "title":       title,
                "authors":     json.dumps(authors),
                "date":        pub_date,
                "url":         page_url,
                "pdf_url":     None,
                "local_path":  None,
                "language":    None,
            })
            stored += 1
            continue

        logger.info("  PDF: %s", pdf_url)
        logger.info("  Authors: %s", authors)

        local_path = None
        if not dry_run:
            slug     = item.get("slug", pub_id)[:80]
            filename = f"{slug}.pdf"
            local    = download_pdf(pdf_url, SOURCE_KEY, filename=filename)
            if local:
                local_path = str(local)
                downloaded += 1
                logger.info("  Downloaded: %s", local_path)
            else:
                failed_download += 1
                logger.warning("  Download failed: %s", pdf_url)
        else:
            logger.info("  [dry-run] would download: %s", pdf_url)

        upsert_publication({
            "id":          pub_id,
            "source":      SOURCE_KEY,
            "title":       title,
            "authors":     json.dumps(authors),
            "date":        pub_date,
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
        row = db.execute(
            "SELECT COUNT(*), MIN(date), MAX(date) FROM publications WHERE source = ?",
            [SOURCE_KEY],
        ).fetchone()
        if row and row[0]:
            total_in_db = row[0]
            date_range  = (row[1] or "n/a", row[2] or "n/a")

    print()
    print("=" * 60)
    print("SAIIA scrape complete")
    print(f"  Cutoff date            : {cutoff} (5 years ago)")
    print(f"  Stored / updated       : {stored}")
    print(f"  Skipped (already in db): {skipped_exists}")
    print(f"  No PDF found           : {skipped_no_pdf}")
    if not dry_run:
        print(f"  PDFs downloaded        : {downloaded}")
        print(f"  Download failures      : {failed_download}")
    print(f"  Total SAIIA in db      : {total_in_db}")
    print(f"  Date range collected   : {date_range[0]}  →  {date_range[1]}")
    print("=" * 60)


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scrape SAIIA publications archive")
    parser.add_argument("--limit", type=int, default=None, metavar="N",
                        help="Cap at N publications (for testing)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Scrape metadata and find PDFs but skip downloads")
    args = parser.parse_args()
    scrape(limit=args.limit, dry_run=args.dry_run)
