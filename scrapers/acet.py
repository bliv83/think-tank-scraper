"""
Scraper for ACET (African Center for Economic Transformation).
https://acetforafrica.org

Uses the WordPress REST API (wp-json/wp/v2/posts) filtered to research-
oriented categories, with a date cutoff applied via the `after` parameter.

PDF discovery:
  1. Pre-fetch all pdfviewer posts (custom post type) and build a
     slug → PDF URL map (PDF URL lives in serialised metadata field
     tnc_pvfw_pdf_viewer_fields under the "file" key).
  2. For each publication post, look up its slug in the map.
  3. Fall back to wp-content/uploads direct links on the landing page.

Author names come from the `authors` custom taxonomy (resolved lazily,
cached in memory).

Categories included:
  471 = Policy Briefs
   70 = Reports
  472 = Articles
  473 = Insights & Ideas
  218 = Multi-Country Studies
  645 = Annual Reports

Usage:
    .venv/bin/python scrapers/acet.py               # scrape last 5 years
    .venv/bin/python scrapers/acet.py --limit 20    # test with 20 pubs
    .venv/bin/python scrapers/acet.py --dry-run     # metadata only, no downloads
"""

import argparse
import hashlib
import json
import logging
import re
import sys
import time
from datetime import date
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

SOURCE_KEY = "acet"
BASE_URL   = SOURCES[SOURCE_KEY]["base_url"]   # https://acetforafrica.org
REST_BASE  = f"{BASE_URL}/wp-json/wp/v2"

# Research-type categories only
PUB_CATEGORIES = "471,70,472,473,218,645"

_last_request: float = 0.0
_author_cache: dict[int, str] = {}


def _wait() -> None:
    global _last_request
    elapsed = time.monotonic() - _last_request
    pause = REQUEST_DELAY - elapsed
    if pause > 0:
        time.sleep(pause)
    _last_request = time.monotonic()


def _get(url: str, **kwargs) -> requests.Response | None:
    _wait()
    try:
        resp = requests.get(
            url, headers={"User-Agent": USER_AGENT},
            timeout=REQUEST_TIMEOUT, **kwargs,
        )
        resp.raise_for_status()
        return resp
    except requests.RequestException as exc:
        logger.warning("Request failed (%s): %s", url, exc)
        return None


# ── PDF viewer map ────────────────────────────────────────────────────────────

def _parse_pdfviewer_url(fields_str: str) -> str | None:
    """Extract the PDF file URL from the PHP-serialised pdfviewer metadata."""
    m = re.search(r's:\d+:"file";s:\d+:"([^"]+\.pdf)"', fields_str)
    return m.group(1) if m else None


def build_pdfviewer_map() -> dict[str, str]:
    """
    Fetch all pdfviewer posts and return a dict of {slug: pdf_url}.
    Typically ~300 posts, fetched in 100-item pages.
    """
    slug_to_pdf: dict[str, str] = {}
    page = 1
    while True:
        url = f"{REST_BASE}/pdfviewer?per_page=100&page={page}&_fields=slug,metadata"
        resp = _get(url)
        if resp is None:
            break
        items = resp.json()
        if not items:
            break
        for item in items:
            slug = item.get("slug", "")
            meta = item.get("metadata", {})
            fields_list = meta.get("tnc_pvfw_pdf_viewer_fields", [])
            if fields_list:
                pdf = _parse_pdfviewer_url(fields_list[0])
                if pdf and slug:
                    slug_to_pdf[slug] = pdf
        total_pages = int(resp.headers.get("X-WP-TotalPages", 1))
        if page >= total_pages:
            break
        page += 1
    logger.info("PDF viewer map: %d entries", len(slug_to_pdf))
    return slug_to_pdf


# ── Author term lookup ────────────────────────────────────────────────────────

def resolve_authors(author_ids: list[int]) -> list[str]:
    """Return display names for a list of author taxonomy term IDs."""
    names = []
    for aid in author_ids:
        if aid in _author_cache:
            names.append(_author_cache[aid])
            continue
        resp = _get(f"{REST_BASE}/authors/{aid}?_fields=id,name")
        if resp and resp.status_code == 200:
            name = resp.json().get("name", "")
            _author_cache[aid] = name
            if name:
                names.append(name)
    return names


# ── REST API pagination ───────────────────────────────────────────────────────

def iter_publications(after_iso: str, limit: int | None = None):
    """Yield post records from the WP REST API, newest first."""
    url = (
        f"{REST_BASE}/posts"
        f"?per_page=100&orderby=date&order=desc"
        f"&categories={PUB_CATEGORIES}"
        f"&after={after_iso}"
        f"&_fields=id,date,slug,link,title,categories,authors"
    )
    count = 0
    page = 1
    while url:
        resp = _get(f"{url}&page={page}")
        if resp is None:
            break
        items = resp.json()
        if not items:
            break
        total = resp.headers.get("X-WP-Total", "?")
        if page == 1:
            logger.info("WP REST API: %s posts in date window", total)
        for item in items:
            if limit and count >= limit:
                return
            yield item
            count += 1
        total_pages = int(resp.headers.get("X-WP-TotalPages", 1))
        if page >= total_pages:
            break
        page += 1


# ── Fallback: pub page PDF scraping ──────────────────────────────────────────

def find_pdf_on_page(page_url: str) -> str | None:
    """
    Fetch the publication landing page and look for a wp-content/uploads PDF.
    Returns the PDF URL or None.
    """
    resp = _get(page_url)
    if resp is None:
        return None
    # Direct wp-content/uploads PDF links
    for m in re.finditer(
        r'https?://acetforafrica\.org/wp-content/uploads/[^\s"\'<>]+\.pdf',
        resp.text,
    ):
        return m.group(0)
    # pdfviewer link → extract slug → already handled via map, skip
    return None


def _pub_id(page_url: str) -> str:
    return hashlib.sha256(page_url.encode()).hexdigest()[:16]


def _format_date(iso_str: str) -> str:
    return iso_str[:10]


# ── Main scrape logic ─────────────────────────────────────────────────────────

def scrape(limit: int | None = None, dry_run: bool = False) -> None:
    init_db()

    cutoff    = date.today().replace(year=date.today().year - 5)
    after_iso = cutoff.isoformat() + "T00:00:00"
    logger.info("Date cutoff: %s (5 years ago)", cutoff)

    # Pre-build PDF viewer map
    logger.info("Building PDF viewer map...")
    pdfviewer_map = build_pdfviewer_map()

    skipped_exists  = 0
    skipped_no_pdf  = 0
    downloaded      = 0
    failed_download = 0
    stored          = 0

    for i, item in enumerate(iter_publications(after_iso, limit=limit), 1):
        # Skip posts that link to external sites (e.g. LSE event pages)
        page_url = item.get("link", "")
        if not page_url.startswith(BASE_URL):
            logger.debug("  Skipping external link: %s", page_url)
            continue

        pub_id   = _pub_id(page_url)
        title    = item["title"]["rendered"].strip()
        pub_date = _format_date(item["date"])
        slug     = item.get("slug", "")

        # Category label
        cat_ids  = item.get("categories", [])
        cat_map  = {471: "Policy Brief", 70: "Report", 472: "Article",
                    473: "Insights & Ideas", 218: "Multi-Country Study", 645: "Annual Report"}
        pub_type = next((cat_map[c] for c in cat_ids if c in cat_map), "Research")

        logger.info("[%d] %s | %s | %s", i, pub_type, pub_date, title[:70])

        # Skip if already in db with PDF
        if publication_exists(pub_id):
            try:
                existing = get_db()["publications"].get(pub_id)
                if existing and existing.get("pdf_url"):
                    logger.debug("  Already in db with PDF — skipping")
                    skipped_exists += 1
                    continue
            except Exception:
                pass

        # Authors
        authors = resolve_authors(item.get("authors") or [])
        logger.info("  Authors: %s", authors)

        # PDF: check pdfviewer map first, then fall back to page scrape
        pdf_url = pdfviewer_map.get(slug)
        if not pdf_url:
            pdf_url = find_pdf_on_page(page_url)

        if not pdf_url:
            logger.info("  No PDF found — storing metadata only")
            skipped_no_pdf += 1
            upsert_publication({
                "id": pub_id, "source": SOURCE_KEY,
                "title": title, "authors": json.dumps(authors),
                "date": pub_date, "url": page_url,
                "pdf_url": None, "local_path": None, "language": None,
            })
            stored += 1
            continue

        logger.info("  PDF: %s", pdf_url)

        local_path = None
        if not dry_run:
            filename = f"{slug[:80]}.pdf"
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
            "id": pub_id, "source": SOURCE_KEY,
            "title": title, "authors": json.dumps(authors),
            "date": pub_date, "url": page_url,
            "pdf_url": pdf_url, "local_path": local_path, "language": None,
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
    print("ACET scrape complete")
    print(f"  Cutoff date            : {cutoff} (5 years ago)")
    print(f"  Stored / updated       : {stored}")
    print(f"  Skipped (already in db): {skipped_exists}")
    print(f"  No PDF found           : {skipped_no_pdf}")
    if not dry_run:
        print(f"  PDFs downloaded        : {downloaded}")
        print(f"  Download failures      : {failed_download}")
    print(f"  Total ACET in db       : {total_in_db}")
    print(f"  Date range collected   : {date_range[0]}  →  {date_range[1]}")
    print("=" * 60)


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scrape ACET publications archive")
    parser.add_argument("--limit", type=int, default=None, metavar="N",
                        help="Cap at N publications (for testing)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Scrape metadata and find PDFs but skip downloads")
    args = parser.parse_args()
    scrape(limit=args.limit, dry_run=args.dry_run)
