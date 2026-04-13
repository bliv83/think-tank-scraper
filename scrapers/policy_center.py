"""
Scraper for Policy Center for the New South.
https://www.policycenter.ma

The site is protected by a JavaScript challenge that blocks plain HTTP
clients; this scraper uses camoufox (patched Firefox) to bypass it.

CMS: Drupal 9/10 with a Views listing.
Listing:  /publications/all?page=N  (15 items/page, 0-indexed, newest-first)

Each listing item (li.publication-item) has:
  • .views-field-field-attachment-document a  — direct PDF on /sites/default/files/...
  • .views-field-title a                      — landing page URL (if item has one)
  • .views-field-field-date .field-content    — "Month D, YYYY"
  • .views-field-field-external-authors       — author names
  • .views-field-field-publication-type       — pub type

When no direct PDF is in the listing item, the landing page is visited
and /pdf/... or /sites/default/files/... links are extracted.

Language is stored as NULL and filled in by pipeline/extractor.py after
text extraction (site publishes in EN/FR/AR).

Usage:
    .venv/bin/python scrapers/policy_center.py               # scrape last 5 years
    .venv/bin/python scrapers/policy_center.py --limit 20    # test with 20 pubs
    .venv/bin/python scrapers/policy_center.py --dry-run     # metadata only, no downloads
    .venv/bin/python scrapers/policy_center.py --no-headless # show browser (debug)
"""

import argparse
import asyncio
import hashlib
import json
import logging
import re
import sys
from datetime import date, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
from bs4 import BeautifulSoup
from camoufox.async_api import AsyncCamoufox

from config import SOURCES, REQUEST_DELAY, REQUEST_TIMEOUT
from pipeline.db import init_db, upsert_publication, publication_exists, get_db
from pipeline.downloader import download_pdf

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

SOURCE_KEY = "policy_center"
BASE_URL   = SOURCES[SOURCE_KEY]["base_url"]   # https://www.policycenter.ma
LIST_PATH  = "/publications/all"

# Seconds to settle after first page load (JS challenge)
CF_SETTLE_S = 5


# ── Helpers ───────────────────────────────────────────────────────────────────

def _pub_id(url: str) -> str:
    return hashlib.sha256(url.encode()).hexdigest()[:16]


def _parse_date(raw: str) -> str:
    """'April 1, 2026' → '2026-04-01'; returns '' on failure."""
    raw = raw.strip()
    for fmt in ("%B %d, %Y", "%B %Y", "%Y"):
        try:
            return datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass
    return raw


def _make_abs(href: str) -> str:
    href = href.strip()
    if href.startswith("http"):
        return href
    if href.startswith("/"):
        return BASE_URL + href
    return href


# ── Listing page parsing ──────────────────────────────────────────────────────

def parse_listing(html: str) -> list[dict]:
    """
    Parse one publications listing page.
    Returns a list of item dicts:
      {title, page_url, pdf_url_direct, date, pub_type, authors}
    pdf_url_direct is set when the listing item already contains a PDF link.

    Each <li> wraps a left-block-pub (thumbnail + attachment PDF) and a
    right-block-pub (title link, type, authors, date) inside an outer
    .views-field-title div — so we scope lookups to each half separately.
    """
    soup = BeautifulSoup(html, "html.parser")
    items = []

    for li in soup.find_all("li", class_="publication-item"):
        left  = li.find("div", class_="left-block-pub")
        right = li.find("div", class_="right-block-pub")

        # ── Direct PDF from attachment in left block ──────────────────────
        pdf_direct = None
        if left:
            att = left.find("div", class_="views-field-field-attachment-document")
            if att:
                a = att.find("a", href=True)
                if a:
                    href = _make_abs(a["href"])
                    if href.lower().endswith(".pdf"):
                        pdf_direct = href

        # ── Title and landing-page URL from right block ───────────────────
        page_url = None
        title    = ""
        if right:
            title_div = right.find("div", class_="views-field-title")
            if title_div:
                a = title_div.find("a", href=True)
                if a:
                    page_url = _make_abs(a["href"])
                    title    = a.get_text(strip=True)

        # ── Date ──────────────────────────────────────────────────────────
        raw_date = ""
        if right:
            date_div = right.find("div", class_="views-field-field-date")
            if date_div:
                fc = date_div.find("div", class_="field-content")
                raw_date = fc.get_text(strip=True) if fc else date_div.get_text(strip=True)

        # ── Pub type ──────────────────────────────────────────────────────
        pub_type = ""
        if right:
            type_div = right.find("div", class_="views-field-field-publication-type")
            if type_div:
                inner = type_div.find("div", class_="field-content")
                if inner:
                    typed    = inner.find("div")
                    pub_type = typed.get_text(strip=True) if typed else inner.get_text(strip=True)

        # ── Authors (two formats co-exist) ────────────────────────────────
        authors: list[str] = []
        seen: set[str]     = set()
        auth_scope = right or li
        auth_div = auth_scope.find("div", class_="views-field-field-external-authors")
        if auth_div:
            # Format 1: plain text in field--name-field-name (e.g. "Edited by X")
            for nd in auth_div.find_all("div", class_="field--name-field-name"):
                name = re.sub(r"^Edited\s+by\s+", "", nd.get_text(strip=True)).strip()
                if name and name not in seen:
                    authors.append(name)
                    seen.add(name)
            # Format 2: link in field--name-field-author (PCNS experts)
            for nd in auth_div.find_all("div", class_="field--name-field-author"):
                a = nd.find("a")
                name = a.get_text(strip=True) if a else nd.get_text(strip=True)
                name = name.strip()
                if name and name not in seen:
                    authors.append(name)
                    seen.add(name)

        # Skip items with no title and no PDF
        if not title and not pdf_direct:
            continue

        items.append({
            "title":      title,
            "page_url":   page_url,
            "pdf_direct": pdf_direct,
            "date":       _parse_date(raw_date),
            "pub_type":   pub_type,
            "authors":    authors,
        })

    return items


# ── Publication page PDF extraction ──────────────────────────────────────────

def find_pdf_on_pub_page(html: str) -> str | None:
    """
    Scan a landing page for a downloadable PDF.
    Prefers /pdf/... links; falls back to /sites/default/files/... .pdf links.
    """
    soup = BeautifulSoup(html, "html.parser")

    # Priority 1: /pdf/ path links that end in .pdf
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if "/pdf/" in href and href.lower().endswith(".pdf"):
            return _make_abs(href)

    # Priority 2: /sites/default/files/ .pdf links
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if "/sites/default/files/" in href and href.lower().endswith(".pdf"):
            return _make_abs(href)

    return None


# ── Browser navigation ────────────────────────────────────────────────────────

async def _navigate(page, url: str, settle_s: float = 0) -> str:
    await asyncio.sleep(REQUEST_DELAY)
    await page.goto(url, wait_until="domcontentloaded", timeout=REQUEST_TIMEOUT * 1000)
    if settle_s:
        await page.wait_for_timeout(int(settle_s * 1000))
    return await page.content()


# ── Main scrape logic ─────────────────────────────────────────────────────────

async def scrape_async(
    limit: int | None = None,
    dry_run: bool = False,
    headless: bool = True,
) -> None:
    init_db()

    cutoff = date.today().replace(year=date.today().year - 5)
    logger.info("Date cutoff: %s (5 years ago)", cutoff)

    skipped_exists  = 0
    skipped_too_old = 0
    skipped_no_pdf  = 0
    downloaded      = 0
    failed_download = 0
    stored          = 0
    processed       = 0

    async with AsyncCamoufox(headless=headless) as browser:
        page = await browser.new_page()

        page_num = 0
        done = False

        while not done:
            list_url = f"{BASE_URL}{LIST_PATH}?page={page_num}"
            settle   = CF_SETTLE_S if page_num == 0 else 0
            logger.info("Fetching listing page %d: %s", page_num, list_url)

            html  = await _navigate(page, list_url, settle_s=settle)
            items = parse_listing(html)
            logger.info("  %d items on page %d", len(items), page_num)

            if not items:
                logger.info("Empty page — stopping.")
                break

            for item in items:
                if limit and processed >= limit:
                    done = True
                    break

                pub_date_str = item["date"]
                pub_date = None
                if pub_date_str:
                    try:
                        pub_date = date.fromisoformat(pub_date_str[:10])
                    except ValueError:
                        pass

                if pub_date and pub_date < cutoff:
                    logger.info(
                        "Reached cutoff (%s) at '%s' (%s) — stopping",
                        cutoff, item["title"][:60], pub_date,
                    )
                    skipped_too_old += 1
                    done = True
                    break

                processed += 1

                # Determine canonical URL (prefer landing page; fall back to PDF)
                page_url = item["page_url"] or item["pdf_direct"] or ""
                if not page_url:
                    logger.debug("  No URL — skipping")
                    continue

                pub_id = _pub_id(page_url)

                logger.info(
                    "[%d] %s | %s | %s",
                    processed, item["pub_type"] or "n/t",
                    pub_date_str or "n/d", item["title"][:70],
                )

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

                # PDF: use listing attachment if available; else scrape pub page
                pdf_url = item["pdf_direct"]
                if not pdf_url and item["page_url"]:
                    pub_html = await _navigate(page, item["page_url"])
                    pdf_url  = find_pdf_on_pub_page(pub_html)

                if not pdf_url:
                    logger.info("  No PDF found — storing metadata only")
                    skipped_no_pdf += 1
                    upsert_publication({
                        "id": pub_id, "source": SOURCE_KEY,
                        "title": item["title"], "authors": json.dumps(item["authors"]),
                        "date": pub_date_str, "url": page_url,
                        "pdf_url": None, "local_path": None, "language": None,
                    })
                    stored += 1
                    continue

                logger.info("  PDF:     %s", pdf_url)
                logger.info("  Authors: %s", item["authors"])

                local_path = None
                if not dry_run:
                    raw_slug   = page_url.rstrip("/").split("/")[-1].split("?")[0]
                    slug       = re.sub(r"[^a-z0-9-]", "-", raw_slug.lower())[:80]
                    filename   = f"{slug}.pdf"
                    local      = download_pdf(pdf_url, SOURCE_KEY, filename=filename, force=False)
                    if local:
                        local_path = str(local)
                        downloaded += 1
                        logger.info("  Downloaded: %s", local_path)
                    else:
                        failed_download += 1
                        logger.warning("  Download failed")
                else:
                    logger.info("  [dry-run] would download: %s", pdf_url)

                upsert_publication({
                    "id": pub_id, "source": SOURCE_KEY,
                    "title": item["title"], "authors": json.dumps(item["authors"]),
                    "date": pub_date_str, "url": page_url,
                    "pdf_url": pdf_url, "local_path": local_path, "language": None,
                })
                stored += 1

            if not done:
                page_num += 1

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
    print("Policy Center scrape complete")
    print(f"  Cutoff date            : {cutoff} (5 years ago)")
    print(f"  Listing pages fetched  : {page_num + 1}")
    print(f"  Stored / updated       : {stored}")
    print(f"  Skipped (already in db): {skipped_exists}")
    print(f"  Skipped (too old)      : {skipped_too_old}")
    print(f"  No PDF found           : {skipped_no_pdf}")
    if not dry_run:
        print(f"  PDFs downloaded        : {downloaded}")
        print(f"  Download failures      : {failed_download}")
    print(f"  Total Policy Center db : {total_in_db}")
    print(f"  Date range collected   : {date_range[0]}  →  {date_range[1]}")
    print("=" * 60)


def scrape(limit: int | None = None, dry_run: bool = False, headless: bool = True) -> None:
    asyncio.run(scrape_async(limit=limit, dry_run=dry_run, headless=headless))


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Scrape Policy Center publications (uses camoufox to bypass JS challenge)"
    )
    parser.add_argument("--limit", type=int, default=None, metavar="N",
                        help="Cap at N publications (for testing)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Scrape metadata and find PDFs but skip downloads")
    parser.add_argument("--no-headless", action="store_true",
                        help="Show the browser window (useful for debugging)")
    args = parser.parse_args()
    scrape(limit=args.limit, dry_run=args.dry_run, headless=not args.no_headless)
