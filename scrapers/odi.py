"""
Scraper for ODI (Overseas Development Institute).
https://odi.org/en/publications/

ODI is behind Cloudflare Turnstile (managed challenge), which blocks all
automated HTTP clients including plain headless browsers. This scraper uses
camoufox — a patched Firefox with randomised fingerprints that passes the
non-interactive Turnstile challenge automatically.

── Page structure ────────────────────────────────────────────────────────────
Listing:   /en/publications/?page=N  (20 items/page, ~404 pages)
           <li class="results__list__item">
             <h3 class="...title"><a href="https://odi.org/en/publications/slug/">Title</a>
             <time class="...date" datetime="YYYY-MM-DD">
             <p class="...type">Type
Pagination: <nav class="pagination">
              <a class="...pagination__page-link" href="?page=N">
            Last link gives total page count.
Pub page:  <li class="author"> → <h3 class="author__name">
           <a href="/documents/{id}/file.pdf"> (relative link)

Usage:
    .venv/bin/python scrapers/odi.py               # scrape last 5 years
    .venv/bin/python scrapers/odi.py --limit 20    # test with 20 pubs
    .venv/bin/python scrapers/odi.py --dry-run     # metadata only, no downloads
    .venv/bin/python scrapers/odi.py --no-headless # show browser window (debug)
"""

import argparse
import asyncio
import hashlib
import json
import logging
import re
import sys
import time
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

SOURCE_KEY = "odi"
BASE_URL   = SOURCES[SOURCE_KEY]["base_url"]     # https://odi.org
LIST_URL   = SOURCES[SOURCE_KEY]["publications_url"]  # https://odi.org/en/publications/

# Seconds to wait after page load before reading DOM (lets Turnstile clear)
CF_SETTLE_S = 8


# ── Helpers ───────────────────────────────────────────────────────────────────

def _pub_id(page_url: str) -> str:
    return hashlib.sha256(page_url.encode()).hexdigest()[:16]


# ── Page parsers (operate on HTML strings) ────────────────────────────────────

def parse_listing(html: str) -> tuple[list[dict], int]:
    """
    Parse one publications listing page.
    Returns (items, max_page).
    Each item: {title, url, date, pub_type}
    max_page is the highest page number found in pagination (0 if not found).
    """
    soup = BeautifulSoup(html, "html.parser")
    items = []

    for li in soup.find_all("li", class_="results__list__item"):
        a        = li.find("a", href=True)
        time_tag = li.find("time", class_="results__list__item__meta__date")
        type_tag = li.find("p",    class_="results__list__item__meta__type")

        if not a:
            continue

        href = a["href"]
        if href.startswith("/"):
            href = BASE_URL + href

        items.append({
            "title":    a.get_text(strip=True),
            "url":      href,
            "date":     time_tag.get("datetime", "") if time_tag else "",
            "pub_type": type_tag.get_text(strip=True) if type_tag else "",
        })

    # Pagination: scan all anchors for ?page=N hrefs
    max_page = 0
    for a in soup.find_all("a", href=re.compile(r"\?page=\d+")):
        m = re.search(r"\?page=(\d+)", a["href"])
        if m:
            max_page = max(max_page, int(m.group(1)))

    return items, max_page


def parse_pub_page(html: str) -> tuple[str | None, list[str]]:
    """
    Parse an individual ODI publication page.
    Returns (pdf_url | None, [author_names]).
    """
    soup = BeautifulSoup(html, "html.parser")

    # PDF: links to /documents/...pdf
    pdf_url = None
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "/documents/" in href and href.lower().endswith(".pdf"):
            pdf_url = BASE_URL + href if href.startswith("/") else href
            break

    # Authors: li.author > h3.author__name
    authors = []
    seen = set()
    for li in soup.find_all("li", class_="author"):
        name_tag = li.find("h3", class_="author__name")
        if name_tag:
            name = name_tag.get_text(strip=True)
            if name and name not in seen:
                authors.append(name)
                seen.add(name)

    return pdf_url, authors


# Timeout for listing pages (shorter); pub pages get PUB_TIMEOUT_S
PUB_TIMEOUT_S  = 45   # seconds — individual pub pages can be slow
PUB_RETRIES    = 3    # max attempts per pub page
RETRY_WAIT_S   = 5    # wait between retries


# ── Browser helpers ───────────────────────────────────────────────────────────

async def _navigate(page, url: str, settle_s: float = 0, timeout_s: int = REQUEST_TIMEOUT) -> str:
    """Navigate to url, wait for DOM content, optionally settle, return HTML."""
    await asyncio.sleep(REQUEST_DELAY)
    await page.goto(url, wait_until="domcontentloaded", timeout=timeout_s * 1000)
    if settle_s:
        await page.wait_for_timeout(int(settle_s * 1000))
    return await page.content()


async def _navigate_pub(page, url: str) -> str:
    """
    Fetch an individual publication page with retry logic.
    Up to PUB_RETRIES attempts; waits RETRY_WAIT_S between them.
    Returns empty string on persistent failure rather than crashing.
    """
    for attempt in range(1, PUB_RETRIES + 1):
        try:
            return await _navigate(page, url, timeout_s=PUB_TIMEOUT_S)
        except Exception as exc:
            if attempt < PUB_RETRIES:
                logger.warning(
                    "  Attempt %d/%d failed for %s (%s) — retrying in %ds",
                    attempt, PUB_RETRIES, url, type(exc).__name__, RETRY_WAIT_S,
                )
                await page.wait_for_timeout(RETRY_WAIT_S * 1000)
            else:
                logger.warning(
                    "  All %d attempts failed for %s — skipping pub page",
                    PUB_RETRIES, url,
                )
    return ""


# ── Main scrape logic ─────────────────────────────────────────────────────────

async def scrape_async(
    limit: int | None = None,
    dry_run: bool = False,
    headless: bool = True,
    start_page: int = 1,
) -> None:
    init_db()

    cutoff = date.today().replace(year=date.today().year - 5)
    logger.info("Date cutoff: %s (5 years ago)", cutoff)
    if start_page > 1:
        logger.info("Resuming from page %d", start_page)

    skipped_exists      = 0
    skipped_too_old     = 0
    skipped_no_pdf      = 0
    downloaded          = 0
    failed_download     = 0
    stored              = 0
    processed           = 0
    consecutive_empty   = 0
    MAX_CONSECUTIVE_EMPTY = 3   # abort if CF blocks this many pages in a row

    async with AsyncCamoufox(headless=headless) as browser:
        page = await browser.new_page()

        # ── Fetch starting page (with CF settle on first load) ────────────────
        first_url = LIST_URL if start_page == 1 else LIST_URL.rstrip("/") + f"/?page={start_page}"
        logger.info("Fetching listing page %d (with Cloudflare settle)...", start_page)
        await page.goto(first_url, wait_until="domcontentloaded", timeout=REQUEST_TIMEOUT * 1000)
        await page.wait_for_timeout(int(CF_SETTLE_S * 1000))
        try:
            await page.wait_for_selector("nav.pagination", timeout=10000)
        except Exception:
            pass
        html = await page.content()
        items, max_page = parse_listing(html)

        if not items and start_page == 1:
            logger.error("No items found on page 1 — Cloudflare may still be blocking.")
            return

        if max_page:
            logger.info("  %d items | %d total pages", len(items), max_page)

        # ── Iterate pages ─────────────────────────────────────────────────────
        current_page = start_page
        done = False

        while not done:
            if current_page > start_page:
                url = LIST_URL.rstrip("/") + f"/?page={current_page}"
                logger.info("Fetching listing page %d / %d", current_page, max_page)
                html = await _navigate(page, url)
                items, _ = parse_listing(html)
                logger.info("  %d items on page %d", len(items), current_page)

            # Detect Cloudflare re-challenge (empty page or challenge title)
            if not items:
                consecutive_empty += 1
                if consecutive_empty >= MAX_CONSECUTIVE_EMPTY:
                    logger.error(
                        "Got %d consecutive empty pages — Cloudflare may have re-challenged. "
                        "Restart with --start-page %d to resume.",
                        consecutive_empty, current_page - consecutive_empty + 1,
                    )
                    done = True
                    break
                current_page += 1
                continue
            else:
                consecutive_empty = 0

            for item in items:
                if limit and processed >= limit:
                    done = True
                    break

                pub_date_str = item["date"]
                pub_date = None
                if pub_date_str:
                    try:
                        pub_date = date.fromisoformat(pub_date_str)
                    except ValueError:
                        pass

                # Listing is newest-first; stop once past the 5-year window
                if pub_date and pub_date < cutoff:
                    logger.info(
                        "Reached cutoff (%s) at '%s' (%s) — stopping pagination",
                        cutoff, item["title"][:60], pub_date,
                    )
                    skipped_too_old += 1
                    done = True
                    break

                processed += 1
                page_url = item["url"]
                pub_id   = _pub_id(page_url)

                logger.info(
                    "[%d] %s | %s | %s",
                    processed, item["pub_type"] or "n/t", pub_date_str or "n/d",
                    item["title"][:70],
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

                # Fetch individual pub page for PDF + authors (with retry)
                pub_html = await _navigate_pub(page, page_url)
                pdf_url, authors = parse_pub_page(pub_html)

                if not pdf_url:
                    logger.info("  No PDF found — storing metadata only")
                    skipped_no_pdf += 1
                    upsert_publication({
                        "id": pub_id, "source": SOURCE_KEY,
                        "title": item["title"], "authors": json.dumps(authors),
                        "date": pub_date_str, "url": page_url,
                        "pdf_url": None, "local_path": None, "language": None,
                    })
                    stored += 1
                    continue

                logger.info("  PDF:     %s", pdf_url)
                logger.info("  Authors: %s", authors)

                local_path = None
                if not dry_run:
                    slug     = re.sub(r"[^a-z0-9-]", "-", page_url.rstrip("/").split("/")[-1].lower())[:80]
                    filename = f"{slug}.pdf"
                    local    = download_pdf(pdf_url, SOURCE_KEY, filename=filename, force=False)
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
                    "title": item["title"], "authors": json.dumps(authors),
                    "date": pub_date_str, "url": page_url,
                    "pdf_url": pdf_url, "local_path": local_path, "language": None,
                })
                stored += 1

            if not done:
                current_page += 1
                if max_page and current_page > max_page:
                    logger.info("Reached last page (%d)", max_page)
                    done = True
                    break

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
    print("ODI scrape complete")
    print(f"  Cutoff date            : {cutoff} (5 years ago)")
    print(f"  Listing pages fetched  : {current_page}")
    print(f"  Stored / updated       : {stored}")
    print(f"  Skipped (already in db): {skipped_exists}")
    print(f"  Skipped (too old)      : {skipped_too_old}")
    print(f"  No PDF found           : {skipped_no_pdf}")
    if not dry_run:
        print(f"  PDFs downloaded        : {downloaded}")
        print(f"  Download failures      : {failed_download}")
    print(f"  Total ODI in db        : {total_in_db}")
    print(f"  Date range collected   : {date_range[0]}  →  {date_range[1]}")
    print("=" * 60)


def scrape(
    limit: int | None = None,
    dry_run: bool = False,
    headless: bool = True,
    start_page: int = 1,
) -> None:
    asyncio.run(scrape_async(limit=limit, dry_run=dry_run, headless=headless, start_page=start_page))


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Scrape ODI publications (uses camoufox to bypass Cloudflare Turnstile)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--limit", type=int, default=None, metavar="N",
                        help="Cap at N publications (for testing)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Scrape metadata and find PDFs but skip downloads")
    parser.add_argument("--no-headless", action="store_true",
                        help="Show the browser window (useful for debugging)")
    parser.add_argument("--start-page", type=int, default=1, metavar="N",
                        help="Resume from listing page N (1-indexed; skips already-stored pubs)")
    args = parser.parse_args()

    scrape(
        limit=args.limit,
        dry_run=args.dry_run,
        headless=not args.no_headless,
        start_page=args.start_page,
    )
