"""
Scraper for TIPS (Trade & Industrial Policy Strategies).
https://www.tips.org.za

Covers two sections, both using Joomla/K2 RSS feeds:
  /research-archive   — main research output (608 items)
  /policy-briefs      — policy brief series

RSS pagination: ?format=feed&type=rss&limit=100&limitstart=N
Items are newest-first; stops when pubDate < 5-year cutoff.

Each item's landing page has:
  - itemExtraFields  → Author(s), Year, Organisation
  - itemAttachmentsBlock  → download link (redirects to PDF)
  - or direct /images/*.pdf link in body

Usage:
    .venv/bin/python scrapers/tips.py               # scrape last 5 years
    .venv/bin/python scrapers/tips.py --limit 20    # test with 20 pubs
    .venv/bin/python scrapers/tips.py --dry-run     # metadata only, no downloads
"""

import argparse
import hashlib
import json
import logging
import re
import sys
import time
from datetime import date, datetime
from email.utils import parsedate_to_datetime
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

SOURCE_KEY = "tips"
BASE_URL   = SOURCES[SOURCE_KEY]["base_url"]   # https://www.tips.org.za

SECTIONS = [
    ("research-archive", "Research"),
    ("policy-briefs",    "Policy Brief"),
]

_last_request: float = 0.0


def _wait() -> None:
    global _last_request
    elapsed = time.monotonic() - _last_request
    pause = REQUEST_DELAY - elapsed
    if pause > 0:
        time.sleep(pause)
    _last_request = time.monotonic()


def _get(url: str) -> requests.Response | None:
    _wait()
    try:
        resp = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        return resp
    except requests.RequestException as exc:
        logger.warning("Request failed (%s): %s", url, exc)
        return None


# ── RSS feed ──────────────────────────────────────────────────────────────────

def iter_rss(section: str, cutoff: date, limit: int | None = None):
    """
    Yield RSS items from a TIPS section, newest-first, stopping at cutoff.
    Each item is a dict: {title, url, date (YYYY-MM-DD), pub_type}.
    """
    limitstart = 0
    count = 0

    while True:
        url = (
            f"{BASE_URL}/{section}"
            f"?format=feed&type=rss&limit=100&limitstart={limitstart}"
        )
        logger.info("Fetching RSS page (offset %d): %s", limitstart, url)
        resp = _get(url)
        if resp is None:
            break

        soup = BeautifulSoup(resp.content, "xml")
        items = soup.find_all("item")
        if not items:
            break

        done = False
        for item in items:
            if limit and count >= limit:
                return

            pub_date_str = ""
            pub_date = None
            pd_tag = item.find("pubDate")
            if pd_tag and pd_tag.text.strip():
                try:
                    dt = parsedate_to_datetime(pd_tag.text.strip())
                    pub_date = dt.date()
                    pub_date_str = pub_date.isoformat()
                except Exception:
                    pass

            if pub_date and pub_date < cutoff:
                logger.info(
                    "Reached cutoff (%s) at '%s' (%s) — stopping section %s",
                    cutoff,
                    item.find("title").text.strip()[:50] if item.find("title") else "",
                    pub_date,
                    section,
                )
                done = True
                break

            title_tag = item.find("title")
            link_tag  = item.find("link")

            # <link> in RSS XML is tricky — it may be a NavigableString after the tag
            link_url = ""
            if link_tag:
                # xml parser puts link text as next sibling NavigableString
                link_url = link_tag.next_sibling
                if link_url:
                    link_url = str(link_url).strip()
                if not link_url:
                    link_url = link_tag.get_text(strip=True)

            yield {
                "title":    title_tag.get_text(strip=True) if title_tag else "",
                "url":      link_url,
                "date":     pub_date_str,
                "pub_type": SECTIONS[[s for s, _ in SECTIONS].index(section)][1]
                            if section in [s for s, _ in SECTIONS] else section,
            }
            count += 1

        if done or len(items) < 100:
            break
        limitstart += 100


# ── Publication page parsing ──────────────────────────────────────────────────

def parse_pub_page(html: str) -> tuple[str | None, list[str]]:
    """
    Parse a TIPS publication page.
    Returns (pdf_url | None, [author_names]).
    """
    soup = BeautifulSoup(html, "html.parser")

    # PDF — priority 1: attachment download link (redirects to PDF)
    pdf_url = None
    att_block = soup.find(class_="itemAttachmentsBlock")
    if att_block:
        a = att_block.find("a", href=True)
        if a:
            href = a["href"]
            pdf_url = BASE_URL + href if href.startswith("/") else href

    # PDF — priority 2: direct .pdf link in body
    if not pdf_url:
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if href.lower().endswith(".pdf"):
                pdf_url = BASE_URL + href if href.startswith("/") else href
                break

    # Authors from itemExtraFields
    authors = []
    ext = soup.find(class_="itemExtraFields")
    if ext:
        for li in ext.find_all("li"):
            label = li.find("span", class_="itemExtraFieldsLabel")
            value = li.find("span", class_="itemExtraFieldsValue")
            if label and value and "Author" in label.get_text():
                # May be comma-separated
                raw = value.get_text(strip=True)
                for name in re.split(r"[,;]", raw):
                    name = name.strip()
                    if name:
                        authors.append(name)

    return pdf_url, authors


def _pub_id(page_url: str) -> str:
    return hashlib.sha256(page_url.encode()).hexdigest()[:16]


# ── Main scrape logic ─────────────────────────────────────────────────────────

def scrape(limit: int | None = None, dry_run: bool = False) -> None:
    init_db()

    cutoff = date.today().replace(year=date.today().year - 5)
    logger.info("Date cutoff: %s (5 years ago)", cutoff)

    skipped_exists  = 0
    skipped_no_pdf  = 0
    downloaded      = 0
    failed_download = 0
    stored          = 0
    processed       = 0

    for section, section_label in SECTIONS:
        logger.info("=== Section: %s ===", section_label)

        for item in iter_rss(section, cutoff, limit=limit):
            if limit and processed >= limit:
                break

            processed += 1
            page_url = item["url"]
            pub_id   = _pub_id(page_url)

            logger.info(
                "[%d] %s | %s | %s",
                processed, item["pub_type"], item["date"] or "n/d", item["title"][:70],
            )

            if publication_exists(pub_id):
                try:
                    existing = get_db()["publications"].get(pub_id)
                    if existing and existing.get("pdf_url"):
                        logger.debug("  Already in db with PDF — skipping")
                        skipped_exists += 1
                        continue
                except Exception:
                    pass

            resp = _get(page_url)
            if resp is None:
                logger.warning("  Could not fetch pub page, storing metadata only")
                upsert_publication({
                    "id": pub_id, "source": SOURCE_KEY,
                    "title": item["title"], "authors": "[]",
                    "date": item["date"], "url": page_url,
                    "pdf_url": None, "local_path": None, "language": None,
                })
                stored += 1
                continue

            pdf_url, authors = parse_pub_page(resp.text)

            if not pdf_url:
                logger.info("  No PDF found — storing metadata only")
                skipped_no_pdf += 1
                upsert_publication({
                    "id": pub_id, "source": SOURCE_KEY,
                    "title": item["title"], "authors": json.dumps(authors),
                    "date": item["date"], "url": page_url,
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
                "date": item["date"], "url": page_url,
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
    print("TIPS scrape complete")
    print(f"  Cutoff date            : {cutoff} (5 years ago)")
    print(f"  Stored / updated       : {stored}")
    print(f"  Skipped (already in db): {skipped_exists}")
    print(f"  No PDF found           : {skipped_no_pdf}")
    if not dry_run:
        print(f"  PDFs downloaded        : {downloaded}")
        print(f"  Download failures      : {failed_download}")
    print(f"  Total TIPS in db       : {total_in_db}")
    print(f"  Date range collected   : {date_range[0]}  →  {date_range[1]}")
    print("=" * 60)


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scrape TIPS publications archive")
    parser.add_argument("--limit", type=int, default=None, metavar="N",
                        help="Cap at N publications (for testing)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Scrape metadata and find PDFs but skip downloads")
    args = parser.parse_args()
    scrape(limit=args.limit, dry_run=args.dry_run)
