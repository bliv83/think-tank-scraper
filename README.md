# Think Tank Scraper

Collects and processes publications from Africa-Europe policy research organisations.

## Sources

| Key | Organisation |
|-----|-------------|
| `ecdpm` | European Centre for Development Policy Management |
| `odi` | Overseas Development Institute |
| `saiia` | South African Institute of International Affairs |
| `tips` | Trade & Industrial Policy Strategies |
| `policy_center` | Policy Center for the New South |
| `acet` | African Center for Economic Transformation |

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
playwright install chromium   # for JS-heavy pages
cp .env.example .env
```

## Project structure

```
think-tank-scraper/
  scrapers/          # one file per organisation (e.g. scrapers/ecdpm.py)
  pipeline/
    db.py            # SQLite schema and upsert helpers
    downloader.py    # polite HTTP fetcher (1.5s delay, identifies bot)
    extractor.py     # PDF text extraction (PyMuPDF) + language detection
  data/
    raw/             # downloaded PDFs, organised by source/
    metadata/        # publications.db (SQLite)
    extracted/       # (optional) plain-text dumps
  config.py          # SOURCES dict, paths, crawl settings
  requirements.txt
```

## Database schema

Table: `publications`

| Column | Type | Notes |
|--------|------|-------|
| `id` | TEXT PK | SHA-256 of `pdf_url` (first 16 hex chars) |
| `source` | TEXT | Key from `config.SOURCES` |
| `title` | TEXT | |
| `authors` | TEXT | JSON array |
| `date` | TEXT | ISO-8601, e.g. `"2024-03"` |
| `url` | TEXT | Landing page |
| `pdf_url` | TEXT | Direct PDF link |
| `local_path` | TEXT | Path relative to project root |
| `language` | TEXT | ISO 639-1 code |
| `extracted_text` | TEXT | Full text from PyMuPDF |
| `word_count` | INTEGER | |
| `scraped_at` | TEXT | ISO-8601 UTC |

## Writing a scraper

Create `scrapers/<source_key>.py` with a `scrape()` function:

```python
from pipeline.db import init_db, upsert_publication
from pipeline.downloader import fetch_html, download_pdf
from config import SOURCES
import hashlib

def scrape():
    init_db()
    source = SOURCES["ecdpm"]
    html = fetch_html(source["publications_url"])
    # parse HTML, find PDF links, then for each:
    #   local = download_pdf(pdf_url, "ecdpm")
    #   upsert_publication({
    #       "id":        hashlib.sha256(pdf_url.encode()).hexdigest()[:16],
    #       "source":    "ecdpm",
    #       "title":     title,
    #       "pdf_url":   pdf_url,
    #       "local_path": str(local),
    #       ...
    #   })
```

Then extract text for all downloaded PDFs:

```python
from pipeline.extractor import process_all_pending
process_all_pending(source="ecdpm")
```

## Politeness

The downloader self-identifies via `User-Agent` and waits 1.5 seconds between
requests. Do not reduce `REQUEST_DELAY` below 1 second. Respect `robots.txt`
and the terms of service of each organisation.
```
