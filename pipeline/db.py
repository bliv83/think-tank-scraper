"""
Database layer — SQLite via sqlite-utils.
Call init_db() once at startup; use upsert_publication() to write records.
"""

import sqlite_utils
from datetime import datetime, timezone
from config import DB_PATH

# Ensure parent directory exists
DB_PATH.parent.mkdir(parents=True, exist_ok=True)


def get_db() -> sqlite_utils.Database:
    return sqlite_utils.Database(DB_PATH)


def init_db() -> None:
    db = get_db()
    db["publications"].insert(
        {
            "id":             None,   # SHA-256 of pdf_url (set by caller)
            "source":         None,   # key from config.SOURCES
            "title":          None,
            "authors":        None,   # JSON array stored as text
            "date":           None,   # ISO-8601 string, e.g. "2024-03"
            "url":            None,   # landing page URL
            "pdf_url":        None,   # direct PDF URL
            "local_path":     None,   # path relative to data/raw/
            "language":       None,   # ISO 639-1 code, e.g. "en"
            "extracted_text": None,
            "word_count":     None,
            "scraped_at":     None,   # ISO-8601 UTC timestamp
        },
        pk="id",
        ignore=True,          # don't raise on duplicate — use upsert instead
    ) if "publications" not in db.table_names() else None

    # Ensure table exists with correct schema even if DB already had it
    db["publications"].transform(
        types={
            "id":             str,
            "source":         str,
            "title":          str,
            "authors":        str,
            "date":           str,
            "url":            str,
            "pdf_url":        str,
            "local_path":     str,
            "language":       str,
            "extracted_text": str,
            "word_count":     int,
            "scraped_at":     str,
        }
    ) if "publications" in db.table_names() else None


def upsert_publication(record: dict) -> None:
    """Insert or update a publication record. 'id' must be present."""
    if "scraped_at" not in record or not record["scraped_at"]:
        record["scraped_at"] = datetime.now(timezone.utc).isoformat()
    db = get_db()
    db["publications"].upsert(record, pk="id")


def publication_exists(pub_id: str) -> bool:
    db = get_db()
    if "publications" not in db.table_names():
        return False
    try:
        db["publications"].get(pub_id)
        return True
    except sqlite_utils.db.NotFoundError:
        return False


def get_publication(pub_id: str) -> dict | None:
    db = get_db()
    try:
        return dict(db["publications"].get(pub_id))
    except sqlite_utils.db.NotFoundError:
        return None
