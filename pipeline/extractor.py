"""
PDF text extraction and language detection.
Uses PyMuPDF (fitz) for extraction and langdetect for language identification.
Writes results back to the publications table via pipeline.db.
"""

import logging
from pathlib import Path

import fitz                      # PyMuPDF
from langdetect import detect, LangDetectException

from pipeline.db import upsert_publication, get_publication

logger = logging.getLogger(__name__)

# Only attempt language detection if we have enough text
MIN_CHARS_FOR_LANGDETECT = 200


def extract_text(pdf_path: Path) -> str:
    """
    Extract all text from a PDF using PyMuPDF.
    Returns a single string with page breaks as double newlines.
    """
    text_pages = []
    try:
        with fitz.open(pdf_path) as doc:
            for page in doc:
                text_pages.append(page.get_text())
    except Exception as exc:
        logger.warning("PyMuPDF error on %s: %s", pdf_path, exc)
        return ""
    return "\n\n".join(text_pages)


def detect_language(text: str) -> str | None:
    """
    Detect ISO 639-1 language code from text.
    Returns None if detection fails or text is too short.
    """
    sample = text[:3000].strip()
    if len(sample) < MIN_CHARS_FOR_LANGDETECT:
        return None
    try:
        return detect(sample)
    except LangDetectException:
        return None


def process_pdf(pub_id: str, pdf_path: Path) -> dict:
    """
    Extract text from `pdf_path`, detect language, compute word count,
    update the database record for `pub_id`, and return the updated fields.

    Skips extraction if extracted_text is already populated (use force=True
    at the call site by clearing extracted_text before calling).
    """
    record = get_publication(pub_id) or {}

    if record.get("extracted_text"):
        logger.debug("Already extracted, skipping: %s", pub_id)
        return record

    logger.info("Extracting text from: %s", pdf_path)
    text = extract_text(pdf_path)

    language   = detect_language(text) if text else None
    word_count = len(text.split()) if text else 0

    updates = {
        "id":             pub_id,
        "extracted_text": text or None,
        "language":       language,
        "word_count":     word_count,
    }
    upsert_publication(updates)
    logger.info(
        "Extracted %d words, language=%s from %s",
        word_count, language, pdf_path.name,
    )
    return {**record, **updates}


def process_all_pending(source: str | None = None) -> None:
    """
    Process all publications that have a local_path but no extracted_text.
    Optionally filter to a single source.
    """
    from pipeline.db import get_db
    db = get_db()
    if "publications" not in db.table_names():
        logger.info("No publications table found.")
        return

    query = "SELECT id, local_path FROM publications WHERE extracted_text IS NULL AND local_path IS NOT NULL"
    params: list = []
    if source:
        query += " AND source = ?"
        params.append(source)

    rows = list(db.execute(query, params).fetchall())
    logger.info("Processing %d unextracted PDFs", len(rows))

    for pub_id, local_path in rows:
        pdf_path = Path(local_path)
        if not pdf_path.exists():
            logger.warning("File not found: %s", pdf_path)
            continue
        process_pdf(pub_id, pdf_path)
