"""
Chunk extracted publication text and store embeddings in a local Chroma DB.

Embedding model : nomic-embed-text via Ollama (http://localhost:11434)
Vector store    : ChromaDB, persisted to data/chroma/
Languages       : English only (language = 'en')

Speed note: uses Ollama's /api/embed batch endpoint — sends EMBED_BATCH_SIZE
chunks per HTTP call instead of one call per chunk. Benchmarked at ~1.4s/chunk
in batch-20 vs ~3.5s/chunk sequential.

Usage:
    .venv/bin/python analysis/embedder.py --source ecdpm
    .venv/bin/python analysis/embedder.py --source all
    .venv/bin/python analysis/embedder.py --source all --embed-batch 20
"""

from __future__ import annotations

import argparse
import re
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import requests
import chromadb
from tqdm import tqdm

from config import DATA_DIR
from pipeline.db import get_db
from analysis.taxonomy import get_all_themes

# ── Constants ─────────────────────────────────────────────────────────────────

OLLAMA_EMBED_URL = "http://localhost:11434/api/embed"       # batch endpoint
EMBED_MODEL      = "nomic-embed-text"
CHROMA_DIR       = DATA_DIR / "chroma"
COLLECTION       = "publications"

CHUNK_WORDS      = 300    # ~300 words fits nomic-embed-text context comfortably
OVERLAP_WORDS    = 30
MAX_CHUNK_CHARS  = 3800   # hard cap for unusually long sentences

EMBED_BATCH_SIZE = 20     # chunks per /api/embed call (sweet spot from benchmarking)
CHROMA_BATCH     = 200    # chunks per Chroma upsert

# Pre-build flat keyword → theme_id lookup (lowercased)
_KW_INDEX: dict[str, str] = {}
for _theme in get_all_themes():
    for _kw in _theme.keywords:
        _KW_INDEX[_kw.lower()] = _theme.id


# ── Chunking ──────────────────────────────────────────────────────────────────

def _split_sentences(text: str) -> list[str]:
    """Split text into sentences on . ! ? boundaries."""
    parts = re.split(r'(?<=[.!?])\s+', text.strip())
    return [p for p in parts if p]


def chunk_text(
    text: str,
    chunk_words: int = CHUNK_WORDS,
    overlap_words: int = OVERLAP_WORDS,
) -> list[str]:
    """
    Split *text* into chunks of approximately *chunk_words* words with
    *overlap_words* overlap. Splits only on sentence boundaries.
    """
    sentences = _split_sentences(text)
    if not sentences:
        return []

    word_counts = [len(s.split()) for s in sentences]
    chunks: list[str] = []
    start_idx = 0

    while start_idx < len(sentences):
        words_so_far = 0
        end_idx = start_idx

        while end_idx < len(sentences):
            words_so_far += word_counts[end_idx]
            end_idx += 1
            if words_so_far >= chunk_words:
                break

        chunk = " ".join(sentences[start_idx:end_idx])
        if len(chunk) > MAX_CHUNK_CHARS:
            chunk = chunk[:MAX_CHUNK_CHARS].rsplit(" ", 1)[0]
        chunks.append(chunk)

        if end_idx >= len(sentences):
            break

        # Overlap: step back from end_idx until overlap_words covered
        overlap_so_far = 0
        overlap_start = end_idx - 1
        while overlap_start > start_idx and overlap_so_far < overlap_words:
            overlap_start -= 1
            overlap_so_far += word_counts[overlap_start]

        start_idx = max(overlap_start, start_idx + 1)

    return chunks


# ── Theme hints ───────────────────────────────────────────────────────────────

def detect_theme_hints(chunk: str) -> str:
    """Return comma-separated taxonomy keywords found in *chunk* (one per theme)."""
    lower = chunk.lower()
    matched: list[str] = []
    seen_themes: set[str] = set()
    for kw, theme_id in _KW_INDEX.items():
        if kw in lower and theme_id not in seen_themes:
            matched.append(kw)
            seen_themes.add(theme_id)
    return ", ".join(matched)


# ── Ollama batch embedding ────────────────────────────────────────────────────

def _embed_single(text: str) -> list[float]:
    """Fallback: embed one text via /api/embed (wraps in list)."""
    resp = requests.post(
        OLLAMA_EMBED_URL,
        json={"model": EMBED_MODEL, "input": [text]},
        timeout=60,
    )
    resp.raise_for_status()
    return resp.json()["embeddings"][0]


def embed_batch(texts: list[str], embed_batch_size: int = EMBED_BATCH_SIZE) -> list[list[float]]:
    """
    Embed *texts* using Ollama's /api/embed batch endpoint.
    Splits into sub-batches of *embed_batch_size*.
    On 4xx/5xx, falls back to half batch size, then single-item requests.
    Returns embeddings in the same order as *texts*.
    """
    all_embeddings: list[list[float]] = []
    for i in range(0, len(texts), embed_batch_size):
        batch = texts[i : i + embed_batch_size]
        sent = False
        for attempt, attempt_batch_size in enumerate([len(batch), max(1, len(batch) // 2), 1]):
            try:
                if attempt > 0:
                    time.sleep(3)   # brief pause before retry
                sub_embeddings: list[list[float]] = []
                for j in range(0, len(batch), attempt_batch_size):
                    sub = batch[j : j + attempt_batch_size]
                    resp = requests.post(
                        OLLAMA_EMBED_URL,
                        json={"model": EMBED_MODEL, "input": sub},
                        timeout=120,
                    )
                    resp.raise_for_status()
                    sub_embeddings.extend(resp.json()["embeddings"])
                all_embeddings.extend(sub_embeddings)
                sent = True
                break
            except requests.HTTPError as exc:
                if attempt_batch_size == 1:
                    # Persistent failure — skip with zero vector to preserve alignment
                    import logging as _logging
                    _logging.getLogger(__name__).warning(
                        "Persistent 400 at offset %d (batch=%d) — skipping with zero vectors: %s",
                        i, len(batch), exc,
                    )
                    dim = 768  # nomic-embed-text output dimension
                    all_embeddings.extend([[0.0] * dim] * len(batch))
                    sent = True
                    break
                # Retry with smaller sub-batch
                continue
        if not sent:
            raise RuntimeError(f"Failed to embed batch at offset {i}")
    return all_embeddings


# ── Main pipeline ─────────────────────────────────────────────────────────────

def run(
    source: str = "all",
    embed_batch_size: int = EMBED_BATCH_SIZE,
    chroma_batch: int = CHROMA_BATCH,
) -> None:
    CHROMA_DIR.mkdir(parents=True, exist_ok=True)

    client     = chromadb.PersistentClient(path=str(CHROMA_DIR))
    collection = client.get_or_create_collection(
        name=COLLECTION,
        metadata={"hnsw:space": "cosine"},
    )

    db = get_db()

    if source == "all":
        rows = list(db.execute(
            "SELECT id, source, title, date, language, extracted_text "
            "FROM publications "
            "WHERE language = 'en' AND extracted_text IS NOT NULL",
        ).fetchall())
    else:
        rows = list(db.execute(
            "SELECT id, source, title, date, language, extracted_text "
            "FROM publications "
            "WHERE language = 'en' AND extracted_text IS NOT NULL AND source = ?",
            [source],
        ).fetchall())

    print(f"Found {len(rows)} English publications for source='{source}'")

    # Identify already-embedded pub IDs from Chroma metadata
    existing_ids: set[str] = set()
    try:
        existing_meta = collection.get(include=["metadatas"])
        for meta in existing_meta["metadatas"]:
            existing_ids.add(meta["publication_id"])
    except Exception:
        pass

    to_process = [r for r in rows if r[0] not in existing_ids]
    print(f"  Already embedded : {len(existing_ids)}")
    print(f"  To embed         : {len(to_process)}")

    if not to_process:
        print("Nothing to do.")
        return

    t0               = time.monotonic()
    total_chunks     = 0
    total_pubs_done  = 0

    # Chroma upsert accumulator
    buf_ids:        list[str]         = []
    buf_embeddings: list[list[float]] = []
    buf_metadatas:  list[dict]        = []
    buf_documents:  list[str]         = []

    def flush_chroma() -> None:
        if not buf_ids:
            return
        collection.upsert(
            ids=buf_ids,
            embeddings=buf_embeddings,
            metadatas=buf_metadatas,
            documents=buf_documents,
        )
        buf_ids.clear()
        buf_embeddings.clear()
        buf_metadatas.clear()
        buf_documents.clear()

    with tqdm(to_process, unit="pub", desc="Embedding") as pbar:
        for pub_id, pub_source, title, date, language, extracted_text in pbar:
            pbar.set_postfix(src=pub_source, chunks=total_chunks)

            chunks = chunk_text(extracted_text or "")
            if not chunks:
                continue

            # Small pause between pubs so Ollama finishes unloading the previous batch
            if total_pubs_done > 0:
                time.sleep(2)

            # Embed all chunks for this pub in batches
            embeddings = embed_batch(chunks, embed_batch_size=embed_batch_size)

            meta_base = {
                "publication_id": pub_id,
                "source":         pub_source or "",
                "title":          (title or "")[:512],
                "date":           date or "",
                "language":       language or "en",
            }

            for i, (chunk, embedding) in enumerate(zip(chunks, embeddings)):
                buf_ids.append(f"{pub_id}_{i}")
                buf_embeddings.append(embedding)
                buf_metadatas.append({**meta_base, "theme_hints": detect_theme_hints(chunk)})
                buf_documents.append(chunk)
                total_chunks += 1

                if len(buf_ids) >= chroma_batch:
                    flush_chroma()

            total_pubs_done += 1

    flush_chroma()

    elapsed = time.monotonic() - t0

    print()
    print("=" * 55)
    print(f"Embedding complete  (source={source})")
    print(f"  Publications embedded : {total_pubs_done}")
    print(f"  Chunks created        : {total_chunks}")
    print(f"  Time taken            : {elapsed:.1f}s  ({elapsed/60:.1f} min)")
    if total_pubs_done:
        print(f"  Avg chunks/pub        : {total_chunks/total_pubs_done:.1f}")
        print(f"  Avg time/pub          : {elapsed/total_pubs_done:.1f}s")
        print(f"  Avg time/chunk        : {elapsed/total_chunks:.2f}s")
    print(f"  Embed batch size      : {embed_batch_size}")
    print(f"  Chroma collection     : {COLLECTION}")
    print(f"  Chroma path           : {CHROMA_DIR}")
    print(f"  Total vectors in DB   : {collection.count()}")
    print("=" * 55)


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Embed publications into ChromaDB using nomic-embed-text via Ollama"
    )
    parser.add_argument(
        "--source", default="all", metavar="SOURCE",
        help="Source key to embed (e.g. ecdpm, saiia) or 'all'",
    )
    parser.add_argument(
        "--embed-batch", type=int, default=EMBED_BATCH_SIZE, metavar="N",
        help=f"Chunks per /api/embed call (default {EMBED_BATCH_SIZE})",
    )
    parser.add_argument(
        "--chroma-batch", type=int, default=CHROMA_BATCH, metavar="N",
        help=f"Chunks per Chroma upsert (default {CHROMA_BATCH})",
    )
    args = parser.parse_args()
    run(
        source=args.source,
        embed_batch_size=args.embed_batch,
        chroma_batch=args.chroma_batch,
    )
