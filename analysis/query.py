"""
Semantic search and gap analysis over the think-tank publication Chroma DB.

Usage:
    .venv/bin/python analysis/query.py --search "carbon border adjustment Africa"
    .venv/bin/python analysis/query.py --search "AfCFTA" --source ecdpm --n 5
    .venv/bin/python analysis/query.py --gap-analysis
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import requests
import chromadb

from analysis.embedder import OLLAMA_EMBED_URL, EMBED_MODEL, CHROMA_DIR, COLLECTION
from analysis.taxonomy import get_all_themes
from config import SOURCES
from pipeline.db import get_publication

# ── Chroma client (lazy) ──────────────────────────────────────────────────────

_collection = None


def _get_collection():
    global _collection
    if _collection is None:
        client = chromadb.PersistentClient(path=str(CHROMA_DIR))
        _collection = client.get_collection(name=COLLECTION)
    return _collection


# ── Embedding ─────────────────────────────────────────────────────────────────

def _embed_query(text: str) -> list[float]:
    """Embed a single query string via Ollama /api/embed."""
    try:
        resp = requests.post(
            OLLAMA_EMBED_URL,
            json={"model": EMBED_MODEL, "input": [text]},
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json()["embeddings"][0]
    except requests.exceptions.ConnectionError:
        sys.exit(
            f"ERROR: Cannot reach Ollama at {OLLAMA_EMBED_URL}. "
            "Ensure Ollama is running: `ollama serve`"
        )
    except requests.exceptions.Timeout:
        sys.exit(
            f"ERROR: Ollama request timed out. "
            "Ollama may be overloaded or still loading the model."
        )
    except requests.HTTPError as exc:
        sys.exit(f"ERROR: Ollama returned HTTP error: {exc}")


# ── Public API ────────────────────────────────────────────────────────────────

def search(
    query_text: str,
    n_results: int = 10,
    source_filter: str | None = None,
) -> list[dict]:
    """
    Semantic search over embedded publication chunks.

    Returns up to *n_results* results, deduplicated to at most 2 chunks per
    publication. Each result dict has:
        title, source, date, url, excerpt, score, theme_hints
    where score is cosine similarity (0–1, higher = more relevant).
    """
    vec = _embed_query(query_text)
    collection = _get_collection()

    fetch_n = min(n_results * 5, 200)
    where = {"source": source_filter} if source_filter else None

    raw = collection.query(
        query_embeddings=[vec],
        n_results=fetch_n,
        where=where,
        include=["documents", "metadatas", "distances"],
    )

    docs      = raw["documents"][0]
    metadatas = raw["metadatas"][0]
    distances = raw["distances"][0]

    results: list[dict] = []
    pub_count: dict[str, int] = {}

    for doc, meta, dist in zip(docs, metadatas, distances):
        pub_id = meta["publication_id"]
        if pub_count.get(pub_id, 0) >= 2:
            continue
        pub_count[pub_id] = pub_count.get(pub_id, 0) + 1

        pub_row = get_publication(pub_id)
        url = pub_row["url"] if pub_row else None

        results.append({
            "title":       meta.get("title", ""),
            "source":      meta.get("source", ""),
            "date":        meta.get("date", ""),
            "url":         url,
            "excerpt":     doc,
            "score":       round(1.0 - dist, 4),
            "theme_hints": meta.get("theme_hints", ""),
        })

        if len(results) >= n_results:
            break

    return results


def gap_analysis() -> dict:
    """
    Count unique publications per (theme, source) cell using theme_hints metadata.

    Returns:
        matrix       — {theme_id: {source: publication_count}}
        gaps         — [(theme_label, source)] where count == 0
        near_gaps    — [(theme_label, source)] where count in {1, 2}
        coverage_pct — {theme_id: float} percentage of sources with count > 0
    """
    collection = _get_collection()

    print("Fetching all chunk metadata from Chroma…", flush=True)
    metadatas: list[dict] = []
    page_size = 5_000
    offset    = 0
    while True:
        page = collection.get(
            include=["metadatas"],
            limit=page_size,
            offset=offset,
        )
        batch = page["metadatas"]
        if not batch:
            break
        metadatas.extend(batch)
        offset += len(batch)
        print(f"  …{offset:,} chunks fetched", flush=True)
        if len(batch) < page_size:
            break
    print(f"  {len(metadatas):,} chunks total.", flush=True)

    themes = get_all_themes()

    # keyword → theme_id lookup (same pattern as embedder._KW_INDEX)
    kw_to_theme_id: dict[str, str] = {}
    for theme in themes:
        for kw in theme.keywords:
            kw_to_theme_id[kw.lower()] = theme.id

    theme_id_to_label = {t.id: t.label for t in themes}

    # Accumulate unique pub_ids per (theme_id, source)
    pub_sets: dict[tuple[str, str], set[str]] = {}

    for meta in metadatas:
        pub_id      = meta.get("publication_id", "")
        source      = meta.get("source", "")
        hints_str   = meta.get("theme_hints", "")

        if not hints_str:
            continue

        for kw in hints_str.split(", "):
            theme_id = kw_to_theme_id.get(kw.lower())
            if theme_id:
                key = (theme_id, source)
                if key not in pub_sets:
                    pub_sets[key] = set()
                pub_sets[key].add(pub_id)

    # Build matrix
    source_keys = list(SOURCES.keys())
    matrix: dict[str, dict[str, int]] = {}
    for theme in themes:
        matrix[theme.id] = {
            s: len(pub_sets.get((theme.id, s), set()))
            for s in source_keys
        }

    # Derive gaps, near_gaps, coverage_pct
    gaps:         list[tuple[str, str]] = []
    near_gaps:    list[tuple[str, str]] = []
    coverage_pct: dict[str, float]      = {}

    for theme in themes:
        nonzero = sum(1 for s in source_keys if matrix[theme.id][s] > 0)
        coverage_pct[theme.id] = round(nonzero / len(source_keys) * 100, 1)
        for s in source_keys:
            count = matrix[theme.id][s]
            if count == 0:
                gaps.append((theme.label, s))
            elif count <= 2:
                near_gaps.append((theme.label, s))

    return {
        "matrix":       matrix,
        "pub_sets":     pub_sets,   # {(theme_id, source): set of pub_ids}
        "gaps":         gaps,
        "near_gaps":    near_gaps,
        "coverage_pct": coverage_pct,
    }


# ── JSON Export ──────────────────────────────────────────────────────────────

def export_gap_json(gap_result: dict, out_path: Path) -> None:
    """
    Write gap analysis results as a self-contained JSON file for the heatmap.
    """
    themes      = get_all_themes()
    source_keys = list(SOURCES.keys())

    total_chunks = _get_collection().count()

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "total_chunks":  total_chunks,
        "sources": [
            {"key": k, "name": SOURCES[k]["name"]}
            for k in source_keys
        ],
        "themes": [
            {
                "id":          t.id,
                "label":       t.label,
                "direction":   t.direction,
                "description": t.description,
                "coverage":    gap_result["matrix"][t.id],
                "coverage_pct": gap_result["coverage_pct"][t.id],
            }
            for t in themes
        ],
        "summary": {
            "gaps":      len(gap_result["gaps"]),
            "near_gaps": len(gap_result["near_gaps"]),
            "gap_list":  [{"theme": g[0], "source": g[1]} for g in gap_result["gaps"]],
        },
    }

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False))
    print(f"Exported gap analysis → {out_path}")


# ── Theme file export ─────────────────────────────────────────────────────────

def _truncate_at_sentence(text: str, max_chars: int = 400) -> str:
    text = text.strip().replace("\n", " ")
    if len(text) <= max_chars:
        return text
    truncated = text[:max_chars]
    last_period = max(truncated.rfind(". "), truncated.rfind(".\n"))
    if last_period > max_chars // 2:
        return truncated[:last_period + 1].strip()
    return truncated.strip() + "…"


def _best_excerpt(chunks: list[dict], theme_keywords: set[str]) -> str:
    """Return the chunk text with the most keyword hits for this theme."""
    def score(c: dict) -> int:
        text = c["text"].lower()
        return sum(1 for kw in theme_keywords if kw in text)
    best = max(chunks, key=score, default=None)
    if not best:
        return ""
    return _truncate_at_sentence(best["text"])


def export_theme_files(gap_result: dict, out_dir: Path) -> None:
    """
    Write one JSON file per theme to out_dir/themes/{theme_id}.json.

    Each file contains per-source publication lists with titles, dates, URLs,
    authors, and a representative text excerpt for that theme.
    """
    from pipeline.db import get_db  # local import to avoid circular issues

    themes      = get_all_themes()
    source_keys = list(SOURCES.keys())
    pub_sets    = gap_result["pub_sets"]
    collection  = _get_collection()

    theme_keywords: dict[str, set[str]] = {
        t.id: {kw.lower() for kw in t.keywords} for t in themes
    }

    themes_dir = out_dir / "themes"
    themes_dir.mkdir(parents=True, exist_ok=True)

    db = get_db()

    for theme in themes:
        print(f"  Exporting theme: {theme.label}", flush=True)

        # Collect all pub_ids that appear in this theme (across all sources)
        all_pub_ids: set[str] = set()
        for src in source_keys:
            all_pub_ids |= pub_sets.get((theme.id, src), set())

        if not all_pub_ids:
            out_path = themes_dir / f"{theme.id}.json"
            out_path.write_text(json.dumps(
                {"id": theme.id, "label": theme.label, "sources": {}}, indent=2
            ))
            continue

        # ── Batch-fetch SQLite metadata ──────────────────────────────────────
        pub_ids_list = list(all_pub_ids)
        placeholders = ",".join("?" * len(pub_ids_list))
        rows = db.execute(
            f"SELECT id, title, date, url, authors FROM publications WHERE id IN ({placeholders})",
            pub_ids_list,
        ).fetchall()
        pub_meta: dict[str, dict] = {
            r[0]: {"title": r[1] or "", "date": r[2] or "", "url": r[3] or "", "authors": r[4] or ""}
            for r in rows
        }

        # ── Batch-fetch Chroma chunks for excerpt selection ──────────────────
        # Fetch 100 pub_ids at a time using $in filter
        pub_chunks: dict[str, list[dict]] = {pid: [] for pid in pub_ids_list}
        BATCH = 100
        for i in range(0, len(pub_ids_list), BATCH):
            batch_ids = pub_ids_list[i : i + BATCH]
            try:
                chroma_res = collection.get(
                    where={"publication_id": {"$in": batch_ids}},
                    include=["documents", "metadatas"],
                )
                for doc, meta in zip(chroma_res["documents"], chroma_res["metadatas"]):
                    pid = meta.get("publication_id", "")
                    if pid in pub_chunks:
                        pub_chunks[pid].append({"text": doc, "meta": meta})
            except Exception:
                pass  # if a batch fails, continue without excerpts for those pubs

        # ── Build per-source publication lists ───────────────────────────────
        sources_out: dict[str, list[dict]] = {}
        kw_set = theme_keywords[theme.id]

        for src in source_keys:
            src_pub_ids = pub_sets.get((theme.id, src), set())
            if not src_pub_ids:
                continue
            pubs = []
            for pid in src_pub_ids:
                meta = pub_meta.get(pid, {})
                excerpt = _best_excerpt(pub_chunks.get(pid, []), kw_set)
                pubs.append({
                    "id":      pid,
                    "title":   meta.get("title", ""),
                    "date":    meta.get("date", ""),
                    "url":     meta.get("url", ""),
                    "authors": meta.get("authors", ""),
                    "excerpt": excerpt,
                })
            # Sort by date descending, empty dates last
            pubs.sort(key=lambda p: p["date"] or "0000", reverse=True)
            sources_out[src] = pubs

        out_path = themes_dir / f"{theme.id}.json"
        out_path.write_text(json.dumps(
            {"id": theme.id, "label": theme.label, "sources": sources_out},
            indent=2, ensure_ascii=False,
        ))
        print(f"    → {out_path} ({sum(len(v) for v in sources_out.values())} pubs)", flush=True)

    print(f"Theme files written to {themes_dir}/")


# ── Formatting ────────────────────────────────────────────────────────────────

_SOURCE_ABBREV = {
    "ecdpm":         "ECDPM",
    "saiia":         "SAIIA",
    "tips":          "TIPS",
    "acet":          "ACET",
    "odi":           "ODI",
    "policy_center": "PolCtr",
}

_COL_W   = 7
_LABEL_W = 38


def _format_gap_table(gap_result: dict) -> str:
    themes      = get_all_themes()
    source_keys = list(SOURCES.keys())
    matrix      = gap_result["matrix"]

    header = f"{'Theme':<{_LABEL_W}}" + "".join(
        f"{_SOURCE_ABBREV.get(s, s):>{_COL_W}}" for s in source_keys
    )
    sep = "─" * len(header)

    rows = [sep, header, sep]
    for theme in themes:
        row = f"{theme.label:<{_LABEL_W}}"
        for s in source_keys:
            count = matrix[theme.id][s]
            cell  = f"{count}*" if count == 0 else str(count)
            row  += f"{cell:>{_COL_W}}"
        rows.append(row)

    rows.append(sep)
    rows.append(f"\n* = gap (0 publications)  |  "
                f"Gaps: {len(gap_result['gaps'])}  |  "
                f"Near-gaps (1–2): {len(gap_result['near_gaps'])}")

    rows.append("\nCoverage by theme (% of sources with ≥1 publication):")
    for theme in themes:
        pct = gap_result["coverage_pct"][theme.id]
        bar = "█" * int(pct / 10)
        rows.append(f"  {theme.label:<{_LABEL_W - 2}} {pct:5.1f}%  [{bar:<10}]")

    return "\n".join(rows)


# ── CLI ───────────────────────────────────────────────────────────────────────

def _print_search_results(results: list[dict], query: str) -> None:
    if not results:
        print("No results found.")
        return

    print(f'\nTop {len(results)} results for: "{query}"\n')
    for i, r in enumerate(results, 1):
        print(f"{i:2}. [{r['score']:.3f}]  {r['title']}")
        print(f"      Source : {r['source']}  |  Date: {r['date'] or 'n/a'}")
        print(f"      URL    : {r['url'] or 'N/A'}")
        if r["theme_hints"]:
            print(f"      Themes : {r['theme_hints']}")
        excerpt = r["excerpt"][:220].strip().replace("\n", " ")
        print(f"      Excerpt: {excerpt}…")
        print()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Semantic search and gap analysis for think-tank publications"
    )

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--search", metavar="QUERY", help="Natural-language search query")
    group.add_argument("--gap-analysis", action="store_true",
                       help="Print theme × source coverage matrix")

    parser.add_argument("--source", metavar="SOURCE",
                        choices=list(SOURCES.keys()),
                        help="Filter search to a single source")
    parser.add_argument("--n", type=int, default=10, metavar="N",
                        help="Number of search results (default: 10)")
    parser.add_argument("--json-out", metavar="PATH",
                        help="(gap-analysis only) Write results as JSON to this path")
    parser.add_argument("--theme-files", metavar="DIR",
                        help="(gap-analysis only) Write per-theme JSON files to DIR/themes/")

    args = parser.parse_args()

    if args.search:
        hits = search(args.search, n_results=args.n, source_filter=args.source)
        _print_search_results(hits, args.search)
    elif args.gap_analysis:
        result = gap_analysis()
        print()
        print(_format_gap_table(result))
        if args.json_out:
            export_gap_json(result, Path(args.json_out))
        if args.theme_files:
            print("\nExporting per-theme files…")
            export_theme_files(result, Path(args.theme_files))
