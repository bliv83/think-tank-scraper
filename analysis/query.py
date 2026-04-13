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


def semantic_gap_analysis(
    similarity_threshold: float = 0.55,
    min_chunks: int = 2,
) -> dict:
    """
    Measure *substantive* thematic engagement using semantic similarity.

    A publication counts only if ≥ min_chunks of its text chunks score
    ≥ similarity_threshold cosine similarity to the theme description.

    Cell values include both absolute count and relative % of each source's
    total output, so large and small think tanks are fairly comparable.

    Returns:
        matrix         — {theme_id: {source: {count, pct, total}}}
        pub_sets       — {(theme_id, source): set of pub_ids}
        theme_vecs     — {theme_id: embedding vector} for excerpt selection
        source_totals  — {source: total_english_publications}
        gaps / near_gaps / coverage_pct — same as gap_analysis()
        params         — {similarity_threshold, min_chunks}
    """
    from pipeline.db import get_db

    themes      = get_all_themes()
    source_keys = list(SOURCES.keys())
    collection  = _get_collection()
    db          = get_db()

    # ── Total English publications per source (denominator) ──────────────────
    source_totals: dict[str, int] = {}
    for src in source_keys:
        row = db.execute(
            "SELECT COUNT(*) FROM publications WHERE source = ? AND language = 'en'",
            [src],
        ).fetchone()
        source_totals[src] = row[0] if row else 0
    print(f"Source totals (English pubs): { {k: v for k, v in source_totals.items()} }", flush=True)

    # ── Embed theme descriptions ─────────────────────────────────────────────
    print("Embedding theme descriptions…", flush=True)
    theme_vecs: dict[str, list[float]] = {}
    for theme in themes:
        theme_vecs[theme.id] = _embed_query(theme.description)
    print(f"  {len(theme_vecs)} theme embeddings done.", flush=True)

    # ── Per-theme × per-source semantic chunk counting ───────────────────────
    # pub_chunk_hits: {(theme_id, source): {pub_id: n_chunks_above_threshold}}
    pub_chunk_hits: dict[tuple[str, str], dict[str, int]] = {}

    # Chroma returns at most n_results docs; 30k safely covers any single source.
    # If n_results > available docs, Chroma returns all available.
    MAX_PER_SRC = 30_000

    for theme in themes:
        print(f"  [{theme.id}] querying…", flush=True)
        vec = theme_vecs[theme.id]
        dist_cutoff = 1.0 - similarity_threshold   # cosine distance ≤ this

        for src in source_keys:
            key = (theme.id, src)
            pub_chunk_hits[key] = {}
            try:
                res = collection.query(
                    query_embeddings=[vec],
                    n_results=MAX_PER_SRC,
                    where={"source": src},
                    include=["metadatas", "distances"],
                )
                for meta, dist in zip(res["metadatas"][0], res["distances"][0]):
                    if dist > dist_cutoff:
                        break  # sorted ascending; everything after is below threshold
                    pid = meta.get("publication_id", "")
                    if pid:
                        pub_chunk_hits[key][pid] = pub_chunk_hits[key].get(pid, 0) + 1
            except Exception as exc:
                print(f"    Warning: {theme.id}/{src} query failed: {exc}", flush=True)

    # ── Apply min_chunks filter → pub_sets ───────────────────────────────────
    pub_sets: dict[tuple[str, str], set[str]] = {
        key: {pid for pid, cnt in hits.items() if cnt >= min_chunks}
        for key, hits in pub_chunk_hits.items()
    }

    # ── Build matrix with count + relative % ─────────────────────────────────
    matrix: dict[str, dict[str, dict]] = {}
    for theme in themes:
        matrix[theme.id] = {}
        for src in source_keys:
            count = len(pub_sets.get((theme.id, src), set()))
            total = source_totals.get(src, 1)
            matrix[theme.id][src] = {
                "count": count,
                "pct":   round(count / total * 100, 1) if total else 0.0,
                "total": total,
            }

    # ── Gaps, near_gaps, coverage_pct ────────────────────────────────────────
    gaps:         list[tuple[str, str]] = []
    near_gaps:    list[tuple[str, str]] = []
    coverage_pct: dict[str, float]      = {}

    for theme in themes:
        nonzero = sum(1 for s in source_keys if matrix[theme.id][s]["count"] > 0)
        coverage_pct[theme.id] = round(nonzero / len(source_keys) * 100, 1)
        for s in source_keys:
            count = matrix[theme.id][s]["count"]
            if count == 0:
                gaps.append((theme.label, s))
            elif count <= 2:
                near_gaps.append((theme.label, s))

    return {
        "matrix":        matrix,
        "pub_sets":      pub_sets,
        "theme_vecs":    theme_vecs,
        "source_totals": source_totals,
        "gaps":          gaps,
        "near_gaps":     near_gaps,
        "coverage_pct":  coverage_pct,
        "params": {
            "similarity_threshold": similarity_threshold,
            "min_chunks":           min_chunks,
        },
    }


# ── JSON Export ──────────────────────────────────────────────────────────────

def export_gap_json(gap_result: dict, out_path: Path) -> None:
    """
    Write gap analysis results as a self-contained JSON file for the heatmap.
    Handles both keyword-based (count-only) and semantic (count+pct+total) matrices.
    """
    themes      = get_all_themes()
    source_keys = list(SOURCES.keys())
    total_chunks = _get_collection().count()

    # Detect matrix format: semantic has dict values, keyword has int values
    sample_val = next(iter(next(iter(gap_result["matrix"].values())).values()), None)
    is_semantic = isinstance(sample_val, dict)

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "total_chunks":  total_chunks,
        "semantic":      is_semantic,
        "sources": [
            {
                "key":   k,
                "name":  SOURCES[k]["name"],
                "total": gap_result.get("source_totals", {}).get(k),
            }
            for k in source_keys
        ],
        "themes": [
            {
                "id":           t.id,
                "label":        t.label,
                "direction":    t.direction,
                "description":  t.description,
                "coverage":     gap_result["matrix"][t.id],
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

    if is_semantic:
        payload["methodology"] = {
            "similarity_threshold": gap_result["params"]["similarity_threshold"],
            "min_chunks":           gap_result["params"]["min_chunks"],
            "description": (
                f"A publication is counted as substantively engaging with a theme "
                f"if ≥{gap_result['params']['min_chunks']} of its text chunks score "
                f"≥{gap_result['params']['similarity_threshold']} cosine similarity "
                f"to the theme description. Cell percentages are computed against each "
                f"think tank's total English-language publication count."
            ),
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
        # If theme_vecs available: use semantic query (returns chunks ranked by
        # similarity to theme — first chunk per pub = best excerpt).
        # Otherwise: unranked get() + keyword scoring fallback.
        theme_vec = gap_result.get("theme_vecs", {}).get(theme.id)
        best_excerpts: dict[str, str] = {}   # pid → best excerpt text
        kw_set = theme_keywords[theme.id]

        BATCH = 100
        for i in range(0, len(pub_ids_list), BATCH):
            batch_ids = pub_ids_list[i : i + BATCH]
            try:
                if theme_vec is not None:
                    # Semantic: ranked by similarity, first hit per pub is best
                    chroma_res = collection.query(
                        query_embeddings=[theme_vec],
                        n_results=min(len(batch_ids) * 5, 500),
                        where={"publication_id": {"$in": batch_ids}},
                        include=["documents", "metadatas"],
                    )
                    docs_iter  = chroma_res["documents"][0]
                    metas_iter = chroma_res["metadatas"][0]
                else:
                    # Keyword fallback: unranked
                    chroma_res = collection.get(
                        where={"publication_id": {"$in": batch_ids}},
                        include=["documents", "metadatas"],
                    )
                    docs_iter  = chroma_res["documents"]
                    metas_iter = chroma_res["metadatas"]

                all_chunks: dict[str, list[str]] = {}
                for doc, meta in zip(docs_iter, metas_iter):
                    pid = meta.get("publication_id", "")
                    if pid not in all_chunks:
                        all_chunks[pid] = []
                    all_chunks[pid].append(doc)

                for pid, chunks in all_chunks.items():
                    if pid not in best_excerpts:
                        if theme_vec is not None:
                            # First chunk is already the most semantically relevant
                            best_excerpts[pid] = _truncate_at_sentence(chunks[0]) if chunks else ""
                        else:
                            best_excerpts[pid] = _best_excerpt(
                                [{"text": c} for c in chunks], kw_set
                            )
            except Exception:
                pass

        # ── Build per-source publication lists ───────────────────────────────
        sources_out: dict[str, list[dict]] = {}

        for src in source_keys:
            src_pub_ids = pub_sets.get((theme.id, src), set())
            if not src_pub_ids:
                continue
            pubs = []
            for pid in src_pub_ids:
                meta = pub_meta.get(pid, {})
                excerpt = best_excerpts.get(pid, "")
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

    # Detect semantic vs keyword matrix
    sample_val = next(iter(next(iter(matrix.values())).values()), None)
    is_semantic = isinstance(sample_val, dict)

    col_w = 8 if is_semantic else _COL_W
    header = f"{'Theme':<{_LABEL_W}}" + "".join(
        f"{_SOURCE_ABBREV.get(s, s):>{col_w}}" for s in source_keys
    )
    sep = "─" * len(header)

    mode_label = " (% of each source's total output)" if is_semantic else ""
    rows = [sep, header + mode_label, sep]

    for theme in themes:
        row = f"{theme.label:<{_LABEL_W}}"
        for s in source_keys:
            val = matrix[theme.id][s]
            if is_semantic:
                pct = val["pct"]
                cell = "0%*" if pct == 0 else (f"{pct:.0f}%" if pct >= 10 else f"{pct:.1f}%")
            else:
                count = val
                cell  = f"{count}*" if count == 0 else str(count)
            row += f"{cell:>{col_w}}"
        rows.append(row)

    rows.append(sep)
    rows.append(f"\n* = gap (0)  |  Gaps: {len(gap_result['gaps'])}  |  "
                f"Near-gaps: {len(gap_result['near_gaps'])}")

    if is_semantic:
        p = gap_result["params"]
        rows.append(f"Methodology: ≥{p['min_chunks']} chunks at ≥{p['similarity_threshold']} "
                    f"cosine similarity to theme description")
        rows.append("\nAbsolute counts (substantive pubs / total English pubs):")
        for theme in themes:
            parts = []
            for s in source_keys:
                v = matrix[theme.id][s]
                parts.append(f"{_SOURCE_ABBREV.get(s,s)} {v['count']}/{v['total']}")
            rows.append(f"  {theme.label:<{_LABEL_W - 2}}  {' | '.join(parts)}")
    else:
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
    parser.add_argument("--threshold", type=float, default=0.55, metavar="T",
                        help="Semantic similarity threshold (default: 0.55)")
    parser.add_argument("--min-chunks", type=int, default=2, metavar="N",
                        help="Min chunks above threshold to count as substantive (default: 2)")
    parser.add_argument("--keyword-only", action="store_true",
                        help="Use keyword-based gap analysis instead of semantic")

    args = parser.parse_args()

    if args.search:
        hits = search(args.search, n_results=args.n, source_filter=args.source)
        _print_search_results(hits, args.search)
    elif args.gap_analysis:
        if args.keyword_only:
            result = gap_analysis()
        else:
            print(f"Semantic gap analysis  threshold={args.threshold}  min_chunks={args.min_chunks}")
            result = semantic_gap_analysis(
                similarity_threshold=args.threshold,
                min_chunks=args.min_chunks,
            )
        print()
        print(_format_gap_table(result))
        if args.json_out:
            export_gap_json(result, Path(args.json_out))
        if args.theme_files:
            print("\nExporting per-theme files…")
            export_theme_files(result, Path(args.theme_files))
