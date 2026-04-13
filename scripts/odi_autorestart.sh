#!/usr/bin/env bash
# odi_autorestart.sh — run scrapers/odi.py, restarting automatically when
# Cloudflare expires or the scraper crashes mid-run.
#
# Resume logic (in priority order):
#   1. "Restart with --start-page N to resume." line  → CF expiry
#   2. Last "Fetching listing page N / M" line         → crash mid-page
#   3. Falls back to START_PAGE (passed as $1 or 1)
#
# Usage:
#   bash scripts/odi_autorestart.sh [start_page]
#   bash scripts/odi_autorestart.sh 44 > logs/odi_autorestart.log 2>&1 &

set -uo pipefail

PYTHON=".venv/bin/python"
SCRAPER="scrapers/odi.py"
RUN_LOG="logs/odi_run.log"    # per-run log (overwritten each restart)

START_PAGE="${1:-1}"

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"
}

# Priority 1: explicit resume hint from CF-expiry abort message
# "Restart with --start-page N to resume."
get_cf_resume_page() {
    grep -oE "\-\-start-page [0-9]+" "$RUN_LOG" 2>/dev/null \
        | tail -1 | grep -oE "[0-9]+" || true
}

# Priority 2: last listing page seen (for crash/timeout mid-run)
# "Fetching listing page N / M" or "Fetching listing page N (with..."
get_last_listing_page() {
    grep -oE "Fetching listing page [0-9]+" "$RUN_LOG" 2>/dev/null \
        | tail -1 | grep -oE "[0-9]+" || true
}

log "ODI auto-restart loop starting (initial start_page=$START_PAGE)"

while true; do
    log "Starting ODI scraper --start-page $START_PAGE  (log: $RUN_LOG)"

    $PYTHON "$SCRAPER" --start-page "$START_PAGE" > "$RUN_LOG" 2>&1 || true
    EXIT_CODE=$?

    log "Scraper exited (code=$EXIT_CODE)"

    # ── Clean finish ──────────────────────────────────────────────────────
    if grep -qE "Reached cutoff|Reached last page" "$RUN_LOG" 2>/dev/null; then
        log "Clean finish — stopping."
        exit 0
    fi

    # ── CF re-challenge (consecutive empty pages) ─────────────────────────
    CF_PAGE=$(get_cf_resume_page)
    if [ -n "$CF_PAGE" ]; then
        log "Cloudflare re-challenge — resuming from page $CF_PAGE"
        START_PAGE="$CF_PAGE"
        log "Waiting 30s for CF to settle..."
        sleep 30
        continue
    fi

    # ── Crash / timeout — resume from last listing page seen ─────────────
    LAST_PAGE=$(get_last_listing_page)
    if [ -n "$LAST_PAGE" ] && [ "$LAST_PAGE" -ge "$START_PAGE" ]; then
        log "Crash/timeout — last confirmed listing page was $LAST_PAGE, resuming there"
        START_PAGE="$LAST_PAGE"
    else
        log "Crash/timeout — retrying same page $START_PAGE"
    fi

    log "Waiting 15s before restart..."
    sleep 15
done
