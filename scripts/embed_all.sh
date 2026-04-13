#!/usr/bin/env bash
# embed_all.sh — run the embedder for all sources in sequence.
#
# Usage:
#   bash scripts/embed_all.sh
#   nohup bash scripts/embed_all.sh > logs/embed_all.log 2>&1 &

set -uo pipefail

PYTHON=".venv/bin/python"
EMBEDDER="analysis/embedder.py"
SOURCES=(ecdpm saiia tips acet odi policy_center)

succeeded=()
failed=()
t_total_start=$(date +%s)

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"
}

for source in "${SOURCES[@]}"; do
    log "━━━ Starting: $source ━━━"
    t_start=$(date +%s)

    "$PYTHON" "$EMBEDDER" --source "$source" \
        > "logs/embedder_${source}.log" 2>&1
    exit_code=$?

    t_end=$(date +%s)
    elapsed=$(( t_end - t_start ))

    if [ "$exit_code" -eq 0 ]; then
        log "✓ $source — done in ${elapsed}s"
        succeeded+=("$source")
    else
        log "✗ $source — FAILED (exit $exit_code) after ${elapsed}s — see logs/embedder_${source}.log"
        failed+=("$source")
    fi
done

t_total_end=$(date +%s)
total=$(( t_total_end - t_total_start ))
mins=$(( total / 60 ))
secs=$(( total % 60 ))

echo ""
echo "════════════════════════════════════════"
echo "  Embedding run complete"
echo "  Total time : ${mins}m ${secs}s"
echo "  Succeeded  : ${#succeeded[@]} — ${succeeded[*]:-none}"
echo "  Failed     : ${#failed[@]} — ${failed[*]:-none}"
echo "════════════════════════════════════════"

[ "${#failed[@]}" -eq 0 ] && exit 0 || exit 1
