#!/bin/bash
# Clean-deploy launcher. Runs the CODE from THIS worktree
# (/home/coder/projects/PDF-proverka-deploy) and redirects ALL runtime DATA roots
# back to MAIN (/home/coder/projects/PDF-proverka), where the single source of
# truth lives: 3 objects (213/214/272), ~109 audit projects, the findings /
# knowledge_base / decisions_log, and the full stage-comparison data (~5.4G,
# session ba413a93c5754f6c with 22 pairs incl. its pipeline_v2 set).
#
# 2026-06-14: redirects restored to MAIN after an incident. They had been flipped
# to the deploy worktree to keep ONE Pipeline V2 panel
# (controlled_enforce_preflight_report for pair pf06effb7) alive, but the deploy
# worktree only holds 1 object + empty projects, so the flip made objects 214
# (Alia) and 272 (Балчуг) and all audit projects disappear from the portal. MAIN
# is authoritative; the deploy-only enforce-preflight smoke artifact can be
# re-run on MAIN if that panel is needed.
set -e
MAIN_DIR="/home/coder/projects/PDF-proverka"
export AUDIT_DATA_DIR="$MAIN_DIR"
export AUDIT_APP_DATA_DIR="$MAIN_DIR/backend/app/data"
export COMPARISON_ROOT="$MAIN_DIR/comparison"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec "$SCRIPT_DIR/start_server.sh" "$@"
