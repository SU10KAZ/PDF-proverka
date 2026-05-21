#!/usr/bin/env bash
# refresh_reports.sh — rerun scoring and report generation after new
# algorithm outputs land. Idempotent.
set -euo pipefail

cd "$(dirname "$0")/../.."

echo "=== 1. Re-scoring all algorithm results ==="
python algorithm_research/metrics/score_algorithms.py

echo ""
echo "=== 2. Phase 0 / Phase 1 comparison table ==="
python algorithm_research/scripts/build_phase_comparison.py

echo ""
echo "=== 3. Gating criteria evaluation ==="
python algorithm_research/scripts/evaluate_gating.py >/dev/null
echo "Wrote: algorithm_research/reports/_gating_evaluation.md"
echo "Wrote: algorithm_research/reports/_gating_evaluation.json"

echo ""
echo "=== 4. A1-v2 FP audit ==="
python algorithm_research/scripts/audit_a1v2_fp.py

echo ""
echo "=== Summary ==="
echo "Reports refreshed under algorithm_research/reports/"
ls -lh algorithm_research/reports/*.md algorithm_research/reports/*.json 2>/dev/null | awk '{print "  " $NF " (" $5 ")"}'
