#!/bin/bash
# Production data-root sanity check (read-only).
#
# /api/info == 200 НЕДОСТАТОЧНО: backend может отвечать 200, но читать пустой
# deploy data root вместо MAIN. Тогда исчезают объекты (Alia/Балчуг), projects=0,
# Excel-решения пишутся не туда, Pipeline V2 панели → not_found.
#
# Этот скрипт собирает live-значения (/api/info, /api/objects, /api/projects,
# Pipeline V2 ui-payload) и прогоняет их через детерминированный чек
# evaluate_production_data_roots. Ничего не пишет, моделей/джоб не запускает,
# очередь не трогает.
#
# Usage:
#   PORTAL_TOKEN=<token> ./scripts/check_production_data_roots.sh [BASE_URL]
# BASE_URL по умолчанию http://127.0.0.1:8081
# Exit code: 0 ok | 1 warning | 2 dangerous | 3 unreachable.
set -euo pipefail
BASE_URL="${1:-http://127.0.0.1:8081}"
TOKEN="${PORTAL_TOKEN:-}"
COOKIE=()
[ -n "$TOKEN" ] && COOKIE=(-b "portal_session=$TOKEN")
PAIR_SESSION="${PAIR_SESSION:-ba413a93c5754f6c}"
PAIR_ID="${PAIR_ID:-pf06effb7}"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

info=$(curl -fsS "${COOKIE[@]}" "$BASE_URL/api/info" 2>/dev/null) || {
    echo "DANGEROUS: /api/info unreachable at $BASE_URL"; exit 3; }
objects=$(curl -fsS "${COOKIE[@]}" "$BASE_URL/api/objects" 2>/dev/null || echo "null")
projects=$(curl -fsS "${COOKIE[@]}" "$BASE_URL/api/projects" 2>/dev/null || echo "null")
uipayload=$(curl -fsS "${COOKIE[@]}" \
    "$BASE_URL/api/stage-comparison/pipeline-v2/$PAIR_SESSION/ui-payload?pair_id=$PAIR_ID" \
    2>/dev/null || echo "null")

# Собираем ответы в один JSON-файл (НЕ через argv — /api/projects большой и
# превышает лимит длины аргументов).
TMP_JSON="$(mktemp)"
trap 'rm -f "$TMP_JSON"' EXIT
{
    echo '{'
    printf '"info": %s,\n' "$info"
    printf '"objects": %s,\n' "${objects:-null}"
    printf '"projects": %s,\n' "${projects:-null}"
    printf '"uipayload": %s\n' "${uipayload:-null}"
    echo '}'
} > "$TMP_JSON"

REPO_ROOT="$REPO_ROOT" python3 - "$TMP_JSON" <<'PYEOF'
import json, os, sys
sys.path.insert(0, os.environ.get("REPO_ROOT", "/home/coder/projects/PDF-proverka-deploy"))
from backend.app.services.stage_comparison.production_root_health import (
    evaluate_production_data_roots, STATUS_OK, STATUS_WARNING, STATUS_DANGEROUS)

with open(sys.argv[1], encoding="utf-8") as fh:
    blob = json.load(fh)
info = blob.get("info") or {}

def _count(d):
    if isinstance(d, dict):
        for k in ("objects", "projects", "items"):
            if isinstance(d.get(k), list):
                return len(d[k])
        return None
    return len(d) if isinstance(d, list) else None

objects_count = _count(blob.get("objects"))
projects_count = _count(blob.get("projects"))
uip = blob.get("uipayload")
pv2_ok = (isinstance(uip, dict) and uip.get("status") == "ok"
          and bool(uip.get("available"))) if uip not in (None, "null") else None
data_roots = info.get("data_roots") or {}
comparison_root = data_roots.get("comparison_root")

res = evaluate_production_data_roots(
    objects_count=objects_count, projects_count=projects_count,
    comparison_root=comparison_root, pipeline_v2_artifacts_present=pv2_ok,
    base_dir=info.get("base_dir"))
print(json.dumps(res, ensure_ascii=False, indent=2))
sys.exit({STATUS_OK: 0, STATUS_WARNING: 1, STATUS_DANGEROUS: 2}.get(res["status"], 1))
PYEOF
