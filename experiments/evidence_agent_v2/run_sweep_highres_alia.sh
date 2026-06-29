#!/bin/bash
# Полный high-res image-only re-run ВСЕХ графических замечаний Алии (Plan 1c).
# qwen видит только чёткий рендер из PDF (без gemma-OCR). Каждая дисциплина = отдельный
# процесс (resilient). ENABLE_MODEL_LOAD=true (переживает eviction). От малых к большим.
cd /home/coder/projects/PDF-proverka || exit 1
export STAGE_COMPARISON_GRAPHIC_LLM_MAX_CONTINUATIONS=0
export STAGE_COMPARISON_GRAPHIC_LLM_MAX_TOKENS=1200
export STAGE_COMPARISON_GRAPHIC_LLM_TIMEOUT_SEC=60
export EV2_PROMPT=c
RES=experiments/evidence_agent_v2/results/audit_alia
SWEEP_LOG=$RES/sweep_highres_progress.log
echo "=== HIGHRES SWEEP START $(date) ===" > "$SWEEP_LOG"
for D in OV KM EOM SS KJ TX AR; do
  echo "=== [$D] START $(date) ===" >> "$SWEEP_LOG"
  python3 -m experiments.evidence_agent_v2.highres_recheck --mode all --disciplines "$D" \
      >> "$RES/highres_${D}.log" 2>&1
  rc=$?
  echo "=== [$D] DONE rc=$rc $(date) ===" >> "$SWEEP_LOG"
done
# отчёт было/стало по всем накопленным visionB2_*.json
python3 -m experiments.evidence_agent_v2.compare_runs >> "$RES/before_after.log" 2>&1
echo "=== HIGHRES SWEEP ALL DONE $(date) ===" >> "$SWEEP_LOG"
