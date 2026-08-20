#!/bin/bash
# Полный reason-aware vision-аудит остальных дисциплин Алии (от малых к большим).
# Каждая дисциплина — отдельный процесс (resilient). ENABLE_MODEL_LOAD=true (переживает eviction).
cd /home/coder/projects/PDF-proverka || exit 1
export EVIDENCE_LOCAL_VISION_MAX_CONTINUATIONS=0
export EVIDENCE_LOCAL_VISION_MAX_TOKENS=1500
export EVIDENCE_LOCAL_VISION_TIMEOUT_SEC=60
export EV2_PROMPT=c
SWEEP_LOG=experiments/evidence_agent_v2/results/audit_alia/sweep_progress.log
echo "=== SWEEP START $(date) ===" > "$SWEEP_LOG"
for D in OV KM EOM SS KJ AR; do
  echo "=== [$D] START $(date) ===" >> "$SWEEP_LOG"
  python3 -m experiments.evidence_agent_v2.run_audit --phase vision --discipline "$D" \
      >> experiments/evidence_agent_v2/results/audit_alia/vision_${D}.log 2>&1
  rc=$?
  echo "=== [$D] DONE rc=$rc $(date) ===" >> "$SWEEP_LOG"
done
# финальный отчёт по всем накопленным кандидатам
python3 -m experiments.evidence_agent_v2.run_audit --phase report \
    >> experiments/evidence_agent_v2/results/audit_alia/sweep_report.log 2>&1
echo "=== SWEEP ALL DONE $(date) ===" >> "$SWEEP_LOG"
