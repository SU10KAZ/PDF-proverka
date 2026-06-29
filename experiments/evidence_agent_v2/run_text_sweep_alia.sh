#!/bin/bash
# Цепочка: дождаться графического sweep (по PID) → прогнать ТЕКСТОВЫЕ замечания qwen3.6-35b →
# отчёт. Одна ночь, один ngrok (без контеншена с Plan 1). От малых дисциплин к большим.
cd /home/coder/projects/PDF-proverka || exit 1
RES=experiments/evidence_agent_v2/results/audit_alia
PROG=$RES/text_sweep_progress.log
GPID=$(cat $RES/sweep_highres.pid 2>/dev/null)
echo "=== TEXT SWEEP: жду графический sweep PID=$GPID $(date) ===" > "$PROG"
while [ -n "$GPID" ] && kill -0 "$GPID" 2>/dev/null; do sleep 60; done
echo "=== графика готова, ТЕКСТ START $(date) ===" >> "$PROG"
for D in OV TX SS EOM KM KJ AR; do
  echo "=== [$D] START $(date) ===" >> "$PROG"
  python3 -m experiments.evidence_agent_v2.text_recheck --mode all --disciplines "$D" \
      >> "$RES/text_${D}.log" 2>&1
  echo "=== [$D] DONE rc=$? $(date) ===" >> "$PROG"
done
python3 -m experiments.evidence_agent_v2.report_text >> "$RES/text_report.log" 2>&1
echo "=== TEXT SWEEP ALL DONE $(date) ===" >> "$PROG"
