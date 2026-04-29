#!/bin/bash
# Batch retry findings_corrector for KJ projects with paragraph_checks issues
# Triggered after KJ6 retry confirmed done (2026-04-28)

PROJECTS=(
  "13АВ-РД-КЖ5.17-23.1-К2 (1) (1).pdf"
  "13АВ-РД-КЖ5.17-23.2-К2 (Изм.1).pdf"
  "13АВ-РД-КЖ5.30-31.2-К2.pdf"
  "13АВ-РД-КЖ5.39.2-К2.pdf"
)

for PID in "${PROJECTS[@]}"; do
  ENC=$(python3 -c "import urllib.parse; print(urllib.parse.quote('${PID}', safe=''))")
  echo ">>> Queuing: $PID"
  curl -sS -m 10 -X POST "http://localhost:8081/api/audit/${ENC}/retry/findings_corrector"
  echo
  sleep 2
done

echo "Done. All 4 projects queued."
