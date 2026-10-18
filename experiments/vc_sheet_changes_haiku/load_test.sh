#!/bin/bash
# Нагрузочный тест параллельности codex gpt-6-luna: 10, 15, 20 … до первой ошибки (потолок 50).
OUT=${1:?каталог}; cd /home/coder/projects/PDF-proverka
for N in 10 15 20 25 30 35 40 45 50; do
  PYTHONPATH=. VERSION_COMPARISON_ROOT=/home/coder/auditmanager/stands/sheet-changes-ar1-l3/vc \
    /home/coder/auditmanager/current/venv/bin/python experiments/vc_sheet_changes_haiku/run_haiku.py "$OUT/p$N" \
    --executor codex --model gpt-6-luna --effort high --parallel $N --repeat $N > "$OUT/p$N.log" 2>&1
  python3 - "$OUT/p$N/summary.json" $N <<'PY'
import json,sys,statistics as st
d=json.load(open(sys.argv[1])); r=d["sheets"]
errs=[e for x in r for e in x.get("errors",[])]+[x["state"] for x in r if x["state"]=="error"]
cs=[c for x in r for c in x.get("call_s",[])]
print(f"N={sys.argv[2]} wall={d['wall_s']}s calls={len(cs)} med_call={st.median(cs) if cs else 0:.1f}s max_call={max(cs) if cs else 0:.1f}s errors={errs} states={sorted(set(x['state'] for x in r))}", flush=True)
sys.exit(1 if errs else 0)
PY
  [ $? -ne 0 ] && { echo "СТОП: ошибки на N=$N"; break; }
done
echo КОНЕЦ
