import sys, os, json, time
sys.path.insert(0,'/home/coder/projects/PDF-proverka')
os.chdir('/home/coder/projects/PDF-proverka')
# Enable fallback for this run
os.environ["STAGE_COMPARISON_ENRICHED_COMPARE_ENABLED"]="true"
os.environ["STAGE_COMPARISON_ENRICHED_COMPARE_MODEL"]="opus"
os.environ["STAGE_COMPARISON_EVIDENCE_FIRST_FALLBACK_ENABLED"]="true"
from backend.app.services.stage_comparison import enriched_comparison as ec
SID="ba413a93c5754f6c"; PID="p2ef68719"
t0=time.time()
print(f"[{time.strftime('%H:%M:%S')}] starting shadow run KR2 (fallback enabled)...", flush=True)
res=ec.run_enriched_comparison(SID,PID,force=True)
dt=time.time()-t0
print(f"[{time.strftime('%H:%M:%S')}] DONE in {dt:.0f}s", flush=True)
print("status:",res.get("status"),"strategy:",res.get("strategy"),"fallback:",res.get("fallback"))
print("changes:",len(res.get("changes") or []))
d=res.get("diagnostics") or {}
print("diag:",json.dumps({k:d.get(k) for k in ("deterministic_changes","llm_changes_raw","llm_changes_dropped_ungrounded","duplicates_removed","final_changes")},ensure_ascii=False))
json.dump(res,open('/tmp/kr2-research/shadow_result.json','w'),ensure_ascii=False,indent=2)
print("saved /tmp/kr2-research/shadow_result.json")
