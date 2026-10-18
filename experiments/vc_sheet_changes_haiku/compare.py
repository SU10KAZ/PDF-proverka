"""Сводка Sol vs остальные (аргументы ИМЯ=каталог_вывода) по листам: время, токены, состояние, изменения/вопросы."""
import json, sys
from pathlib import Path
SOL = Path("/home/coder/auditmanager/stands/sheet-changes-ar1-l3/vc/objects/0b540226/runs/run2026101516030799ee/ai/ai202610170128096927/sheets")
RUNS = [("SOL", SOL)] + [(a.split("=")[0], Path(a.split("=")[1]) / "sheets") for a in sys.argv[1:]]
def load(d):
    f = d / "sheet_changes.json"
    return json.loads(f.read_text()) if f.exists() else None
def agg(r):
    c = r["diagnostics"]["calls"]
    return dict(calls=len(c), sec=round(sum(x["seconds"] for x in c),1), total=r["diagnostics"]["total_s"],
                inp=sum(x["usage"].get("input_tokens",0) for x in c), cached=sum(x["usage"].get("cached_input_tokens",0) for x in c),
                out=sum(x["usage"].get("output_tokens",0) for x in c), ok=[x["ok"] for x in c])
T = {n.lower(): {} for n, _ in RUNS}
for sd in sorted(SOL.iterdir()):
    s = load(sd)
    print("=" * 100); print(sd.name, s["title"] if s else "")
    for name, d in RUNS:
        r = load(d / sd.name)
        if not r: print(f"  {name}: нет результата"); continue
        a = agg(r)
        for k in ("calls","sec","inp","cached","out"): T[name.lower()][k] = T[name.lower()].get(k,0) + a[k]
        print(f"  {name}: {r['state']} | изм {len(r['changes'])} | вопр {len(r['unresolved'])} | искл {len(r.get('excluded',[]))} | {a}")
        for ch in r["changes"]: print("     -", ch["summary"][:260])
        for u in r["unresolved"]: print("     ?", (u.get("question") or u.get("summary") or "")[:260])
print("ИТОГО", json.dumps(T, ensure_ascii=False))
