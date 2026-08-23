"""mine · driver — run mine_screen.py once per version pair in its own process
with a hard wall-clock timeout (a single get_pixmap on a monster CAD page cannot be
interrupted from inside Python).  Parts are concatenated into mine_screen.jsonl.
"""
import json, subprocess, sys, time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

HERE = Path(__file__).resolve().parent
ART = HERE.parents[0] / "artifacts"
TIMEOUT = 240
WORKERS = 4


def run(i):
    t = time.time()
    try:
        r = subprocess.run([sys.executable, str(HERE / "mine_screen.py"), str(i)],
                           capture_output=True, text=True, timeout=TIMEOUT, cwd=str(HERE.parents[2]))
        ok = r.returncode == 0
        tail = (r.stdout or "").strip().splitlines()[-1:] or [(r.stderr or "")[-200:]]
    except subprocess.TimeoutExpired:
        ok, tail = False, ["TIMEOUT"]
    return i, ok, round(time.time() - t, 1), tail[0][:200]


def main():
    pm = json.load(open(ART / "mine_pagematch.json", encoding="utf-8"))
    n = len([r for r in pm["rows"] if "r3" in r])
    print("version pairs:", n, flush=True)
    res = []
    with ThreadPoolExecutor(WORKERS) as ex:
        for i, ok, dt, tail in ex.map(run, range(n)):
            res.append({"i": i, "ok": ok, "s": dt, "tail": tail})
            print(f"[{i}] ok={ok} {dt}s {tail}", flush=True)
    parts = sorted((ART / "mine_screen_parts").glob("*.jsonl"))
    with open(ART / "mine_screen.jsonl", "w", encoding="utf-8") as out:
        for p in parts:
            out.write(p.read_text(encoding="utf-8"))
    agg = {"n_version_pairs": n, "n_ok": sum(1 for r in res if r["ok"]),
           "n_timeout_or_fail": sum(1 for r in res if not r["ok"]),
           "runs": res}
    for f in sorted((ART / "mine_screen_parts").glob("*.summary.json")):
        d = json.loads(f.read_text(encoding="utf-8"))
        for k, v in d.items():
            if isinstance(v, (int, float)):
                agg.setdefault("totals", {})
                agg["totals"][k] = agg["totals"].get(k, 0) + v
    agg["n_rows"] = sum(1 for _ in open(ART / "mine_screen.jsonl", encoding="utf-8"))
    (ART / "mine_screen_summary.json").write_text(json.dumps(
        {"schema_version": "mine_screen/1", "research_only": True, "timeout_s": TIMEOUT,
         "summary": agg}, ensure_ascii=False, indent=1), encoding="utf-8")
    print(json.dumps({k: v for k, v in agg.items() if k != "runs"}, ensure_ascii=False))


if __name__ == "__main__":
    main()
