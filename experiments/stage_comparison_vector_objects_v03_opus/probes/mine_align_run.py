import json, subprocess, sys, time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
HERE = Path(__file__).resolve().parent
ART = HERE.parents[0] / "artifacts"
TIMEOUT = 600
WORKERS = 4

def run(i):
    t = time.time()
    try:
        r = subprocess.run([sys.executable, str(HERE / "mine_align.py"), str(i)],
                           capture_output=True, text=True, timeout=TIMEOUT, cwd=str(HERE.parents[2]))
        return i, r.returncode == 0, round(time.time() - t, 1), (r.stdout or r.stderr or "")[-160:].strip()
    except subprocess.TimeoutExpired:
        return i, False, round(time.time() - t, 1), "TIMEOUT"

def main():
    rows = [json.loads(l) for l in open(ART / "mine_screen.jsonl", encoding="utf-8")]
    keys = sorted({(r["doc_id"], r["ver_a"], r["ver_b"]) for r in rows})
    print("groups", len(keys), flush=True)
    res = []
    with ThreadPoolExecutor(WORKERS) as ex:
        for i, ok, dt, tail in ex.map(run, range(len(keys))):
            res.append({"i": i, "ok": ok, "s": dt})
            print(f"[{i}] ok={ok} {dt}s {tail}", flush=True)
    with open(ART / "mine_align.jsonl", "w", encoding="utf-8") as out:
        for p in sorted((ART / "mine_align_parts").glob("*.jsonl")):
            out.write(p.read_text(encoding="utf-8"))
    n = sum(1 for _ in open(ART / "mine_align.jsonl", encoding="utf-8"))
    (ART / "mine_align_summary.json").write_text(json.dumps(
        {"schema_version": "mine_align/1", "research_only": True, "groups": len(keys),
         "n_rows": n, "n_fail": sum(1 for r in res if not r["ok"]), "runs": res},
        ensure_ascii=False, indent=1), encoding="utf-8")
    print("rows", n)

if __name__ == "__main__":
    main()
