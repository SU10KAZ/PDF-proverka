"""Чистая проверка параллельности codex: N обращений одновременно по готовым пакетам, без подготовки.

Пакеты (prompt.md + images/) берутся из прогона load_test.sh; все N потоков стартуют по барьеру.
Ответ проверяется только на разбор JSON по схеме — проверка листа здесь не нужна.

    python experiments/vc_sheet_changes_haiku/burst_codex.py <каталог с sheets/*/packet> OUT --n 10,20,30,40,50
"""
import argparse
import json
import statistics as st
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from backend.app.services.stage_comparison.ai import gateway
from backend.app.services.version_comparison_ai import executor as ex
from backend.app.services.version_comparison_ai.sheet_changes import schema as sc

CLI = "/home/coder/auditmanager/shared/codex-cli/codex-0.162.0-alpha.17.2/codex"


def packets(src: Path) -> list[tuple[str, list[str]]]:
    out = []
    for p in sorted(src.glob("sheets/*/packet")):
        man = json.loads((p / "packet.json").read_text(encoding="utf-8"))
        out.append(((p / "prompt.md").read_text(encoding="utf-8"), [str(p / i["file"]) for i in man["images"]]))
    return out


def burst(n: int, pk: list, out: Path, model: str, effort: str) -> dict:
    d = ex.CodexExecDescriber(model=model, effort=effort, binary=CLI)
    bar = threading.Barrier(n)
    live = {"now": 0, "peak": 0}
    lock = threading.Lock()

    def one(i: int) -> dict:
        prompt, imgs = pk[i % len(pk)]
        task = ex.DescribeTask(prompt=prompt, schema=sc.SHEET_CHANGES_SCHEMA, image_paths=imgs, timeout_s=1800,
                               marker=f"burst{n}-{i}", raw_path=str(out / f"n{n}" / f"{i:03d}.raw.txt"))
        bar.wait()
        with lock:
            live["now"] += 1
            live["peak"] = max(live["peak"], live["now"])
        t = time.time()
        r = d.run(task, gateway.CancelToken())
        with lock:
            live["now"] -= 1
        return {"i": i, "ok": r.ok, "kind": r.error_kind, "err": (r.error or "")[:200], "s": round(time.time() - t, 1),
                "out": int((r.usage or {}).get("output_tokens") or 0), "tail": "" if r.ok else r.raw_excerpt[-400:]}

    t0 = time.time()
    with ThreadPoolExecutor(n) as pool:
        rows = list(pool.map(one, range(n)))
    s = [r["s"] for r in rows if r["ok"]]
    res = {"n": n, "wall_s": round(time.time() - t0, 1), "ok": sum(r["ok"] for r in rows),
           "errors": [(r["kind"], r["err"]) for r in rows if not r["ok"]], "peak": live["peak"],
           "med_s": round(st.median(s), 1) if s else None, "max_s": max(s) if s else None, "rows": rows}
    (out / f"n{n}.json").write_text(json.dumps(res, ensure_ascii=False, indent=1), encoding="utf-8")
    return res


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("src")
    ap.add_argument("out")
    ap.add_argument("--n", default="10,20,30,40,50")
    ap.add_argument("--model", default="gpt-6-luna")
    ap.add_argument("--effort", default="high")
    a = ap.parse_args()
    out = Path(a.out)
    out.mkdir(parents=True, exist_ok=True)
    pk = packets(Path(a.src))
    print(f"пакетов: {len(pk)}", flush=True)
    for n in map(int, a.n.split(",")):
        r = burst(n, pk, out, a.model, a.effort)
        print(f"N={n} пик={r['peak']} ok={r['ok']}/{n} wall={r['wall_s']}s med={r['med_s']}s max={r['max_s']}s "
              f"errors={r['errors'][:3]}", flush=True)
        if r["errors"]:
            print(f"СТОП: ошибки на N={n}", flush=True)
            break


if __name__ == "__main__":
    main()
