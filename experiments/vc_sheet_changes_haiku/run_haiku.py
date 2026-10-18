"""A/B «Изменения в листе»: codex gpt-6.1-sol high → Claude-сессия (Haiku 5.5).

Тот же пакет, промпт, схема, проверка и дозапрос контекста (``sheet_changes.run.run_sheet``);
подменяется только исполнитель. Данные — стенд АР1 V16→V17 (только чтение: индекс прогона
пишется в каталог вывода, а не в стенд). Эталон сравнения — задание codex
ai202610170128096927 того же стенда.

    VERSION_COMPARISON_ROOT=<стенд>/vc python experiments/vc_sheet_changes_haiku/run_haiku.py OUT \
        [--model claude-haiku-5-5] [--effort high] [--parallel 10] [--spids a,b]
"""
from __future__ import annotations

import argparse
import json
import re
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from backend.app.services.stage_comparison.ai import gateway
from backend.app.services.version_comparison_ai import executor as ex
from backend.app.services.version_comparison_ai.sheet_changes import run as sc_run

STAND_JOB = ("objects/0b540226/runs/run2026101516030799ee/ai/ai202610170128096927/job.json")


def _cost(text: str | None) -> float:
    m = re.findall(r'"total_cost_usd":\s*([0-9.eE+-]+)', text or "")
    return float(m[-1]) if m else 0.0


class ClaudeDescriber:
    executor_id = "claude_session"

    def __init__(self, model: str, effort: str):
        self.model, self.effort = model, effort

    def describe_runtime(self) -> dict:
        return {"executor": self.executor_id, "model": self.model, "effort": self.effort,
                "account": "подписка Claude (~/.claude)", "transport": "claude -p stream-json"}

    def validate_runtime(self) -> dict:
        return {"ok": True, "problems": [], "checks": {}}

    def run(self, task: ex.DescribeTask, cancel: ex.CancelToken) -> ex.DescribeResult:
        res, wire = gateway.call_claude_multimodal(
            task.prompt, model=self.model, schema=task.schema, reasoning_level=self.effort,
            images=task.image_paths, timeout_s=task.timeout_s, cancel=cancel, run_id=task.marker)
        u = res.usage or {}
        usage = {"input_tokens": int(u.get("total_input_tokens") or 0),
                 "cached_input_tokens": int(u.get("cache_read_input_tokens") or 0),
                 "cache_write_input_tokens": int(u.get("cache_creation_input_tokens") or 0),
                 "output_tokens": int(u.get("output_tokens") or 0),
                 "cost_usd_equiv": _cost(res.raw_excerpt)}
        if task.raw_path:
            p = Path(task.raw_path)
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_text(json.dumps(res.parsed, ensure_ascii=False) if res.parsed else res.raw_excerpt,
                         encoding="utf-8")
            p.with_suffix(".wire.json").write_text(json.dumps(wire, ensure_ascii=False, default=str),
                                                   encoding="utf-8")
        kind = res.error_kind or ""
        kind = {"NO_STRUCTURED_OUTPUT": "INVALID_JSON"}.get(kind, kind)
        return ex.DescribeResult(res.ok, parsed=res.parsed, error_kind=kind, error=res.error or "",
                                 duration_ms=res.duration_ms or 0, usage=usage,
                                 raw_excerpt=res.raw_excerpt or "")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("out")
    ap.add_argument("--model", default="claude-haiku-5-5")
    ap.add_argument("--effort", default="high")
    ap.add_argument("--parallel", type=int, default=10)
    ap.add_argument("--spids", default="")
    ap.add_argument("--repeat", type=int, default=0, help="повторить листы по кругу до N (нагрузочный тест)")
    ap.add_argument("--executor", choices=("claude", "codex"), default="claude")
    ap.add_argument("--codex-bin", default="/home/coder/auditmanager/stands/sheet-changes-ar1-l3/cli/"
                                         "codex-0.162.0-alpha.17.2/codex")
    a = ap.parse_args()
    from backend.app.services.version_comparison import paths
    root = paths.root() if hasattr(paths, "root") else None
    job = json.loads((Path(root or "") / STAND_JOB).read_text(encoding="utf-8")) if root else None
    out = Path(a.out)
    out.mkdir(parents=True, exist_ok=True)
    job = dict(job, job_id=f"{a.executor}-{int(time.time())}", bypass_cache=True)
    spids = a.spids.split(",") if a.spids else job["scope"]["spids"]
    tasks = [(f"{i:03d}_{spids[i % len(spids)]}" if a.repeat else spids[i], spids[i % len(spids)])
             for i in range(a.repeat or len(spids))]
    sc_run.index_add = lambda *args, **kw: None       # стенд только читаем
    describer = (ex.CodexExecDescriber(model=a.model, effort=a.effort, binary=a.codex_bin)
                 if a.executor == "codex" else ClaudeDescriber(a.model, a.effort))
    tok = gateway.CancelToken()
    totals: dict = {}

    def one(t: tuple) -> dict:
        name, spid = t
        usage: dict = {}
        t = time.time()
        try:
            r = sc_run.run_sheet(job["object_id"], job["run_id"], spid, out / "sheets" / name, describer, tok,
                                 usage, job, out / "cache")
            calls = r["result"]["diagnostics"]["calls"]
            return {"spid": name, "state": r["state"], "seconds": round(time.time() - t, 1), "usage": usage,
                    "errors": [c["error_kind"] for c in calls if not c["ok"]],
                    "call_s": [c["seconds"] for c in calls]}
        except Exception as e:  # noqa: BLE001 — эксперимент: фиксируем и идём дальше
            return {"spid": name, "state": "error", "error": repr(e)[:400], "seconds": round(time.time() - t, 1),
                    "usage": usage}

    t0 = time.time()
    with ThreadPoolExecutor(a.parallel) as pool:
        rows = list(pool.map(one, tasks))
    for r in rows:
        for k, v in r["usage"].items():
            if isinstance(v, (int, float)):
                totals[k] = totals.get(k, 0) + v
    summary = {"model": a.model, "effort": a.effort, "parallel": a.parallel, "wall_s": round(time.time() - t0, 1),
               "sheets": rows, "totals": totals}
    (out / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=1), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=1))


if __name__ == "__main__":
    main()
