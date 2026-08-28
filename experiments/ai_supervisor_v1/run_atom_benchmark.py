"""Эксперимент 2: разрешение атомов моделями на одной и той же выборке.

Один и тот же пакет доказательств, одна и та же схема, один и тот же
детерминированный верификатор для всех конфигураций. Иначе сравниваются не
модели, а условия.

Только чтение. Результаты в results/atom_benchmark.json.
"""
from __future__ import annotations

import dataclasses
import json
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from evidence_package import Run, build_package
from gateway import call_claude, call_codex
from prompts import analyst_prompt, PROMPT_VERSION
from sample import build_sample
from schema import ANALYST_SCHEMA
from verifier import verify

PROD = ("/home/coder/projects/PDF-proverka/comparison/sessions/"
        "121d764109184c13/pairs/p16b108b9f5/production")
RESULTS = Path(__file__).parent / "results"

CONFIGS = [
    ("CLAUDE_SESSION", "claude-sonnet-5", None),
    ("CLAUDE_SESSION", "claude-opus-5", None),
    ("CODEX_SESSION", "gpt-5.6-sol", "low"),
    ("CODEX_SESSION", "gpt-5.6-sol", "medium"),
    ("CODEX_SESSION", "gpt-5.6-sol", "xhigh"),
]

MAX_PARALLEL = 6


def call_one(cfg, prompt):
    provider, model, effort = cfg
    if provider == "CLAUDE_SESSION":
        return call_claude(prompt, model=model, schema=ANALYST_SCHEMA,
                           effort=effort, timeout_s=420)
    return call_codex(prompt, model=model, schema=ANALYST_SCHEMA,
                      effort=effort, timeout_s=600)


def main(per_cell: int = 3) -> None:
    run = Run(PROD)
    qs = {q["question_id"]: q for q in run.questions["questions"]}
    sample = build_sample(run, per_cell=per_cell)
    print(f"выборка: {len(sample)} случаев, {len(CONFIGS)} конфигураций "
          f"= {len(sample) * len(CONFIGS)} вызовов\n")

    packages = {}
    for s in sample:
        pkg = dataclasses.asdict(build_package(run, qs[s["question_id"]]))
        packages[s["question_id"]] = pkg

    jobs = [(cfg, s) for cfg in CONFIGS for s in sample]
    out: list[dict] = []
    done = 0
    t0 = time.time()

    with ThreadPoolExecutor(max_workers=MAX_PARALLEL) as ex:
        futs = {}
        for cfg, s in jobs:
            pkg = packages[s["question_id"]]
            futs[ex.submit(call_one, cfg, analyst_prompt(pkg))] = (cfg, s)
        for fut in as_completed(futs):
            cfg, s = futs[fut]
            try:
                r = fut.result()
            except Exception as exc:  # изолируем сбой одного вызова
                r = None
                err = repr(exc)
            done += 1
            key = f"{cfg[0]}|{cfg[1]}|{cfg[2] or '-'}"
            rec = {
                "config": key, "question_id": s["question_id"], "stratum": s["stratum"],
            }
            if r is None:
                rec |= {"ok": False, "error": err, "duration_s": 0.0}
            else:
                rec |= {
                    "ok": r.ok, "duration_s": round(r.duration_s, 1),
                    "error": r.error[:300], "response": r.parsed,
                }
                if r.ok and r.parsed:
                    v = verify(packages[s["question_id"]], r.parsed)
                    rec["verifier"] = v.as_dict()
            out.append(rec)
            if done % 20 == 0 or done == len(jobs):
                print(f"  {done}/{len(jobs)}  ({time.time() - t0:.0f}s)")

    RESULTS.mkdir(exist_ok=True)
    path = RESULTS / "atom_benchmark.json"
    path.write_text(json.dumps({
        "prompt_version": PROMPT_VERSION,
        "schema_version": "analyst.v1",
        "sample_size": len(sample),
        "configs": [f"{c[0]}|{c[1]}|{c[2] or '-'}" for c in CONFIGS],
        "packages": packages,
        "records": out,
    }, ensure_ascii=False, indent=1), encoding="utf-8")
    print(f"\nзаписано: {path}  ({path.stat().st_size // 1024} КБ)")
    print(f"общее время: {time.time() - t0:.0f}s")


if __name__ == "__main__":
    main(per_cell=int(sys.argv[1]) if len(sys.argv) > 1 else 3)
