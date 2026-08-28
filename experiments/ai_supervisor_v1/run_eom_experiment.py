"""Эксперимент 5: контрольная пара ЭОМ, где детерминированный конвейер работает.

Зачем нужен контроль. На паре АР конвейер выдал 0 фактов и 1720 review-элементов,
и модели дружно отвечают «сравнение неправомерно». Это правильный ответ, но он не
показывает, СПОСОБЕН ли AI-слой разрешать элементы, когда сравнение законно.

Пара p19cd7f695a (ЭОМ, АА_БЭ-03-ДС3-ИОС1.1) — противоположный случай:
Text Fact Producer распознал 320 фактов правилом recognized_electrical_load_table,
синтез выдал 320 полноценных changes и только 12 review-элементов. Эти 12 —
общие указания к чертежу, добавленные в новой редакции: содержательные
изменения, которые электрические правила распознать не умеют.

Именно на них измеряется реальная способность AI-слоя закрывать остаток.
"""
from __future__ import annotations

import dataclasses
import json
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from evidence_package import Run, build_package
from gateway import call_claude, call_codex
from prompts import analyst_prompt
from schema import ANALYST_SCHEMA
from verifier import verify

PROD = ("/home/coder/projects/PDF-proverka/comparison/sessions/"
        "7cccec69bb0b4327/pairs/p19cd7f695a/production")
RESULTS = Path(__file__).parent / "results"

CONFIGS = [
    ("CLAUDE_SESSION", "claude-sonnet-5", None),
    ("CLAUDE_SESSION", "claude-opus-5", None),
    ("CLAUDE_SESSION", "claude-haiku-4-5-20251001", None),
    ("CODEX_SESSION", "gpt-5.6-sol", "low"),
    ("CODEX_SESSION", "gpt-5.6-sol", "medium"),
    ("CODEX_SESSION", "gpt-5.6-sol", "xhigh"),
    ("CODEX_SESSION", "gpt-5.4-mini", "medium"),
]


def call_one(job):
    cfg, qid, pkg = job
    provider, model, effort = cfg
    p = analyst_prompt(pkg)
    if provider == "CLAUDE_SESSION":
        r = call_claude(p, model=model, schema=ANALYST_SCHEMA, effort=effort, timeout_s=420)
    else:
        r = call_codex(p, model=model, schema=ANALYST_SCHEMA, effort=effort, timeout_s=600)
    rec = {"config": f"{provider}|{model}|{effort or '-'}", "question_id": qid,
           "ok": r.ok, "duration_s": round(r.duration_s, 1), "error": r.error[:250],
           "input_tokens": r.usage.get("total_input_tokens")}
    if r.ok and r.parsed:
        rec["response"] = r.parsed
        rec["verifier"] = verify(pkg, r.parsed).as_dict()
    return rec


def main() -> None:
    run = Run(PROD)
    changes = [q for q in run.questions["questions"] if q["category"] == "CHANGE"]
    packages = {q["question_id"]: dataclasses.asdict(build_package(run, q)) for q in changes}
    print(f"контрольная пара ЭОМ: {len(changes)} CHANGE-вопросов × {len(CONFIGS)} конфигураций "
          f"= {len(changes) * len(CONFIGS)} вызовов\n")

    jobs = [(cfg, q["question_id"], packages[q["question_id"]]) for cfg in CONFIGS for q in changes]
    out = []
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=6) as ex:
        for i, rec in enumerate(ex.map(call_one, jobs), 1):
            out.append(rec)
            if i % 15 == 0 or i == len(jobs):
                print(f"  {i}/{len(jobs)} ({time.time() - t0:.0f}s)")

    RESULTS.mkdir(exist_ok=True)
    (RESULTS / "eom_experiment.json").write_text(
        json.dumps({"production_dir": PROD, "packages": packages, "records": out},
                   ensure_ascii=False, indent=1), encoding="utf-8")

    print("\n=== РАЗРЕШЕНИЕ ОСТАТКА НА ЗАКОННОМ СРАВНЕНИИ ===")
    hdr = (f"{'конфигурация':<40}{'схема':>7}{'верифик':>9}{'AI_RES':>8}"
           f"{'годн.реш':>10}{'человек':>9}{'медиана':>9}{'вход,ток':>10}")
    print(hdr); print("-" * len(hdr))
    import statistics
    for cfg in CONFIGS:
        key = f"{cfg[0]}|{cfg[1]}|{cfg[2] or '-'}"
        r = [x for x in out if x["config"] == key]
        ok = [x for x in r if x.get("ok") and x.get("response")]
        ver = [x for x in ok if x.get("verifier", {}).get("ok")]
        res = [x for x in ok if x["response"].get("resolution_status") == "AI_RESOLVED"]
        resv = [x for x in ver if x["response"].get("resolution_status") == "AI_RESOLVED"]
        hum = [x for x in ok if x["response"].get("resolution_status") == "HUMAN_REQUIRED"]
        d = [x["duration_s"] for x in r if x["duration_s"]]
        toks = [x["input_tokens"] for x in r if x.get("input_tokens")]
        n = len(r) or 1
        print(f"{key:<40}{len(ok) / n:>6.0%}{(len(ver) / len(ok) if ok else 0):>9.0%}"
              f"{(len(res) / len(ok) if ok else 0):>8.0%}{len(resv) / n:>10.0%}"
              f"{(len(hum) / len(ok) if ok else 0):>9.0%}"
              f"{(statistics.median(d) if d else 0):>8.1f}s"
              f"{(int(statistics.median(toks)) if toks else 0):>10d}")

    print("\n=== ЧТО ИМЕННО ВЕРНУЛИ МОДЕЛИ (первый вопрос) ===")
    qid = changes[0]["question_id"]
    print(f"AFTER: {packages[qid]['after_value']!r}\n")
    for x in out:
        if x["question_id"] == qid and x.get("response"):
            rsp = x["response"]
            print(f"  {x['config']:<40} {rsp['resolution_status']:<20} dim={rsp['dimension']:<12} "
                  f"out={rsp['outcome']:<16} conf={rsp['confidence']}")
            print(f"      {rsp['engineering_significance'][:110]}")
    print(f"\nзаписано: {RESULTS / 'eom_experiment.json'}")


if __name__ == "__main__":
    main()
