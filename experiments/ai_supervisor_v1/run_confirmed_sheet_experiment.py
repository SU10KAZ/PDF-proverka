"""Эксперимент 6: тот же вход, но сопоставление листов подтверждено.

Во всех предыдущих замерах модели дружно отвечали HUMAN_REQUIRED с причиной
SHEET_RELATION_WRONG. Это правильный ответ: в пакете доказательств честно
написано «статус POSSIBLE, уверенность 0,22-0,71». Но такой результат не
отвечает на вопрос, ради которого затевался AI-слой: способен ли он закрывать
элементы, когда сравнение законно.

Здесь единственное отличие — в пакет подставлено подтверждённое отношение
листов (как если бы его дал детерминированный матчер по штампу). Всё
остальное — тот же промпт, та же схема, тот же верификатор.

Разница между прогонами и есть цена неопределённости листов.
"""
from __future__ import annotations

import copy
import dataclasses
import json
import statistics
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

EOM = ("/home/coder/projects/PDF-proverka/comparison/sessions/"
       "7cccec69bb0b4327/pairs/p19cd7f695a/production")
RESULTS = Path(__file__).parent / "results"

CONFIGS = [
    ("CLAUDE_SESSION", "claude-sonnet-5", None),
    ("CLAUDE_SESSION", "claude-opus-5", None),
    ("CODEX_SESSION", "gpt-5.6-sol", "low"),
    ("CODEX_SESSION", "gpt-5.6-sol", "medium"),
    ("CODEX_SESSION", "gpt-5.6-sol", "xhigh"),
]


def confirm_sheet(pkg: dict) -> dict:
    """Подставить подтверждённое отношение листов, ничего больше не трогая."""
    out = copy.deepcopy(pkg)
    rel = out.setdefault("sheet_relation", {})
    rel["status"] = "CONFIRMED"
    rel["confidence"] = 1.0
    rel["relation_type"] = "MATCHED"
    rel["evidence"] = [{"feature": "stamp_identity", "score": 1.0,
                        "note": "лист идентифицирован по строке штампа, совпадение точное"}]
    return out


def call_one(job):
    cfg, qid, pkg, arm = job
    provider, model, effort = cfg
    p = analyst_prompt(pkg)
    if provider == "CLAUDE_SESSION":
        r = call_claude(p, model=model, schema=ANALYST_SCHEMA, effort=effort, timeout_s=420)
    else:
        r = call_codex(p, model=model, schema=ANALYST_SCHEMA, effort=effort, timeout_s=600)
    rec = {"config": f"{provider}|{model}|{effort or '-'}", "arm": arm, "question_id": qid,
           "ok": r.ok, "duration_s": round(r.duration_s, 1), "error": r.error[:200]}
    if r.ok and r.parsed:
        rec["response"] = r.parsed
        rec["verifier"] = verify(pkg, r.parsed).as_dict()
    return rec


def main() -> None:
    run = Run(EOM)
    changes = [q for q in run.questions["questions"] if q["category"] == "CHANGE"]
    base = {q["question_id"]: dataclasses.asdict(build_package(run, q)) for q in changes}
    conf = {k: confirm_sheet(v) for k, v in base.items()}

    jobs = ([(c, q["question_id"], base[q["question_id"]], "POSSIBLE") for c in CONFIGS for q in changes]
            + [(c, q["question_id"], conf[q["question_id"]], "CONFIRMED") for c in CONFIGS for q in changes])
    print(f"{len(changes)} вопросов × {len(CONFIGS)} конфигураций × 2 плеча = {len(jobs)} вызовов\n")

    out, t0 = [], time.time()
    with ThreadPoolExecutor(max_workers=6) as ex:
        for i, rec in enumerate(ex.map(call_one, jobs), 1):
            out.append(rec)
            if i % 20 == 0 or i == len(jobs):
                print(f"  {i}/{len(jobs)} ({time.time() - t0:.0f}s)")

    RESULTS.mkdir(exist_ok=True)
    (RESULTS / "confirmed_sheet_experiment.json").write_text(
        json.dumps({"packages_base": base, "records": out}, ensure_ascii=False, indent=1),
        encoding="utf-8")

    print("\n=== ЦЕНА НЕОПРЕДЕЛЁННОСТИ ЛИСТОВ ===")
    hdr = (f"{'конфигурация':<34}{'плечо':<11}{'верифик':>9}{'AI_RES':>8}"
           f"{'годн.реш':>10}{'человек':>9}{'медиана':>9}")
    print(hdr); print("-" * len(hdr))
    for cfg in CONFIGS:
        key = f"{cfg[0]}|{cfg[1]}|{cfg[2] or '-'}"
        for arm in ("POSSIBLE", "CONFIRMED"):
            r = [x for x in out if x["config"] == key and x["arm"] == arm]
            ok = [x for x in r if x.get("ok") and x.get("response")]
            ver = [x for x in ok if x.get("verifier", {}).get("ok")]
            res = [x for x in ok if x["response"].get("resolution_status") == "AI_RESOLVED"]
            resv = [x for x in ver if x["response"].get("resolution_status") == "AI_RESOLVED"]
            hum = [x for x in ok if x["response"].get("resolution_status") == "HUMAN_REQUIRED"]
            d = [x["duration_s"] for x in r if x["duration_s"]]
            n = len(r) or 1
            print(f"{key if arm == 'POSSIBLE' else '':<34}{arm:<11}"
                  f"{(len(ver) / len(ok) if ok else 0):>9.0%}"
                  f"{(len(res) / len(ok) if ok else 0):>8.0%}{len(resv) / n:>10.0%}"
                  f"{(len(hum) / len(ok) if ok else 0):>9.0%}"
                  f"{(statistics.median(d) if d else 0):>8.1f}s")

    print("\n=== ИЗМЕРЕНИЯ, ПРИСВОЕННЫЕ ПРИ ПОДТВЕРЖДЁННЫХ ЛИСТАХ ===")
    import collections
    for cfg in CONFIGS:
        key = f"{cfg[0]}|{cfg[1]}|{cfg[2] or '-'}"
        c = collections.Counter(
            x["response"]["dimension"] for x in out
            if x["config"] == key and x["arm"] == "CONFIRMED" and x.get("response")
            and x["response"]["resolution_status"] == "AI_RESOLVED")
        print(f"  {key:<34}{dict(c) or '—'}")

    print(f"\nзаписано: {RESULTS / 'confirmed_sheet_experiment.json'}")


if __name__ == "__main__":
    main()
