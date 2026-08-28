"""Эксперимент 4: перекрёстный критик (§20-21).

Проверяем, снижает ли критик другой семьёй моделей коррелированные ошибки:
  A) Claude аналитик  -> Codex критик
  B) Codex аналитик   -> Claude критик
  C) Claude аналитик  -> Claude критик  (контроль: та же семья)
  D) Codex аналитик   -> Codex критик   (контроль)

Вход критика: пакет доказательств + предложение аналитика + вердикт
детерминированного верификатора. Критик обязан искать ошибку, а не соглашаться.

Только чтение.
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
from prompts import analyst_prompt, critic_prompt, CRITIC_VERSION
from sample import build_sample
from schema import ANALYST_SCHEMA, CRITIC_SCHEMA
from verifier import verify

PROD = ("/home/coder/projects/PDF-proverka/comparison/sessions/"
        "121d764109184c13/pairs/p16b108b9f5/production")
RESULTS = Path(__file__).parent / "results"

ANALYSTS = {
    "claude": lambda p: call_claude(p, model="claude-sonnet-5", schema=ANALYST_SCHEMA, timeout_s=420),
    "codex": lambda p: call_codex(p, model="gpt-5.6-sol", schema=ANALYST_SCHEMA,
                                  effort="medium", timeout_s=600),
}
CRITICS = {
    "claude": lambda p: call_claude(p, model="claude-opus-5", schema=CRITIC_SCHEMA, timeout_s=420),
    "codex": lambda p: call_codex(p, model="gpt-5.6-sol", schema=CRITIC_SCHEMA,
                                  effort="high", timeout_s=600),
}
PAIRS = [("claude", "codex"), ("codex", "claude"), ("claude", "claude"), ("codex", "codex")]


def one_chain(args):
    analyst_name, critic_name, qid, pkg = args
    a = ANALYSTS[analyst_name](analyst_prompt(pkg))
    if not (a.ok and a.parsed):
        return {"analyst": analyst_name, "critic": critic_name, "question_id": qid,
                "analyst_ok": False, "error": a.error[:200]}
    v = verify(pkg, a.parsed)
    c = CRITICS[critic_name](critic_prompt(pkg, a.parsed, v.as_dict()))
    return {
        "analyst": analyst_name, "critic": critic_name, "question_id": qid,
        "analyst_ok": True,
        "proposal": a.parsed, "verifier": v.as_dict(),
        "analyst_s": round(a.duration_s, 1),
        "critic_ok": c.ok, "critic_response": c.parsed if c.ok else None,
        "critic_error": c.error[:200] if not c.ok else "",
        "critic_s": round(c.duration_s, 1),
    }


def main(n_cases: int = 12) -> None:
    run = Run(PROD)
    qs = {q["question_id"]: q for q in run.questions["questions"]}
    sample = build_sample(run, per_cell=1)[:n_cases]
    packages = {s["question_id"]: dataclasses.asdict(build_package(run, qs[s["question_id"]]))
                for s in sample}
    print(f"случаев: {len(sample)}, цепочек: {len(PAIRS)} -> {len(sample) * len(PAIRS)} пар вызовов\n")

    jobs = [(a, c, s["question_id"], packages[s["question_id"]])
            for a, c in PAIRS for s in sample]
    out = []
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=6) as ex:
        for i, rec in enumerate(ex.map(one_chain, jobs), 1):
            out.append(rec)
            if i % 8 == 0 or i == len(jobs):
                print(f"  {i}/{len(jobs)} ({time.time() - t0:.0f}s)")

    RESULTS.mkdir(exist_ok=True)
    (RESULTS / "critic_experiment.json").write_text(
        json.dumps({"critic_version": CRITIC_VERSION, "records": out},
                   ensure_ascii=False, indent=1), encoding="utf-8")

    print("\n=== РЕЗУЛЬТАТ ===")
    hdr = f"{'аналитик -> критик':<24}{'n':>4}{'ACCEPT':>9}{'HUMAN':>8}{'RETRY':>8}{'нашёл проблем':>15}{'сек':>7}"
    print(hdr)
    print("-" * len(hdr))
    for a, c in PAIRS:
        r = [x for x in out if x["analyst"] == a and x["critic"] == c and x.get("critic_response")]
        if not r:
            print(f"{a + ' -> ' + c:<24}{0:>4}  (нет ответов)")
            continue
        v = [x["critic_response"]["verdict"] for x in r]
        probs = sum(1 for x in r
                    if any(p.get("code") != "NONE" for p in x["critic_response"].get("problems", [])))
        dur = sum(x["analyst_s"] + x["critic_s"] for x in r) / len(r)
        print(f"{a + ' -> ' + c:<24}{len(r):>4}{v.count('ACCEPT'):>9}"
              f"{v.count('HUMAN_REQUIRED'):>8}{v.count('RETRY'):>8}{probs:>15}{dur:>6.0f}s")

    print("\n=== КОДЫ ПРОБЛЕМ, НАЙДЕННЫХ КРИТИКАМИ ===")
    import collections
    for a, c in PAIRS:
        cnt = collections.Counter()
        for x in out:
            if x["analyst"] == a and x["critic"] == c and x.get("critic_response"):
                for p in x["critic_response"].get("problems", []):
                    cnt[p.get("code")] += 1
        if cnt:
            print(f"  {a} -> {c}: " + ", ".join(f"{k}={v}" for k, v in cnt.most_common()))
    print(f"\nзаписано: {RESULTS / 'critic_experiment.json'}")


if __name__ == "__main__":
    main(int(sys.argv[1]) if len(sys.argv) > 1 else 12)
