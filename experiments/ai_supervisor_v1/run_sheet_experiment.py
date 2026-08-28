"""Эксперимент 1: сопоставление листов моделью против детерминированного матчера.

Почему это первый эксперимент: все 1720 вопросов CHANGE построены поверх 12 пар
листов, у которых уверенность алгоритма 0.29-0.71 и title_is_primary=false.
Если пара листов неверна, каждое расхождение на ней — артефакт. Значит рычаг
здесь в 143 раза дешевле, чем разбор атомов по одному.

Только чтение. Результаты пишутся в results/.
"""
from __future__ import annotations

import json
import re
import sys
import collections
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from evidence_package import Run
from gateway import call_claude, call_codex
from prompts import sheet_prompt, SHEET_VERSION
from schema import SHEET_MATCH_SCHEMA

PROD = ("/home/coder/projects/PDF-proverka/comparison/sessions/"
        "121d764109184c13/pairs/p16b108b9f5/production")
RESULTS = Path(__file__).parent / "results"

TITLE_RE = re.compile(r"экспликац|кровл|ведомост|спецификац", re.I)


def page_titles(run: Run, side: str) -> dict[int, list[str]]:
    out: dict[int, list[str]] = {}
    for (s, page), frags in run.frags.items():
        if s != side:
            continue
        seen, titles = set(), []
        for f in frags:
            t = (f.get("text") or "").strip()
            if TITLE_RE.search(t) and t not in seen:
                seen.add(t)
                titles.append(t)
        if titles:
            out[page] = titles[:6]
    return out


CONFIGS = [
    ("CLAUDE_SESSION", "claude-sonnet-5", None),
    ("CLAUDE_SESSION", "claude-opus-5", None),
    ("CODEX_SESSION", "gpt-5.6-sol", "low"),
    ("CODEX_SESSION", "gpt-5.6-sol", "medium"),
    ("CODEX_SESSION", "gpt-5.6-sol", "xhigh"),
]


def run_one(cfg, prompt):
    provider, model, effort = cfg
    if provider == "CLAUDE_SESSION":
        r = call_claude(prompt, model=model, schema=SHEET_MATCH_SCHEMA,
                        effort=effort, timeout_s=420)
    else:
        r = call_codex(prompt, model=model, schema=SHEET_MATCH_SCHEMA,
                       effort=effort, timeout_s=600)
    return cfg, r


def main() -> None:
    run = Run(PROD)
    left, right = page_titles(run, "LEFT"), page_titles(run, "RIGHT")
    prompt = sheet_prompt(left, right)
    print(f"страниц с заголовками: LEFT {len(left)}, RIGHT {len(right)}")
    print(f"длина промпта: {len(prompt)} символов\n")

    # что дал production-матчер
    system_pairs = {}
    for g in run.differences["sheet_groups"]:
        for lp in g["left_pages"]:
            for rp in g["right_pages"]:
                system_pairs[lp] = rp

    results = {}
    with ThreadPoolExecutor(max_workers=5) as ex:
        for cfg, r in ex.map(lambda c: run_one(c, prompt), CONFIGS):
            key = f"{cfg[0]}|{cfg[1]}|{cfg[2] or '-'}"
            results[key] = {
                "ok": r.ok, "duration_s": round(r.duration_s, 1),
                "error": r.error[:400], "parsed": r.parsed,
            }
            print(f"[{key}] ok={r.ok} {r.duration_s:.1f}s")
            if r.ok and r.parsed:
                pairs = {p["left_page"]: p["right_page"] for p in r.parsed.get("pairs", [])}
                agree = sum(1 for lp, rp in pairs.items() if system_pairs.get(lp) == rp)
                print(f"    пар: {len(pairs)}  совпало с production: {agree}")
                for p in sorted(r.parsed.get("pairs", []), key=lambda x: x["left_page"]):
                    lp, rp = p["left_page"], p["right_page"]
                    mark = "=" if system_pairs.get(lp) == rp else "x"
                    print(f"      {mark} L{lp:>3} -> R{rp:<3} [{p['confidence']}] {p['basis'][:60]}")
                print(f"    не сопоставлено LEFT: {r.parsed.get('unmatched_left')}")
                print(f"    не сопоставлено RIGHT: {r.parsed.get('unmatched_right')}")
            else:
                print(f"    ошибка: {r.error[:200]}")
            print()

    RESULTS.mkdir(exist_ok=True)
    (RESULTS / "sheet_experiment.json").write_text(json.dumps({
        "prompt_version": SHEET_VERSION,
        "production_pairs": system_pairs,
        "left_titles": left, "right_titles": right,
        "results": results,
    }, ensure_ascii=False, indent=1), encoding="utf-8")
    print(f"записано: {RESULTS / 'sheet_experiment.json'}")

    # согласие моделей между собой — оценка без эталона
    votes = collections.Counter()
    voters = 0
    for key, res in results.items():
        if res["ok"] and res["parsed"]:
            voters += 1
            for p in res["parsed"].get("pairs", []):
                votes[(p["left_page"], p["right_page"])] += 1
    if voters:
        print(f"\nСОГЛАСИЕ МОДЕЛЕЙ ({voters} конфигураций):")
        for (lp, rp), n in sorted(votes.items(), key=lambda kv: (-kv[1], kv[0])):
            mark = "=" if system_pairs.get(lp) == rp else "x"
            print(f"  {n}/{voters}  {mark} L{lp} -> R{rp}")


if __name__ == "__main__":
    main()
