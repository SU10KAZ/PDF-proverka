#!/usr/bin/env python3
"""EV2 честный замер lift «глаз» — сбалансированный 50+50 Alia, prompt B,
ПРАВИЛЬНЫЙ блок (context._rank всегда включён), голосование по ВОЗМУЩЕНИЯМ
масштаба (не temp=0 ре-раны). Ground truth = решения эксперта (accept/reject).

Отличия от run_benchmark.py (специально для честного замера):
  - промпт по умолчанию B (средняя точка), а не C;
  - K голосов = K РАЗНЫХ масштабов картинки одного и того же правильного блока
    (--scales), а не K идентичных прогонов при temp=0;
  - выборка = Alia (13АВ) 50+50, либо готовый корпус-файл (--sample-file);
  - агрегация голосов — та же политика verify._aggregate (аудируемая, на Python).

Запуск (под ngrok_guard, concurrency=1, окно без Cursor):
  EV2_PROMPT=b python3 -m experiments.evidence_agent_v2.run_ev2_measure \
      --per-class 50 --alia-only --scales 1.0,0.7,1.4 --require-idle
"""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
from collections import Counter
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv
load_dotenv(ROOT / ".env")

from experiments.evidence_agent_v2 import ngrok_guard
from experiments.evidence_agent_v2.context import load_context
from experiments.evidence_agent_v2.extract import perceive_async, _select_prompt
from experiments.evidence_agent_v2.golden import (
    build_balanced_sample, expected_should_reject, is_visual_misread_reject,
)
from experiments.evidence_agent_v2.verify import _aggregate

OUT_DIR = ROOT / "experiments" / "evidence_agent_v2" / "results"


def _prompt_id() -> str:
    import os
    return os.environ.get("EV2_PROMPT", "c").strip().lower()


def _load_sample(args) -> list[dict]:
    if args.sample_file:
        cases = json.load(open(args.sample_file, encoding="utf-8"))
        if args.alia_only:
            cases = [c for c in cases if str(c.get("source_project", "")).startswith("13АВ")]
        return cases
    return build_balanced_sample(
        per_class=args.per_class,
        classes=("graphic_confirmed", "graphic_rejected"),
        alia_only=args.alia_only,
    )


async def _run(args) -> int:
    needs_ngrok = True
    print("[ev2-measure] preflight…")
    ngrok_guard.preflight(require_idle=args.require_idle)

    scales = [float(s) for s in args.scales.split(",") if s.strip()]
    print(f"[ev2-measure] prompt={_prompt_id()} scales(K)={scales} model={args.model}")

    sample = _load_sample(args)
    dist = Counter(c.get("case_class") for c in sample)
    print(f"[ev2-measure] выборка: {dict(dist)} всего={len(sample)}")
    if not sample:
        print("Пустая выборка", file=sys.stderr)
        return 1

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    rows, lat = [], []

    with ngrok_guard.LocalLLMLock(owner="ev2", note="run_ev2_measure"):
        ngrok_guard.print_loaded("before")
        # Прогрев холодного сервера (JIT auto-serve): первый вызов раскочегаривает
        # модель, его латентность в метрики не считаем.
        if args.warmup:
            for c in sample:
                f = {**c["finding"], "id": c["item_id"]}
                ctx = load_context(c["source_project"], f, section=c.get("section") or "")
                if ctx and ctx.has_png:
                    print("[ev2-measure] прогрев модели (первый JIT-вызов может ждать)…", flush=True)
                    t0 = time.time()
                    await perceive_async(ctx, model=args.model, scale=1.0)
                    print(f"[ev2-measure] прогрев готов за {time.time()-t0:.0f}с", flush=True)
                    break

        for i, case in enumerate(sample, 1):
            finding = {**case["finding"], "id": case["item_id"]}
            should_reject = expected_should_reject(case)
            try:
                ctx = load_context(case["source_project"], finding, section=case.get("section") or "")
                if ctx is None or not ctx.has_png:
                    rows.append({"item": case["item_id"], "status": "no_png"}); continue
                t0 = time.time()
                perceptions = []
                for sc in scales:
                    perceptions.append(await perceive_async(ctx, model=args.model, scale=sc))
                dt = time.time() - t0
                lat.append(dt)
                block_ids = [b.block_id for b in ctx.blocks if b.png_path]
                v = _aggregate(str(case["item_id"]), perceptions, "graphic", block_ids)
                rows.append({
                    "item": case["item_id"], "project": case["source_project"],
                    "class": case["case_class"], "should_reject": should_reject,
                    "misread": is_visual_misread_reject(case),
                    "decision": v.decision, "confidence": v.confidence, "votes": v.votes,
                    "primary_png": str(ctx.primary_png), "latency_sec": round(dt, 1),
                })
                print(f"[{i}/{len(sample)}] {case['item_id']} sr={should_reject} "
                      f"-> {v.decision} votes={v.votes} {dt:.0f}s", flush=True)
            except Exception as exc:
                rows.append({"item": case["item_id"], "status": "error", "error": str(exc)})
                print(f"[{i}/{len(sample)}] {case['item_id']} ERROR {exc}", flush=True)

    scored = [r for r in rows if "decision" in r]
    conf = [r for r in scored if not r["should_reject"]]   # реальные (accepted экспертом)
    rej = [r for r in scored if r["should_reject"]]         # ложные (rejected экспертом)
    # ЧЕСТНАЯ ось recall: только те rejected, что эксперт отклонил как ВИЗУАЛЬНЫЙ
    # misread (зрение способно опровергнуть). Нормативные отклонения по картинке не
    # ловятся — спрашивать с них recall нечестно, они разбавляют метрику вниз.
    rej_misread = [r for r in rej if r.get("misread")]
    false_reject = sum(1 for r in conf if r["decision"] == "reject")
    true_reject = sum(1 for r in rej if r["decision"] == "reject")
    true_reject_misread = sum(1 for r in rej_misread if r["decision"] == "reject")
    abstain = sum(1 for r in scored if r["decision"] in ("needs_human", "borderline"))
    reject_total = false_reject + true_reject
    report = {
        "generated_at": datetime.now().isoformat(),
        "algorithm": "EV2-measure",
        "prompt": _prompt_id(),
        "model": args.model,
        "scales": scales,
        "runs_per_case": len(scales),
        "sample_size": len(sample),
        "sample_dist": dict(dist),
        "scored": len(scored),
        "confirmed_n": len(conf),
        "rejected_n": len(rej),
        "rejected_misread_n": len(rej_misread),
        "false_reject_rate": round(false_reject / len(conf), 3) if conf else None,
        "true_reject_rate": round(true_reject / len(rej), 3) if rej else None,
        "true_reject_rate_misread": round(true_reject_misread / len(rej_misread), 3) if rej_misread else None,
        "reject_precision": round(true_reject / reject_total, 3) if reject_total else None,
        "abstain_rate": round(abstain / len(scored), 3) if scored else None,
        "avg_latency_sec": round(sum(lat) / len(lat), 1) if lat else None,
        "no_png": sum(1 for r in rows if r.get("status") == "no_png"),
        "errors": sum(1 for r in rows if r.get("status") == "error"),
        "rows": rows,
    }
    out = OUT_DIR / f"measure_{_prompt_id()}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    out.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print("\n[ev2-measure] ИТОГ:")
    print(f"  false_reject_rate = {report['false_reject_rate']}  (реальных снесено {false_reject}/{len(conf)}) — БЕЗОПАСНОСТЬ ↓")
    print(f"  true_reject_rate  = {report['true_reject_rate']}  (ложных поймано {true_reject}/{len(rej)}) — RECALL (все rejected)")
    print(f"  true_reject_misread = {report['true_reject_rate_misread']}  (misread поймано {true_reject_misread}/{len(rej_misread)}) — ЧЕСТНЫЙ RECALL (где зрение способно) ↑")
    print(f"  reject_precision  = {report['reject_precision']}  (из reject правы {true_reject}/{reject_total})")
    print(f"  abstain_rate      = {report['abstain_rate']}  err={report['errors']} no_png={report['no_png']}")
    print(f"[ev2-measure] отчёт: {out}")
    return 0


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--per-class", type=int, default=50)
    p.add_argument("--alia-only", action="store_true")
    p.add_argument("--sample-file", default="", help="готовый JSON-корпус (список кейсов)")
    p.add_argument("--model", default="qwen/qwen3.6-35b-a3b")
    p.add_argument("--scales", default="1.0,0.7,1.4", help="CSV масштабов = K голосов")
    p.add_argument("--require-idle", action="store_true")
    p.add_argument("--no-warmup", dest="warmup", action="store_false")
    p.set_defaults(warmup=True)
    a = p.parse_args()
    return asyncio.run(_run(a))


if __name__ == "__main__":
    raise SystemExit(main())
