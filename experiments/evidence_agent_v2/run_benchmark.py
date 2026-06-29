#!/usr/bin/env python3
"""EV2 benchmark — сравнить vision-модели LM Studio на сбалансированном golden-set.

Метрики (ключевой принцип: НЕ удалять реальные замечания):
  - false_reject_rate  — доля ПОДТВЕРЖДЁННЫХ замечаний, ошибочно reject'нутых
                         (САМАЯ опасная ошибка; выбираем модель по её минимуму);
  - true_reject_rate   — доля ЛОЖНЫХ замечаний (rejected экспертом), верно reject'нутых
                         (полезность: ловим ошибки ИИ-чтения чертежа);
  - abstain_rate       — доля needs_human/borderline (честное «не знаю»).

Координация с Cursor: ngrok-guard preflight + кооперативный lock; concurrency=1.
Перед запуском убедись, что Cursor НЕ гоняет ngrok одновременно.

Запуск:
  python3 -m experiments.evidence_agent_v2.run_benchmark \
      --models qwen/qwen3.6-35b-a3b qwen/qwen3.6-27b google/gemma-4-26b-a4b \
      --per-class 12 --runs 1
"""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv
load_dotenv(ROOT / ".env")

from experiments.evidence_agent_v2 import ngrok_guard
from experiments.evidence_agent_v2.context import load_context
from experiments.evidence_agent_v2.extract import perceive_async
from experiments.evidence_agent_v2.golden import (
    build_balanced_sample,
    expected_should_reject,
    is_visual_misread_reject,
)
from experiments.evidence_agent_v2.verify import verify_finding_multi_async, verify_graphic_async
from experiments.evidence_agent_v2.norm_check import run_norm_check
from experiments.evidence_agent_v2.cross_block import run_cross_block
from experiments.evidence_agent_v2.fusion import fuse

OUT_DIR = ROOT / "experiments" / "evidence_agent_v2" / "results"

# Реально доступные на LM Studio vision-кандидаты (см. /v1/models).
DEFAULT_MODELS = [
    "qwen/qwen3.6-35b-a3b",     # прод stage-comparison
    "qwen/qwen3.6-27b",         # меньше/быстрее
    "google/gemma-4-26b-a4b",   # gemma vision
]


async def _bench_multi(model: str, sample: list[dict], *, offline_only: bool, runs: int = 1) -> dict:
    """Многоисточниковый верификатор. offline_only=True → без vision (норма+кросс-блок).

    Метрики: false_reject (главная), recall по источникам, abstain, vision_calls_saved.
    """
    from collections import Counter
    rows = []
    for case in sample:
        finding = {**case["finding"], "id": case["item_id"]}
        should_reject = expected_should_reject(case)
        try:
            if offline_only:
                ctx = load_context(case["source_project"], finding, section=case.get("section") or "")
                if ctx is None:
                    rows.append({"item": case["item_id"], "status": "no_ctx"}); continue
                ns = run_norm_check(finding)
                xb = run_cross_block(finding, ctx.graph)
                fv = fuse(None, ns, xb, finding_id=case["item_id"])
            else:
                fv = await verify_finding_multi_async(
                    case["source_project"], finding,
                    section=case.get("section") or "", model=model, runs=runs)
            rows.append({
                "item": case["item_id"], "class": case["case_class"],
                "should_reject": should_reject, "decision": fv.decision,
                "source": fv.source, "taxonomy": fv.taxonomy,
                "sources_used": fv.sources_used, "confidence": fv.confidence,
            })
        except Exception as exc:
            rows.append({"item": case["item_id"], "status": "error", "error": str(exc)})

    scored = [r for r in rows if "decision" in r]
    conf = [r for r in scored if not r["should_reject"]]
    rej = [r for r in scored if r["should_reject"]]
    false_reject = sum(1 for r in conf if r["decision"] == "reject")
    true_reject = sum(1 for r in rej if r["decision"] == "reject")
    abstain = sum(1 for r in scored if r["decision"] in ("needs_human", "borderline"))
    vision_used = sum(1 for r in scored if "visual" in (r.get("sources_used") or []))
    return {
        "model": model, "mode": "multi", "offline_only": offline_only, "scored": len(scored),
        "false_reject_rate": round(false_reject / len(conf), 3) if conf else None,
        "true_reject_rate": round(true_reject / len(rej), 3) if rej else None,
        "abstain_rate": round(abstain / len(scored), 3) if scored else None,
        "vision_calls_used": vision_used,
        "vision_calls_saved": len(scored) - vision_used,
        "decision_dist": dict(Counter(r["decision"] for r in scored)),
        "source_dist": dict(Counter(r["source"] for r in scored)),
        "errors": sum(1 for r in rows if r.get("status") == "error"),
        "rows": rows,
    }


async def _bench_perception(model: str, sample: list[dict]) -> dict:
    """Режим ВЫБОРА модели: меряем чистое восприятие (K=1), без 4-вердикт-политики.

    Это честная метрика качества модели, не смешанная с консервативной политикой EV2:
      - на confirmed (реальные): contradicts=yes = ЛОЖНОЕ противоречие (плохо);
      - на rejected-misread:      contradicts=yes = верно пойман misread (хорошо);
      - cannot_tell = честное «не вижу».
    """
    rows, lat = [], []
    for case in sample:
        finding = {**case["finding"], "id": case["item_id"]}
        try:
            ctx = load_context(case["source_project"], finding, section=case.get("section") or "")
            if ctx is None or not ctx.has_png:
                rows.append({"item": case["item_id"], "status": "no_png"})
                continue
            t0 = time.time()
            p = await perceive_async(ctx, model=model)
            dt = time.time() - t0
            lat.append(dt)
            rows.append({
                "item": case["item_id"], "project": case["source_project"],
                "class": case["case_class"],
                "should_reject": expected_should_reject(case),
                "misread": is_visual_misread_reject(case),
                "contradicts": p.contradicts if p.ok else "invalid",
                "legible": p.region_legible,
                "value": p.value_on_drawing[:120],
                "latency_sec": round(dt, 1),
            })
        except Exception as exc:
            rows.append({"item": case["item_id"], "status": "error", "error": str(exc)})

    scored = [r for r in rows if "contradicts" in r]
    conf = [r for r in scored if not r["should_reject"]]
    misread = [r for r in scored if r["should_reject"] and r["misread"]]
    false_contra = sum(1 for r in conf if r["contradicts"] == "yes")
    recall = sum(1 for r in misread if r["contradicts"] == "yes")
    abstain = sum(1 for r in scored if r["contradicts"] in ("cannot_tell", "invalid"))
    return {
        "model": model, "mode": "perception", "scored": len(scored),
        "confirmed_n": len(conf), "misread_n": len(misread),
        "false_contradict_rate": round(false_contra / len(conf), 3) if conf else None,
        "misread_recall": round(recall / len(misread), 3) if misread else None,
        "abstain_rate": round(abstain / len(scored), 3) if scored else None,
        "avg_latency_sec": round(sum(lat) / len(lat), 1) if lat else None,
        "no_png": sum(1 for r in rows if r.get("status") == "no_png"),
        "errors": sum(1 for r in rows if r.get("status") == "error"),
        "rows": rows,
    }


async def _bench_model(model: str, sample: list[dict], runs: int) -> dict:
    rows = []
    lat = []
    for case in sample:
        finding = {**case["finding"], "id": case["item_id"]}
        should_reject = expected_should_reject(case)
        try:
            ctx = load_context(case["source_project"], finding, section=case.get("section") or "")
            if ctx is None or not ctx.has_png:
                rows.append({"item": case["item_id"], "status": "no_png"})
                continue
            t0 = time.time()
            v = await verify_graphic_async(ctx, model=model, runs=runs)
            dt = time.time() - t0
            lat.append(dt)
            rows.append({
                "item": case["item_id"],
                "project": case["source_project"],
                "class": case["case_class"],
                "should_reject": should_reject,
                "decision": v.decision,
                "confidence": v.confidence,
                "votes": v.votes,
                "latency_sec": round(dt, 1),
            })
        except Exception as exc:
            rows.append({"item": case["item_id"], "status": "error", "error": str(exc)})

    scored = [r for r in rows if "decision" in r]
    conf_cases = [r for r in scored if not r["should_reject"]]   # подтверждённые
    rej_cases = [r for r in scored if r["should_reject"]]         # ложные
    false_rejects = sum(1 for r in conf_cases if r["decision"] == "reject")
    true_rejects = sum(1 for r in rej_cases if r["decision"] == "reject")
    abstain = sum(1 for r in scored if r["decision"] in ("needs_human", "borderline"))
    return {
        "model": model,
        "scored": len(scored),
        "confirmed_n": len(conf_cases),
        "rejected_n": len(rej_cases),
        "false_reject_rate": round(false_rejects / len(conf_cases), 3) if conf_cases else None,
        "true_reject_rate": round(true_rejects / len(rej_cases), 3) if rej_cases else None,
        "abstain_rate": round(abstain / len(scored), 3) if scored else None,
        "avg_latency_sec": round(sum(lat) / len(lat), 1) if lat else None,
        "no_png": sum(1 for r in rows if r.get("status") == "no_png"),
        "errors": sum(1 for r in rows if r.get("status") == "error"),
        "rows": rows,
    }


async def main_async(models, per_class, runs, require_idle, mode, offline_only=False) -> int:
    needs_ngrok = not (mode == "multi" and offline_only)
    if needs_ngrok:
        print("[ev2-bench] preflight…")
        ngrok_guard.preflight(require_idle=require_idle)
    else:
        print("[ev2-bench] OFFLINE-ONLY (без нейросети, можно при работающем Cursor)")

    print(f"[ev2-bench] строю сбалансированную выборку: {per_class}/класс…")
    sample = build_balanced_sample(per_class=per_class)
    from collections import Counter
    dist = Counter(c["case_class"] for c in sample)
    print(f"[ev2-bench] выборка: {dict(dist)} (всего {len(sample)}), режим={mode}")
    if not sample:
        print("Пустая выборка — нет PNG-резолвимых кейсов", file=sys.stderr)
        return 1

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    report = {
        "generated_at": datetime.now().isoformat(),
        "algorithm": "EV2",
        "mode": mode,
        "runs_per_case": 1 if mode == "perception" else runs,
        "sample_size": len(sample),
        "sample_dist": dict(dist),
        "models": [],
    }

    import contextlib
    lock_cm = (ngrok_guard.LocalLLMLock(owner="ev2", note=f"run_benchmark:{mode}")
               if needs_ngrok else contextlib.nullcontext())
    with lock_cm:
        for model in models:
            print(f"[ev2-bench] === {model} (mode={mode}) ===", flush=True)
            if needs_ngrok:
                ngrok_guard.print_loaded(f"before {model}")
            if mode == "perception":
                res = await _bench_perception(model, sample)
                print(f"[ev2-bench]   false_contradict={res['false_contradict_rate']} "
                      f"misread_recall={res['misread_recall']} "
                      f"abstain={res['abstain_rate']} lat={res['avg_latency_sec']}s "
                      f"no_png={res['no_png']} err={res['errors']}", flush=True)
            elif mode == "multi":
                res = await _bench_multi(model, sample, offline_only=offline_only, runs=runs)
                print(f"[ev2-bench]   false_reject={res['false_reject_rate']} "
                      f"true_reject={res['true_reject_rate']} abstain={res['abstain_rate']} "
                      f"vision_saved={res['vision_calls_saved']}/{res['scored']} "
                      f"decisions={res['decision_dist']} err={res['errors']}", flush=True)
            else:
                res = await _bench_model(model, sample, runs)
                print(f"[ev2-bench]   false_reject={res['false_reject_rate']} "
                      f"true_reject={res['true_reject_rate']} "
                      f"abstain={res['abstain_rate']} lat={res['avg_latency_sec']}s "
                      f"no_png={res['no_png']} err={res['errors']}", flush=True)
            report["models"].append(res)
            if mode == "multi":
                break  # multi не сравнивает модели — один прогон

    if mode == "perception":
        def _key(m):
            return (m["false_contradict_rate"] if m["false_contradict_rate"] is not None else 1.0,
                    -(m["misread_recall"] if m["misread_recall"] is not None else 0.0),
                    m["avg_latency_sec"] if m["avg_latency_sec"] is not None else 1e9)
        report["models"].sort(key=_key)
    elif mode == "verdict":
        def _key(m):
            return (m["false_reject_rate"] if m["false_reject_rate"] is not None else 1.0,
                    -(m["true_reject_rate"] if m["true_reject_rate"] is not None else 0.0),
                    m["avg_latency_sec"] if m["avg_latency_sec"] is not None else 1e9)
        report["models"].sort(key=_key)

    suffix = "multi_offline" if (mode == "multi" and offline_only) else mode
    out = OUT_DIR / f"bench_{suffix}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    out.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print("\n[ev2-bench] ИТОГ:")
    for m in report["models"]:
        if mode == "perception":
            print(f"   {m['model']:28} false_contradict={m['false_contradict_rate']} "
                  f"misread_recall={m['misread_recall']} abstain={m['abstain_rate']} "
                  f"lat={m['avg_latency_sec']}s")
        elif mode == "multi":
            print(f"   false_reject={m['false_reject_rate']} true_reject={m['true_reject_rate']} "
                  f"abstain={m['abstain_rate']} vision_saved={m['vision_calls_saved']}/{m['scored']}")
            print(f"   decisions={m['decision_dist']}  sources={m['source_dist']}")
        else:
            print(f"   {m['model']:28} false_reject={m['false_reject_rate']} "
                  f"true_reject={m['true_reject_rate']} abstain={m['abstain_rate']} "
                  f"lat={m['avg_latency_sec']}s")
    print(f"[ev2-bench] отчёт: {out}")
    return 0


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--models", nargs="*", default=DEFAULT_MODELS)
    p.add_argument("--per-class", type=int, default=12)
    p.add_argument("--runs", type=int, default=2, help="K прогонов (только mode=verdict)")
    p.add_argument("--mode", choices=("perception", "verdict", "multi"), default="perception",
                   help="perception=выбор модели; verdict=4-вердикт EV2; multi=многоисточниковый")
    p.add_argument("--offline-only", action="store_true",
                   help="mode=multi без vision (норма+кросс-блок), можно при работающем Cursor")
    p.add_argument("--require-idle", action="store_true",
                   help="упасть, если LM Studio занят/чужой lock (защита от пересечения с Cursor)")
    a = p.parse_args()
    return asyncio.run(main_async(a.models, a.per_class, a.runs, a.require_idle, a.mode,
                                  offline_only=a.offline_only))


if __name__ == "__main__":
    raise SystemExit(main())
