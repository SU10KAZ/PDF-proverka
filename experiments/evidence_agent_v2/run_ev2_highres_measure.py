#!/usr/bin/env python3
"""EV2 замер lift «глаз» с ДВУМЯ рычагами против misread: HIGH-RES рендер из
вектор-PDF + REASONING ON. Всё остальное идентично baseline-замеру
(run_ev2_measure.py: тот же golden-корпус, ground truth = решения эксперта,
prompt B, агрегация verify._aggregate, те же метрики) — чтобы изолировать лифт.

Отличия от baseline (ровно два):
  1. картинка — high-res рендер лучшего блока ИЗ PDF (long_side 2400/2000/1600),
     а не gemma-кроп ~1100px; фолбэк на кроп, если coords не резолвятся;
  2. enable_thinking=True — модель РАССУЖДАЕТ (перечитывает мелкие числа),
     читаем чистый JSON из `content` (reasoning уходит в reasoning_content).

K голосов = K РАЗНЫХ high-res разрешений (перестановка, как scales в baseline).
MIN_REJECT_VOTES=2 → K>=2 обязателен, иначе reject недостижим.

Запуск (под ngrok_guard, concurrency=1, окно согласовано с Cursor):
  python3 -m experiments.evidence_agent_v2.run_ev2_highres_measure --per-class 100
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
from collections import Counter
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv
load_dotenv(ROOT / ".env")

# --- режим: живой старый ngrok (basic) + прямой путь + reasoning; .env не трогаем ---
os.environ["STAGE_COMPARISON_GRAPHIC_LLM_BASE_URL"] = os.environ.get(
    "EV2_NGROK_BASE_URL", "https://louvred-madie-gigglier.ngrok-free.dev")
os.environ["STAGE_COMPARISON_GRAPHIC_LLM_AUTH"] = "basic"
os.environ["EV2_DIRECT_NOTHINK"] = "1"       # прямой путь (чтение content)
os.environ.setdefault("EV2_PROMPT", "b")      # тот же prompt B, что baseline

from experiments.evidence_agent_v2 import ngrok_guard
from experiments.evidence_agent_v2.context import load_context
from experiments.evidence_agent_v2.extract import perceive_async, _select_prompt
from experiments.evidence_agent_v2.golden import (
    build_balanced_sample, expected_should_reject, is_visual_misread_reject,
)
from experiments.evidence_agent_v2.verify import _aggregate
from experiments.evidence_agent_v2.diag_highres_feasibility import _resolve_result_and_pdf
from experiments.evidence_agent_v2.highres_recheck import render_block_highres, _ordered_block_ids

OUT_DIR = ROOT / "experiments" / "evidence_agent_v2" / "results"
CACHE = Path("/tmp/claude-1001/-home-coder-projects-PDF-proverka/"
             "e6d95221-24b1-41f6-a376-f6b86c3d7b59/scratchpad/hr_measure")


def _coords_idx(result_path: Path) -> dict:
    try:
        res = json.loads(result_path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    idx = {}
    for page in res.get("pages", []):
        pw, ph, pn = page.get("width"), page.get("height"), page.get("page_number")
        for b in page.get("blocks", []):
            bid = b.get("id") or b.get("block_id")
            co = b.get("coords_px")
            if bid and co and pw and ph and pn:
                idx[bid] = (pn, co, pw, ph)
    return idx


def _render_hr(ctx, long_side: int):
    """High-res рендер лучшего блока кейса из PDF. (png|None, block_id|None)."""
    result, pdf = _resolve_result_and_pdf(ctx.output_dir)
    if not (result and pdf):
        return None, None
    idx = _coords_idx(result)
    for bid in _ordered_block_ids(ctx, ctx.finding):
        if bid in idx:
            pn, co, pw, ph = idx[bid]
            out = CACHE / f"{ctx.finding.get('id','x')}_{bid}_{long_side}.png"
            png = render_block_highres(pdf, pn, co, (pw, ph), long_side=long_side, out_path=out)
            if png:
                return png, bid
    return None, None


_SALVAGE_LONG_SIDE = 1500   # безопасное разрешение для повтора invalid high-res голоса


async def _perceive_case(ctx, model, long_sides, max_tokens):
    """K восприятий на K high-res разрешениях (фолбэк на gemma-кроп).

    Salvage: reasoning-runaway на предельном разрешении (fin=length, пустой
    content → invalid) спасаем одним повтором на 1500px — иначе invalid-голос
    роняет эффективный K и искусственно давит recall (MIN_REJECT_VOTES=2)."""
    perceptions, srcs, salvaged = [], [], 0
    for ls in long_sides:
        hr, _bid = _render_hr(ctx, ls)
        if hr:
            p = await perceive_async(ctx, model=model, png_override=hr, long_side_abs=ls,
                                     enable_thinking=True,
                                     max_tokens_override=max_tokens or None)
            if not p.ok and ls > _SALVAGE_LONG_SIDE:
                hr2, _ = _render_hr(ctx, _SALVAGE_LONG_SIDE)
                if hr2:
                    p2 = await perceive_async(ctx, model=model, png_override=hr2,
                                              long_side_abs=_SALVAGE_LONG_SIDE,
                                              enable_thinking=True,
                                              max_tokens_override=max_tokens or None)
                    if p2.ok:
                        p, salvaged = p2, salvaged + 1
            srcs.append("highres")
        else:
            # фолбэк: gemma-кроп + reasoning (перестановка через scale)
            p = await perceive_async(ctx, model=model, enable_thinking=True,
                                     scale=ls / 1100.0,
                                     max_tokens_override=max_tokens or None)
            srcs.append("fallback")
        perceptions.append(p)
    src = ("highres" if all(s == "highres" for s in srcs)
           else "fallback" if all(s == "fallback" for s in srcs) else "mixed")
    return perceptions, src, salvaged


def _load_sample(args):
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
    CACHE.mkdir(parents=True, exist_ok=True)
    print("[hr-measure] preflight…")
    ngrok_guard.preflight(require_idle=args.require_idle)
    long_sides = [int(s) for s in args.long_sides.split(",") if s.strip()]
    print(f"[hr-measure] reasoning=ON prompt={os.environ.get('EV2_PROMPT')} "
          f"long_sides(K)={long_sides} model={args.model}")

    sample = _load_sample(args)
    dist = Counter(c.get("case_class") for c in sample)
    print(f"[hr-measure] выборка: {dict(dist)} всего={len(sample)}")
    if not sample:
        print("Пустая выборка", file=sys.stderr)
        return 1

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    rows, lat = [], []

    with ngrok_guard.LocalLLMLock(owner="ev2", note="run_ev2_highres_measure"):
        ngrok_guard.print_loaded("before")
        if args.warmup:
            for c in sample:
                f = {**c["finding"], "id": c["item_id"]}
                ctx = load_context(c["source_project"], f, section=c.get("section") or "")
                if ctx and ctx.has_png:
                    print("[hr-measure] прогрев модели…", flush=True)
                    t0 = time.time()
                    await _perceive_case(ctx, args.model, long_sides[:1], args.max_tokens)
                    print(f"[hr-measure] прогрев за {time.time()-t0:.0f}с", flush=True)
                    break

        for i, case in enumerate(sample, 1):
            finding = {**case["finding"], "id": case["item_id"]}
            should_reject = expected_should_reject(case)
            try:
                ctx = load_context(case["source_project"], finding, section=case.get("section") or "")
                if ctx is None or not ctx.has_png:
                    rows.append({"item": case["item_id"], "status": "no_png"}); continue
                t0 = time.time()
                perceptions, src, salvaged = await _perceive_case(
                    ctx, args.model, long_sides, args.max_tokens)
                dt = time.time() - t0
                lat.append(dt)
                block_ids = [b.block_id for b in ctx.blocks if b.png_path]
                v = _aggregate(str(case["item_id"]), perceptions, "graphic", block_ids)
                rows.append({
                    "item": case["item_id"], "project": case["source_project"],
                    "class": case["case_class"], "should_reject": should_reject,
                    "misread": is_visual_misread_reject(case), "source": src,
                    "salvaged": salvaged,
                    "decision": v.decision, "confidence": v.confidence, "votes": v.votes,
                    "latency_sec": round(dt, 1),
                })
                print(f"[{i}/{len(sample)}] {case['item_id']} sr={should_reject} src={src} "
                      f"-> {v.decision} votes={v.votes} {dt:.0f}s", flush=True)
            except Exception as exc:
                rows.append({"item": case["item_id"], "status": "error", "error": str(exc)})
                print(f"[{i}/{len(sample)}] {case['item_id']} ERROR {exc}", flush=True)

    _summarize(args, sample, dist, rows, lat, long_sides)
    return 0


def _metrics(scored: list) -> dict:
    conf = [r for r in scored if not r["should_reject"]]
    rej = [r for r in scored if r["should_reject"]]
    rej_misread = [r for r in rej if r.get("misread")]
    false_reject = sum(1 for r in conf if r["decision"] == "reject")
    true_reject = sum(1 for r in rej if r["decision"] == "reject")
    true_reject_misread = sum(1 for r in rej_misread if r["decision"] == "reject")
    abstain = sum(1 for r in scored if r["decision"] in ("needs_human", "borderline"))
    reject_total = false_reject + true_reject
    return {
        "n": len(scored), "confirmed_n": len(conf), "rejected_n": len(rej),
        "rejected_misread_n": len(rej_misread),
        "false_reject": false_reject, "true_reject": true_reject,
        "true_reject_misread": true_reject_misread,
        "false_reject_rate": round(false_reject / len(conf), 3) if conf else None,
        "true_reject_rate": round(true_reject / len(rej), 3) if rej else None,
        "true_reject_rate_misread": round(true_reject_misread / len(rej_misread), 3) if rej_misread else None,
        "reject_precision": round(true_reject / reject_total, 3) if reject_total else None,
        "abstain_rate": round(abstain / len(scored), 3) if scored else None,
    }


def _summarize(args, sample, dist, rows, lat, long_sides):
    scored = [r for r in rows if "decision" in r]
    overall = _metrics(scored)
    hr_only = _metrics([r for r in scored if r.get("source") == "highres"])
    src_dist = Counter(r.get("source") for r in scored)
    total_salvaged = sum(r.get("salvaged", 0) for r in scored)
    cases_with_invalid = sum(1 for r in scored if r.get("votes", {}).get("invalid", 0) > 0)
    report = {
        "generated_at": datetime.now().isoformat(),
        "algorithm": "EV2-highres-reasoning",
        "reasoning": True, "prompt": os.environ.get("EV2_PROMPT"),
        "model": args.model, "long_sides": long_sides, "runs_per_case": len(long_sides),
        "sample_size": len(sample), "sample_dist": dict(dist),
        "source_dist": dict(src_dist),
        "salvaged_votes": total_salvaged, "cases_with_invalid_vote": cases_with_invalid,
        "scored": len(scored),
        "avg_latency_sec": round(sum(lat) / len(lat), 1) if lat else None,
        "no_png": sum(1 for r in rows if r.get("status") == "no_png"),
        "errors": sum(1 for r in rows if r.get("status") == "error"),
        "metrics_overall": overall,
        "metrics_highres_only": hr_only,
        "baseline_ref": {"true_reject_rate_misread": 0.10, "false_reject_rate": 0.06,
                         "reject_precision": 0.625, "note": "50+50, reasoning OFF, gemma-кроп"},
        "rows": rows,
    }
    out = OUT_DIR / f"measure_highres_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    out.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print("\n[hr-measure] ИТОГ (overall):")
    for k in ("false_reject_rate", "true_reject_rate", "true_reject_rate_misread",
              "reject_precision", "abstain_rate"):
        print(f"  {k:26s} = {overall.get(k)}")
    print(f"  источники: {dict(src_dist)}  err={report['errors']} no_png={report['no_png']} "
          f"avg={report['avg_latency_sec']}с")
    print(f"  salvage: спасено голосов={total_salvaged}, кейсов с оставшимся invalid={cases_with_invalid}")
    print(f"[hr-measure] highres-only: recall_misread={hr_only.get('true_reject_rate_misread')} "
          f"false_reject={hr_only.get('false_reject_rate')} n={hr_only.get('n')}")
    print(f"[hr-measure] baseline: recall_misread=0.10 false_reject=0.06 precision=0.625")
    print(f"[hr-measure] отчёт: {out}")


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--per-class", type=int, default=100)
    p.add_argument("--alia-only", action="store_true", default=True)
    p.add_argument("--sample-file", default="")
    p.add_argument("--model", default="qwen/qwen3.6-35b-a3b")
    p.add_argument("--long-sides", default="2200,1700,1300", help="CSV high-res разрешений = K голосов")
    p.add_argument("--max-tokens", type=int, default=0, help="0 = из конфига (5500)")
    p.add_argument("--require-idle", action="store_true")
    p.add_argument("--no-warmup", dest="warmup", action="store_false")
    p.set_defaults(warmup=True)
    return asyncio.run(_run(p.parse_args()))


if __name__ == "__main__":
    raise SystemExit(main())
