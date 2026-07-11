#!/usr/bin/env python3
"""Диагностика ПЕРЕД high-res+reasoning замером (без вызовов модели):
  1) размер golden-пула Alia по классам (хватает ли 100+100 с PNG после gate);
  2) резолвятся ли high-res координаты (result.json coords_px + PDF) у кейсов —
     иначе high-res молча фолбэкнется на gemma-кроп и эксперимент выродится.

Запуск:
  python3 -m experiments.evidence_agent_v2.diag_highres_feasibility --per-class 100
"""
from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv
load_dotenv(ROOT / ".env")

from experiments.evidence_agent_v2.context import load_context
from experiments.evidence_agent_v2.golden import build_balanced_sample, is_visual_misread_reject
from experiments.evidence_agent_v2.highres_recheck import (
    block_coords_index, _pdf_path, _result_path, _ordered_block_ids,
)


def _try_coords_dirs(output_dir: Path) -> list[Path]:
    """Кандидаты version-dir, где может лежать result.json/document.pdf.
    Раскладка V2 фиксированной глубины неизвестна — пробуем несколько уровней."""
    cands = []
    for up in (output_dir, output_dir.parent, output_dir.parent.parent,
               output_dir.parent.parent.parent):
        if up and up != up.parent:
            cands.append(up)
    # уникализировать сохранив порядок
    seen, out = set(), []
    for c in cands:
        if c not in seen:
            seen.add(c); out.append(c)
    return out


def _resolve_result_and_pdf(output_dir: Path):
    """Ищем result.json (coords) и pdf по нескольким уровням раскладки."""
    result, pdf = None, None
    for vd in _try_coords_dirs(output_dir):
        for p in (vd / "02_work" / "result.json", vd / "01_input" / "result.json",
                  vd / "result.json"):
            if p.is_file():
                result = p; break
        for p in ([vd / "02_work" / "document.pdf"] + list((vd / "01_input").glob("*.pdf"))
                  + list(vd.glob("*.pdf"))):
            if p and p.is_file():
                pdf = p; break
        if result and pdf:
            break
    return result, pdf


def _coords_for_case(ctx) -> dict:
    """Проверить, есть ли coords_px для лучшего блока кейса + PDF для рендера."""
    out = {"has_png": ctx.has_png, "result_json": False, "pdf": False,
           "coords_hit": False, "primary_block": None}
    result, pdf = _resolve_result_and_pdf(ctx.output_dir)
    out["result_json"] = bool(result)
    out["pdf"] = bool(pdf)
    if not result:
        return out
    try:
        res = json.loads(result.read_text(encoding="utf-8"))
    except Exception:
        return out
    idx = {}
    for page in res.get("pages", []):
        pw, ph, pn = page.get("width"), page.get("height"), page.get("page_number")
        for b in page.get("blocks", []):
            bid = b.get("id") or b.get("block_id")
            co = b.get("coords_px")
            if bid and co and pw and ph and pn:
                idx[bid] = True
    ordered = _ordered_block_ids(ctx, ctx.finding)
    for bid in ordered:
        if bid in idx:
            out["coords_hit"] = True
            out["primary_block"] = bid
            break
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--per-class", type=int, default=100)
    ap.add_argument("--sample-detail", type=int, default=200,
                    help="сколько кейсов детально проверить на coords")
    a = ap.parse_args()

    sample = build_balanced_sample(
        per_class=a.per_class,
        classes=("graphic_confirmed", "graphic_rejected"),
        alia_only=True,
    )
    dist = Counter(c.get("case_class") for c in sample)
    misread = sum(1 for c in sample if is_visual_misread_reject(c))
    print(f"[diag] golden Alia пул (per_class={a.per_class}, с PNG, gate ON):")
    print(f"       классы: {dict(dist)}  всего={len(sample)}  misread(rejected)={misread}")

    agg = Counter()
    detail = sample[:a.sample_detail]
    print(f"[diag] проверяю coords/pdf на {len(detail)} кейсах…", flush=True)
    per_class_coords = Counter()
    for i, case in enumerate(detail, 1):
        finding = {**case["finding"], "id": case["item_id"]}
        try:
            ctx = load_context(case["source_project"], finding, section=case.get("section") or "")
        except Exception as exc:
            agg["ctx_error"] += 1
            continue
        if ctx is None:
            agg["no_ctx"] += 1
            continue
        info = _coords_for_case(ctx)
        if info["has_png"]:
            agg["has_png"] += 1
        if info["result_json"]:
            agg["result_json"] += 1
        if info["pdf"]:
            agg["pdf"] += 1
        if info["coords_hit"]:
            agg["coords_hit"] += 1
            per_class_coords[case.get("case_class")] += 1
        if i <= 6:
            print(f"   [{i}] {case['item_id'][:14]} cls={case.get('case_class')[:16]} "
                  f"png={info['has_png']} result={info['result_json']} pdf={info['pdf']} "
                  f"coords={info['coords_hit']} blk={info['primary_block']}")
        if i % 25 == 0:
            print(f"   …{i}/{len(detail)} coords_hit={agg['coords_hit']}", flush=True)

    n = len(detail)
    print(f"\n[diag] ИТОГ по {n} кейсам:")
    for k in ("has_png", "result_json", "pdf", "coords_hit", "no_ctx", "ctx_error"):
        v = agg.get(k, 0)
        print(f"   {k:14s} = {v:3d}  ({100*v//max(1,n)}%)")
    print(f"   coords по классам: {dict(per_class_coords)}")
    if agg.get("coords_hit", 0) == 0:
        print("\n[diag] ⚠️  coords НЕ резолвятся → high-res выродится в gemma-кроп. "
              "Нужен другой путь к PDF/result.json.")
    elif agg.get("coords_hit", 0) < n * 0.5:
        print(f"\n[diag] ⚠️  coords только у {100*agg['coords_hit']//max(1,n)}% — "
              "high-res частичный, замер смешанный.")
    else:
        print(f"\n[diag] ✅ coords у большинства ({100*agg['coords_hit']//max(1,n)}%) — "
              "high-res рендер применим.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
