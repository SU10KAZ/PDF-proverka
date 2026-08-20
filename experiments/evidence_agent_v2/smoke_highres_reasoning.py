#!/usr/bin/env python3
"""СМОУК перед high-res+reasoning замером (2 кейса, сырой инспект ответа).

Отвечает на вопросы ДО тяжёлого прогона:
  - жив ли старый ngrok-эндпоинт (basic), не занят ли Cursor'ом;
  - при enable_thinking=True: content чист (серверный split) или reasoning утёк?
  - режется ли утечка `_parse`, парсится ли вердикт;
  - латентность одного high-res+reasoning вызова (для выбора K и оценки времени).

Запуск:
  python3 -m experiments.evidence_agent_v2.smoke_highres_reasoning
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv
load_dotenv(ROOT / ".env")

# --- override на ЖИВОЙ старый ngrok (basic); .env указывает на TLS-битый vibe ---
os.environ["EVIDENCE_LOCAL_VISION_BASE_URL"] = os.environ.get(
    "EV2_NGROK_BASE_URL", "https://louvred-madie-gigglier.ngrok-free.dev")
os.environ["EVIDENCE_LOCAL_VISION_AUTH"] = "basic"

from experiments.evidence_agent_v2 import ngrok_guard
from experiments.evidence_agent_v2.context import load_context
from experiments.evidence_agent_v2.golden import build_balanced_sample, is_visual_misread_reject
from experiments.evidence_agent_v2.diag_highres_feasibility import _resolve_result_and_pdf
from experiments.evidence_agent_v2.highres_recheck import render_block_highres, _ordered_block_ids
from experiments.evidence_agent_v2.extract import (
    _select_prompt, _finding_text, _parse, _to_perception,
)

CACHE = Path("/tmp/claude-1001/-home-coder-projects-PDF-proverka/"
             "e6d95221-24b1-41f6-a376-f6b86c3d7b59/scratchpad/hr_smoke")


def _coords_idx(result_path: Path) -> dict:
    res = json.loads(result_path.read_text(encoding="utf-8"))
    idx = {}
    for page in res.get("pages", []):
        pw, ph, pn = page.get("width"), page.get("height"), page.get("page_number")
        for b in page.get("blocks", []):
            bid = b.get("id") or b.get("block_id")
            co = b.get("coords_px")
            if bid and co and pw and ph and pn:
                idx[bid] = (pn, co, pw, ph)
    return idx


def render_hr(ctx, long_side: int):
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


async def raw_call(png_path: Path, prompt: str, model: str, long_side: int, enable_thinking: bool):
    import httpx
    from backend.app.services.common.local_vision_provider import (
        load_local_vision_config, _build_headers,
        _resize_png_to_long_side, _png_bytes_to_data_url,
    )
    cfg = load_local_vision_config()
    url = _png_bytes_to_data_url(_resize_png_to_long_side(Path(png_path), long_side))
    payload = {
        "model": model, "max_tokens": int(cfg.max_tokens), "temperature": float(cfg.temperature),
        "chat_template_kwargs": {"enable_thinking": enable_thinking},
        "messages": [{"role": "user", "content": [
            {"type": "text", "text": prompt},
            {"type": "image_url", "image_url": {"url": url}},
        ]}],
    }
    t0 = time.time()
    async with httpx.AsyncClient(timeout=cfg.timeout_sec) as client:
        r = await client.post(f"{cfg.base_url}/v1/chat/completions",
                              headers=_build_headers(cfg), json=payload)
    dt = time.time() - t0
    return r, dt, cfg


async def main() -> int:
    CACHE.mkdir(parents=True, exist_ok=True)
    print("[smoke] preflight…")
    ngrok_guard.preflight(require_idle=False)

    from backend.app.services.common.local_vision_provider import load_local_vision_config
    cfg = load_local_vision_config()
    print(f"[smoke] base_url={cfg.base_url} auth={cfg.auth} max_tokens={cfg.max_tokens} "
          f"long_side_cfg={cfg.image_long_side} timeout={cfg.timeout_sec}")

    sample = build_balanced_sample(per_class=2, classes=("graphic_confirmed", "graphic_rejected"),
                                   alia_only=True)
    picks = []
    conf = [c for c in sample if c.get("case_class") == "graphic_confirmed"][:1]
    mis = [c for c in sample if is_visual_misread_reject(c)][:1]
    picks = conf + mis
    model = "qwen/qwen3.6-35b-a3b"
    long_side = 2200

    with ngrok_guard.LocalLLMLock(owner="ev2", note="smoke_highres_reasoning"):
        for case in picks:
            finding = {**case["finding"], "id": case["item_id"]}
            ctx = load_context(case["source_project"], finding, section=case.get("section") or "")
            hr, bid = render_hr(ctx, long_side)
            src = "highres" if hr else "gemma_fallback"
            png = hr or ctx.primary_png
            kb = png.stat().st_size / 1024 if png and png.is_file() else 0
            prompt = _select_prompt().format(
                finding=_finding_text(ctx.finding, case.get("section") or ""),
                ocr="\n\n".join(f"### {b.block_id}\n{b.gemma_text[:2500]}"
                                for b in ctx.blocks if b.gemma_text) or "(нет OCR)")
            print(f"\n===== {case['item_id']} [{case['case_class']}] misread="
                  f"{is_visual_misread_reject(case)} src={src} blk={bid} png={kb:.0f}KB =====")
            try:
                r, dt, _ = await raw_call(png, prompt, model, long_side, enable_thinking=True)
            except Exception as exc:
                print(f"  ВЫЗОВ УПАЛ: {type(exc).__name__}: {exc}")
                continue
            print(f"  HTTP {r.status_code} за {dt:.0f}с")
            if r.status_code != 200:
                print(f"  тело: {r.text[:300]}")
                continue
            data = r.json()
            msg = data["choices"][0]["message"]
            content = msg.get("content") or ""
            reasoning = msg.get("reasoning_content") or ""
            fin = data["choices"][0].get("finish_reason")
            usage = data.get("usage", {})
            print(f"  finish_reason={fin} usage={usage}")
            print(f"  reasoning_content: {'ЕСТЬ ' + str(len(reasoning)) + ' симв' if reasoning else 'НЕТ'}")
            print(f"  content len={len(content)} has<think>={'<think>' in content}")
            print(f"  content[:500]={content[:500]!r}")
            parsed = _parse(content)
            perc = _to_perception(parsed, model)
            print(f"  → PARSED ok={perc.ok} contradicts={perc.contradicts} "
                  f"legible={perc.region_legible} quote={perc.evidence_quote[:60]!r}")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
