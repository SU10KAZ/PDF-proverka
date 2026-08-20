#!/usr/bin/env python3
"""Диагностика причины invalid-голосов: печатает per-vote finish_reason,
content_len, reasoning_len, long_side для одного misread-кейса."""
from __future__ import annotations

import asyncio, json, os, sys, time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from dotenv import load_dotenv
load_dotenv(ROOT / ".env")
os.environ["EVIDENCE_LOCAL_VISION_BASE_URL"] = "https://louvred-madie-gigglier.ngrok-free.dev"
os.environ["EVIDENCE_LOCAL_VISION_AUTH"] = "basic"

from experiments.evidence_agent_v2 import ngrok_guard
from experiments.evidence_agent_v2.context import load_context
from experiments.evidence_agent_v2.golden import build_balanced_sample, is_visual_misread_reject
from experiments.evidence_agent_v2.run_ev2_highres_measure import _render_hr
from experiments.evidence_agent_v2.extract import _select_prompt, _finding_text, _parse


async def one(png, prompt, model, ls, max_tokens):
    import httpx
    from backend.app.services.common.local_vision_provider import (
        load_local_vision_config, _build_headers,
        _resize_png_to_long_side, _png_bytes_to_data_url)
    cfg = load_local_vision_config()
    url = _png_bytes_to_data_url(_resize_png_to_long_side(Path(png), ls))
    payload = {"model": model, "max_tokens": max_tokens, "temperature": float(cfg.temperature),
               "chat_template_kwargs": {"enable_thinking": True},
               "messages": [{"role": "user", "content": [
                   {"type": "text", "text": prompt},
                   {"type": "image_url", "image_url": {"url": url}}]}]}
    t0 = time.time()
    async with httpx.AsyncClient(timeout=cfg.timeout_sec) as c:
        r = await c.post(f"{cfg.base_url}/v1/chat/completions", headers=_build_headers(cfg), json=payload)
    dt = time.time() - t0
    d = r.json(); ch = d["choices"][0]; m = ch["message"]
    content = m.get("content") or ""; reasoning = m.get("reasoning_content") or ""
    parsed = _parse(content)
    print(f"  ls={ls} fin={ch.get('finish_reason')} usage={d.get('usage',{}).get('completion_tokens')} "
          f"reasoning={len(reasoning)} content={len(content)} parsed_ok={parsed is not None} {dt:.0f}s")
    if parsed is None:
        print(f"    content_tail={content[-200:]!r}")


async def main():
    ngrok_guard.preflight(require_idle=False)
    sample = build_balanced_sample(per_class=6, classes=("graphic_confirmed", "graphic_rejected"),
                                   alia_only=True)
    mis = [c for c in sample if is_visual_misread_reject(c)][:3]
    model = "qwen/qwen3.6-35b-a3b"
    with ngrok_guard.LocalLLMLock(owner="ev2", note="debug_invalid"):
        for case in mis:
            finding = {**case["finding"], "id": case["item_id"]}
            ctx = load_context(case["source_project"], finding, section=case.get("section") or "")
            prompt = _select_prompt().format(
                finding=_finding_text(ctx.finding, case.get("section") or ""),
                ocr="\n\n".join(f"### {b.block_id}\n{b.gemma_text[:2500]}"
                                for b in ctx.blocks if b.gemma_text) or "(нет OCR)")
            print(f"\n== {case['item_id']} ==")
            for ls, mt in [(2400, 5500), (2000, 5500), (1600, 5500), (2400, 9000)]:
                hr, _ = _render_hr(ctx, ls)
                if hr:
                    try:
                        await one(hr, prompt, model, ls, mt)
                    except Exception as e:
                        print(f"  ls={ls} FAIL {type(e).__name__}: {e}")


if __name__ == "__main__":
    asyncio.run(main())
