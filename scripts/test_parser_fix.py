"""
Тест нового парсера: прогоняем 10 блоков через Qwen и смотрим
что вернул _parse_qwen_payload после фикса.
"""
import asyncio
import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
load_dotenv()

from qwen_enrich import (
    _auth_header, _qwen_call_attempt, _parse_qwen_payload,
    _extract_response_texts, _load_page_text, _load_sheet_no,
    _png_to_data_url, SYSTEM_PROMPT, USER_INSTRUCTION,
    DEFAULT_MAX_OUTPUT_TOKENS, DEFAULT_TIMEOUT_S,
)
import httpx

PROJECT = Path("projects/214. Alia (ASTERUS)/KJ/13АВ-РД-КЖ7.2-К2 (Изм.2).pdf")
BLOCK_IDS = [
    "7DUT-L6PU-4HU", "7EEK-9AWU-DTE", "494X-3DKA-747",
    "4QAP-WYAV-JVV", "47HE-9YWX-UKH", "9R4U-V9YR-HL6",
    "9QNF-EMAR-NAH", "7K6F-DGVG-XCE", "A3AP-44UF-DTV", "4P3M-YKXU-JE9",
]

async def main():
    base_url = os.environ["CHANDRA_BASE_URL"]
    model = "qwen/qwen3.6-35b-a3b"

    out_dir = PROJECT / "_output"
    graph = json.loads((out_dir / "document_graph.json").read_text())
    idx_data = json.loads((out_dir / "blocks_qwen_100" / "index.json").read_text())
    index = {b["block_id"]: b for b in idx_data.get("blocks", idx_data)}
    blocks_dir = out_dir / "blocks_qwen_100"

    ok = 0
    template = 0
    fail = 0

    async with httpx.AsyncClient(timeout=DEFAULT_TIMEOUT_S + 30) as client:
        for block_id in BLOCK_IDS:
            block = index.get(block_id)
            if not block:
                print(f"[{block_id}] НЕТ В ИНДЕКСЕ")
                continue

            page = block.get("page", 0)
            png_path = blocks_dir / f"block_{block_id}.png"
            if not png_path.exists():
                print(f"[{block_id}] PNG НЕ НАЙДЕН")
                continue

            page_text = _load_page_text(graph, page)
            sheet_no = _load_sheet_no(graph, page)
            ocr_label = block.get("ocr_label", "")

            user_text = USER_INSTRUCTION.format(
                block_id=block_id, page=page, sheet_no=sheet_no,
                ocr_label=ocr_label, page_text=page_text,
            )

            print(f"[{block_id}] стр.{page} — отправляю...", flush=True)
            status, data, raw, elapsed = await _qwen_call_attempt(
                client, base_url, user_text, png_path,
                scale=1.0, model=model,
                timeout=DEFAULT_TIMEOUT_S,
                max_output_tokens=DEFAULT_MAX_OUTPUT_TOKENS,
            )

            content_text, reasoning_text, finish_reason = _extract_response_texts(data)
            result, err, source, partial = _parse_qwen_payload(
                content_text=content_text,
                reasoning_text=reasoning_text,
                finish_reason=finish_reason,
            )

            content_len = len(content_text or "")
            reasoning_len = len(reasoning_text or "")


            if result is None:
                fail += 1
                print(f"  ✗ FAIL ({elapsed/1000:.1f}s) source={source} err={err}")
            else:
                # Проверяем на шаблон
                is_tpl = any(marker in str(result.get(f, "")) for f in ("block_type", "subject") for marker in (
                    "план|план_армирования", "до 120 симв", "марки/позиции"
                ))
                if is_tpl:
                    template += 1
                    print(f"  ~ ШАБЛОН ({elapsed/1000:.1f}s) source={source} content={content_len}ch reasoning={reasoning_len}ch")
                else:
                    ok += 1
                    bt = result.get("block_type", "?")
                    subj = (result.get("subject") or "")[:60]
                    axes = result.get("axes") or []
                    marks = result.get("marks") or []
                    print(f"  ✓ OK ({elapsed/1000:.1f}s) source={source} content={content_len}ch reasoning={reasoning_len}ch")
                    print(f"    type={bt} | subject={subj}")
                    print(f"    axes={axes[:5]} marks={marks[:5]}")

    print(f"\n{'='*50}")
    print(f"OK: {ok}/10  ШАБЛОН: {template}/10  FAIL: {fail}/10")

asyncio.run(main())
