"""
Experimental: Qwen multimodal block enrichment on КЖ5.1 (25 blocks).

Цель: проверить, может ли локальный QWEN вместо Gemini Flash делать
структурированный enrichment блоков (block_type, marks, dimensions, references),
чтобы на stage 02 Gemini Pro мог получать блок + готовое описание и выдавать
только findings, экономя output.

Пишет ТОЛЬКО в _experiments/qwen_enrichment/<ts>/, production не трогает.
"""
from __future__ import annotations

import argparse
import asyncio
import base64
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import httpx
from dotenv import load_dotenv
from io import BytesIO
from PIL import Image

_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(_ROOT / ".env")

import os

CHANDRA_BASE_URL = os.environ["CHANDRA_BASE_URL"].rstrip("/")
CHANDRA_USER = os.environ["NGROK_AUTH_USER"]
CHANDRA_PASS = os.environ["NGROK_AUTH_PASS"]
QWEN_MODEL = "qwen/qwen3.6-35b-a3b"

DEFAULT_PROJECT_DIR = _ROOT / "projects" / "214. Alia (ASTERUS)" / "KJ" / "13АВ-РД-КЖ5.1-К1К2 (2).pdf"
# Устанавливаются в main() из --project.
PROJECT_DIR: Path = DEFAULT_PROJECT_DIR
BLOCKS_DIR: Path = PROJECT_DIR / "_output" / "blocks"
DOCUMENT_GRAPH: Path = PROJECT_DIR / "_output" / "document_graph.json"


SYSTEM_PROMPT = """Ты — инженер-проектировщик, читающий чертёж железобетонных конструкций (раздел КЖ).
Твоя задача — СТРУКТУРИРОВАННО извлечь содержимое одного блока чертежа.

ВАЖНО:
- НЕ ищи ошибки и нарушения — это не твоя задача, её делает другая модель.
- НЕ оценивай качество проекта.
- ТОЛЬКО извлекай факты: что нарисовано, какие марки, размеры, ссылки.
- Отвечай строго JSON по указанной схеме, без преамбулы, без markdown-обёртки.

Если чего-то не видно — пустой список [] или null. Не выдумывай.
"""

USER_INSTRUCTION = """Проанализируй блок чертежа КЖ и верни JSON строго по этой схеме:

{{
  "block_type": "одно из: план_опалубки | план_армирования | разрез | сечение | ведомость_деталей | схема_расположения | таблица_условных_обозначений | узел | схема_стыковки | другое",
  "subject": "одно короткое предложение — что изображено (до 120 символов)",
  "marks": ["строки — марки арматуры/позиции (пример: '16-Г-2', 'АК1', 'ВК-3')"],
  "rebar_specs": ["строки — параметры арматуры (пример: 'Ø16 А500С шаг 200', 'Ø12 А240')"],
  "dimensions": ["строки — размеры и отметки (пример: 'Ø1200', 'h=2800', 'отм. -1,800')"],
  "references_on_block": ["строки — ссылки на другие листы/разрезы/сечения (пример: 'см. лист 7', 'сечение 1-1', 'разрез 2-2')"],
  "axes": ["строки — буквенно-цифровые оси если видно (пример: 'П.3-П.5', 'А-Б')"],
  "level_marks": ["строки — высотные отметки (пример: '-1,800', '+0,000')"],
  "concrete_class": "строка или null — класс бетона если указан (пример: 'B30')",
  "notes": "1-2 предложения — ключевое, что ВАЖНО знать другому инженеру, чтобы понять контекст блока без картинки"
}}

Контекст блока:
- block_id: {block_id}
- страница PDF: {page}
- лист (из штампа): {sheet_no}
- OCR-label из пайплайна: "{ocr_label}"

Текст, уже извлечённый OCR со страницы (используй как подсказку, но НЕ копируй в ответ — структурируй):
---
{page_text}
---
"""


def _auth_header() -> dict[str, str]:
    token = base64.b64encode(f"{CHANDRA_USER}:{CHANDRA_PASS}".encode()).decode()
    return {
        "Authorization": f"Basic {token}",
        "ngrok-skip-browser-warning": "true",
        "content-type": "application/json",
    }


def _load_page_text(graph: dict, page: int) -> str:
    """Собрать краткий текстовый контекст страницы из document_graph.json."""
    for p in graph.get("pages", []):
        if p.get("page") == page:
            parts: list[str] = []
            sheet = p.get("sheet_name") or ""
            if sheet:
                parts.append(f"[SHEET] {sheet}")
            for tb in p.get("text_blocks", [])[:20]:
                txt = (tb.get("text") or "").strip()
                if txt:
                    parts.append(txt[:500])
            full = "\n".join(parts)
            return full[:4000]  # cap
    return ""


def _load_sheet_no(graph: dict, page: int) -> str:
    for p in graph.get("pages", []):
        if p.get("page") == page:
            return str(p.get("sheet_no_normalized") or p.get("sheet_no_raw") or "")
    return ""


def _png_to_data_url(path: Path, scale: float = 1.0) -> str:
    """Читает PNG и опционально даунскейлит (для Qwen, который режет большие PNG)."""
    if scale >= 0.999:
        data = path.read_bytes()
    else:
        img = Image.open(path)
        new_size = (max(1, int(img.width * scale)), max(1, int(img.height * scale)))
        img = img.resize(new_size, Image.LANCZOS)
        buf = BytesIO()
        img.save(buf, format="PNG", optimize=True)
        data = buf.getvalue()
    b64 = base64.b64encode(data).decode()
    return f"data:image/png;base64,{b64}"


# Progressive downscale tiers — первый = без ресайза. При "Invalid image" / HTML retry опускаемся.
_IMAGE_SCALE_TIERS = [1.0, 0.6, 0.35, 0.2]


async def _post_one_attempt(
    client: httpx.AsyncClient,
    user_text: str,
    png_path: Path,
    scale: float,
    timeout: int,
) -> tuple[int, dict | None, str, int]:
    """Один attempt. Возвращает (status, parsed_json_or_none, raw_text, elapsed_ms)."""
    data_url = _png_to_data_url(png_path, scale=scale)
    payload = {
        "model": QWEN_MODEL,
        "system_prompt": SYSTEM_PROMPT,
        "input": [
            {"type": "text", "content": user_text},
            {"type": "image", "data_url": data_url},
        ],
        "temperature": 0.1,
        "max_output_tokens": 4096,
        "reasoning": "off",
        "store": False,
    }
    started = time.monotonic()
    try:
        resp = await client.post(
            f"{CHANDRA_BASE_URL}/api/v1/chat",
            headers=_auth_header(),
            json=payload,
            timeout=timeout,
        )
    except Exception as exc:
        return -1, None, f"HTTP exception: {exc}", int((time.monotonic() - started) * 1000)
    elapsed = int((time.monotonic() - started) * 1000)
    raw = resp.text
    try:
        data = resp.json()
    except Exception:
        data = None
    return resp.status_code, data, raw, elapsed


def _is_invalid_image_error(data: dict | None, raw: str) -> bool:
    if isinstance(data, dict):
        err = data.get("error")
        if isinstance(err, dict) and "Invalid image" in str(err.get("message", "")):
            return True
    return "Invalid image" in (raw or "")


def _is_ngrok_html(data: dict | None, raw: str) -> bool:
    """ngrok warning page: приходит HTML вместо JSON."""
    if data is not None:
        return False
    return raw.lstrip().lower().startswith("<!doctype") or "<html" in raw[:500].lower()


async def enrich_one_block(
    client: httpx.AsyncClient,
    block: dict,
    graph: dict,
    blocks_dir: Path,
    out_dir: Path,
    timeout: int,
) -> dict:
    block_id = block["block_id"]
    page = block["page"]
    file_name = block["file"]
    png_path = blocks_dir / file_name

    page_text = _load_page_text(graph, page)
    sheet_no = _load_sheet_no(graph, page)

    user_text = USER_INSTRUCTION.format(
        block_id=block_id,
        page=page,
        sheet_no=sheet_no or "(не определён)",
        ocr_label=block.get("ocr_label", "")[:250],
        page_text=page_text or "(текст страницы недоступен)",
    )

    meta: dict = {
        "block_id": block_id,
        "page": page,
        "sheet_no": sheet_no,
        "file": file_name,
        "size_kb": block.get("size_kb"),
        "ocr_text_len": block.get("ocr_text_len"),
        "ocr_label": block.get("ocr_label", ""),
    }

    # Retry loop: на "Invalid image" опускаем scale, на ngrok HTML — retry на том же scale.
    attempts: list[dict] = []
    ngrok_retries_left = 2
    scale_idx = 0
    last_status = 0
    last_data = None
    last_raw = ""
    total_elapsed_ms = 0

    while scale_idx < len(_IMAGE_SCALE_TIERS):
        scale = _IMAGE_SCALE_TIERS[scale_idx]
        status, data, raw, elapsed = await _post_one_attempt(
            client, user_text, png_path, scale, timeout
        )
        total_elapsed_ms += elapsed
        attempts.append({"scale": scale, "status": status, "elapsed_ms": elapsed, "raw_preview": raw[:200] if data is None else None})
        last_status, last_data, last_raw = status, data, raw

        # ngrok HTML — retry на том же scale
        if _is_ngrok_html(data, raw):
            if ngrok_retries_left > 0:
                ngrok_retries_left -= 1
                await asyncio.sleep(1.5)
                continue
            break

        # Invalid image — опускаем scale
        if status >= 400 and _is_invalid_image_error(data, raw):
            scale_idx += 1
            continue

        break  # любой успех или неизвестная ошибка
    resp_status = last_status
    data = last_data
    raw = last_raw
    elapsed_ms = total_elapsed_ms

    if resp_status < 0 or resp_status >= 400 or data is None:
        err = raw[:500] if resp_status >= 0 else raw
        fail_record = {
            **meta,
            "ok": False,
            "http_status": resp_status,
            "error": err,
            "elapsed_ms": elapsed_ms,
            "attempts": attempts,
        }
        (out_dir / f"block_{block_id}.json").write_text(
            json.dumps(fail_record, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        return fail_record

    # extract message text
    msg_parts: list[str] = []
    reason_parts: list[str] = []
    for item in data.get("output", []) or []:
        if not isinstance(item, dict):
            continue
        t = item.get("type")
        c = str(item.get("content") or "")
        if t == "message":
            msg_parts.append(c)
        elif t == "reasoning":
            reason_parts.append(c)
    text = "\n".join(msg_parts).strip()
    reasoning_text = "\n".join(reason_parts).strip()

    # parse JSON
    parsed = None
    parse_error = None
    for candidate in (text, reasoning_text):
        if not candidate:
            continue
        # try direct
        try:
            parsed = json.loads(candidate)
            break
        except json.JSONDecodeError as e:
            parse_error = str(e)
        # try extract {...}
        import re
        m = re.search(r"\{.*\}", candidate, re.DOTALL)
        if m:
            try:
                parsed = json.loads(m.group(0))
                break
            except json.JSONDecodeError as e:
                parse_error = str(e)

    stats = data.get("stats", {}) if isinstance(data, dict) else {}

    record = {
        **meta,
        "ok": parsed is not None,
        "elapsed_ms": elapsed_ms,
        "input_tokens": stats.get("input_tokens"),
        "output_tokens": stats.get("total_output_tokens"),
        "reasoning_tokens": stats.get("reasoning_output_tokens"),
        "model_instance_id": data.get("model_instance_id"),
        "raw_text_len": len(text),
        "attempts": attempts,
        "enrichment": parsed,
        "raw_text_preview": text[:400] if parsed is None else None,
        "parse_error": parse_error if parsed is None else None,
    }

    # persist per-block
    per_block_path = out_dir / f"block_{block_id}.json"
    per_block_path.write_text(json.dumps(record, ensure_ascii=False, indent=2), encoding="utf-8")

    return record


async def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--parallelism", type=int, default=2, help="concurrent Qwen requests (default 2)")
    parser.add_argument("--limit", type=int, default=0, help="process only first N blocks (0 = all)")
    parser.add_argument("--timeout", type=int, default=300, help="per-request timeout seconds")
    parser.add_argument("--only-blocks", type=str, default="", help="comma-separated block_ids to process (subset)")
    parser.add_argument("--project", type=str, default=str(DEFAULT_PROJECT_DIR), help="absolute path to project dir")
    args = parser.parse_args()

    global PROJECT_DIR, BLOCKS_DIR, DOCUMENT_GRAPH
    PROJECT_DIR = Path(args.project).resolve()
    BLOCKS_DIR = PROJECT_DIR / "_output" / "blocks"
    DOCUMENT_GRAPH = PROJECT_DIR / "_output" / "document_graph.json"

    if not BLOCKS_DIR.exists():
        print(f"blocks dir not found: {BLOCKS_DIR}", file=sys.stderr)
        return 2

    index = json.loads((BLOCKS_DIR / "index.json").read_text(encoding="utf-8"))
    graph = json.loads(DOCUMENT_GRAPH.read_text(encoding="utf-8"))

    blocks = index["blocks"]
    if args.only_blocks:
        wanted = {s.strip() for s in args.only_blocks.split(",") if s.strip()}
        blocks = [b for b in blocks if b["block_id"] in wanted]
    if args.limit:
        blocks = blocks[: args.limit]
    print(f"[plan] {len(blocks)} blocks, parallelism={args.parallelism}, model={QWEN_MODEL}")

    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    exp_root = PROJECT_DIR / "_experiments" / "qwen_enrichment" / ts
    exp_root.mkdir(parents=True, exist_ok=True)
    print(f"[out]  {exp_root}")

    sem = asyncio.Semaphore(args.parallelism)
    overall_start = time.monotonic()

    async with httpx.AsyncClient(timeout=args.timeout + 30) as client:
        async def _runner(block):
            async with sem:
                t0 = time.monotonic()
                print(f"  -> block {block['block_id']} page={block['page']} size={block.get('size_kb'):.0f}KB", flush=True)
                rec = await enrich_one_block(client, block, graph, BLOCKS_DIR, exp_root, args.timeout)
                status = "OK " if rec["ok"] else "FAIL"
                took = time.monotonic() - t0
                print(
                    f"  <- {status} block {rec['block_id']} t={took:.1f}s in={rec.get('input_tokens')} out={rec.get('output_tokens')} reason={rec.get('reasoning_tokens')}",
                    flush=True,
                )
                return rec

        results = await asyncio.gather(*(_runner(b) for b in blocks))

    overall_elapsed = time.monotonic() - overall_start

    ok_count = sum(1 for r in results if r["ok"])
    total_in = sum((r.get("input_tokens") or 0) for r in results)
    total_out = sum((r.get("output_tokens") or 0) for r in results)
    total_reason = sum((r.get("reasoning_tokens") or 0) for r in results)
    elapsed_avg = sum(r["elapsed_ms"] for r in results) / max(1, len(results)) / 1000

    summary = {
        "project": str(PROJECT_DIR.relative_to(_ROOT)),
        "timestamp": ts,
        "model": QWEN_MODEL,
        "parallelism": args.parallelism,
        "blocks_total": len(blocks),
        "blocks_ok": ok_count,
        "blocks_failed": len(blocks) - ok_count,
        "wall_clock_s": round(overall_elapsed, 1),
        "avg_per_block_s": round(elapsed_avg, 1),
        "total_input_tokens": total_in,
        "total_output_tokens": total_out,
        "total_reasoning_tokens": total_reason,
        "per_block": [
            {
                "block_id": r["block_id"],
                "page": r["page"],
                "ok": r["ok"],
                "elapsed_ms": r["elapsed_ms"],
                "input_tokens": r.get("input_tokens"),
                "output_tokens": r.get("output_tokens"),
                "reasoning_tokens": r.get("reasoning_tokens"),
                "block_type": (r.get("enrichment") or {}).get("block_type"),
                "subject": (r.get("enrichment") or {}).get("subject"),
                "error": r.get("error") if not r["ok"] else None,
            }
            for r in results
        ],
    }

    (exp_root / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    # markdown quick-view
    lines = [
        f"# Qwen Block Enrichment — {ts}",
        "",
        f"Project: `{PROJECT_DIR.name}`",
        f"Model: `{QWEN_MODEL}`  Parallelism: {args.parallelism}",
        "",
        f"**Blocks:** {ok_count}/{len(blocks)} OK  |  **Wall:** {overall_elapsed:.1f}s  |  **Avg/block:** {elapsed_avg:.1f}s",
        f"**Tokens:** input={total_in:,}  output={total_out:,}  reasoning={total_reason:,}",
        "",
        "| block_id | page | ok | t,s | in | out | block_type | subject |",
        "|---|---|---|---|---|---|---|---|",
    ]
    for r in results:
        enr = r.get("enrichment") or {}
        lines.append(
            f"| {r['block_id']} | {r['page']} | {'✓' if r['ok'] else '✗'} | "
            f"{r['elapsed_ms']/1000:.1f} | {r.get('input_tokens') or '-'} | {r.get('output_tokens') or '-'} | "
            f"{enr.get('block_type','') or ''} | {(enr.get('subject','') or '')[:80]} |"
        )
    (exp_root / "summary.md").write_text("\n".join(lines), encoding="utf-8")

    print("")
    print(f"[done] {ok_count}/{len(blocks)} OK in {overall_elapsed:.1f}s")
    print(f"[out]  {exp_root / 'summary.md'}")
    return 0 if ok_count == len(blocks) else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
