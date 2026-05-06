"""
Сравнение Stage 02 findings при разных источниках enrichment.

Шаг 1: Прогоняем Gemma4 26B-A4B и Qwen3.6 35B-A3B на 10 блоках → полный enrichment JSON.
Шаг 2: Для каждого блока вызываем GPT Stage 02 с каждым enrichment.
Шаг 3: Выводим сравнение findings.

Запуск:
    python scripts/stage02_enrichment_compare.py
"""
import asyncio
import json
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from dotenv import load_dotenv
load_dotenv()

from qwen_enrich import (
    _qwen_call_attempt, _extract_response_texts, _parse_qwen_payload,
    _load_page_text, _load_sheet_no,
    USER_INSTRUCTION, DEFAULT_TIMEOUT_S,
)
from qwen_findings_only import call_gpt_for_block, build_system_prompt
from webapp.services import lms_service
import httpx

PROJECT = Path("projects/214. Alia (ASTERUS)/KJ/13АВ-РД-КЖ7.2-К2 (Изм.2).pdf")

TEST_BLOCKS = [
    "7DUT-L6PU-4HU", "6QTN-HNRL-JGG", "7TWM-HGGR-D7W", "9P9L-D9MT-D6T",
    "7NPH-AAXX-444", "7Q4V-VHF7-HYA", "6LXG-K4CH-7H7", "7FXC-9LVU-YUA",
    "AAPH-HKQ7-AKA", "A6AN-CCTF-PGJ",
]

OCR_MODELS = [
    {"key": "google/gemma-4-26b-a4b", "label": "Gemma4 26B-A4B"},
    {"key": "qwen/qwen3.6-35b-a3b",   "label": "Qwen3.6 35B-A3B"},
]

CONTEXT_LENGTH = 16000
MAX_OUTPUT_TOKENS = 4096
GPT_MODEL = "openai/gpt-5.4"
GPT_MAX_TOKENS = 4000
GPT_REASONING = "low"
SECTION = "KJ"


def unload_all():
    client = lms_service._client()
    for handle in client.llm.list_loaded():
        try:
            handle.unload()
        except Exception:
            pass
    lms_service.invalidate_loaded_cache()
    import time; time.sleep(3)


def load_model(key: str):
    import time
    for attempt in range(3):
        try:
            lms_service.load_model(key, context_length=CONTEXT_LENGTH)
            time.sleep(5)
            return
        except Exception as e:
            print(f"\n  [warn] попытка {attempt+1}/3 загрузки {key}: {e}")
            time.sleep(10)
    raise RuntimeError(f"Не удалось загрузить {key} за 3 попытки")


async def get_enrichment(client: httpx.AsyncClient, base_url: str, model_key: str,
                         block: dict, graph: dict, blocks_dir: Path) -> dict | None:
    block_id = block["block_id"]
    page = block.get("page", 0)
    png_path = blocks_dir / f"block_{block_id}.png"
    if not png_path.exists():
        return None

    user_text = USER_INSTRUCTION.format(
        block_id=block_id, page=page,
        sheet_no=_load_sheet_no(graph, page),
        ocr_label=block.get("ocr_label", ""),
        page_text=_load_page_text(graph, page),
    )

    _, data, _, _ = await _qwen_call_attempt(
        client, base_url, user_text, png_path,
        scale=1.0, model=model_key,
        timeout=DEFAULT_TIMEOUT_S,
        max_output_tokens=MAX_OUTPUT_TOKENS,
    )
    ct, rt, fr = _extract_response_texts(data)
    result, _, _, _ = _parse_qwen_payload(content_text=ct, reasoning_text=rt, finish_reason=fr)
    return result


async def run_ocr_pass(model_key: str, label: str, index: dict, graph: dict,
                       blocks_dir: Path) -> dict[str, dict]:
    base_url = os.environ["CHANDRA_BASE_URL"]
    enrichments = {}

    print(f"\n  Выгружаю все модели...", end="", flush=True)
    unload_all()
    print(f" OK")
    print(f"  Загружаю {label}...", end="", flush=True)
    load_model(model_key)
    print(f" OK")

    async with httpx.AsyncClient(timeout=DEFAULT_TIMEOUT_S + 30) as client:
        for block_id in TEST_BLOCKS:
            block = index.get(block_id)
            if not block:
                continue
            print(f"    [{block_id}] ...", end="", flush=True)
            t0 = time.monotonic()
            result = await get_enrichment(client, base_url, model_key, block, graph, blocks_dir)
            elapsed = time.monotonic() - t0
            if result:
                # Проверяем на шаблон
                is_tpl = any(m in str(result.get(f, "")) for f in ("block_type", "subject")
                             for m in ("план|план_армирования", "до 120 симв", "до 5 ключ"))
                status = "~TPL" if is_tpl else "✓"
                bt = result.get("block_type", "?")[:15]
                marks = len(result.get("marks") or [])
                dims = len(result.get("dimensions") or [])
                levels = len(result.get("level_marks") or [])
                rebar = len(result.get("rebar_specs") or [])
                print(f" {status} {elapsed:.1f}s | {bt} | m={marks} d={dims} l={levels} r={rebar}")
                if not is_tpl:
                    enrichments[block_id] = result
            else:
                print(f" ✗ FAIL {elapsed:.1f}s")

    return enrichments


async def run_stage02(enrichments_by_model: dict[str, dict[str, dict]],
                      index: dict, graph: dict, blocks_dir_stage02: Path) -> dict:
    """Для каждого блока прогоняем GPT с enrichment от каждой модели."""
    api_key = os.environ.get("OPENROUTER_API_KEY", "")
    system_prompt = build_system_prompt(SECTION, extended=True)
    results = {}  # block_id -> {model_label -> findings}

    print(f"\n{'='*60}")
    print(f"STAGE 02: GPT анализ блоков")
    print(f"{'='*60}")

    async with httpx.AsyncClient(timeout=120) as client:
        for block_id in TEST_BLOCKS:
            block = index.get(block_id)
            if not block:
                continue
            page = block.get("page", 0)
            page_text = _load_page_text(graph, page)
            results[block_id] = {}

            print(f"\n  [{block_id}] стр.{page} | {block.get('ocr_label','')[:50]}")

            for model_label, enrichments in enrichments_by_model.items():
                enrichment = enrichments.get(block_id)
                if not enrichment:
                    print(f"    [{model_label}] нет enrichment — пропуск")
                    results[block_id][model_label] = None
                    continue

                print(f"    [{model_label}] GPT...", end="", flush=True)
                t0 = time.monotonic()
                r = await call_gpt_for_block(
                    client, block, enrichment, page_text, blocks_dir_stage02,
                    api_key=api_key,
                    model=GPT_MODEL,
                    reasoning_effort=GPT_REASONING,
                    max_tokens=GPT_MAX_TOKENS,
                    system_prompt=system_prompt,
                    timeout=120,
                )
                elapsed = time.monotonic() - t0
                findings = r.get("findings", []) if r.get("ok") else []
                results[block_id][model_label] = findings
                n = len(findings)
                sevs = [f.get("severity", "?")[:5] for f in findings]
                print(f" {elapsed:.1f}s | {n} findings: {sevs}")

    return results


def print_comparison(results: dict, enrichments_by_model: dict):
    print(f"\n{'='*80}")
    print(f"ИТОГОВОЕ СРАВНЕНИЕ")
    print(f"{'='*80}")

    model_labels = list(enrichments_by_model.keys())

    total_findings = {label: 0 for label in model_labels}
    total_blocks = {label: 0 for label in model_labels}

    for block_id in TEST_BLOCKS:
        block_results = results.get(block_id, {})
        if not any(block_results.values()):
            continue

        print(f"\n  [{block_id}]")
        for label in model_labels:
            findings = block_results.get(label) or []
            total_findings[label] += len(findings)
            if findings is not None:
                total_blocks[label] += 1
            sevs = ", ".join(f.get("severity", "?")[:4] for f in findings) or "—"
            cats = ", ".join(f.get("category", "?") for f in findings[:3])
            print(f"    {label:<25} {len(findings):>2} findings | {sevs[:60]}")
            if findings:
                print(f"    {'':25} категории: {cats}")

    print(f"\n{'─'*60}")
    print(f"ИТОГО findings по моделям:")
    for label in model_labels:
        n = total_findings[label]
        b = total_blocks[label]
        avg = n / b if b else 0
        print(f"  {label:<25} {n:>3} findings в {b} блоках (avg {avg:.1f}/блок)")

    # Сохраняем
    out = {
        "enrichments": {k: {bid: e for bid, e in v.items()} for k, v in enrichments_by_model.items()},
        "stage02_results": results,
        "summary": {label: {"total": total_findings[label], "blocks": total_blocks[label]} for label in model_labels},
    }
    out_path = Path("/tmp/stage02_compare.json")
    out_path.write_text(json.dumps(out, ensure_ascii=False, indent=2))
    print(f"\nРезультаты: {out_path}")


async def main():
    out_dir = PROJECT / "_output"
    graph = json.loads((out_dir / "document_graph.json").read_text())
    idx_data = json.loads((out_dir / "blocks_qwen_100" / "index.json").read_text())
    index = {b["block_id"]: b for b in idx_data.get("blocks", idx_data)}
    blocks_dir_100 = out_dir / "blocks_qwen_100"
    blocks_dir_stage02 = out_dir / "blocks_stage02_100"
    if not blocks_dir_stage02.exists():
        blocks_dir_stage02 = blocks_dir_100  # fallback

    print(f"Проект: {PROJECT.name}")
    print(f"Блоков: {len(TEST_BLOCKS)}")
    print(f"OCR моделей: {len(OCR_MODELS)}")
    print(f"Stage 02 модель: {GPT_MODEL}")

    # Шаг 1: OCR enrichment от каждой модели
    checkpoint_path = Path("/tmp/stage02_enrichments_checkpoint.json")
    if checkpoint_path.exists():
        print(f"\nЗагружаю checkpoint: {checkpoint_path}")
        enrichments_by_model = json.loads(checkpoint_path.read_text())
        print(f"  Загружено моделей: {list(enrichments_by_model.keys())}")
    else:
        enrichments_by_model = {}

    for m in OCR_MODELS:
        if m["label"] in enrichments_by_model:
            print(f"\nПропускаю {m['label']} — уже есть в checkpoint ({len(enrichments_by_model[m['label']])} блоков)")
            continue
        print(f"\n{'='*60}")
        print(f"OCR: {m['label']}")
        print(f"{'='*60}")
        enrichments = await run_ocr_pass(
            m["key"], m["label"], index, graph, blocks_dir_100
        )
        enrichments_by_model[m["label"]] = enrichments
        print(f"  Итого enrichment: {len(enrichments)}/{len(TEST_BLOCKS)}")
        # Сохраняем checkpoint после каждой модели
        checkpoint_path.write_text(json.dumps(enrichments_by_model, ensure_ascii=False, indent=2))
        print(f"  Checkpoint сохранён: {checkpoint_path}")

    # Выгружаем OCR модели
    print(f"\nВыгружаю OCR модели...", end="", flush=True)
    unload_all()
    print(" OK")

    # Шаг 2: Stage 02 GPT
    results = await run_stage02(enrichments_by_model, index, graph, blocks_dir_stage02)

    # Шаг 3: Сравнение
    print_comparison(results, enrichments_by_model)


asyncio.run(main())
