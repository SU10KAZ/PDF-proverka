"""
Бенчмарк OCR-качества vision-моделей на 2 сложных блоках.
Для каждой модели: выгружает текущую, загружает новую, прогоняет 2 блока, выводит результат.

Запуск:
    python scripts/model_ocr_benchmark.py
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
    SYSTEM_PROMPT, USER_INSTRUCTION, DEFAULT_TIMEOUT_S,
)
from webapp.services import lms_service
import httpx

PROJECT = Path("projects/214. Alia (ASTERUS)/KJ/13АВ-РД-КЖ7.2-К2 (Изм.2).pdf")

# 10 блоков разных типов равномерно по страницам проекта
TEST_BLOCKS = [
    "7DUT-L6PU-4HU",  # p=8  схема расположения секций
    "6QTN-HNRL-JGG",  # p=9  вертикальный разрез с отметками
    "7TWM-HGGR-D7W",  # p=9  длинный вертикальный разрез
    "9P9L-D9MT-D6T",  # p=10 узел армирования марша
    "7NPH-AAXX-444",  # p=11 армирование площадки
    "7Q4V-VHF7-HYA",  # p=12 схемы армирования маршей
    "6LXG-K4CH-7H7",  # p=13 разрез лестничной клетки
    "7FXC-9LVU-YUA",  # p=14 узел армирования
    "AAPH-HKQ7-AKA",  # p=15 армирование площадки
    "A6AN-CCTF-PGJ",  # p=16 схемы гнутых элементов
]

# Топ-7 моделей по результатам первого теста
MODELS = [
    {"key": "qwen/qwen3.6-35b-a3b",       "label": "Qwen3.6 35B-A3B"},
    {"key": "qwen/qwen3.6-27b",            "label": "Qwen3.6 27B"},
    {"key": "nvidia/nemotron-3-nano-omni", "label": "Nemotron Nano Omni 30B-A3B"},
    {"key": "google/gemma-4-31b",          "label": "Gemma4 31B"},
    {"key": "google/gemma-4-26b-a4b",      "label": "Gemma4 26B-A4B"},
    {"key": "qwen/qwen3.5-35b-a3b",       "label": "Qwen3.5 35B-A3B"},
    {"key": "qwen/qwen3.5-9b",            "label": "Qwen3.5 9B"},
    {"key": "mradermacher/holo2-8b",       "label": "Holo2 8B"},
]

CONTEXT_LENGTH = 16000
MAX_OUTPUT_TOKENS = 4096


def unload_all():
    """Выгружает все загруженные LLM-модели."""
    client = lms_service._client()
    unloaded = []
    for handle in client.llm.list_loaded():
        try:
            handle.unload()
            unloaded.append(handle.identifier)
        except Exception as e:
            print(f"  [warn] не удалось выгрузить {handle.identifier}: {e}")
    lms_service.invalidate_loaded_cache()
    # Ждём пока все выгрузятся
    import time
    for _ in range(10):
        time.sleep(1)
        if not list(lms_service._client().llm.list_loaded()):
            break
    return unloaded


def load_model(key: str) -> bool:
    try:
        lms_service.load_model(key, context_length=CONTEXT_LENGTH)
        # Проверяем что загрузилась с нужным контекстом
        import time; time.sleep(2)
        for handle in lms_service._client().llm.list_loaded():
            if handle.identifier.startswith(key.split("@")[0]):
                ctx = getattr(handle, "context_length", None) or (
                    handle.config.get("context_length") if hasattr(handle, "config") else None
                )
                print(f" [ctx={ctx}]", end="")
        return True
    except Exception as e:
        print(f"\n  [error] загрузка {key}: {e}")
        return False


def is_template(result: dict | None) -> bool:
    if not result:
        return True
    markers = ("план|план_армирования", "до 120 симв", "марки/позиции", "<120 chars")
    for f in ("block_type", "subject"):
        v = str(result.get(f, ""))
        if any(m in v for m in markers):
            return True
    return False


async def run_block(client: httpx.AsyncClient, base_url: str, model_key: str,
                    block_id: str, index: dict, graph: dict, blocks_dir: Path) -> dict:
    block = index.get(block_id)
    if not block:
        return {"block_id": block_id, "status": "no_index"}

    page = block.get("page", 0)
    png_path = blocks_dir / f"block_{block_id}.png"
    if not png_path.exists():
        return {"block_id": block_id, "status": "no_png"}

    page_text = _load_page_text(graph, page)
    sheet_no = _load_sheet_no(graph, page)
    ocr_label = block.get("ocr_label", "")

    user_text = USER_INSTRUCTION.format(
        block_id=block_id, page=page, sheet_no=sheet_no,
        ocr_label=ocr_label, page_text=page_text,
    )

    t0 = time.monotonic()
    status_code, data, raw, elapsed_ms = await _qwen_call_attempt(
        client, base_url, user_text, png_path,
        scale=1.0, model=model_key,
        timeout=DEFAULT_TIMEOUT_S,
        max_output_tokens=MAX_OUTPUT_TOKENS,
    )
    elapsed = time.monotonic() - t0

    content_text, reasoning_text, finish_reason = _extract_response_texts(data)
    result, err, source, partial = _parse_qwen_payload(
        content_text=content_text,
        reasoning_text=reasoning_text,
        finish_reason=finish_reason,
    )

    tpl = is_template(result)
    return {
        "block_id": block_id,
        "elapsed": round(elapsed, 1),
        "status": "template" if tpl else ("fail" if result is None else "ok"),
        "source": source,
        "content_len": len(content_text or ""),
        "reasoning_len": len(reasoning_text or ""),
        "finish_reason": finish_reason,
        "block_type": result.get("block_type", "") if result else "",
        "subject": (result.get("subject") or "")[:70] if result else "",
        "marks": result.get("marks", []) if result else [],
        "rebar_specs": result.get("rebar_specs", []) if result else [],
        "dimensions": result.get("dimensions", []) if result else [],
        "references_on_block": result.get("references_on_block", []) if result else [],
        "axes": result.get("axes", []) if result else [],
        "level_marks": result.get("level_marks", []) if result else [],
        "concrete_class": result.get("concrete_class") if result else None,
        "notes": (result.get("notes") or "")[:200] if result else "",
        "err": err or "",
    }


async def benchmark_model(model_key: str, label: str):
    base_url = os.environ["CHANDRA_BASE_URL"]
    out_dir = PROJECT / "_output"
    graph = json.loads((out_dir / "document_graph.json").read_text())
    idx_data = json.loads((out_dir / "blocks_qwen_100" / "index.json").read_text())
    index = {b["block_id"]: b for b in idx_data.get("blocks", idx_data)}
    blocks_dir = out_dir / "blocks_qwen_100"

    results = []
    async with httpx.AsyncClient(timeout=DEFAULT_TIMEOUT_S + 30) as client:
        for block_id in TEST_BLOCKS:
            print(f"    [{block_id}] ...", flush=True, end="")
            r = await run_block(client, base_url, model_key, block_id, index, graph, blocks_dir)
            results.append(r)
            status_icon = "✓" if r["status"] == "ok" else ("~" if r["status"] == "template" else "✗")
            m = len(r.get("marks") or [])
            d = len(r.get("dimensions") or [])
            l = len(r.get("level_marks") or [])
            rb = len(r.get("rebar_specs") or [])
            ref = len(r.get("references_on_block") or [])
            cc = r.get("concrete_class") or "-"
            print(f" {status_icon} {r['elapsed']}s | {r['block_type'][:12]} | m={m} d={d} l={l} r={rb} ref={ref} cc={cc}")
    return results


def print_summary(all_results: list[dict]):
    n = len(TEST_BLOCKS)
    print(f"\n{'='*100}")
    hdr = f"{'МОДЕЛЬ':<38} {'OK':^5} {'avg':^6} {'marks':^6} {'dims':^6} {'rebar':^6} {'levels':^6} {'refs':^6} " + " ".join(f"B{i+1:02d}" for i in range(n))
    print(hdr)
    print(f"{'-'*100}")
    for entry in all_results:
        label = entry["label"][:37]
        blocks = entry["blocks"]
        statuses, times = [], []
        total_m = total_d = total_r = total_l = total_ref = ok_count = 0
        for b in blocks:
            s = b.get("status", "?")
            statuses.append("O" if s == "ok" else ("T" if s == "template" else "F"))
            times.append(b.get("elapsed", 0))
            if s == "ok":
                ok_count += 1
                total_m   += len(b.get("marks") or [])
                total_d   += len(b.get("dimensions") or [])
                total_r   += len(b.get("rebar_specs") or [])
                total_l   += len(b.get("level_marks") or [])
                total_ref += len(b.get("references_on_block") or [])
        avg_t = f"{sum(times)/len(times):.1f}s" if times else "—"
        k = max(ok_count, 1)
        status_str = " ".join(statuses)
        print(f"{label:<38} {ok_count:^5} {avg_t:^6} {total_m//k:^6} {total_d//k:^6} {total_r//k:^6} {total_l//k:^6} {total_ref//k:^6} {status_str}")
    print(f"{'='*100}")


async def main():
    all_results = []

    print(f"Тестовые блоки: {TEST_BLOCKS}")
    print(f"Моделей: {len(MODELS)}\n")

    for model_info in MODELS:
        key = model_info["key"]
        label = model_info["label"]

        print(f"\n{'─'*60}")
        print(f"  {label}")
        print(f"  key: {key}")

        # Выгружаем все
        print("  → Выгружаю все модели...", flush=True, end="")
        unloaded = unload_all()
        print(f" выгружено: {unloaded}")

        # Ждём немного после выгрузки
        await asyncio.sleep(2)

        # Загружаем нужную
        print(f"  → Загружаю {key} (ctx={CONTEXT_LENGTH})...", flush=True, end="")
        ok = load_model(key)
        if not ok:
            print(f"\n  [SKIP] не удалось загрузить")
            all_results.append({"label": label, "blocks": [{"status": "load_failed", "elapsed": 0, "source": "", "block_type": "", "subject": ""}]})
            continue
        print(" OK")

        # Даём время модели загрузиться
        await asyncio.sleep(3)

        # Прогоняем блоки
        blocks = await benchmark_model(key, label)
        all_results.append({"label": label, "key": key, "blocks": blocks})

    print_summary(all_results)

    # Сохраняем результаты
    out_path = Path("/tmp/model_ocr_benchmark.json")
    out_path.write_text(json.dumps(all_results, ensure_ascii=False, indent=2))
    print(f"\nРезультаты: {out_path}")


asyncio.run(main())
