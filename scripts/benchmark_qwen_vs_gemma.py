"""
Сравнение Qwen3.6 35B-A3B (без reasoning) vs Gemma4 26B-A4B на 10 блоках.
Запуск:
    python scripts/benchmark_qwen_vs_gemma.py
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
from webapp.services import lms_service
import httpx

PROJECT = Path("projects/214. Alia (ASTERUS)/KJ/13АВ-РД-КЖ7.2-К2 (Изм.2).pdf")

TEST_BLOCKS = [
    "7DUT-L6PU-4HU",
    "6QTN-HNRL-JGG",
    "7TWM-HGGR-D7W",
    "9P9L-D9MT-D6T",
    "7NPH-AAXX-444",
    "7Q4V-VHF7-HYA",
    "6LXG-K4CH-7H7",
    "7FXC-9LVU-YUA",
    "AAPH-HKQ7-AKA",
    "A6AN-CCTF-PGJ",
]

MODELS = [
    {"key": "qwen/qwen3.6-35b-a3b", "label": "Qwen3.6 35B-A3B (no reasoning)"},
    {"key": "google/gemma-4-26b-a4b", "label": "Gemma4 26B-A4B"},
]

CONTEXT_LENGTH = 16000
MAX_OUTPUT_TOKENS = 4096


def unload_all():
    client = lms_service._client()
    unloaded = []
    for handle in client.llm.list_loaded():
        try:
            handle.unload()
            unloaded.append(handle.identifier)
        except Exception as e:
            print(f"  [warn] не удалось выгрузить {handle.identifier}: {e}")
    lms_service.invalidate_loaded_cache()
    for _ in range(10):
        time.sleep(1)
        if not list(lms_service._client().llm.list_loaded()):
            break
    return unloaded


def load_model(key: str) -> bool:
    try:
        lms_service.load_model(key, context_length=CONTEXT_LENGTH)
        time.sleep(2)
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


async def run_block(client, base_url, model_key, block_id, index, graph, blocks_dir):
    block = index.get(block_id)
    if not block:
        return {"block_id": block_id, "elapsed": 0, "status": "no_index", "source": "", "block_type": "", "subject": "", "content_len": 0, "reasoning_len": 0}

    page = block.get("page", 0)
    png_path = blocks_dir / f"block_{block_id}.png"
    if not png_path.exists():
        return {"block_id": block_id, "elapsed": 0, "status": "no_png", "source": "", "block_type": "", "subject": "", "content_len": 0, "reasoning_len": 0}

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
    }


async def benchmark_model(model_key, label):
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
            icon = "✓" if r["status"] == "ok" else ("~" if r["status"] == "template" else "✗")
            clen = r.get("content_len", 0)
            rlen = r.get("reasoning_len", 0)
            print(f" {icon} {r['elapsed']}s | src={r['source']} | c={clen} r={rlen} | {r['block_type']} | {r.get('subject','')[:45]}")
    return results


def print_summary(all_results):
    n = len(TEST_BLOCKS)
    print(f"\n{'='*95}")
    hdr = f"{'МОДЕЛЬ':<40} {'OK':^5} {'avg':^6} {'min':^5} {'max':^5} " + " ".join(f"B{i+1:02d}" for i in range(n))
    print(hdr)
    print(f"{'-'*95}")
    for entry in all_results:
        label = entry["label"][:39]
        blocks = entry["blocks"]
        statuses = []
        times = []
        for b in blocks:
            s = b.get("status", "?")
            statuses.append("O" if s == "ok" else ("T" if s == "template" else "F"))
            times.append(b.get("elapsed", 0))
        ok_count = statuses.count("O")
        avg_t = f"{sum(times)/len(times):.1f}s" if times else "—"
        min_t = f"{min(times):.1f}s" if times else "—"
        max_t = f"{max(times):.1f}s" if times else "—"
        status_str = " ".join(statuses)
        print(f"{label:<40} {ok_count:^5} {avg_t:^6} {min_t:^5} {max_t:^5} {status_str}")
    print(f"{'='*95}")

    # Детальное сравнение по блокам
    if len(all_results) == 2:
        print(f"\n── Сравнение по блокам (content_len / reasoning_len / время) ──")
        a, b = all_results[0], all_results[1]
        print(f"{'Блок':<18} {'':^3} {a['label'][:28]:<28} {'':^3} {b['label'][:28]:<28}")
        print(f"{'-'*95}")
        for i, block_id in enumerate(TEST_BLOCKS):
            ab = a["blocks"][i] if i < len(a["blocks"]) else {}
            bb = b["blocks"][i] if i < len(b["blocks"]) else {}
            a_icon = "✓" if ab.get("status") == "ok" else ("~" if ab.get("status") == "template" else "✗")
            b_icon = "✓" if bb.get("status") == "ok" else ("~" if bb.get("status") == "template" else "✗")
            a_str = f"{a_icon} {ab.get('elapsed',0):.1f}s c={ab.get('content_len',0)} r={ab.get('reasoning_len',0)}"
            b_str = f"{b_icon} {bb.get('elapsed',0):.1f}s c={bb.get('content_len',0)} r={bb.get('reasoning_len',0)}"
            print(f"{block_id:<18}     {a_str:<28}     {b_str:<28}")
        print()


async def main():
    print(f"Сравнение: Qwen3.6 35B-A3B (без reasoning) vs Gemma4 26B-A4B")
    print(f"Блоков: {len(TEST_BLOCKS)}, проект: КЖ7.2-К2\n")

    all_results = []
    for model_info in MODELS:
        key = model_info["key"]
        label = model_info["label"]

        print(f"\n{'─'*65}")
        print(f"  {label}")
        print(f"  key: {key}")

        print("  → Выгружаю все модели...", flush=True, end="")
        unloaded = unload_all()
        print(f" выгружено: {unloaded}")

        await asyncio.sleep(2)

        print(f"  → Загружаю {key} (ctx={CONTEXT_LENGTH})...", flush=True, end="")
        ok = load_model(key)
        if not ok:
            print(f"\n  [SKIP] не удалось загрузить")
            all_results.append({"label": label, "key": key, "blocks": []})
            continue
        print(" OK")

        await asyncio.sleep(3)

        blocks = await benchmark_model(key, label)
        all_results.append({"label": label, "key": key, "blocks": blocks})

    print_summary(all_results)

    # Сохраняем результаты
    out_path = Path("scripts/benchmark_qwen_vs_gemma_results.json")
    out_path.write_text(json.dumps(all_results, ensure_ascii=False, indent=2))
    print(f"Результаты сохранены: {out_path}")


if __name__ == "__main__":
    asyncio.run(main())
