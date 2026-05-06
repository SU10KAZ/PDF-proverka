"""
Бенчмарк reasoning=medium для GPT-5.4 и GPT-5.5.
Сравниваем 4 варианта: 5.4/low (уже есть), 5.4/medium (новый), 5.5/low (уже есть), 5.5/medium (новый).

Те же 10 блоков, тот же промпт. Старые low-результаты читаем из предыдущих прогонов.
Новые medium-запросы идут параллельно (оба блока → оба medium одновременно).
"""
import asyncio
import json
import time
from pathlib import Path

import httpx
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))

from qwen_findings_only import (
    build_system_prompt,
    get_enrichment,
    load_page_text,
    png_to_data_url,
    RESPONSE_SCHEMA,
)

# ── Пути ────────────────────────────────────────────────────────────────────
PROJ_DIR = Path(
    "/home/coder/projects/PDF-proverka/projects/214. Alia (ASTERUS)"
    "/EOM/13АВ-РД-ЭМ-К3 (от 19.03.2026).pdf"
)
BLOCKS_DIR = PROJ_DIR / "_output/blocks"
GPT54_LOW_DIR = PROJ_DIR / "_output/_stage02_findings_only_runs/20260429_170359__openai_gpt-5.4_low"
GPT55_LOW_DIR = Path(__file__).parent / "benchmark_gpt55_results"

RESULTS_DIR = Path(__file__).parent / "benchmark_medium_results"
OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"
API_KEY_FILE = Path("/home/coder/projects/PDF-proverka/.env")

SELECTED_BLOCKS = [
    "9VVU-HLDA-6LV",
    "9GAF-W3LU-E9G",
    "4MFL-3QN7-XN9",
    "7XG4-9NU6-7CG",
    "6L74-AQWV-GKK",
    "HEL3-CFYW-Q6N",
    "7CX3-XNQK-3KK",
    "JY9A-6K9N-7CN",
    "CVW9-9E4P-DVL",
    "97FW-WYLN-KVJ",
]

# Цены OpenRouter за 1M токенов
PRICES = {
    "openai/gpt-5.4": {"in": 2.50, "out": 15.0},
    "openai/gpt-5.5": {"in": 5.00, "out": 30.0},
}


def load_api_key() -> str:
    for line in API_KEY_FILE.read_text().splitlines():
        if line.startswith("OPENROUTER_API_KEY="):
            return line.split("=", 1)[1].strip()
    raise RuntimeError("OPENROUTER_API_KEY not found in .env")


def load_project_data():
    pi = json.loads((PROJ_DIR / "project_info.json").read_text())
    graph = json.loads((PROJ_DIR / "_output/document_graph.json").read_text())
    blocks_index = json.loads((BLOCKS_DIR / "index.json").read_text())
    blocks_map = {b["block_id"]: b for b in blocks_index.get("blocks", [])}
    return pi, graph, blocks_map


async def call_model(
    *,
    client: httpx.AsyncClient,
    api_key: str,
    model: str,
    reasoning_effort: str,
    system_prompt: str,
    block_id: str,
    page: int,
    enrichment: dict,
    page_text: str,
    png_path: Path,
) -> dict:
    user_text = (
        f"# Блок {block_id} | страница PDF {page}\n\n"
        f"## Уже извлечённое описание блока (контекст, считай верным):\n"
        f"```json\n{json.dumps(enrichment, ensure_ascii=False, indent=2)}\n```\n\n"
        f"## Текст страницы (общие указания, спецификации и т.д.):\n"
        f"{page_text or '(недоступен)'}\n\n"
        f"## Задача:\n"
        f"Посмотри на изображение блока и верни findings[]. Только проблемы. "
        f"Не описывай что видишь. Если всё корректно — пустой массив."
    )

    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": system_prompt},
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": user_text},
                    {"type": "image_url", "image_url": {"url": png_to_data_url(png_path)}},
                ],
            },
        ],
        "temperature": 0.2,
        "max_tokens": 16000,
        "response_format": {"type": "json_schema", "json_schema": RESPONSE_SCHEMA},
        "reasoning": {"effort": reasoning_effort},
    }

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "HTTP-Referer": "https://localhost",
        "X-Title": f"stage02-benchmark-{model.split('/')[-1]}-{reasoning_effort}",
    }

    started = time.monotonic()
    try:
        resp = await client.post(OPENROUTER_URL, headers=headers, json=payload, timeout=180)
    except Exception as exc:
        return {"ok": False, "error": str(exc), "elapsed_ms": int((time.monotonic() - started) * 1000)}
    elapsed_ms = int((time.monotonic() - started) * 1000)

    if resp.status_code >= 400:
        return {"ok": False, "http_status": resp.status_code, "error": resp.text[:300], "elapsed_ms": elapsed_ms}

    data = resp.json()
    choice = (data.get("choices") or [{}])[0]
    raw = (choice.get("message") or {}).get("content") or ""
    usage = data.get("usage") or {}
    completion_details = usage.get("completion_tokens_details") or {}

    try:
        parsed = json.loads(raw) if raw else None
        parse_err = None
    except Exception as e:
        parsed = None
        parse_err = str(e)

    return {
        "ok": parsed is not None,
        "parse_error": parse_err,
        "elapsed_ms": elapsed_ms,
        "input_tokens": usage.get("prompt_tokens"),
        "output_tokens": usage.get("completion_tokens"),
        "reasoning_tokens": completion_details.get("reasoning_tokens"),
        "raw_content": raw,
        "parsed": parsed,
    }


def load_old_low(block_id: str) -> tuple[list, dict | None]:
    """Загружает старые low-результаты для GPT-5.4 и GPT-5.5."""
    # GPT-5.4 low
    p54 = GPT54_LOW_DIR / f"block_{block_id}.json"
    f54 = []
    meta54 = None
    if p54.exists():
        d = json.loads(p54.read_text())
        r = d.get("result", {})
        f54 = (r.get("parsed") or {}).get("findings", [])
        meta54 = {"input_tokens": r.get("input_tokens"), "output_tokens": r.get("output_tokens"),
                  "reasoning_tokens": r.get("reasoning_tokens"), "elapsed_ms": r.get("elapsed_ms")}

    # GPT-5.5 low
    p55 = GPT55_LOW_DIR / f"block_{block_id}.json"
    f55 = []
    meta55 = None
    if p55.exists():
        d = json.loads(p55.read_text())
        g = d.get("gpt55", {})
        f55 = g.get("findings", [])
        meta55 = {"input_tokens": g.get("input_tokens"), "output_tokens": g.get("output_tokens"),
                  "reasoning_tokens": None, "elapsed_ms": g.get("elapsed_ms")}

    return f54, meta54, f55, meta55


def cost(model: str, in_tok: int, out_tok: int) -> float:
    p = PRICES.get(model, {"in": 0, "out": 0})
    return (in_tok or 0) / 1e6 * p["in"] + (out_tok or 0) / 1e6 * p["out"]


async def main():
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    api_key = load_api_key()
    pi, graph, blocks_map = load_project_data()
    system_prompt = build_system_prompt("EOM", extended=False)
    md_cache: dict = {}

    print("Запускаем GPT-5.4/medium и GPT-5.5/medium параллельно для каждого блока")
    print(f"Блоков: {len(SELECTED_BLOCKS)}\n")

    all_results = []

    async with httpx.AsyncClient() as client:
        for i, block_id in enumerate(SELECTED_BLOCKS, 1):
            block_meta = blocks_map.get(block_id)
            if not block_meta:
                print(f"[{i:2d}] {block_id} — нет в индексе, пропуск")
                continue

            page = block_meta.get("page", 0)
            png_path = BLOCKS_DIR / block_meta["file"]
            if not png_path.exists():
                print(f"[{i:2d}] {block_id} — PNG отсутствует, пропуск")
                continue

            enrichment, src = get_enrichment(PROJ_DIR, md_cache, pi, block_id)
            if enrichment is None:
                enrichment = {}
            page_text = load_page_text(graph, page)

            kwargs = dict(
                client=client, api_key=api_key, system_prompt=system_prompt,
                block_id=block_id, page=page, enrichment=enrichment,
                page_text=page_text, png_path=png_path,
            )

            print(f"[{i:2d}] {block_id} p.{page} → 5.4/medium + 5.5/medium параллельно ...", flush=True)

            # Параллельный запуск обоих medium
            r54m, r55m = await asyncio.gather(
                call_model(model="openai/gpt-5.4", reasoning_effort="medium", **kwargs),
                call_model(model="openai/gpt-5.5", reasoning_effort="medium", **kwargs),
            )

            f54m = (r54m.get("parsed") or {}).get("findings", []) if r54m.get("ok") else []
            f55m = (r55m.get("parsed") or {}).get("findings", []) if r55m.get("ok") else []

            # Старые low результаты
            f54l, meta54l, f55l, meta55l = load_old_low(block_id)

            print(
                f"       5.4: low={len(f54l)}f  medium={len(f54m)}f ({r54m.get('elapsed_ms',0)}ms "
                f"in={r54m.get('input_tokens')} out={r54m.get('output_tokens')} reason={r54m.get('reasoning_tokens')})"
            )
            print(
                f"       5.5: low={len(f55l)}f  medium={len(f55m)}f ({r55m.get('elapsed_ms',0)}ms "
                f"in={r55m.get('input_tokens')} out={r55m.get('output_tokens')} reason={r55m.get('reasoning_tokens')})"
            )

            record = {
                "block_id": block_id,
                "page": page,
                "gpt54_low":    {"findings": f54l, **(meta54l or {})},
                "gpt54_medium": {"findings": f54m, "input_tokens": r54m.get("input_tokens"),
                                 "output_tokens": r54m.get("output_tokens"),
                                 "reasoning_tokens": r54m.get("reasoning_tokens"),
                                 "elapsed_ms": r54m.get("elapsed_ms"), "ok": r54m.get("ok")},
                "gpt55_low":    {"findings": f55l, **(meta55l or {})},
                "gpt55_medium": {"findings": f55m, "input_tokens": r55m.get("input_tokens"),
                                 "output_tokens": r55m.get("output_tokens"),
                                 "reasoning_tokens": r55m.get("reasoning_tokens"),
                                 "elapsed_ms": r55m.get("elapsed_ms"), "ok": r55m.get("ok")},
            }
            (RESULTS_DIR / f"block_{block_id}.json").write_text(
                json.dumps(record, ensure_ascii=False, indent=2)
            )
            all_results.append(record)

    # ── Сводка ──────────────────────────────────────────────────────────────
    def totals(key_prefix: str, model: str) -> dict:
        tok_in  = sum((r[key_prefix].get("input_tokens") or 0) for r in all_results)
        tok_out = sum((r[key_prefix].get("output_tokens") or 0) for r in all_results)
        tok_r   = sum((r[key_prefix].get("reasoning_tokens") or 0) for r in all_results)
        n_f     = sum(len(r[key_prefix]["findings"]) for r in all_results)
        elapsed = [r[key_prefix].get("elapsed_ms") or 0 for r in all_results]
        total_ms = sum(elapsed)
        avg_ms   = round(total_ms / len(elapsed)) if elapsed else 0
        return {"findings": n_f, "input_tokens": tok_in, "output_tokens": tok_out,
                "reasoning_tokens": tok_r, "cost_usd": round(cost(model, tok_in, tok_out), 4),
                "total_elapsed_ms": total_ms, "avg_elapsed_ms": avg_ms}

    summary = {
        "blocks_tested": len(all_results),
        "gpt54_low":    totals("gpt54_low",    "openai/gpt-5.4"),
        "gpt54_medium": totals("gpt54_medium", "openai/gpt-5.4"),
        "gpt55_low":    totals("gpt55_low",    "openai/gpt-5.5"),
        "gpt55_medium": totals("gpt55_medium", "openai/gpt-5.5"),
        "blocks": [
            {
                "block_id": r["block_id"],
                "page": r["page"],
                "54_low":    len(r["gpt54_low"]["findings"]),
                "54_medium": len(r["gpt54_medium"]["findings"]),
                "55_low":    len(r["gpt55_low"]["findings"]),
                "55_medium": len(r["gpt55_medium"]["findings"]),
            }
            for r in all_results
        ],
    }
    (RESULTS_DIR / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2))

    print("\n" + "=" * 80)
    print(f"{'Вариант':<18} {'findings':>8} {'in tok':>8} {'out tok':>8} {'reason':>8} {'cost $':>8} {'avg s':>7}")
    print("-" * 80)
    for label, key in [
        ("GPT-5.4 low",    "gpt54_low"),
        ("GPT-5.4 medium", "gpt54_medium"),
        ("GPT-5.5 low",    "gpt55_low"),
        ("GPT-5.5 medium", "gpt55_medium"),
    ]:
        t = summary[key]
        avg_s = t["avg_elapsed_ms"] / 1000
        print(f"{label:<18} {t['findings']:>8} {t['input_tokens']:>8} {t['output_tokens']:>8} "
              f"{t['reasoning_tokens']:>8} {t['cost_usd']:>8.4f} {avg_s:>6.1f}s")
    print(f"\nРезультаты: {RESULTS_DIR}")


if __name__ == "__main__":
    asyncio.run(main())
