"""
Бенчмарк GPT-5.5 vs GPT-5.4 на 10 выбранных блоках из ЭМ-К3.

Отправляет точно такой же промпт (system + user + image), что и production Stage 02.
GPT-5.4 не перегоняется — берём результаты из _stage02_findings_only_runs.
Результаты пишутся в scripts/benchmark_gpt55_results/.
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

# ── Конфигурация ────────────────────────────────────────────────────────────
PROJ_DIR = Path(
    "/home/coder/projects/PDF-proverka/projects/214. Alia (ASTERUS)"
    "/EOM/13АВ-РД-ЭМ-К3 (от 19.03.2026).pdf"
)
GPT54_RUNDIR = PROJ_DIR / "_output/_stage02_findings_only_runs/20260429_170359__openai_gpt-5.4_low"
BLOCKS_DIR = PROJ_DIR / "_output/blocks"
RESULTS_DIR = Path(__file__).parent / "benchmark_gpt55_results"

OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"
API_KEY_FILE = Path("/home/coder/projects/PDF-proverka/.env")
MODEL_55 = "openai/gpt-5.5"

# 10 отобранных блоков — разные категории и типы замечаний
SELECTED_BLOCKS = [
    "9VVU-HLDA-6LV",   # 2 КРИТИЧЕСКИХ: ТТ + заземление вторичных цепей
    "9GAF-W3LU-E9G",   # КРИТИЧЕСКОЕ: противопожарная автоматика
    "4MFL-3QN7-XN9",   # КРИТИЧЕСКОЕ: проход через строительную конструкцию
    "7XG4-9NU6-7CG",   # КРИТИЧЕСКОЕ: лоток СПЗ, PE, высотные отметки
    "6L74-AQWV-GKK",   # ЭКСПЛУАТАЦИОННОЕ: огнезащитный короб, привязки
    "HEL3-CFYW-Q6N",   # ЭКОНОМИЧЕСКОЕ: размер консоли
    "7CX3-XNQK-3KK",   # ЭКОНОМИЧЕСКОЕ: узел крепления на кровле
    "JY9A-6K9N-7CN",   # ЭКСПЛУАТАЦИОННОЕ: временный щит, незаполненные данные
    "CVW9-9E4P-DVL",   # ПРОВЕРИТЬ ПО СМЕЖНЫМ: полюсность, марка счетчика
    "97FW-WYLN-KVJ",   # ЭКСПЛУАТАЦИОННОЕ: линии СКУД без марок
]


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


async def call_gpt55(
    *,
    client: httpx.AsyncClient,
    api_key: str,
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

    messages = [
        {"role": "system", "content": system_prompt},
        {
            "role": "user",
            "content": [
                {"type": "text", "text": user_text},
                {"type": "image_url", "image_url": {"url": png_to_data_url(png_path)}},
            ],
        },
    ]

    payload = {
        "model": MODEL_55,
        "messages": messages,
        "temperature": 0.2,
        "max_tokens": 32768,
        "response_format": {"type": "json_schema", "json_schema": RESPONSE_SCHEMA},
        "reasoning": {"effort": "low"},
    }

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "HTTP-Referer": "https://localhost",
        "X-Title": "stage02-benchmark-gpt55",
    }

    started = time.monotonic()
    resp = await client.post(OPENROUTER_URL, headers=headers, json=payload, timeout=120)
    elapsed_ms = int((time.monotonic() - started) * 1000)

    if resp.status_code >= 400:
        return {"ok": False, "http_status": resp.status_code, "error": resp.text[:500], "elapsed_ms": elapsed_ms}

    data = resp.json()
    choice = (data.get("choices") or [{}])[0]
    msg = choice.get("message") or {}
    raw = msg.get("content") or ""
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


def load_gpt54_result(block_id: str) -> dict | None:
    path = GPT54_RUNDIR / f"block_{block_id}.json"
    if not path.exists():
        return None
    return json.loads(path.read_text())


async def main():
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    api_key = load_api_key()
    pi, graph, blocks_map = load_project_data()
    system_prompt = build_system_prompt("EOM", extended=False)
    md_cache: dict = {}

    print(f"Модель: {MODEL_55}")
    print(f"Блоков: {len(SELECTED_BLOCKS)}")
    print(f"Результаты: {RESULTS_DIR}\n")

    summary_rows = []

    async with httpx.AsyncClient() as client:
        for i, block_id in enumerate(SELECTED_BLOCKS, 1):
            block_meta = blocks_map.get(block_id)
            if block_meta is None:
                print(f"[{i:2d}] {block_id} — НЕТ в blocks/index.json, пропуск")
                continue

            page = block_meta.get("page", 0)
            png_path = BLOCKS_DIR / block_meta["file"]
            if not png_path.exists():
                print(f"[{i:2d}] {block_id} — PNG отсутствует: {png_path.name}, пропуск")
                continue

            enrichment, src = get_enrichment(PROJ_DIR, md_cache, pi, block_id)
            if enrichment is None:
                enrichment = {}
            page_text = load_page_text(graph, page)

            print(f"[{i:2d}] {block_id} p.{page} enrichment={src} img={block_meta['file']} ...", end=" ", flush=True)

            result_55 = await call_gpt55(
                client=client,
                api_key=api_key,
                system_prompt=system_prompt,
                block_id=block_id,
                page=page,
                enrichment=enrichment,
                page_text=page_text,
                png_path=png_path,
            )

            findings_55 = (result_55.get("parsed") or {}).get("findings", []) if result_55.get("ok") else []
            n55 = len(findings_55)

            # Загружаем GPT-5.4 результат
            gpt54_raw = load_gpt54_result(block_id)
            findings_54 = []
            if gpt54_raw:
                findings_54 = (gpt54_raw.get("result", {}).get("parsed") or {}).get("findings", [])
            n54 = len(findings_54)

            status = "OK" if result_55.get("ok") else f"ERR:{result_55.get('error','?')[:40]}"
            print(f"{status} | 5.4={n54}f | 5.5={n55}f | {result_55.get('elapsed_ms',0)}ms "
                  f"in={result_55.get('input_tokens')} out={result_55.get('output_tokens')}")

            out = {
                "block_id": block_id,
                "page": page,
                "enrichment_source": src,
                "gpt54": {
                    "findings": findings_54,
                    "input_tokens": gpt54_raw["result"].get("input_tokens") if gpt54_raw else None,
                    "output_tokens": gpt54_raw["result"].get("output_tokens") if gpt54_raw else None,
                    "elapsed_ms": gpt54_raw["result"].get("elapsed_ms") if gpt54_raw else None,
                },
                "gpt55": {
                    "ok": result_55.get("ok"),
                    "findings": findings_55,
                    "input_tokens": result_55.get("input_tokens"),
                    "output_tokens": result_55.get("output_tokens"),
                    "elapsed_ms": result_55.get("elapsed_ms"),
                    "parse_error": result_55.get("parse_error"),
                },
            }
            out_path = RESULTS_DIR / f"block_{block_id}.json"
            out_path.write_text(json.dumps(out, ensure_ascii=False, indent=2))

            summary_rows.append({
                "block_id": block_id,
                "page": page,
                "gpt54_findings": n54,
                "gpt55_findings": n55,
                "gpt54_in": gpt54_raw["result"].get("input_tokens") if gpt54_raw else None,
                "gpt54_out": gpt54_raw["result"].get("output_tokens") if gpt54_raw else None,
                "gpt55_in": result_55.get("input_tokens"),
                "gpt55_out": result_55.get("output_tokens"),
                "gpt55_ok": result_55.get("ok"),
                "gpt55_elapsed_ms": result_55.get("elapsed_ms"),
            })

    # Считаем стоимость
    total_54_in = sum(r["gpt54_in"] or 0 for r in summary_rows)
    total_54_out = sum(r["gpt54_out"] or 0 for r in summary_rows)
    total_55_in = sum(r["gpt55_in"] or 0 for r in summary_rows)
    total_55_out = sum(r["gpt55_out"] or 0 for r in summary_rows)

    cost_54 = total_54_in / 1e6 * 2.50 + total_54_out / 1e6 * 15.0
    cost_55 = total_55_in / 1e6 * 5.00 + total_55_out / 1e6 * 30.0

    summary = {
        "model_54": "openai/gpt-5.4",
        "model_55": MODEL_55,
        "blocks_tested": len(summary_rows),
        "totals": {
            "gpt54_findings": sum(r["gpt54_findings"] for r in summary_rows),
            "gpt55_findings": sum(r["gpt55_findings"] for r in summary_rows),
            "gpt54_tokens_in": total_54_in,
            "gpt54_tokens_out": total_54_out,
            "gpt55_tokens_in": total_55_in,
            "gpt55_tokens_out": total_55_out,
            "gpt54_cost_usd": round(cost_54, 4),
            "gpt55_cost_usd": round(cost_55, 4),
            "cost_ratio": round(cost_55 / cost_54, 2) if cost_54 > 0 else None,
        },
        "blocks": summary_rows,
    }
    summary_path = RESULTS_DIR / "summary.json"
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2))

    print("\n" + "=" * 60)
    print(f"Итого блоков: {len(summary_rows)}")
    print(f"GPT-5.4 findings: {summary['totals']['gpt54_findings']}")
    print(f"GPT-5.5 findings: {summary['totals']['gpt55_findings']}")
    print(f"GPT-5.4 cost: ${cost_54:.4f}")
    print(f"GPT-5.5 cost: ${cost_55:.4f}")
    print(f"Коэффициент стоимости: {summary['totals']['cost_ratio']}x")
    print(f"Результаты: {RESULTS_DIR}")


if __name__ == "__main__":
    asyncio.run(main())
