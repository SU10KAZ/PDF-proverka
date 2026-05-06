"""
Чистый бенчмарк: все 4 варианта прогоняются заново с идентичными условиями.
5.4/low, 5.4/medium, 5.5/low, 5.5/medium — один и тот же payload кроме model+effort.

Параметры:
  temperature=0.2, max_tokens=16000, response_format=json_schema
  reasoning={"effort": "low"} или {"effort": "medium"}

Для каждого блока запускаем все 4 варианта параллельно.
"""
import asyncio
import json
import time
from pathlib import Path
import sys

import httpx

sys.path.insert(0, str(Path(__file__).parent.parent))

from qwen_findings_only import (
    build_system_prompt,
    get_enrichment,
    load_page_text,
    png_to_data_url,
    RESPONSE_SCHEMA,
)

PROJ_DIR = Path(
    "/home/coder/projects/PDF-proverka/projects/214. Alia (ASTERUS)"
    "/EOM/13АВ-РД-ЭМ-К3 (от 19.03.2026).pdf"
)
BLOCKS_DIR   = PROJ_DIR / "_output/blocks"
RESULTS_DIR  = Path(__file__).parent / "benchmark_all_fresh_results"
OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"
API_KEY_FILE   = Path("/home/coder/projects/PDF-proverka/.env")

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

VARIANTS = [
    ("openai/gpt-5.4", "low"),
    ("openai/gpt-5.4", "medium"),
    ("openai/gpt-5.5", "low"),
    ("openai/gpt-5.5", "medium"),
]

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
    pi          = json.loads((PROJ_DIR / "project_info.json").read_text())
    graph       = json.loads((PROJ_DIR / "_output/document_graph.json").read_text())
    idx         = json.loads((BLOCKS_DIR / "index.json").read_text())
    blocks_map  = {b["block_id"]: b for b in idx.get("blocks", [])}
    return pi, graph, blocks_map


async def call_model(
    *,
    client: httpx.AsyncClient,
    api_key: str,
    model: str,
    effort: str,
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
        "reasoning": {"effort": effort},
    }

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "HTTP-Referer": "https://localhost",
        "X-Title": f"benchmark-{model.split('/')[-1]}-{effort}",
    }

    started = time.monotonic()
    try:
        resp = await client.post(OPENROUTER_URL, headers=headers, json=payload, timeout=240)
    except Exception as exc:
        return {"ok": False, "error": str(exc), "elapsed_ms": int((time.monotonic() - started) * 1000)}
    elapsed_ms = int((time.monotonic() - started) * 1000)

    if resp.status_code >= 400:
        return {"ok": False, "http_status": resp.status_code, "error": resp.text[:300], "elapsed_ms": elapsed_ms}

    data   = resp.json()
    choice = (data.get("choices") or [{}])[0]
    raw    = (choice.get("message") or {}).get("content") or ""
    usage  = data.get("usage") or {}
    det    = usage.get("completion_tokens_details") or {}

    try:
        parsed    = json.loads(raw) if raw else None
        parse_err = None
    except Exception as e:
        parsed    = None
        parse_err = str(e)

    return {
        "ok":               parsed is not None,
        "parse_error":      parse_err,
        "elapsed_ms":       elapsed_ms,
        "input_tokens":     usage.get("prompt_tokens"),
        "output_tokens":    usage.get("completion_tokens"),
        "reasoning_tokens": det.get("reasoning_tokens"),
        "parsed":           parsed,
    }


def variant_key(model: str, effort: str) -> str:
    tag = model.split("/")[-1].replace("-", "").replace(".", "")
    return f"{tag}_{effort}"


def cost(model: str, in_tok: int, out_tok: int) -> float:
    p = PRICES.get(model, {"in": 0, "out": 0})
    return (in_tok or 0) / 1e6 * p["in"] + (out_tok or 0) / 1e6 * p["out"]


async def main():
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    api_key = load_api_key()
    pi, graph, blocks_map = load_project_data()
    system_prompt = build_system_prompt("EOM", extended=False)
    md_cache: dict = {}

    print("Все 4 варианта — свежие прогоны с идентичными условиями")
    print(f"temperature=0.2  max_tokens=16000  response_format=json_schema")
    print(f"Блоков: {len(SELECTED_BLOCKS)}\n")

    all_results = []

    async with httpx.AsyncClient() as client:
        for i, block_id in enumerate(SELECTED_BLOCKS, 1):
            bm = blocks_map.get(block_id)
            if not bm:
                print(f"[{i:2d}] {block_id} — нет в индексе, пропуск")
                continue

            page     = bm.get("page", 0)
            png_path = BLOCKS_DIR / bm["file"]
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

            print(f"[{i:2d}] {block_id} p.{page} — 4 варианта параллельно ...", flush=True)

            results = await asyncio.gather(*[
                call_model(model=m, effort=e, **kwargs)
                for m, e in VARIANTS
            ])

            record = {"block_id": block_id, "page": page, "enrichment_source": src}
            for (m, e), r in zip(VARIANTS, results):
                key = variant_key(m, e)
                findings = (r.get("parsed") or {}).get("findings", []) if r.get("ok") else []
                record[key] = {
                    "model":            m,
                    "effort":           e,
                    "ok":               r.get("ok"),
                    "findings":         findings,
                    "input_tokens":     r.get("input_tokens"),
                    "output_tokens":    r.get("output_tokens"),
                    "reasoning_tokens": r.get("reasoning_tokens"),
                    "elapsed_ms":       r.get("elapsed_ms"),
                    "parse_error":      r.get("parse_error"),
                }

            # Вывод построчно
            for (m, e) in VARIANTS:
                key = variant_key(m, e)
                v   = record[key]
                tag = m.split("/")[-1]
                nf  = len(v["findings"])
                ok  = "OK" if v["ok"] else f"ERR:{v.get('parse_error','?')[:30]}"
                print(
                    f"   {tag}/{e:<6} {ok}  findings={nf}  "
                    f"in={v['input_tokens']}  out={v['output_tokens']}  "
                    f"reason={v['reasoning_tokens']}  {(v['elapsed_ms'] or 0)//1000}s"
                )

            (RESULTS_DIR / f"block_{block_id}.json").write_text(
                json.dumps(record, ensure_ascii=False, indent=2)
            )
            all_results.append(record)
            print()

    # ── Сводка ──────────────────────────────────────────────────────────────
    def totals(key: str, model: str) -> dict:
        tok_in  = sum((r[key].get("input_tokens")     or 0) for r in all_results)
        tok_out = sum((r[key].get("output_tokens")    or 0) for r in all_results)
        tok_r   = sum((r[key].get("reasoning_tokens") or 0) for r in all_results)
        n_f     = sum(len(r[key]["findings"])               for r in all_results)
        times   = [(r[key].get("elapsed_ms") or 0)         for r in all_results]
        return {
            "findings":         n_f,
            "input_tokens":     tok_in,
            "output_tokens":    tok_out,
            "reasoning_tokens": tok_r,
            "cost_usd":         round(cost(model, tok_in, tok_out), 4),
            "total_elapsed_ms": sum(times),
            "avg_elapsed_ms":   round(sum(times) / len(times)) if times else 0,
        }

    summary = {
        "note": "all fresh runs, identical payload except model+effort",
        "params": {"temperature": 0.2, "max_tokens": 16000},
        "blocks_tested": len(all_results),
    }
    for m, e in VARIANTS:
        key = variant_key(m, e)
        summary[key] = totals(key, m)

    summary["blocks"] = [
        {
            "block_id": r["block_id"],
            "page":     r["page"],
            **{variant_key(m, e): len(r[variant_key(m, e)]["findings"]) for m, e in VARIANTS},
        }
        for r in all_results
    ]

    (RESULTS_DIR / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2))

    # Печать итоговой таблицы
    print("=" * 85)
    print(f"{'Вариант':<20} {'findings':>8} {'in tok':>8} {'out tok':>8} {'reason':>8} {'cost $':>8} {'avg s':>7}")
    print("-" * 85)
    for m, e in VARIANTS:
        key  = variant_key(m, e)
        t    = summary[key]
        tag  = m.split("/")[-1]
        avg_s = t["avg_elapsed_ms"] / 1000
        print(
            f"{tag}/{e:<9} {t['findings']:>8} {t['input_tokens']:>8} "
            f"{t['output_tokens']:>8} {t['reasoning_tokens']:>8} "
            f"{t['cost_usd']:>8.4f} {avg_s:>6.1f}s"
        )

    print(f"\nРезультаты: {RESULTS_DIR}")


if __name__ == "__main__":
    asyncio.run(main())
