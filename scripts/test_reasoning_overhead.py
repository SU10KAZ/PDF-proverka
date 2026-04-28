"""Single-block GPT-5.4: замер input-токенов с reasoning="low" vs без reasoning.

Прогоняет 5 первых блоков КЖ5.17-23.1 в двух режимах и печатает таблицу
prompt_tokens / completion_tokens / стоимость. Никаких файлов не пишет.
"""
import asyncio, json, os, time, sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import httpx
from qwen_findings_only import (
    build_system_prompt,
    get_enrichment,
    load_page_text,
    png_to_data_url,
    RESPONSE_SCHEMA,
    OPENROUTER_URL,
)

PROJECT = Path('projects/214. Alia (ASTERUS)/KJ/13АВ-РД-КЖ5.17-23.1-К2 (1) (1).pdf')
MODEL = "openai/gpt-5.4"
MAX_TOKENS = 16000
PRICE_IN = 2.5 / 1_000_000
PRICE_OUT = 15.0 / 1_000_000


async def call_one(client, block, enrichment, page_text, blocks_dir, system_prompt, *, api_key, reasoning_effort):
    user_text = (
        f"# Блок {block['block_id']} | страница PDF {block['page']}\n\n"
        f"## Уже извлечённое описание блока (контекст, считай верным):\n"
        f"```json\n{json.dumps(enrichment, ensure_ascii=False, indent=2)}\n```\n\n"
        f"## Текст страницы (общие указания, спецификации и т.д.):\n"
        f"{page_text or '(недоступен)'}\n\n"
        f"## Задача:\n"
        f"Посмотри на изображение блока и верни findings[]. Только проблемы. "
        f"Не описывай что видишь. Если всё корректно — пустой массив."
    )
    png_path = blocks_dir / block["file"]
    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": [
            {"type": "text", "text": user_text},
            {"type": "image_url", "image_url": {"url": png_to_data_url(png_path)}},
        ]},
    ]
    payload = {
        "model": MODEL,
        "messages": messages,
        "temperature": 0.2,
        "max_tokens": MAX_TOKENS,
        "response_format": {"type": "json_schema", "json_schema": RESPONSE_SCHEMA},
    }
    if reasoning_effort:
        payload["reasoning"] = {"effort": reasoning_effort}

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "HTTP-Referer": "https://localhost",
        "X-Title": "reasoning-overhead-test",
    }
    t0 = time.monotonic()
    resp = await client.post(OPENROUTER_URL, headers=headers, json=payload, timeout=180)
    elapsed = time.monotonic() - t0
    if resp.status_code >= 400:
        return {"ok": False, "error": resp.text[:300], "elapsed": elapsed}
    data = resp.json()
    usage = data.get("usage") or {}
    cd = usage.get("completion_tokens_details") or {}
    return {
        "ok": True,
        "elapsed": elapsed,
        "prompt_tokens": usage.get("prompt_tokens"),
        "completion_tokens": usage.get("completion_tokens"),
        "reasoning_tokens": cd.get("reasoning_tokens"),
        "cost_usd": (usage.get("prompt_tokens", 0) * PRICE_IN
                     + usage.get("completion_tokens", 0) * PRICE_OUT),
    }


async def main():
    api_key = None
    for line in (Path(".env").read_text().splitlines()):
        if line.startswith("OPENROUTER_API_KEY="):
            api_key = line.split("=", 1)[1].strip().strip('"').strip("'")
            break
    if not api_key:
        api_key = os.environ.get("OPENROUTER_API_KEY")
    assert api_key, "no OPENROUTER_API_KEY"

    project_info = json.loads((PROJECT / "project_info.json").read_text())
    section = project_info.get("section", "KJ")
    system_prompt = build_system_prompt(section, extended=True)

    graph = json.loads((PROJECT / "_output" / "document_graph.json").read_text())
    blocks_dir = PROJECT / "_output" / "blocks"
    idx = json.loads((blocks_dir / "index.json").read_text())
    blocks = idx["blocks"][:5]

    items = []
    md_cache: dict = {}
    for b in blocks:
        enr, _src = get_enrichment(PROJECT, md_cache, project_info, b["block_id"])
        if not enr:
            print(f"⚠ no enrichment for {b['block_id']}, skip")
            continue
        pt = load_page_text(graph, b["page"])
        items.append((b, enr, pt))

    print(f"Готовых блоков: {len(items)}")
    print(f"system_prompt: {len(system_prompt)} chars\n")

    async with httpx.AsyncClient() as client:
        for label, eff in [("low (текущий)", "low"), ("none (без reasoning)", "")]:
            print(f"\n=== reasoning_effort = {label} ===")
            print(f"{'block_id':<18} {'PNG KB':>7} {'prompt':>7} {'compl':>6} {'reason':>7} {'sec':>5} {'$':>7}")
            tot_in = tot_out = tot_reason = 0
            tot_cost = 0.0
            for b, enr, pt in items:
                r = await call_one(client, b, enr, pt, blocks_dir, system_prompt,
                                   api_key=api_key, reasoning_effort=eff)
                if not r["ok"]:
                    print(f"  {b['block_id']:<18}  ERROR: {r['error']}")
                    continue
                tot_in += r["prompt_tokens"] or 0
                tot_out += r["completion_tokens"] or 0
                tot_reason += r["reasoning_tokens"] or 0
                tot_cost += r["cost_usd"]
                print(f"  {b['block_id']:<18} {b.get('size_kb',0):>6.0f} "
                      f"{r['prompt_tokens']:>7} {r['completion_tokens']:>6} "
                      f"{(r['reasoning_tokens'] or 0):>7} {r['elapsed']:>5.1f} "
                      f"${r['cost_usd']:>6.3f}")
            print(f"  {'ИТОГО':<18} {'':>7} {tot_in:>7} {tot_out:>6} {tot_reason:>7} "
                  f"{'':>5} ${tot_cost:>6.3f}")
            print(f"  Среднее prompt_tokens на блок: {tot_in/max(len(items),1):.0f}")


if __name__ == "__main__":
    asyncio.run(main())
