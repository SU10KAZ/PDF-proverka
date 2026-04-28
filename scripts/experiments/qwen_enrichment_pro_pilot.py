"""
Pilot (Variant A): Qwen-enrichment + Gemini Pro (findings-only) comparison.

Берёт 3 блока КЖ5.1 с известными baseline findings, прогоняет их через Pro
с новой архитектурой:
  - Pro видит PNG блока
  - Pro получает Qwen enrichment как контекст
  - Pro ДОЛЖЕН вернуть ТОЛЬКО findings[] без summary/key_values

Сравнивает output tokens и coverage findings с baseline 02_blocks_analysis.json.

Пишет в _experiments/qwen_enrichment_pro_pilot/<ts>/, ничего production не трогает.
"""
from __future__ import annotations

import argparse
import asyncio
import base64
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import httpx
from dotenv import load_dotenv

_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(_ROOT / ".env")

OPENROUTER_API_KEY = os.environ["OPENROUTER_API_KEY"]
OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"
MODEL_PRO = "google/gemini-3.1-pro-preview"
MODEL_FLASH = "google/gemini-2.5-flash"

DEFAULT_PROJECT_DIR = _ROOT / "projects" / "214. Alia (ASTERUS)" / "KJ" / "13АВ-РД-КЖ5.1-К1К2 (2).pdf"
PROJECT_DIR: Path = DEFAULT_PROJECT_DIR
BLOCKS_DIR: Path = PROJECT_DIR / "_output" / "blocks"
BASELINE_STAGE02: Path = PROJECT_DIR / "_output" / "02_blocks_analysis.json"
DOCUMENT_GRAPH: Path = PROJECT_DIR / "_output" / "document_graph.json"
QWEN_ENRICHMENT_ROOT: Path = PROJECT_DIR / "_experiments" / "qwen_enrichment"
KJ_CATEGORIES_FILE = _ROOT / "prompts" / "disciplines" / "KJ" / "finding_categories.md"

TARGET_BLOCKS = [
    "9GNP-D7CE-RYM",  # разрез колонны (92KB), 3 baseline findings
    "43L7-P9UL-VYD",  # схема расположения (174KB), 1 finding
    "4MQJ-6NXP-4YH",  # сечение стены (106KB), 2 findings
]


# Базовый findings-only prompt.
SYSTEM_PROMPT_BASE = """Ты — инженер КЖ (железобетонные конструкции), проверяющий чертёж на ошибки.

На вход ты получишь:
  1. ИЗОБРАЖЕНИЕ одного блока чертежа.
  2. Уже извлечённое структурированное ОПИСАНИЕ блока (block_type, marks, dimensions, references, level_marks, rebar_specs) — считай его корректным контекстом.
  3. Текстовый контекст страницы (общие указания, спецификации и т.д.).

Твоя ЕДИНСТВЕННАЯ задача — вернуть массив findings[] с найденными проблемами.
НЕ описывай что видишь на блоке. НЕ пересказывай описание. НЕ делай summary.
Если проблем не нашёл — верни {"findings": []}.

Каждое finding:
  - severity: одно из "КРИТИЧЕСКОЕ" | "ЭКОНОМИЧЕСКОЕ" | "ЭКСПЛУАТАЦИОННОЕ" | "РЕКОМЕНДАТЕЛЬНОЕ" | "ПРОВЕРИТЬ ПО СМЕЖНЫМ"
  - category: короткий тег (snake_case) — см. список ниже
  - finding: суть замечания (конкретно, с цифрами и марками, 1-3 предложения)
  - norm_quote: цитата или ссылка на пункт нормы РФ если применимо, иначе null
  - recommendation: что делать (1 предложение)

Строго JSON, без markdown-обёртки, без преамбулы.
"""


# Расширенная версия: приклеивается checklist категорий КЖ + явное требование проходить по ВСЕМУ списку.
_EXTENDED_CATEGORIES_HEADER = """

## Категории замечаний КЖ (пройди мысленно по ВСЕМУ списку — это чек-лист направлений поиска)

Для КАЖДОЙ категории ниже проверь, применима ли она к этому блоку, и если применима — нет ли в блоке соответствующей проблемы. НЕ пропускай категории «для красоты» — особенно cross-discipline и cross-section (mep_coordination, construction_sequence, fire_rating, ar_kj_coordination, km_kj_coordination, progressive_collapse, spec_mismatch, documentation, normative_refs). Эти категории часто выпадают из фокуса, но именно там находятся важнейшие замечания.

"""


def _load_categories_table() -> str:
    """Читает markdown-таблицу категорий КЖ."""
    try:
        return KJ_CATEGORIES_FILE.read_text(encoding="utf-8").strip()
    except FileNotFoundError:
        return ""


def build_system_prompt(extended: bool) -> str:
    if not extended:
        return SYSTEM_PROMPT_BASE
    cats = _load_categories_table()
    if not cats:
        return SYSTEM_PROMPT_BASE
    return SYSTEM_PROMPT_BASE + _EXTENDED_CATEGORIES_HEADER + cats + "\n"


# Backward-compat for places that still reference SYSTEM_PROMPT directly (none now, but kept safe).
SYSTEM_PROMPT = SYSTEM_PROMPT_BASE


RESPONSE_SCHEMA = {
    "name": "findings_only",
    "strict": True,
    "schema": {
        "type": "object",
        "properties": {
            "findings": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "severity": {"type": "string"},
                        "category": {"type": "string"},
                        "finding": {"type": "string"},
                        "norm_quote": {"type": ["string", "null"]},
                        "recommendation": {"type": "string"},
                    },
                    "required": ["severity", "category", "finding", "norm_quote", "recommendation"],
                    "additionalProperties": False,
                },
            }
        },
        "required": ["findings"],
        "additionalProperties": False,
    },
}


def _latest_qwen_enrichment(block_id: str) -> dict | None:
    """Собрать enrichment для block_id из самого свежего успешного прогона."""
    if not QWEN_ENRICHMENT_ROOT.exists():
        return None
    for run_dir in sorted(QWEN_ENRICHMENT_ROOT.iterdir(), reverse=True):
        path = run_dir / f"block_{block_id}.json"
        if path.exists():
            rec = json.loads(path.read_text(encoding="utf-8"))
            if rec.get("ok") and rec.get("enrichment"):
                return rec["enrichment"]
    return None


def _load_page_text(graph: dict, page: int) -> str:
    for p in graph.get("pages", []):
        if p.get("page") == page:
            parts = []
            if p.get("sheet_name"):
                parts.append(f"[SHEET] {p['sheet_name']}")
            for tb in p.get("text_blocks", [])[:20]:
                txt = (tb.get("text") or "").strip()
                if txt:
                    parts.append(txt[:500])
            return "\n".join(parts)[:4000]
    return ""


def _png_to_data_url(path: Path) -> str:
    return f"data:image/png;base64,{base64.b64encode(path.read_bytes()).decode()}"


def _build_baseline_stats(block_id: str, baseline: dict) -> dict:
    """Извлечь baseline output stats для этого block_id."""
    for ba in baseline.get("block_analyses", []):
        if ba.get("block_id") != block_id:
            continue
        baseline_json_size = len(json.dumps(ba, ensure_ascii=False))
        findings = ba.get("findings", []) or []
        return {
            "block_id": block_id,
            "baseline_json_chars": baseline_json_size,
            "baseline_finding_count": len(findings),
            "baseline_findings": findings,
            "baseline_summary_chars": len(ba.get("summary") or ""),
            "baseline_key_values_count": len(ba.get("key_values_read") or []),
        }
    return {"block_id": block_id, "baseline_not_found": True}


async def run_pro_for_block(
    client: httpx.AsyncClient,
    block: dict,
    enrichment: dict,
    page_text: str,
    blocks_dir: Path,
    *,
    model: str = MODEL_PRO,
    max_tokens: int = 16000,
    disable_thinking: bool = False,
    reasoning_effort: str | None = None,
    extended_prompt: bool = False,
) -> dict:
    png_path = blocks_dir / block["file"]
    data_url = _png_to_data_url(png_path)

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

    messages = [
        {"role": "system", "content": build_system_prompt(extended_prompt)},
        {
            "role": "user",
            "content": [
                {"type": "text", "text": user_text},
                {"type": "image_url", "image_url": {"url": data_url}},
            ],
        },
    ]

    payload = {
        "model": model,
        "messages": messages,
        "temperature": 0.2,
        "max_tokens": max_tokens,
        "response_format": {"type": "json_schema", "json_schema": RESPONSE_SCHEMA},
    }
    if disable_thinking:
        # OpenRouter reasoning controls: max_tokens=0 принудительно выключает thinking
        # на моделях, где thinking опционален (Gemini 2.5 Pro/Flash).
        # Дополнительно пробрасываем нативный Gemini параметр через extra_body.
        payload["reasoning"] = {"max_tokens": 0, "exclude": True}
        payload["extra_body"] = {
            "google": {"thinking_config": {"thinking_budget": 0}},
        }
    elif reasoning_effort:
        # GPT-5.x поддерживает effort: "minimal" | "low" | "medium" | "high"
        payload["reasoning"] = {"effort": reasoning_effort}

    headers = {
        "Authorization": f"Bearer {OPENROUTER_API_KEY}",
        "Content-Type": "application/json",
        "HTTP-Referer": "https://localhost",
        "X-Title": "qwen-enrichment-pilot",
    }

    started = time.monotonic()
    try:
        resp = await client.post(OPENROUTER_URL, headers=headers, json=payload, timeout=180)
    except Exception as exc:
        return {"ok": False, "error": f"httpx exception: {exc}", "elapsed_ms": int((time.monotonic() - started) * 1000)}
    elapsed_ms = int((time.monotonic() - started) * 1000)

    if resp.status_code >= 400:
        return {"ok": False, "http_status": resp.status_code, "error": resp.text[:500], "elapsed_ms": elapsed_ms}

    data = resp.json()
    choice = (data.get("choices") or [{}])[0]
    msg = choice.get("message") or {}
    raw_content = msg.get("content") or ""
    reasoning_content = msg.get("reasoning") or msg.get("reasoning_content") or ""
    usage = data.get("usage") or {}
    # OpenRouter usage breakdown: completion_tokens_details.reasoning_tokens
    completion_details = usage.get("completion_tokens_details") or {}
    reasoning_tokens = completion_details.get("reasoning_tokens")

    try:
        parsed = json.loads(raw_content) if raw_content else None
    except Exception as e:
        parsed = None
        parse_err = str(e)
    else:
        parse_err = None

    return {
        "ok": parsed is not None,
        "parse_error": parse_err,
        "elapsed_ms": elapsed_ms,
        "input_tokens": usage.get("prompt_tokens"),
        "output_tokens": usage.get("completion_tokens"),
        "reasoning_tokens": reasoning_tokens,
        "content_tokens_est": (usage.get("completion_tokens") or 0) - (reasoning_tokens or 0),
        "total_tokens": usage.get("total_tokens"),
        "raw_content_len": len(raw_content),
        "reasoning_content_len": len(reasoning_content),
        "raw_content": raw_content,
        "parsed": parsed,
    }


async def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--blocks", type=str, default=",".join(TARGET_BLOCKS))
    parser.add_argument("--model", type=str, default=MODEL_PRO, help=f"OpenRouter model id (default: {MODEL_PRO})")
    parser.add_argument("--max-tokens", type=int, default=16000)
    parser.add_argument("--disable-thinking", action="store_true", help="Отключить thinking/reasoning (для моделей где опционально)")
    parser.add_argument("--reasoning-effort", type=str, default=None, help="minimal|low|medium|high (для GPT-5.x)")
    parser.add_argument("--parallelism", type=int, default=1)
    parser.add_argument("--all", action="store_true", help="прогнать все блоки из index.json")
    parser.add_argument("--extended-prompt", action="store_true", help="добавить в system prompt полный чек-лист категорий КЖ")
    parser.add_argument("--project", type=str, default=str(DEFAULT_PROJECT_DIR), help="absolute path to project dir")
    args = parser.parse_args()

    global PROJECT_DIR, BLOCKS_DIR, BASELINE_STAGE02, DOCUMENT_GRAPH, QWEN_ENRICHMENT_ROOT
    PROJECT_DIR = Path(args.project).resolve()
    BLOCKS_DIR = PROJECT_DIR / "_output" / "blocks"
    BASELINE_STAGE02 = PROJECT_DIR / "_output" / "02_blocks_analysis.json"
    DOCUMENT_GRAPH = PROJECT_DIR / "_output" / "document_graph.json"
    QWEN_ENRICHMENT_ROOT = PROJECT_DIR / "_experiments" / "qwen_enrichment"

    index = json.loads((BLOCKS_DIR / "index.json").read_text(encoding="utf-8"))
    if args.all:
        wanted = [b["block_id"] for b in index["blocks"]]
    else:
        wanted = [s.strip() for s in args.blocks.split(",") if s.strip()]
    baseline = json.loads(BASELINE_STAGE02.read_text(encoding="utf-8"))
    graph = json.loads(DOCUMENT_GRAPH.read_text(encoding="utf-8"))

    by_id = {b["block_id"]: b for b in index["blocks"]}

    model_tag = args.model.replace("/", "_").replace(":", "_")
    prompt_tag = "_extended" if args.extended_prompt else ""
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    out_dir = PROJECT_DIR / "_experiments" / "qwen_enrichment_pro_pilot" / f"{ts}__{model_tag}{prompt_tag}"
    out_dir.mkdir(parents=True, exist_ok=True)
    print(f"[out] {out_dir}  model={args.model}  extended_prompt={args.extended_prompt}")

    results: list[dict] = []
    sem = asyncio.Semaphore(args.parallelism)

    async def _one(bid: str, client: httpx.AsyncClient) -> dict | None:
        async with sem:
            if bid not in by_id:
                print(f"  [skip] {bid} — not in blocks index", flush=True)
                return None
            block = by_id[bid]
            enrichment = _latest_qwen_enrichment(bid)
            if enrichment is None:
                print(f"  [skip] {bid} — no Qwen enrichment", flush=True)
                return None
            page_text = _load_page_text(graph, block["page"])
            baseline_info = _build_baseline_stats(bid, baseline)
            print(f"  -> {bid} page={block['page']}  baseline: {baseline_info.get('baseline_json_chars')} chars / {baseline_info.get('baseline_finding_count')} findings", flush=True)
            pro_result = await run_pro_for_block(
                client, block, enrichment, page_text, BLOCKS_DIR,
                model=args.model, max_tokens=args.max_tokens,
                disable_thinking=args.disable_thinking,
                reasoning_effort=args.reasoning_effort,
                extended_prompt=args.extended_prompt,
            )
            status = "OK " if pro_result["ok"] else "FAIL"
            new_findings = (pro_result.get("parsed") or {}).get("findings", [])
            print(
                f"  <- {status} {bid} t={pro_result['elapsed_ms']/1000:.1f}s in={pro_result.get('input_tokens')} out={pro_result.get('output_tokens')} reason={pro_result.get('reasoning_tokens')} findings={len(new_findings)} (baseline {baseline_info.get('baseline_finding_count')})",
                flush=True,
            )
            record = {
                "block_id": bid,
                "page": block["page"],
                "size_kb": block.get("size_kb"),
                "baseline": baseline_info,
                "pro_with_enrichment": pro_result,
                "enrichment_used": enrichment,
            }
            (out_dir / f"block_{bid}.json").write_text(
                json.dumps(record, ensure_ascii=False, indent=2), encoding="utf-8"
            )
            return record

    async with httpx.AsyncClient(timeout=200) as client:
        gathered = await asyncio.gather(*(_one(bid, client) for bid in wanted))
    results = [r for r in gathered if r is not None]

    # Aggregate summary
    rows = []
    total_baseline_chars = 0
    total_new_out_tokens = 0
    total_new_in_tokens = 0
    total_reasoning = 0
    total_baseline_findings = 0
    total_new_findings = 0
    ok_count = 0
    blocks_with_baseline_findings = 0
    blocks_new_zero_where_baseline_had = 0
    for r in results:
        bl = r["baseline"]
        pr = r["pro_with_enrichment"]
        new_findings = (pr.get("parsed") or {}).get("findings", [])
        bl_count = bl.get("baseline_finding_count") or 0
        new_count = len(new_findings)
        rows.append({
            "block_id": r["block_id"],
            "page": r["page"],
            "baseline_json_chars": bl.get("baseline_json_chars"),
            "baseline_findings": bl_count,
            "new_output_tokens": pr.get("output_tokens"),
            "new_input_tokens": pr.get("input_tokens"),
            "new_reasoning_tokens": pr.get("reasoning_tokens"),
            "new_findings": new_count,
            "new_elapsed_ms": pr.get("elapsed_ms"),
            "ok": pr.get("ok"),
        })
        total_baseline_chars += bl.get("baseline_json_chars", 0) or 0
        total_new_out_tokens += pr.get("output_tokens") or 0
        total_new_in_tokens += pr.get("input_tokens") or 0
        total_reasoning += pr.get("reasoning_tokens") or 0
        total_baseline_findings += bl_count
        total_new_findings += new_count
        if pr.get("ok"):
            ok_count += 1
        if bl_count > 0:
            blocks_with_baseline_findings += 1
            if new_count == 0:
                blocks_new_zero_where_baseline_had += 1

    # Cost estimate (OpenRouter GPT-5.4)
    cost_in = total_new_in_tokens * 2.5 / 1_000_000
    cost_out = total_new_out_tokens * 15.0 / 1_000_000
    cost_total = cost_in + cost_out

    summary = {
        "timestamp": ts,
        "model": args.model,
        "reasoning_effort": args.reasoning_effort,
        "extended_prompt": args.extended_prompt,
        "blocks_total": len(results),
        "blocks_ok": ok_count,
        "blocks_with_baseline_findings": blocks_with_baseline_findings,
        "blocks_new_zero_where_baseline_had": blocks_new_zero_where_baseline_had,
        "totals": {
            "baseline_json_chars_total": total_baseline_chars,
            "baseline_findings_total": total_baseline_findings,
            "new_findings_total": total_new_findings,
            "new_input_tokens_total": total_new_in_tokens,
            "new_output_tokens_total": total_new_out_tokens,
            "new_reasoning_tokens_total": total_reasoning,
            "estimated_cost_usd_in": round(cost_in, 4),
            "estimated_cost_usd_out": round(cost_out, 4),
            "estimated_cost_usd_total": round(cost_total, 4),
            "estimated_cost_per_block_usd": round(cost_total / max(1, len(results)), 4),
        },
        "blocks": rows,
    }
    (out_dir / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    # Markdown report
    lines = [
        f"# Qwen-Enrichment + Pro (findings-only) Pilot — {ts}",
        "",
        f"Model: `{args.model}`  effort: `{args.reasoning_effort}`  Project: `{PROJECT_DIR.name}`",
        "",
        f"**Blocks:** {ok_count}/{len(results)} OK  "
        f"**Baseline findings:** {total_baseline_findings}  "
        f"**New findings:** {total_new_findings}  "
        f"**Blocks with baseline findings → zero new:** {blocks_new_zero_where_baseline_had}/{blocks_with_baseline_findings}",
        "",
        f"**Tokens (total):** in={total_new_in_tokens:,} / out={total_new_out_tokens:,} (reasoning={total_reasoning:,})",
        f"**Estimated cost:** ${cost_total:.3f} (in ${cost_in:.3f} + out ${cost_out:.3f}) — ~${cost_total/max(1,len(results)):.4f}/block",
        "",
        "## Per-block comparison",
        "",
        "| block_id | page | baseline_ch | baseline_f | in | out | reason | new_f | t,s |",
        "|---|---|---|---|---|---|---|---|---|",
    ]
    for row in rows:
        mark = ""
        if (row["baseline_findings"] or 0) > 0 and row["new_findings"] == 0:
            mark = " ⚠️"
        lines.append(
            f"| {row['block_id']}{mark} | {row['page']} | {row['baseline_json_chars']} | "
            f"{row['baseline_findings']} | {row['new_input_tokens']} | {row['new_output_tokens']} | "
            f"{row.get('new_reasoning_tokens') or 0} | {row['new_findings']} | {(row['new_elapsed_ms'] or 0)/1000:.1f} |"
        )
    (out_dir / "summary.md").write_text("\n".join(lines), encoding="utf-8")

    print()
    print(f"[done] {len(results)} blocks processed")
    print(f"[out]  {out_dir / 'summary.md'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
