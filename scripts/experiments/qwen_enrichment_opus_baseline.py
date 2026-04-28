"""
Opus 4.x baseline для сравнения с qwen_enrichment_pro_pilot.

Цель: запустить Claude Opus (через локальный Claude CLI / subscription) на тех же
блоках, с тем же findings-only + extended prompt, что и GPT-5.4 / Gemini Pro.
Это даёт чистое сравнение по модели, без смешивания с разными промптами.

Архитектура аналогична qwen_enrichment_pro_pilot.py, но вместо OpenRouter HTTP
используется subprocess Claude CLI:

    claude -p --model claude-opus-4-6 \
           --allowedTools Read,Write \
           --output-format json

Claude CLI сам читает PNG блока через Read tool и пишет findings JSON через
Write tool. Скрипт собирает stdout (там usage stats) и читает output JSON.

Артефакты в `_experiments/qwen_enrichment_opus_baseline/<ts>__<model>_extended/`.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(_ROOT / ".env")

# Pricing (OpenRouter parity, used for cost estimation only — реальная оплата идёт через subscription)
PRICING_USD_PER_1M = {
    "claude-opus-4-6": (5.0, 25.0),    # in, out
    "claude-opus-4.6": (5.0, 25.0),
    "claude-opus-4-7": (5.0, 25.0),
    "claude-opus-4.7": (5.0, 25.0),
}

DEFAULT_PROJECT_DIR = _ROOT / "projects" / "214. Alia (ASTERUS)" / "KJ" / "13АВ-РД-КЖ5.1-К1К2 (2).pdf"
DEFAULT_MODEL = "claude-opus-4-6"
KJ_CATEGORIES_FILE = _ROOT / "prompts" / "disciplines" / "KJ" / "finding_categories.md"
CLAUDE_CLI_BIN = os.environ.get("CLAUDE_CLI_BIN", str(Path.home() / ".local" / "bin" / "claude"))


SYSTEM_PROMPT_BASE = """Ты — инженер КЖ (железобетонные конструкции), проверяющий чертёж на ошибки.

На вход ты получишь:
  1. ИЗОБРАЖЕНИЕ одного блока чертежа (через Read tool по указанному пути).
  2. Уже извлечённое структурированное ОПИСАНИЕ блока (block_type, marks, dimensions, references, level_marks, rebar_specs) — считай его корректным контекстом.
  3. Текстовый контекст страницы.

Твоя ЕДИНСТВЕННАЯ задача — выдать массив findings[] с найденными проблемами.
НЕ описывай что видишь на блоке. НЕ пересказывай описание. НЕ делай summary.
Если проблем не нашёл — пустой массив [].

Каждое finding:
  - severity: одно из "КРИТИЧЕСКОЕ" | "ЭКОНОМИЧЕСКОЕ" | "ЭКСПЛУАТАЦИОННОЕ" | "РЕКОМЕНДАТЕЛЬНОЕ" | "ПРОВЕРИТЬ ПО СМЕЖНЫМ"
  - category: короткий тег (snake_case) — см. список в задаче
  - finding: суть замечания (конкретно, с цифрами и марками, 1-3 предложения)
  - norm_quote: цитата или ссылка на пункт нормы РФ если применимо, иначе null
  - recommendation: что делать (1 предложение)

В конце ОБЯЗАТЕЛЬНО запиши результат через Write tool в файл, путь которого указан в задаче.
Формат: {"findings": [...]}.
"""

# Минимальная версия — без описания входа (Sonnet выводит структуру из тела задачи).
# Используется с --minimal-prompt: page_text и block-метаданные в задаче отсутствуют.
SYSTEM_PROMPT_MINIMAL = """Ты — инженер КЖ (железобетонные конструкции), проверяющий чертёж на ошибки.

Твоя ЕДИНСТВЕННАЯ задача — выдать массив findings[] с найденными проблемами.
НЕ описывай что видишь на блоке. НЕ пересказывай описание. НЕ делай summary.
Если проблем не нашёл — пустой массив [].

Каждое finding:
  - severity: одно из "КРИТИЧЕСКОЕ" | "ЭКОНОМИЧЕСКОЕ" | "ЭКСПЛУАТАЦИОННОЕ" | "РЕКОМЕНДАТЕЛЬНОЕ" | "ПРОВЕРИТЬ ПО СМЕЖНЫМ"
  - category: короткий тег (snake_case) — см. список в задаче
  - finding: суть замечания (конкретно, с цифрами и марками, 1-3 предложения)
  - norm_quote: цитата или ссылка на пункт нормы РФ если применимо, иначе null
  - recommendation: что делать (1 предложение)

В конце ОБЯЗАТЕЛЬНО запиши результат через Write tool в файл, путь которого указан в задаче.
Формат: {"findings": [...]}.
"""

_EXTENDED_HEADER = """

## Категории замечаний КЖ (пройди мысленно по ВСЕМУ списку — это чек-лист направлений поиска)

Для КАЖДОЙ категории ниже проверь, применима ли она к этому блоку, и если применима — нет ли в блоке соответствующей проблемы. НЕ пропускай категории «для красоты» — особенно cross-discipline и cross-section (mep_coordination, construction_sequence, fire_rating, ar_kj_coordination, km_kj_coordination, progressive_collapse, spec_mismatch, documentation, normative_refs). Эти категории часто выпадают из фокуса, но именно там находятся важнейшие замечания.

"""


def _load_categories_table() -> str:
    try:
        return KJ_CATEGORIES_FILE.read_text(encoding="utf-8").strip()
    except FileNotFoundError:
        return ""


def build_system_prompt(extended: bool, minimal: bool = False) -> str:
    base = SYSTEM_PROMPT_MINIMAL if minimal else SYSTEM_PROMPT_BASE
    if not extended:
        return base
    cats = _load_categories_table()
    if not cats:
        return base
    return base + _EXTENDED_HEADER + cats + "\n"


def _latest_qwen_enrichment(project_dir: Path, block_id: str) -> dict | None:
    root = project_dir / "_experiments" / "qwen_enrichment"
    if not root.exists():
        return None
    for run_dir in sorted(root.iterdir(), reverse=True):
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


def _build_task_text(
    *,
    system_prompt: str,
    block_id: str,
    page: int,
    sheet_no: str,
    enrichment: dict | None,
    page_text: str,
    png_path: Path,
    output_path: Path,
    no_image: bool = False,
    minimal_prompt: bool = False,
) -> str:
    """Сформировать текст задачи для Claude CLI (он сам читает PNG и пишет результат).

    no_image=True — text-only режим: не передаём PNG, только Qwen JSON + текст страницы.
    minimal_prompt=True — убираем page_text и блок-метаданные (block_id/page/sheet) из тела задачи.
        Sonnet работает только с PNG + Qwen enrichment + extended prompt. Все per-block-поля
        (page/sheet/block_id) post-enrichment'ом проставляет runner через document_graph.
    """
    if enrichment is not None:
        enrichment_section = (
            "## Описание блока (Qwen enrichment, считай контекст верным):\n"
            f"```json\n{json.dumps(enrichment, ensure_ascii=False, indent=2)}\n```\n"
        )
        if minimal_prompt:
            steps_2 = "2. Используй приведённое ниже описание блока (Qwen enrichment) как контекст.\n"
        else:
            steps_2 = "2. Используй приведённое ниже описание блока (Qwen enrichment) и текст страницы как контекст.\n"
    else:
        enrichment_section = ""
        steps_2 = "2. Используй текст страницы как дополнительный контекст.\n"

    if no_image:
        steps_block = (
            "1. У тебя НЕТ доступа к изображению блока — анализируй ТОЛЬКО по описанию ниже и тексту страницы.\n"
            f"{steps_2}"
            "3. Найди проблемы согласно правилам выше. Если для категории нужно увидеть чертёж — пропусти её или пометь severity=ПРОВЕРИТЬ ПО СМЕЖНЫМ.\n"
            f"4. Запиши результат через Write tool в файл: `{output_path}`\n"
        )
    else:
        steps_block = (
            f"1. Прочитай изображение блока через Read tool: `{png_path}`\n"
            f"{steps_2}"
            "3. Найди проблемы согласно правилам выше.\n"
            f"4. Запиши результат через Write tool в файл: `{output_path}`\n"
        )

    if minimal_prompt:
        block_header = ""
        page_text_section = ""
    else:
        block_header = f"# Блок {block_id} | страница PDF {page} | лист {sheet_no or '(не определён)'}\n\n"
        page_text_section = f"## Текст страницы:\n{page_text or '(недоступен)'}\n"

    return f"""{system_prompt}

# ЗАДАЧА

Шаги:
{steps_block}   Формат файла: один JSON объект `{{"findings": [...]}}`.
   Никаких других файлов не создавай. Никакого markdown-обёртывания JSON в файле.

{block_header}{enrichment_section}{page_text_section}"""


def _parse_cli_stdout(stdout: str) -> dict:
    """Claude CLI с --output-format json возвращает в stdout структурированный JSON.

    Извлекаем usage / cost / result. Возвращает словарь полей или {} если не распарсилось.
    """
    try:
        return json.loads(stdout)
    except Exception:
        # try last JSON object in stdout
        m = re.search(r"\{[\s\S]*\}\s*$", stdout)
        if m:
            try:
                return json.loads(m.group(0))
            except Exception:
                pass
        return {}


async def run_one_block(
    block: dict,
    graph: dict,
    blocks_dir: Path,
    out_dir: Path,
    *,
    model: str,
    extended: bool,
    use_enrichment: bool,
    no_image: bool,
    timeout_sec: int,
    sem: asyncio.Semaphore,
    minimal_prompt: bool = False,
    clean_cwd: Path | None = None,
) -> dict:
    block_id = block["block_id"]
    page = block["page"]
    file_name = block["file"]
    png_path = (blocks_dir / file_name).resolve()
    sheet_no = ""
    for p in graph.get("pages", []):
        if p.get("page") == page:
            sheet_no = str(p.get("sheet_no_normalized") or p.get("sheet_no_raw") or "")
            break

    if use_enrichment:
        enrichment = _latest_qwen_enrichment(blocks_dir.parent.parent, block_id)
        if enrichment is None:
            return {
                "block_id": block_id,
                "page": page,
                "ok": False,
                "error": "no Qwen enrichment available",
            }
    else:
        enrichment = None
    page_text = _load_page_text(graph, page)

    output_json = (out_dir / f"block_{block_id}.findings.json").resolve()

    # В minimal-режиме page_text не передаётся в Sonnet
    effective_page_text = "" if minimal_prompt else page_text
    system_prompt = build_system_prompt(extended, minimal=minimal_prompt)
    task_text = _build_task_text(
        system_prompt=system_prompt,
        block_id=block_id,
        page=page,
        sheet_no=sheet_no,
        enrichment=enrichment,
        page_text=effective_page_text,
        png_path=png_path,
        output_path=output_json,
        no_image=no_image,
        minimal_prompt=minimal_prompt,
    )

    allowed_tools = "Write" if no_image else "Read,Write"
    cmd = [
        CLAUDE_CLI_BIN, "-p",
        "--model", model,
        "--allowedTools", allowed_tools,
        "--output-format", "json",
    ]

    # clean_cwd: запускаем claude -p из чистой папки + урезанным env, чтобы не подгружать
    # CLAUDE.md проекта, .claude/settings.json, hooks, project memory, skills manifest.
    # Эмпирически даёт −44% input/блок и −52% cli_cost (см. /tmp/test_clean_cwd.py).
    if clean_cwd is not None:
        proc_cwd = str(clean_cwd)
        proc_env = {k: v for k, v in os.environ.items()
                    if k in {"HOME", "PATH", "LANG", "LC_ALL", "USER", "SHELL"}
                    or k.startswith("XDG_")}
    else:
        proc_cwd = None
        proc_env = {**os.environ, **{k: "" for k in os.environ if k.startswith("CLAUDE_CODE")}}

    async with sem:
        started = time.monotonic()
        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=proc_cwd,
                env=proc_env,
            )
        except FileNotFoundError as exc:
            return {"block_id": block_id, "page": page, "ok": False, "error": f"CLI not found: {exc}"}

        try:
            stdout_b, stderr_b = await asyncio.wait_for(
                proc.communicate(task_text.encode("utf-8")),
                timeout=timeout_sec,
            )
        except asyncio.TimeoutError:
            try:
                proc.kill()
            except Exception:
                pass
            return {"block_id": block_id, "page": page, "ok": False, "error": f"CLI timeout after {timeout_sec}s"}

        elapsed_ms = int((time.monotonic() - started) * 1000)
        stdout_text = stdout_b.decode("utf-8", errors="replace")
        stderr_text = stderr_b.decode("utf-8", errors="replace")
        exit_code = proc.returncode or 0

    cli_meta = _parse_cli_stdout(stdout_text)
    usage = cli_meta.get("usage", {}) or {}
    in_tokens = usage.get("input_tokens") or cli_meta.get("input_tokens")
    out_tokens = usage.get("output_tokens") or cli_meta.get("output_tokens")
    cache_read = usage.get("cache_read_input_tokens") or cli_meta.get("cache_read")
    cache_creation = usage.get("cache_creation_input_tokens") or cli_meta.get("cache_creation")
    total_cost = cli_meta.get("total_cost_usd") or cli_meta.get("cost_usd")

    findings = None
    parse_error = None
    if output_json.exists():
        try:
            data = json.loads(output_json.read_text(encoding="utf-8"))
            findings = data.get("findings") if isinstance(data, dict) else None
            if findings is None and isinstance(data, list):
                findings = data
        except Exception as e:
            parse_error = f"output JSON parse failed: {e}"

    record = {
        "block_id": block_id,
        "page": page,
        "size_kb": block.get("size_kb"),
        "ok": exit_code == 0 and findings is not None,
        "exit_code": exit_code,
        "elapsed_ms": elapsed_ms,
        "input_tokens": in_tokens,
        "output_tokens": out_tokens,
        "cache_read_tokens": cache_read,
        "cache_creation_tokens": cache_creation,
        "cli_reported_cost_usd": total_cost,
        "findings": findings or [],
        "findings_count": len(findings or []),
        "parse_error": parse_error,
        "stderr_tail": stderr_text[-400:] if stderr_text else None,
        "stdout_tail": stdout_text[-400:] if not findings else None,
    }
    (out_dir / f"block_{block_id}.json").write_text(
        json.dumps(record, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    return record


async def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", type=str, default=str(DEFAULT_PROJECT_DIR))
    parser.add_argument("--model", type=str, default=DEFAULT_MODEL)
    parser.add_argument("--all", action="store_true")
    parser.add_argument("--blocks", type=str, default="", help="comma-separated block_ids")
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--extended-prompt", action="store_true")
    parser.add_argument("--no-enrichment", action="store_true", help="не добавлять Qwen enrichment в prompt (ablation)")
    parser.add_argument("--no-image", action="store_true", help="text-only режим: не передавать PNG, только Qwen JSON + текст страницы")
    parser.add_argument("--minimal-prompt", action="store_true", help="убрать page_text и блок-метаданные (block_id/page/sheet) из тела задачи; ужать system prompt")
    parser.add_argument("--clean-cwd", action="store_true", help="запускать claude -p из чистой папки + stripped env (без CLAUDE.md проекта, hooks, memory, skills) — экономит ~47K input/блок")
    parser.add_argument("--clean-cwd-path", type=str, default="/tmp/sonnet_clean", help="путь к чистой cwd (создаётся если нет)")
    parser.add_argument("--parallelism", type=int, default=2)
    parser.add_argument("--timeout", type=int, default=900)
    args = parser.parse_args()

    project_dir = Path(args.project).resolve()
    blocks_dir = project_dir / "_output" / "blocks"
    document_graph = project_dir / "_output" / "document_graph.json"
    if not blocks_dir.exists():
        print(f"blocks dir not found: {blocks_dir}", file=sys.stderr)
        return 2

    index = json.loads((blocks_dir / "index.json").read_text(encoding="utf-8"))
    graph = json.loads(document_graph.read_text(encoding="utf-8"))

    blocks_all = index["blocks"]
    by_id = {b["block_id"]: b for b in blocks_all}
    if args.all:
        wanted_blocks = blocks_all
    elif args.blocks:
        wanted_ids = {s.strip() for s in args.blocks.split(",") if s.strip()}
        wanted_blocks = [b for b in blocks_all if b["block_id"] in wanted_ids]
    else:
        wanted_blocks = blocks_all
    if args.limit:
        wanted_blocks = wanted_blocks[: args.limit]

    # Подготавливаем чистый cwd, если запрошено
    clean_cwd_path: Path | None = None
    if args.clean_cwd:
        clean_cwd_path = Path(args.clean_cwd_path).resolve()
        clean_cwd_path.mkdir(exist_ok=True, parents=True)
        # На всякий случай чистим: claude CLI не должен видеть ничего постороннего
        for f in clean_cwd_path.iterdir():
            if f.is_file() and not f.name.startswith("out_"):
                f.unlink()

    model_tag = args.model.replace("/", "_").replace(":", "_")
    prompt_tag = "_extended" if args.extended_prompt else ""
    enrich_tag = "_no-enrichment" if args.no_enrichment else ""
    image_tag = "_no-image" if args.no_image else ""
    minimal_tag = "_minimal" if args.minimal_prompt else ""
    cwd_tag = "_clean-cwd" if args.clean_cwd else ""
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    out_dir = project_dir / "_experiments" / "qwen_enrichment_opus_baseline" / f"{ts}__{model_tag}{prompt_tag}{enrich_tag}{image_tag}{minimal_tag}{cwd_tag}"
    out_dir.mkdir(parents=True, exist_ok=True)
    print(f"[plan] {len(wanted_blocks)} blocks, model={args.model}, parallelism={args.parallelism}, extended={args.extended_prompt}")
    print(f"[out]  {out_dir}")
    print(f"[cli]  {CLAUDE_CLI_BIN}")

    sem = asyncio.Semaphore(args.parallelism)
    overall_start = time.monotonic()

    async def _runner(b):
        print(f"  -> {b['block_id']} page={b['page']}", flush=True)
        rec = await run_one_block(
            b, graph, blocks_dir, out_dir,
            model=args.model, extended=args.extended_prompt,
            use_enrichment=not args.no_enrichment,
            no_image=args.no_image,
            timeout_sec=args.timeout, sem=sem,
            minimal_prompt=args.minimal_prompt,
            clean_cwd=clean_cwd_path,
        )
        status = "OK " if rec["ok"] else "FAIL"
        took = rec.get("elapsed_ms", 0) / 1000
        print(
            f"  <- {status} {rec['block_id']} t={took:.1f}s in={rec.get('input_tokens')} out={rec.get('output_tokens')} cache_read={rec.get('cache_read_tokens')} findings={rec.get('findings_count')}",
            flush=True,
        )
        if not rec["ok"]:
            print(f"     err={(rec.get('error') or '')[:120]} stderr_tail={(rec.get('stderr_tail') or '')[:160]}", flush=True)
        return rec

    results = await asyncio.gather(*(_runner(b) for b in wanted_blocks))
    overall_elapsed = time.monotonic() - overall_start

    ok_count = sum(1 for r in results if r["ok"])
    total_in = sum((r.get("input_tokens") or 0) for r in results)
    total_out = sum((r.get("output_tokens") or 0) for r in results)
    total_findings = sum(r.get("findings_count", 0) for r in results)
    total_cost_reported = sum((r.get("cli_reported_cost_usd") or 0) for r in results)

    # OR pricing estimate (для информации, реально оплата идёт через subscription)
    prices = PRICING_USD_PER_1M.get(args.model.replace("anthropic/", ""), (5.0, 25.0))
    estimated_cost = total_in * prices[0] / 1_000_000 + total_out * prices[1] / 1_000_000

    summary = {
        "timestamp": ts,
        "model": args.model,
        "extended_prompt": args.extended_prompt,
        "minimal_prompt": args.minimal_prompt,
        "clean_cwd": args.clean_cwd,
        "clean_cwd_path": str(clean_cwd_path) if clean_cwd_path else None,
        "no_image": args.no_image,
        "no_enrichment": args.no_enrichment,
        "parallelism": args.parallelism,
        "blocks_total": len(results),
        "blocks_ok": ok_count,
        "wall_clock_s": round(overall_elapsed, 1),
        "totals": {
            "input_tokens": total_in,
            "output_tokens": total_out,
            "findings": total_findings,
            "estimated_cost_usd_or_parity": round(estimated_cost, 4),
            "cli_reported_cost_usd_sum": round(total_cost_reported, 4),
        },
        "blocks": [
            {
                "block_id": r["block_id"],
                "page": r["page"],
                "ok": r["ok"],
                "findings_count": r.get("findings_count"),
                "input_tokens": r.get("input_tokens"),
                "output_tokens": r.get("output_tokens"),
                "elapsed_ms": r.get("elapsed_ms"),
                "error": r.get("error"),
            }
            for r in results
        ],
    }
    (out_dir / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    lines = [
        f"# Opus baseline (Claude CLI) — {ts}",
        "",
        f"Model: `{args.model}`  Extended prompt: {args.extended_prompt}  Parallelism: {args.parallelism}",
        "",
        f"**Blocks:** {ok_count}/{len(results)} OK  |  **Wall:** {overall_elapsed:.1f}s",
        f"**Tokens:** in={total_in:,}  out={total_out:,}  findings={total_findings}",
        f"**Cost estimate (OR parity):** ${estimated_cost:.3f}  |  **CLI-reported sum:** ${total_cost_reported:.3f}",
        "",
        "| block_id | page | ok | findings | in | out | t,s | error |",
        "|---|---|---|---|---|---|---|---|",
    ]
    for r in results:
        lines.append(
            f"| {r['block_id']} | {r['page']} | {'✓' if r['ok'] else '✗'} | {r.get('findings_count')} | "
            f"{r.get('input_tokens') or '-'} | {r.get('output_tokens') or '-'} | "
            f"{(r.get('elapsed_ms') or 0)/1000:.1f} | {(r.get('error') or '')[:60]} |"
        )
    (out_dir / "summary.md").write_text("\n".join(lines), encoding="utf-8")

    print(f"\n[done] {ok_count}/{len(results)} OK in {overall_elapsed:.1f}s, ~${estimated_cost:.3f}")
    print(f"[out]  {out_dir / 'summary.md'}")
    return 0 if ok_count == len(results) else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
