"""
qwen_findings_only.py
---------------------
Production-модуль stage 02 в режиме findings-only + Qwen-enrichment.

Поддерживает два transport'а:
  - OpenRouter (GPT-5.4, Gemini Flash/Pro)  — HTTP + json_schema
  - Claude CLI (Sonnet/Opus через subscription) — subprocess `claude -p`

Выбирается по model id: "claude-*" → Claude CLI, иначе → OpenRouter.

Используется и из CLI-скрипта (scripts/run_stage02_findings_only_gpt54.py),
и из webapp pipeline_service (вместо batched stage 02). Оба пути делятся
одной функцией `run_findings_only_for_project()`.

Per-block flow:
  PNG из _output/blocks/  +  qwen-описание (JSON или MD-парсинг)  +  page text
  → модель single-block + findings-only + extended <SECTION>/finding_categories.md
  → {"findings": [...]}
  → адаптируется под production block_analyses[] формат stage 03.

Перезапись _output/02_blocks_analysis.json опциональна (write_target=True).
"""
from __future__ import annotations

import asyncio
import base64
import json
import os
import re
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Optional

import httpx

_ROOT = Path(__file__).resolve().parent

OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"
DEFAULT_MODEL = "openai/gpt-5.4"
DEFAULT_EFFORT = "low"
DEFAULT_MAX_TOKENS = 16000
DEFAULT_PARALLELISM = 3
DEFAULT_TIMEOUT_S = 200

PRICE_IN = 2.50
PRICE_OUT = 15.0

# Claude CLI binary (subscription transport). Можно переопределить через env.
CLAUDE_CLI_BIN = os.environ.get("CLAUDE_CLI_BIN", str(Path.home() / ".local" / "bin" / "claude"))

# clean_cwd: запуск `claude -p` из чистой папки + урезанным env, чтобы не подгружать
# CLAUDE.md проекта, .claude/settings.json, hooks, project memory, skills manifest.
# Эмпирически даёт −44% input/блок и −52% cli_cost для stage 02 (см. ideas.md, Идея 6).
_CLEAN_CWD_PATH = "/tmp/sonnet_clean"
_CLEAN_ENV_KEEP = {"HOME", "PATH", "LANG", "LC_ALL", "USER", "SHELL"}


def _ensure_clean_cwd() -> str:
    """Создать (если нужно) и очистить /tmp/sonnet_clean. Возвращает путь."""
    p = _CLEAN_CWD_PATH
    os.makedirs(p, exist_ok=True)
    for entry in os.listdir(p):
        full = os.path.join(p, entry)
        if os.path.isfile(full):
            try:
                os.unlink(full)
            except OSError:
                pass
    return p


def _build_clean_env() -> dict:
    """Минимальный env (HOME/PATH/LANG/LC_ALL/USER/SHELL/XDG_*) — исключает project memory,
    skills manifest и прочие context-dependent артефакты Claude CLI."""
    keep = {}
    for k, v in os.environ.items():
        if k in _CLEAN_ENV_KEEP or k.startswith("XDG_"):
            keep[k] = v
    return keep


def is_claude_cli_model(model: str) -> bool:
    """Sonnet/Opus через Claude CLI subscription (`claude-sonnet-4-6`, `claude-opus-4-7`, …)."""
    return model.startswith("claude-")


# ─── Prompt ──────────────────────────────────────────────────────────────────

SYSTEM_PROMPT_BASE = """Ты — инженер-проверяющий проектную документацию жилого здания, проверяющий чертёж на ошибки.

На вход ты получишь:
  1. ИЗОБРАЖЕНИЕ одного блока чертежа.
  2. Уже извлечённое структурированное ОПИСАНИЕ блока (block_type, marks, dimensions, references, level_marks, rebar_specs и т.п.) — считай его корректным контекстом.
  3. Текстовый контекст страницы (общие указания, спецификации и т.д.).

Твоя ЕДИНСТВЕННАЯ задача — вернуть массив findings[] с найденными проблемами.
НЕ описывай что видишь на блоке. НЕ пересказывай описание. НЕ делай summary.
Если проблем не нашёл — верни {"findings": []}.

Каждое finding:
  - severity: одно из "КРИТИЧЕСКОЕ" | "ЭКОНОМИЧЕСКОЕ" | "ЭКСПЛУАТАЦИОННОЕ" | "РЕКОМЕНДАТЕЛЬНОЕ" | "ПРОВЕРИТЬ ПО СМЕЖНЫМ"
  - category: короткий тег (snake_case)
  - finding: суть замечания (конкретно, с цифрами и марками, 1-3 предложения)
  - norm_quote: цитата или ссылка на пункт нормы РФ если применимо, иначе null
  - value_found: точная цитата с чертежа (значение, марка, размер) — или пустая строка
  - recommendation: что делать (1 предложение)

Строго JSON, без markdown-обёртки, без преамбулы.
"""

_EXTENDED_HEADER = """

## Категории замечаний (пройди мысленно по ВСЕМУ списку — это чек-лист направлений поиска)

Для КАЖДОЙ категории ниже проверь, применима ли она к этому блоку, и если применима — нет ли в блоке соответствующей проблемы. НЕ пропускай категории «для красоты» — особенно cross-discipline и cross-section. Эти категории часто выпадают из фокуса, но именно там находятся важнейшие замечания.

"""


def load_categories_for_section(section: str) -> str:
    """Подгрузить prompts/disciplines/<SECTION>/finding_categories.md (или пусто, если нет)."""
    path = _ROOT / "prompts" / "disciplines" / section / "finding_categories.md"
    if path.exists():
        return path.read_text(encoding="utf-8").strip()
    return ""


def build_system_prompt(section: str, extended: bool) -> str:
    if not extended:
        return SYSTEM_PROMPT_BASE
    cats = load_categories_for_section(section)
    if not cats:
        return SYSTEM_PROMPT_BASE
    return SYSTEM_PROMPT_BASE + _EXTENDED_HEADER + cats + "\n"


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
                        "value_found": {"type": "string"},
                        "recommendation": {"type": "string"},
                    },
                    "required": [
                        "severity", "category", "finding",
                        "norm_quote", "value_found", "recommendation",
                    ],
                    "additionalProperties": False,
                },
            }
        },
        "required": ["findings"],
        "additionalProperties": False,
    },
}


# ─── Enrichment loaders ──────────────────────────────────────────────────────

def latest_qwen_enrichment(project_dir: Path, block_id: str) -> Optional[dict]:
    """Свежий enrichment-JSON из _experiments/qwen_enrichment/<latest>/block_<id>.json."""
    root = project_dir / "_experiments" / "qwen_enrichment"
    if not root.exists():
        return None
    for run_dir in sorted(root.iterdir(), reverse=True):
        if not run_dir.is_dir():
            continue
        path = run_dir / f"block_{block_id}.json"
        if path.exists():
            try:
                rec = json.loads(path.read_text(encoding="utf-8"))
            except json.JSONDecodeError:
                continue
            if rec.get("ok") and rec.get("enrichment"):
                return rec["enrichment"]
    return None


_BLOCK_HEADER_RE = re.compile(r"^### BLOCK \[IMAGE\]: (?P<id>[A-Z0-9-]+)\s*$", re.MULTILINE)
_ENRICHED_LINE_RE = re.compile(r"^\*\*\[ENRICHED [^\]]+\]\*\*\s*$", re.MULTILINE)
_ENRICHED_BULLET_RE = re.compile(r"^- \*\*(?P<key>[^:*]+):\*\*\s*(?P<val>.+)$")


def parse_enrichment_from_md(md_text: str, block_id: str) -> Optional[dict]:
    """Fallback: из MD-секции **[ENRICHED ...]** для конкретного block_id."""
    headers = list(_BLOCK_HEADER_RE.finditer(md_text))
    target_idx = next((i for i, m in enumerate(headers) if m.group("id") == block_id), None)
    if target_idx is None:
        return None
    start = headers[target_idx].end()
    end = headers[target_idx + 1].start() if target_idx + 1 < len(headers) else len(md_text)
    body = md_text[start:end]

    er_match = _ENRICHED_LINE_RE.search(body)
    if not er_match:
        return None
    section = body[er_match.end():]
    section_end = re.search(r"^### BLOCK ", section, re.MULTILINE)
    if section_end:
        section = section[: section_end.start()]

    label_to_key = {
        "Тип блока": "block_type", "Содержание": "subject",
        "Марки": "marks", "Арматура": "rebar_specs",
        "Размеры": "dimensions", "Оси": "axes",
        "Отметки": "level_marks", "Бетон": "concrete_class",
        "Ссылки": "references_on_block", "Заметки": "notes",
    }
    list_keys = {"marks", "rebar_specs", "dimensions", "axes", "level_marks", "references_on_block"}

    out: dict[str, Any] = {}
    for line in section.splitlines():
        m = _ENRICHED_BULLET_RE.match(line.strip())
        if not m:
            continue
        label = m.group("key").strip()
        val = m.group("val").strip()
        key = label_to_key.get(label)
        if not key:
            continue
        if key in list_keys:
            out[key] = [v.strip() for v in val.split(",") if v.strip()]
        else:
            out[key] = val
    return out or None


def _resolve_md_path(project_dir: Path, project_info: dict) -> Optional[Path]:
    md_name = project_info.get("md_file")
    if md_name:
        cand = project_dir / md_name
        if cand.exists():
            return cand
    for p in project_dir.glob("*_document.md"):
        return p
    return None


def get_enrichment(
    project_dir: Path,
    md_text_cache: dict,
    project_info: dict,
    block_id: str,
) -> tuple[Optional[dict], str]:
    """Возвращает (enrichment, source) — source = 'experiments' | 'md' | 'none'."""
    enr = latest_qwen_enrichment(project_dir, block_id)
    if enr is not None:
        return enr, "experiments"

    md_text = md_text_cache.get("text")
    if md_text is None:
        md_path = _resolve_md_path(project_dir, project_info)
        if md_path is None:
            md_text_cache["text"] = ""
            return None, "none"
        md_text = md_path.read_text(encoding="utf-8")
        md_text_cache["text"] = md_text

    enr = parse_enrichment_from_md(md_text, block_id)
    if enr is not None:
        return enr, "md"
    return None, "none"


# ─── Page text from document graph ──────────────────────────────────────────

def load_page_text(graph: dict, page: int) -> str:
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


def sheet_for_page(graph: dict, page: int) -> Optional[str]:
    for p in graph.get("pages", []):
        if p.get("page") == page:
            sno = p.get("sheet_no")
            if sno:
                return f"Лист {sno}"
            return p.get("sheet_name")
    return None


# ─── PNG → data URL ──────────────────────────────────────────────────────────

def png_to_data_url(path: Path) -> str:
    return f"data:image/png;base64,{base64.b64encode(path.read_bytes()).decode()}"


# ─── OpenRouter call ────────────────────────────────────────────────────────

async def call_gpt_for_block(
    client: httpx.AsyncClient,
    block: dict,
    enrichment: dict,
    page_text: str,
    blocks_dir: Path,
    *,
    api_key: str,
    model: str,
    reasoning_effort: str,
    max_tokens: int,
    system_prompt: str,
    timeout: int,
) -> dict:
    png_path = blocks_dir / block["file"]
    if not png_path.exists():
        return {"ok": False, "error": f"PNG missing: {png_path.name}", "elapsed_ms": 0}

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
        {"role": "system", "content": system_prompt},
        {
            "role": "user",
            "content": [
                {"type": "text", "text": user_text},
                {"type": "image_url", "image_url": {"url": png_to_data_url(png_path)}},
            ],
        },
    ]

    payload: dict[str, Any] = {
        "model": model,
        "messages": messages,
        "temperature": 0.2,
        "max_tokens": max_tokens,
        "response_format": {"type": "json_schema", "json_schema": RESPONSE_SCHEMA},
    }
    if reasoning_effort:
        payload["reasoning"] = {"effort": reasoning_effort}

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "HTTP-Referer": "https://localhost",
        "X-Title": "stage02-findings-only",
    }

    started = time.monotonic()
    try:
        resp = await client.post(OPENROUTER_URL, headers=headers, json=payload, timeout=timeout)
    except Exception as exc:
        return {"ok": False, "error": f"httpx: {exc}", "elapsed_ms": int((time.monotonic() - started) * 1000)}
    elapsed_ms = int((time.monotonic() - started) * 1000)

    if resp.status_code >= 400:
        return {
            "ok": False,
            "http_status": resp.status_code,
            "error": resp.text[:500],
            "elapsed_ms": elapsed_ms,
        }

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


# ─── Claude CLI transport (subscription) ────────────────────────────────────

def _build_claude_cli_task_text(
    *,
    system_prompt: str,
    block_id: str,
    page: int,
    sheet_no: str,
    enrichment: dict,
    page_text: str,
    png_path: Path,
    output_path: Path,
) -> str:
    """Промпт-текст для `claude -p` (Claude CLI сам читает PNG через Read tool и пишет findings через Write tool)."""
    enrichment_section = (
        "## Описание блока (Qwen enrichment, считай контекст верным):\n"
        f"```json\n{json.dumps(enrichment, ensure_ascii=False, indent=2)}\n```\n"
    )
    page_text_section = f"## Текст страницы:\n{page_text or '(недоступен)'}\n"
    block_header = f"# Блок {block_id} | страница PDF {page} | лист {sheet_no or '(не определён)'}\n\n"
    steps_block = (
        f"1. Прочитай изображение блока через Read tool: `{png_path}`\n"
        "2. Используй приведённое ниже описание блока (Qwen enrichment) и текст страницы как контекст.\n"
        "3. Найди проблемы согласно правилам выше.\n"
        f"4. Запиши результат через Write tool в файл: `{output_path}`\n"
    )
    return f"""{system_prompt}

# ЗАДАЧА

Шаги:
{steps_block}   Формат файла: один JSON объект `{{"findings": [...]}}`.
   Никаких других файлов не создавай. Никакого markdown-обёртывания JSON в файле.

{block_header}{enrichment_section}{page_text_section}"""


def _parse_claude_cli_stdout(stdout: str) -> dict:
    """Claude CLI с `--output-format json` возвращает структурированный JSON в stdout."""
    try:
        return json.loads(stdout)
    except Exception:
        m = re.search(r"\{[\s\S]*\}\s*$", stdout)
        if m:
            try:
                return json.loads(m.group(0))
            except Exception:
                pass
        return {}


async def call_claude_cli_for_block(
    block: dict,
    enrichment: dict,
    page_text: str,
    blocks_dir: Path,
    sheet_no: str,
    *,
    model: str,
    system_prompt: str,
    timeout: int,
    clean_cwd: bool = True,
) -> dict:
    """Вызов Claude CLI через subprocess `claude -p --model X --allowedTools Read,Write --output-format json`.

    PNG читается через Read tool, findings пишутся через Write tool в temp-файл,
    из которого мы парсим результат.

    clean_cwd=True (default): subprocess запускается из /tmp/sonnet_clean с минимальным env
    (без project CLAUDE.md, hooks, memory, skills manifest). Даёт −44% input/блок и −52% cost.
    """
    png_path = (blocks_dir / block["file"]).resolve()
    if not png_path.exists():
        return {"ok": False, "error": f"PNG missing: {png_path.name}", "elapsed_ms": 0}

    # Временный output файл — Claude CLI запишет туда findings.json.
    tmp_fd, tmp_name = tempfile.mkstemp(suffix=".findings.json", prefix=f"block_{block['block_id']}_")
    os.close(tmp_fd)
    output_path = Path(tmp_name)
    try:
        output_path.unlink()  # удалим пустой файл — CLI напишет свой
    except FileNotFoundError:
        pass

    task_text = _build_claude_cli_task_text(
        system_prompt=system_prompt,
        block_id=block["block_id"],
        page=block["page"],
        sheet_no=sheet_no,
        enrichment=enrichment,
        page_text=page_text,
        png_path=png_path,
        output_path=output_path,
    )

    cmd = [
        CLAUDE_CLI_BIN, "-p",
        "--model", model,
        "--allowedTools", "Read,Write",
        "--output-format", "json",
    ]

    if clean_cwd:
        proc_cwd = _ensure_clean_cwd()
        proc_env = _build_clean_env()
    else:
        proc_cwd = None
        proc_env = {**os.environ, **{k: "" for k in os.environ if k.startswith("CLAUDE_CODE")}}

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
        return {"ok": False, "error": f"Claude CLI not found: {exc}", "elapsed_ms": 0}

    try:
        stdout_b, stderr_b = await asyncio.wait_for(
            proc.communicate(task_text.encode("utf-8")),
            timeout=timeout,
        )
    except asyncio.TimeoutError:
        try:
            proc.kill()
        except Exception:
            pass
        return {"ok": False, "error": f"Claude CLI timeout after {timeout}s",
                "elapsed_ms": int((time.monotonic() - started) * 1000)}

    elapsed_ms = int((time.monotonic() - started) * 1000)
    stdout_text = stdout_b.decode("utf-8", errors="replace")
    stderr_text = stderr_b.decode("utf-8", errors="replace")
    exit_code = proc.returncode or 0

    cli_meta = _parse_claude_cli_stdout(stdout_text)
    usage = cli_meta.get("usage", {}) or {}
    in_tokens = usage.get("input_tokens") or cli_meta.get("input_tokens")
    out_tokens = usage.get("output_tokens") or cli_meta.get("output_tokens")
    total_cost = cli_meta.get("total_cost_usd") or cli_meta.get("cost_usd")

    findings = None
    parse_err = None
    if output_path.exists():
        try:
            data = json.loads(output_path.read_text(encoding="utf-8"))
            findings = data.get("findings") if isinstance(data, dict) else (data if isinstance(data, list) else None)
        except Exception as e:
            parse_err = f"output JSON parse failed: {e}"
        finally:
            try:
                output_path.unlink()
            except FileNotFoundError:
                pass
    elif exit_code != 0:
        parse_err = f"exit code {exit_code}: {stderr_text[-200:]}"

    parsed = {"findings": findings or []} if findings is not None else None
    return {
        "ok": parsed is not None,
        "parse_error": parse_err,
        "elapsed_ms": elapsed_ms,
        "input_tokens": in_tokens,
        "output_tokens": out_tokens,
        "reasoning_tokens": None,
        "cli_reported_cost_usd": total_cost,
        "raw_content": json.dumps(parsed, ensure_ascii=False) if parsed else "",
        "parsed": parsed,
        "exit_code": exit_code,
    }


# ─── Adapter: pilot finding → production format ─────────────────────────────

def adapt_findings_to_production(
    raw_findings: list[dict],
    block_id: str,
    finding_id_counter: list[int],
) -> list[dict]:
    """Адаптируем findings из findings-only schema под формат stage 03."""
    out = []
    for f in raw_findings:
        finding_id_counter[0] += 1
        recommendation = (f.get("recommendation") or "").strip()
        finding_text = (f.get("finding") or "").strip()
        if recommendation and recommendation.lower() not in finding_text.lower():
            finding_text = f"{finding_text}\n\nРекомендация: {recommendation}"
        out.append({
            "id": f"G-{finding_id_counter[0]:03d}",
            "severity": f.get("severity") or "ПРОВЕРИТЬ ПО СМЕЖНЫМ",
            "category": f.get("category") or "uncategorized",
            "finding": finding_text,
            "norm": None,
            "norm_quote": f.get("norm_quote"),
            "block_evidence": block_id,
            "value_found": f.get("value_found") or "",
            "highlight_regions": [],
        })
    return out


# ─── Main runner ────────────────────────────────────────────────────────────

class FindingsOnlyError(Exception):
    """Прерывание прогона (отсутствие prerequisites, отмена и т.п.)."""


async def run_findings_only_for_project(
    project_dir: Path,
    *,
    model: str = DEFAULT_MODEL,
    reasoning_effort: str = DEFAULT_EFFORT,
    extended_prompt: bool = True,
    max_tokens: int = DEFAULT_MAX_TOKENS,
    parallelism: int = DEFAULT_PARALLELISM,
    blocks_filter: Optional[list[str]] = None,
    timeout_s: int = DEFAULT_TIMEOUT_S,
    api_key: Optional[str] = None,
    on_progress: Optional[Callable[[dict], None]] = None,
    cancel_event: Optional[asyncio.Event] = None,
    write_target: bool = True,
    write_run_log: bool = True,
    claude_clean_cwd: bool = True,
) -> dict:
    """Прогнать stage 02 findings-only для проекта.

    Возвращает dict:
      {"output_doc": <02_blocks_analysis.json content>,
       "summary": <metrics dict>,
       "plan": <per-block plan list>,
       "run_dir": Path | None}

    on_progress(event) callback — webapp может подписаться:
      {"type": "started",  "blocks_total": N, "model": ..., "section": ...}
      {"type": "block_done", "block_id": ..., "page": ..., "ok": True, "findings": N,
       "input_tokens": ..., "output_tokens": ..., "reasoning_tokens": ...,
       "elapsed_ms": ..., "completed": K, "total": N}
      {"type": "block_skip", "block_id": ..., "reason": "no_enrichment", ...}
      {"type": "completed", "summary": {...}}

    cancel_event — webapp может set() для прерывания между блоками.
    """
    output_dir = project_dir / "_output"
    blocks_dir = output_dir / "blocks"
    index_path = blocks_dir / "index.json"
    graph_path = output_dir / "document_graph.json"
    info_path = project_dir / "project_info.json"
    target_path = output_dir / "02_blocks_analysis.json"

    if not index_path.exists():
        raise FindingsOnlyError("no _output/blocks/index.json — сначала: blocks.py crop")
    if not graph_path.exists():
        raise FindingsOnlyError("no _output/document_graph.json — сначала: process_project.py")

    project_info = json.loads(info_path.read_text(encoding="utf-8")) if info_path.exists() else {}
    section = (project_info.get("section") or "_generic").strip() or "_generic"
    index = json.loads(index_path.read_text(encoding="utf-8"))
    graph = json.loads(graph_path.read_text(encoding="utf-8"))

    by_id = {b["block_id"]: b for b in index.get("blocks", [])}
    if blocks_filter:
        unknown = [b for b in blocks_filter if b not in by_id]
        if unknown:
            raise FindingsOnlyError(f"unknown block_ids: {unknown}")
        wanted = list(blocks_filter)
    else:
        wanted = [b["block_id"] for b in index.get("blocks", [])]

    use_claude_cli = is_claude_cli_model(model)
    if not use_claude_cli:
        if api_key is None:
            api_key = os.environ.get("OPENROUTER_API_KEY")
        if not api_key:
            raise FindingsOnlyError("OPENROUTER_API_KEY not set")

    system_prompt = build_system_prompt(section, extended=extended_prompt)
    cats_loaded = bool(load_categories_for_section(section)) and extended_prompt

    md_cache: dict = {}
    enr_sources = {"experiments": 0, "md": 0, "none": 0}
    plan: list[dict] = []
    for bid in wanted:
        block = by_id[bid]
        enr, src = get_enrichment(project_dir, md_cache, project_info, bid)
        enr_sources[src] += 1
        plan.append({"block_id": bid, "page": block["page"], "enrichment": enr, "src": src})

    skip_no_enrich = [p for p in plan if p["enrichment"] is None]

    if on_progress:
        on_progress({
            "type": "started",
            "blocks_total": len(wanted),
            "model": model,
            "reasoning_effort": reasoning_effort,
            "extended_prompt": cats_loaded,
            "section": section,
            "enrichment_sources": dict(enr_sources),
            "skipped_no_enrichment": len(skip_no_enrich),
        })

    run_dir: Optional[Path] = None
    if write_run_log:
        model_tag = model.replace("/", "_").replace(":", "_")
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        run_dir = output_dir / "_stage02_findings_only_runs" / f"{ts}__{model_tag}_{reasoning_effort or 'none'}"
        run_dir.mkdir(parents=True, exist_ok=True)

    sem = asyncio.Semaphore(parallelism)
    completed_count = 0
    completed_lock = asyncio.Lock()
    results: list[dict] = []

    async def _one(item: dict, client: Optional[httpx.AsyncClient]) -> Optional[dict]:
        nonlocal completed_count
        if item["enrichment"] is None:
            if on_progress:
                on_progress({
                    "type": "block_skip", "block_id": item["block_id"],
                    "page": item["page"], "reason": "no_enrichment",
                })
            return None
        if cancel_event is not None and cancel_event.is_set():
            return None
        async with sem:
            if cancel_event is not None and cancel_event.is_set():
                return None
            block = by_id[item["block_id"]]
            page_text = load_page_text(graph, block["page"])
            if use_claude_cli:
                sheet = sheet_for_page(graph, block["page"]) or ""
                res = await call_claude_cli_for_block(
                    block, item["enrichment"], page_text, blocks_dir, sheet,
                    model=model, system_prompt=system_prompt, timeout=timeout_s,
                    clean_cwd=claude_clean_cwd,
                )
            else:
                res = await call_gpt_for_block(
                    client, block, item["enrichment"], page_text, blocks_dir,
                    api_key=api_key, model=model,
                    reasoning_effort=reasoning_effort,
                    max_tokens=max_tokens, system_prompt=system_prompt,
                    timeout=timeout_s,
                )
            n = len((res.get("parsed") or {}).get("findings", [])) if res.get("ok") else 0
            record = {
                "block_id": item["block_id"],
                "page": block["page"],
                "size_kb": block.get("size_kb"),
                "enrichment_source": item["src"],
                "result": res,
            }
            if run_dir is not None:
                (run_dir / f"block_{item['block_id']}.json").write_text(
                    json.dumps(record, ensure_ascii=False, indent=2), encoding="utf-8"
                )
            async with completed_lock:
                completed_count += 1
                cur = completed_count
            if on_progress:
                on_progress({
                    "type": "block_done",
                    "block_id": item["block_id"],
                    "page": block["page"],
                    "ok": res.get("ok"),
                    "findings": n,
                    "input_tokens": res.get("input_tokens"),
                    "output_tokens": res.get("output_tokens"),
                    "reasoning_tokens": res.get("reasoning_tokens"),
                    "elapsed_ms": res.get("elapsed_ms"),
                    "completed": cur,
                    "total": len(wanted),
                    "error": res.get("error") or res.get("parse_error") if not res.get("ok") else None,
                })
            return record

    started_at = time.monotonic()
    if use_claude_cli:
        # Claude CLI работает через subprocess — httpx-клиент не нужен.
        gathered = await asyncio.gather(*(_one(p, None) for p in plan))
    else:
        async with httpx.AsyncClient(timeout=timeout_s + 20) as client:
            gathered = await asyncio.gather(*(_one(p, client) for p in plan))
    wall_clock_s = round(time.monotonic() - started_at, 1)
    results = [r for r in gathered if r is not None]

    cancelled = cancel_event is not None and cancel_event.is_set()

    # Build production-format 02_blocks_analysis.json
    finding_id_counter = [0]
    block_analyses = []
    for p in plan:
        bid = p["block_id"]
        block = by_id[bid]
        sheet = sheet_for_page(graph, block["page"])
        rec = next((r for r in results if r["block_id"] == bid), None)

        if rec is None:
            block_analyses.append({
                "block_id": bid, "page": block["page"], "sheet": sheet,
                "label": block.get("ocr_label", ""), "sheet_type": None,
                "unreadable_text": False,
                "unreadable_details": "Блок пропущен: отсутствует qwen-описание (запустите qwen_enrich)" if p["enrichment"] is None else "Прерывание/отмена",
                "summary": "", "key_values_read": [], "evidence_text_refs": [],
                "findings": [],
                "_skip_reason": "no_enrichment" if p["enrichment"] is None else "cancelled",
            })
            continue

        res = rec["result"]
        if not res.get("ok"):
            block_analyses.append({
                "block_id": bid, "page": block["page"], "sheet": sheet,
                "label": block.get("ocr_label", ""), "sheet_type": None,
                "unreadable_text": False,
                "unreadable_details": f"GPT call failed: {res.get('error') or res.get('parse_error')}",
                "summary": "", "key_values_read": [], "evidence_text_refs": [],
                "findings": [],
                "_error": res.get("error") or res.get("parse_error"),
            })
            continue

        raw_findings = (res.get("parsed") or {}).get("findings", [])
        block_analyses.append({
            "block_id": bid, "page": block["page"], "sheet": sheet,
            "label": block.get("ocr_label", ""), "sheet_type": None,
            "unreadable_text": False, "unreadable_details": None,
            "summary": "", "key_values_read": [], "evidence_text_refs": [],
            "findings": adapt_findings_to_production(raw_findings, bid, finding_id_counter),
        })

    output_doc = {
        "batch_id": 0,
        "project_id": project_info.get("project_id", project_dir.name),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "stage02_mode": "findings_only_qwen_pair",
        "stage02_meta": {
            "model": model,
            "reasoning_effort": reasoning_effort,
            "extended_prompt": cats_loaded,
            "section": section,
            "blocks_total": len(wanted),
            "blocks_ok": sum(1 for r in results if r["result"].get("ok")),
            "blocks_failed": sum(1 for r in results if not r["result"].get("ok")),
            "blocks_skipped_no_enrichment": len(skip_no_enrich),
            "wall_clock_s": wall_clock_s,
            "cancelled": cancelled,
        },
        "block_analyses": block_analyses,
    }

    if write_target:
        if target_path.exists():
            bak = target_path.with_suffix(".classic.bak.json")
            if not bak.exists():
                bak.write_text(target_path.read_text(encoding="utf-8"), encoding="utf-8")
        target_path.write_text(json.dumps(output_doc, ensure_ascii=False, indent=2), encoding="utf-8")

    # Run summary
    ok = [r for r in results if r["result"].get("ok")]
    fail = [r for r in results if not r["result"].get("ok")]
    total_in = sum((r["result"].get("input_tokens") or 0) for r in results)
    total_out = sum((r["result"].get("output_tokens") or 0) for r in results)
    total_reason = sum((r["result"].get("reasoning_tokens") or 0) for r in results)
    total_findings = sum(len(b["findings"]) for b in block_analyses)
    if use_claude_cli:
        # Claude CLI subscription: суммируем cost_usd, отчитанный самим CLI.
        cost_total = sum((r["result"].get("cli_reported_cost_usd") or 0.0) for r in results)
        cost_in = 0.0
        cost_out = 0.0
    else:
        cost_in = total_in * PRICE_IN / 1_000_000
        cost_out = total_out * PRICE_OUT / 1_000_000
        cost_total = cost_in + cost_out

    summary = {
        "project_dir": str(project_dir),
        "model": model,
        "reasoning_effort": reasoning_effort,
        "extended_prompt": cats_loaded,
        "blocks_total": len(wanted),
        "blocks_with_enrichment": sum(1 for p in plan if p["enrichment"] is not None),
        "blocks_ok": len(ok),
        "blocks_failed": len(fail),
        "blocks_skipped_no_enrichment": len(skip_no_enrich),
        "wall_clock_s": wall_clock_s,
        "cancelled": cancelled,
        "totals": {
            "input_tokens": total_in,
            "output_tokens": total_out,
            "reasoning_tokens": total_reason,
            "findings": total_findings,
            "estimated_cost_usd_in": round(cost_in, 4),
            "estimated_cost_usd_out": round(cost_out, 4),
            "estimated_cost_usd_total": round(cost_total, 4),
            "estimated_cost_per_block_usd": round(cost_total / max(1, len(ok)), 4),
        },
        "enrichment_sources": enr_sources,
        "failed_blocks": [
            {"block_id": r["block_id"], "error": r["result"].get("error") or r["result"].get("parse_error")}
            for r in fail
        ],
    }

    if run_dir is not None:
        (run_dir / "summary.json").write_text(
            json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8"
        )

    if on_progress:
        on_progress({"type": "completed", "summary": summary})

    return {
        "output_doc": output_doc,
        "summary": summary,
        "plan": plan,
        "run_dir": run_dir,
    }


def check_prerequisites(project_dir: Path) -> dict:
    """Проверить готовность проекта к findings_only_qwen_pair.

    Возвращает dict {"ok": bool, "reasons": [...], "blocks_total": N, "with_enrichment": M}.
    """
    output_dir = project_dir / "_output"
    blocks_dir = output_dir / "blocks"
    index_path = blocks_dir / "index.json"
    graph_path = output_dir / "document_graph.json"

    reasons: list[str] = []
    if not index_path.exists():
        reasons.append("Нет _output/blocks/index.json (запустите 'crop blocks')")
    if not graph_path.exists():
        reasons.append("Нет _output/document_graph.json")

    if reasons:
        return {"ok": False, "reasons": reasons, "blocks_total": 0, "with_enrichment": 0}

    info_path = project_dir / "project_info.json"
    project_info = json.loads(info_path.read_text(encoding="utf-8")) if info_path.exists() else {}
    index = json.loads(index_path.read_text(encoding="utf-8"))

    md_cache: dict = {}
    blocks = index.get("blocks", [])
    with_enr = 0
    for b in blocks:
        enr, _src = get_enrichment(project_dir, md_cache, project_info, b["block_id"])
        if enr is not None:
            with_enr += 1

    if with_enr == 0:
        reasons.append("Ни у одного блока нет qwen-обогащения (запустите 'Подготовить данные' с Qwen)")
    elif with_enr < len(blocks):
        reasons.append(f"Только {with_enr}/{len(blocks)} блоков имеют qwen-обогащение — остальные будут пропущены")

    return {
        "ok": with_enr > 0,
        "reasons": reasons,
        "blocks_total": len(blocks),
        "with_enrichment": with_enr,
    }
