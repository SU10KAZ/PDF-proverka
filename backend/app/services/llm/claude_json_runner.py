"""Claude Code CLI (`claude -p`) как JSON-модель с изображениями.

Двойник ``codex_runner.run_codex_json_messages`` с той же сигнатурой и тем же
типом результата, чтобы потребитель мог подставить его как ``runner=`` без
правок своей логики. Первый потребитель — перепроверка отклонённых экспертами
замечаний (``rejected_audit_service.run_codex_audit``).

Как устроен вызов и почему именно так (всё проверено живыми вызовами):

* изображения уходят БАЙТАМИ — content-блоками ``type=image`` (base64) в
  сообщении ``--input-format stream-json``. Флага ``--image`` у ``claude`` нет,
  а путь «дать модели путь к файлу и Read» при выключенных инструментах
  заставляет её выдумывать содержимое (experiments/ai_supervisor_v1/REPORT.md);
* ``--input-format stream-json`` требует ``--output-format stream-json`` и
  ``--verbose`` — ответ разбирается построчно, берётся последнее событие
  ``type=result``;
* ``--json-schema`` реализован в CLI принудительным инструментом
  ``StructuredOutput``: enum держит движок, а готовый объект приходит в поле
  ``structured_output`` события ``result``;
* ``--tools=`` + ``--setting-sources=`` + ``--strict-mcp-config`` +
  собственный ``--system-prompt`` + временный пустой cwd — модель не видит ни
  файловой системы, ни CLAUDE.md/навыков/хуков владельца машины, ни MCP (в том
  числе 5,6-ГБ сервера норм). Без них накладные расходы — ~34 тыс. входных
  токенов на вызов, с ними — сотни.

Ограничения API на изображения (≤ 8000 px по длинной стороне, ≤ 2000 px при
числе изображений в запросе больше 20, ≤ 5 МБ на изображение) соблюдаются
уменьшением копии В ПАМЯТИ: файлы на диске и их SHA-256 не меняются, число
уменьшенных изображений возвращается в ``finish_reason`` для аудитного следа.
"""
from __future__ import annotations

from backend.app.services.llm.openrouter_gate import inference_entrypoint
import base64
import io
import json
import logging
import os
import shutil
import tempfile
import time
from pathlib import Path
from typing import Any, Awaitable, Callable, Optional, Sequence

from backend.app.core.config import get_claude_cli
from backend.app.models.usage import LLMResult
from backend.app.services.common.cli_utils import is_rate_limited
from backend.app.services.common.process_runner import run_command
from backend.app.services.llm.llm_runner import _try_parse_json_content

logger = logging.getLogger(__name__)

OnOutput = Optional[Callable[[str], Awaitable[None]]]

DEFAULT_CLAUDE_JSON_MODEL = "claude-opus-5"
MODEL_PREFIX = "claude/"

#: Лимиты Messages API на изображения.
MAX_IMAGE_EDGE = 8000
MAX_IMAGE_EDGE_MANY = 2000
MANY_IMAGES_THRESHOLD = 20
MAX_IMAGE_BYTES = 5 * 1024 * 1024
#: Запрос целиком ограничен 32 МБ; держим запас на текст и конверт.
MAX_TOTAL_IMAGE_BYTES = 24 * 1024 * 1024

_ALLOWED_SUFFIXES = {".png": "image/png", ".jpg": "image/jpeg", ".jpeg": "image/jpeg",
                     ".webp": "image/webp", ".gif": "image/gif"}
_EFFORTS = {"low", "medium", "high", "xhigh", "max"}


def resolve_claude_json_model(model: str | None) -> str:
    value = str(model or "").strip()
    if value.startswith(MODEL_PREFIX):
        value = value[len(MODEL_PREFIX):]
    return value or DEFAULT_CLAUDE_JSON_MODEL


def _effort(reasoning_effort: str | None) -> str | None:
    value = str(reasoning_effort or "").strip().lower()
    if not value:
        return None
    if value in {"none", "minimal"}:
        return "low"
    return value if value in _EFFORTS else None


def _content_text(content: Any) -> str:
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        return "\n\n".join(
            str(item.get("text") or "") if isinstance(item, dict) else str(item)
            for item in content
            if not (isinstance(item, dict) and item.get("type") not in (None, "text"))
        )
    return "" if content is None else json.dumps(content, ensure_ascii=False)


def _encode_image(path: Path, *, max_edge: int) -> tuple[str, bytes, bool]:
    """Вернуть (media_type, байты, уменьшено ли) с соблюдением лимитов API."""
    raw = path.read_bytes()
    media_type = _ALLOWED_SUFFIXES[path.suffix.lower()]
    from PIL import Image  # локальный импорт: Pillow нужен только при картинках

    with Image.open(io.BytesIO(raw)) as image:
        width, height = image.size
        if max(width, height) <= max_edge and len(raw) <= MAX_IMAGE_BYTES:
            return media_type, raw, False
        if image.mode in ("RGB", "L"):
            base = image.copy()
        else:
            # Прозрачность — на белый лист, иначе convert("RGB") даёт чёрный фон.
            rgba = image.convert("RGBA")
            base = Image.new("RGB", rgba.size, "white")
            base.paste(rgba, mask=rgba.getchannel("A"))
        edge = min(max_edge, max(width, height))
        while True:
            scale = edge / max(width, height)
            resized = base
            if scale < 1:
                resized = resized.resize(
                    (max(1, round(width * scale)), max(1, round(height * scale))),
                    Image.LANCZOS,
                )
            buffer = io.BytesIO()
            resized.save(buffer, format="PNG", optimize=True)
            data = buffer.getvalue()
            if len(data) <= MAX_IMAGE_BYTES or edge <= 512:
                return "image/png", data, True
            edge = int(edge * 0.8)


def _image_blocks(image_paths: Sequence[str | Path] | None) -> tuple[list[dict], int, str]:
    """Content-блоки изображений в заданном порядке; (блоки, уменьшено, ошибка)."""
    paths: list[Path] = []
    for raw_path in image_paths or []:
        try:
            path = Path(raw_path).expanduser().resolve(strict=True)
        except (OSError, RuntimeError):
            return [], 0, f"claude_image_missing: {raw_path}"
        if not path.is_file() or path.suffix.lower() not in _ALLOWED_SUFFIXES:
            return [], 0, f"claude_image_unsupported: {path}"
        paths.append(path)
    max_edge = MAX_IMAGE_EDGE_MANY if len(paths) > MANY_IMAGES_THRESHOLD else MAX_IMAGE_EDGE
    blocks: list[dict] = []
    resized_count = 0
    total = 0
    for index, path in enumerate(paths, start=1):
        media_type, data, resized = _encode_image(path, max_edge=max_edge)
        resized_count += int(resized)
        total += len(data)
        # Подпись перед каждым изображением — чтобы image_index из задачи
        # однозначно совпадал с порядком байтов, а не угадывался моделью.
        blocks.append({"type": "text", "text": f"Изображение {index} из {len(paths)}"})
        blocks.append({
            "type": "image",
            "source": {
                "type": "base64",
                "media_type": media_type,
                "data": base64.b64encode(data).decode("ascii"),
            },
        })
    if total > MAX_TOTAL_IMAGE_BYTES:
        return [], resized_count, (
            f"claude_images_too_large: {total} байт в {len(paths)} изображениях "
            f"при пределе {MAX_TOTAL_IMAGE_BYTES}; уменьшите --max-batch-images"
        )
    return blocks, resized_count, ""


def _last_result_event(stdout: str) -> dict | None:
    result = None
    for line in (stdout or "").splitlines():
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(event, dict) and event.get("type") == "result":
            result = event
    return result


@inference_entrypoint
async def run_claude_json_messages(
    messages: list[dict],
    *,
    timeout: int,
    on_output: OnOutput = None,
    stage: str = "",
    project_id: str = "",
    model: str | None = None,
    image_paths: Sequence[str | Path] | None = None,
    reasoning_effort: str | None = None,
    output_schema: dict[str, Any] | None = None,
    allowed_tools: str | None = None,
) -> LLMResult:
    """Один изолированный вызов ``claude -p``: весь контекст в запросе, ответ — JSON."""
    resolved_model = resolve_claude_json_model(model)
    label = f"{MODEL_PREFIX}{resolved_model}"
    if allowed_tools:
        # Инструменты (в т.ч. MCP норм) этим путём не прокидываются: стадия
        # обязана передать весь контекст в запросе. Молча выключить заявленные
        # инструменты было бы хуже, чем отказать.
        msg = f"claude_json_tools_unsupported: {allowed_tools}"
        return LLMResult(text=msg, model=label, is_error=True, error_message=msg)
    cli = get_claude_cli()
    if not cli or (not shutil.which(cli) and not Path(cli).exists()):
        msg = "claude_cli_not_found"
        return LLMResult(text=msg, model=label, is_error=True, error_message=msg)

    system_text = "\n\n".join(
        _content_text(m.get("content")) for m in messages if m.get("role") == "system"
    ).strip() or "Отвечай строго по заданной JSON-схеме."
    user_text = "\n\n".join(
        _content_text(m.get("content")) for m in messages if m.get("role") != "system"
    ).strip()

    try:
        image_blocks, resized_count, image_error = _image_blocks(image_paths)
    except Exception as exc:  # битый файл изображения — ошибка вызова, не падение прогона
        image_error = f"claude_image_unreadable: {type(exc).__name__}: {exc}"
        image_blocks, resized_count = [], 0
    if image_error:
        return LLMResult(text=image_error, model=label, is_error=True, error_message=image_error)

    content = [*image_blocks, {"type": "text", "text": user_text}]
    stdin_line = json.dumps(
        {"type": "user", "message": {"role": "user", "content": content}},
        ensure_ascii=False,
    ) + "\n"

    cmd = [
        cli, "-p",
        "--model", resolved_model,
        # Вариадические флаги — только в форме `--флаг=`, иначе они съедают
        # соседний аргумент (audit_worker/providers/claude_adapter.py).
        "--tools=",
        "--setting-sources=",
        "--strict-mcp-config",
        "--system-prompt", system_text,
        "--no-session-persistence",
        "--disable-slash-commands",
        "--input-format", "stream-json",
        "--output-format", "stream-json",
        "--verbose",
    ]
    effort = _effort(reasoning_effort)
    if effort:
        cmd += ["--effort", effort]
    if output_schema is not None:
        cmd += ["--json-schema", json.dumps(output_schema, ensure_ascii=False)]

    workdir = tempfile.mkdtemp(prefix=f"claude_{stage or 'json'}_")
    started = time.monotonic()
    try:
        exit_code, stdout, stderr = await run_command(
            cmd,
            on_output=on_output,
            # CLAUDECODE=None снимает ВСЕ переменные родительской сессии Claude
            # Code: вложенный CLI не должен считать себя её частью.
            env_overrides={"CLAUDECODE": None},
            cwd=workdir,
            timeout=timeout,
            input_text=stdin_line,
            project_id=project_id or None,
        )
    finally:
        shutil.rmtree(workdir, ignore_errors=True)
    duration_ms = int((time.monotonic() - started) * 1000)

    event = _last_result_event(stdout)
    usage = dict((event or {}).get("usage") or {})
    input_tokens = sum(
        int(usage.get(key) or 0)
        for key in ("input_tokens", "cache_creation_input_tokens", "cache_read_input_tokens")
    )
    common = dict(
        model=label,
        duration_ms=duration_ms,
        input_tokens=input_tokens,
        output_tokens=int(usage.get("output_tokens") or 0),
        cached_tokens=int(usage.get("cache_read_input_tokens") or 0),
        cache_write_tokens=int(usage.get("cache_creation_input_tokens") or 0),
        reasoning_tokens=int((usage.get("output_tokens_details") or {}).get("thinking_tokens") or 0),
        cost_source="subscription",
        response_id=str((event or {}).get("session_id") or ""),
        finish_reason=f"images_resized={resized_count}",
    )

    result_text = str((event or {}).get("result") or "")
    failed = exit_code != 0 or event is None or bool(event.get("is_error"))
    if failed:
        tail = "\n".join(part for part in (result_text, stderr[-2000:]) if part).strip()
        if exit_code == -1:
            message = f"claude_timeout после {timeout} с"
        elif is_rate_limited(exit_code or 1, f"{result_text}\n{stdout[-4000:]}", stderr):
            # Маркер «usage limit» понимает _looks_like_subscription_limit
            # потребителя: прогон остановится, а не сожжёт оставшиеся пачки.
            message = f"usage limit (claude): {tail[:1500]}"
        else:
            message = f"claude_exit_{exit_code}: {tail[:1500] or 'нет события result'}"
        return LLMResult(text=message, is_error=True, error_message=message, **common)

    json_data = event.get("structured_output")
    if not isinstance(json_data, (dict, list)):
        json_data = _try_parse_json_content(result_text)
    if json_data is None:
        message = "claude_json_not_found"
        return LLMResult(text=result_text or message, is_error=True, error_message=message, **common)
    return LLMResult(
        text=json.dumps(json_data, ensure_ascii=False),
        json_data=json_data,
        **common,
    )
