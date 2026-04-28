"""
Сервис обсуждений замечаний/оптимизаций.
Чат пользователя с LLM через OpenRouter или Claude CLI, хранение истории, резолюции.
"""

import asyncio
import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from webapp.config import (
    DISCUSSION_SUMMARY_THRESHOLD,
    DISCUSSION_TEMPERATURE,
    DISCUSSION_TIMEOUT,
    DISCUSSION_MAX_OUTPUT_TOKENS,
    DISCUSSION_CLI_TIMEOUT,
    CLAUDE_CLI,
    get_claude_cli,
)
from webapp.models.discussion import (
    Discussion,
    DiscussionMessage,
    DiscussionListItem,
)
from webapp.models.usage import LLMResult
from webapp.services.llm_runner import run_llm, make_image_content
from webapp.services.project_service import resolve_project_dir
from webapp.services.usage_service import paid_cost_tracker

logger = logging.getLogger(__name__)


def _is_cli_model(model: str) -> bool:
    """Проверить, нужно ли использовать Claude CLI."""
    return model == "claude-cli"


# ─── Хранение ────────────────────────────────────────────────

def _discussions_dir(project_id: str) -> Path:
    return resolve_project_dir(project_id) / "_output" / "discussions"


def _discussion_path(project_id: str, item_id: str) -> Path:
    return _discussions_dir(project_id) / f"{item_id}.json"


def get_discussion(project_id: str, item_id: str) -> Optional[Discussion]:
    """Загрузить обсуждение из файла."""
    path = _discussion_path(project_id, item_id)
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return Discussion(**data)
    except (json.JSONDecodeError, Exception) as e:
        logger.error("Failed to load discussion %s: %s", path, e)
        return None


def _save_discussion(project_id: str, discussion: Discussion):
    """Сохранить обсуждение в файл."""
    dir_path = _discussions_dir(project_id)
    dir_path.mkdir(parents=True, exist_ok=True)
    path = dir_path / f"{discussion.item_id}.json"
    path.write_text(
        discussion.model_dump_json(indent=2),
        encoding="utf-8",
    )


def _ensure_discussion(project_id: str, item_id: str, item_type: str, model: str) -> Discussion:
    """Получить или создать обсуждение."""
    disc = get_discussion(project_id, item_id)
    if disc is None:
        disc = Discussion(item_id=item_id, item_type=item_type, model=model)
    return disc


# ─── Контекст для LLM ────────────────────────────────────────

def _load_json(path: Path) -> Optional[dict]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError):
        return None


def _build_finding_context(project_id: str, item_id: str) -> tuple[str, list[dict]]:
    """Собрать текстовый контекст и PNG блоков для замечания.

    Returns:
        (context_text, image_content_blocks)
    """
    project_dir = resolve_project_dir(project_id)
    output_dir = project_dir / "_output"

    parts: list[str] = []
    images: list[dict] = []

    # 1. Само замечание
    findings_data = _load_json(output_dir / "03_findings.json")
    finding = None
    if findings_data:
        for f in findings_data.get("findings", findings_data.get("items", [])):
            if f.get("id") == item_id:
                finding = f
                break
    if finding:
        parts.append(f"=== ЗАМЕЧАНИЕ {item_id} ===")
        parts.append(json.dumps(finding, ensure_ascii=False, indent=2))

    # 2. Evidence-блоки из 02_blocks_analysis.json
    blocks_data = _load_json(output_dir / "02_blocks_analysis.json")
    evidence_block_ids = set()
    if finding:
        for ev in finding.get("evidence", []):
            if ev.get("block_id"):
                evidence_block_ids.add(ev["block_id"])
        for bid in finding.get("related_block_ids", []):
            evidence_block_ids.add(bid)

    if blocks_data and evidence_block_ids:
        block_list = blocks_data.get("blocks") or blocks_data.get("block_analyses") or []
        parts.append("\n=== EVIDENCE БЛОКИ ===")
        for block in block_list:
            bid = block.get("block_id", "")
            if bid in evidence_block_ids:
                parts.append(f"\n--- Блок {bid} ---")
                parts.append(json.dumps(block, ensure_ascii=False, indent=2))
                # PNG
                block_file = block.get("file", f"block_{bid}.png")
                png_path = output_dir / "blocks" / block_file
                if not png_path.exists():
                    png_path = output_dir / "blocks" / f"block_{bid}.png"
                if png_path.exists():
                    images.append(make_image_content(png_path))

    # 3. Страницы из document_graph
    graph_data = _load_json(output_dir / "document_graph.json")
    if graph_data and finding:
        pages_of_interest = set()
        page_val = finding.get("page")
        if isinstance(page_val, list):
            pages_of_interest.update(page_val)
        elif isinstance(page_val, int):
            pages_of_interest.add(page_val)

        if pages_of_interest:
            parts.append("\n=== КОНТЕКСТ СТРАНИЦ (document_graph) ===")
            for p in graph_data.get("pages", []):
                if p.get("page") in pages_of_interest:
                    parts.append(f"\n--- Страница {p.get('page')} (Лист {p.get('sheet_no', '?')}) ---")
                    for tb in p.get("text_blocks", []):
                        parts.append(tb.get("text", ""))

    # 4. Вердикт критика
    review_data = _load_json(output_dir / "03_findings_review.json")
    if review_data:
        verdicts = review_data.get("verdicts", review_data.get("reviews", []))
        if isinstance(verdicts, list):
            for v in verdicts:
                if v.get("finding_id") == item_id or v.get("id") == item_id:
                    parts.append(f"\n=== ВЕРДИКТ КРИТИКА ===")
                    parts.append(json.dumps(v, ensure_ascii=False, indent=2))
                    break

    # 5. Norm checks
    norm_data = _load_json(output_dir / "norm_checks.json")
    if norm_data and finding:
        norm_ref = finding.get("norm_reference", finding.get("requirement", ""))
        checks = norm_data.get("checks", [])
        for check in checks:
            doc_name = check.get("document", "")
            if doc_name and doc_name in norm_ref:
                parts.append(f"\n=== СТАТУС НОРМЫ: {doc_name} ===")
                parts.append(json.dumps(check, ensure_ascii=False, indent=2))

    return "\n".join(parts), images


def _build_optimization_context(project_id: str, item_id: str) -> tuple[str, list[dict]]:
    """Собрать контекст для обсуждения оптимизации."""
    project_dir = resolve_project_dir(project_id)
    output_dir = project_dir / "_output"

    parts: list[str] = []
    images: list[dict] = []

    # 1. Сама оптимизация
    opt_data = _load_json(output_dir / "optimization.json")
    opt_item = None
    if opt_data:
        for item in opt_data.get("items", []):
            if item.get("id") == item_id:
                opt_item = item
                break
    if opt_item:
        parts.append(f"=== ОПТИМИЗАЦИЯ {item_id} ===")
        parts.append(json.dumps(opt_item, ensure_ascii=False, indent=2))

    # 2. Блоки по page
    if opt_item:
        page_val = opt_item.get("page")
        pages = page_val if isinstance(page_val, list) else ([page_val] if page_val else [])

        blocks_data = _load_json(output_dir / "02_blocks_analysis.json")
        if blocks_data and pages:
            block_list = blocks_data.get("blocks") or blocks_data.get("block_analyses") or []
            parts.append("\n=== БЛОКИ СО СТРАНИЦ ОПТИМИЗАЦИИ ===")
            for block in block_list:
                if block.get("page") in pages:
                    bid = block.get("block_id", "")
                    parts.append(f"\n--- Блок {bid} ---")
                    parts.append(json.dumps(block, ensure_ascii=False, indent=2))
                    block_file = block.get("file", f"block_{bid}.png")
                    png_path = output_dir / "blocks" / block_file
                    if not png_path.exists():
                        png_path = output_dir / "blocks" / f"block_{bid}.png"
                    if png_path.exists():
                        images.append(make_image_content(png_path))

    # 3. Review вердикт
    review_data = _load_json(output_dir / "optimization_review.json")
    if review_data:
        verdicts = review_data.get("verdicts", review_data.get("reviews", []))
        if isinstance(verdicts, list):
            for v in verdicts:
                if v.get("id") == item_id or v.get("optimization_id") == item_id:
                    parts.append(f"\n=== ВЕРДИКТ КРИТИКА ===")
                    parts.append(json.dumps(v, ensure_ascii=False, indent=2))
                    break

    return "\n".join(parts), images


# ─── Формирование промпта ─────────────────────────────────────

SYSTEM_PROMPT = """Ты — эксперт по проверке проектной документации жилых многоквартирных домов (МКД).
Пользователь обсуждает с тобой конкретное замечание или оптимизационное предложение из аудита.

Правила:
- Отвечай на русском языке
- Ссылайся на конкретные пункты норм (СП, ГОСТ, ПУЭ)
- Если ссылаешься на норму — ОБЯЗАТЕЛЬНО проверь её актуальность через WebSearch
- После каждой ссылки на норму добавь сноску с URL источника, например: [источник](https://url)
- Если не уверен в номере пункта — скажи прямо и проверь через WebSearch
- Используй данные из контекста (замечание, блоки, document_graph)
- Будь конкретен и лаконичен

Формат ответа:
- Сначала кратко опиши ход рассуждений в блоке <details><summary>Ход мыслей</summary>...</details>
- Затем дай основной ответ
- В конце — список источников (если использовал WebSearch)
"""


def _build_messages(
    discussion: Discussion,
    context_text: str,
    image_blocks: list[dict],
    user_message: str,
    is_first: bool,
) -> list[dict]:
    """Сформировать messages для OpenRouter API."""
    messages: list[dict] = [
        {"role": "system", "content": SYSTEM_PROMPT},
    ]

    # Контекст — всегда в первом user-сообщении
    context_content: list[dict] = [{"type": "text", "text": context_text}]
    # PNG только при первом сообщении
    if is_first and image_blocks:
        context_content.extend(image_blocks)

    # Если есть summary старых сообщений — добавить
    if discussion.summary_of_old:
        messages.append({
            "role": "user",
            "content": f"[Краткое содержание предыдущего обсуждения]\n{discussion.summary_of_old}",
        })
        messages.append({
            "role": "assistant",
            "content": "Понял, продолжаю обсуждение с учётом предыдущего контекста.",
        })

    # История сообщений (исключая последнее если == user_message, чтобы не дублировать)
    history_msgs = list(discussion.messages)
    if history_msgs and history_msgs[-1].role == "user" and history_msgs[-1].content == user_message:
        history_msgs = history_msgs[:-1]

    for msg in history_msgs:
        messages.append({"role": msg.role, "content": msg.content})

    # Новое сообщение пользователя с контекстом
    if is_first and not history_msgs:
        # Первое сообщение — контекст + вопрос вместе
        user_content = context_content + [{"type": "text", "text": f"\n\nВопрос пользователя:\n{user_message}"}]
        messages.append({"role": "user", "content": user_content})
    else:
        # Последующие — контекст как system-note, вопрос отдельно
        messages.insert(1, {
            "role": "user",
            "content": f"[Контекст замечания/оптимизации — для справки]\n{context_text}",
        })
        messages.insert(2, {
            "role": "assistant",
            "content": "Контекст принят.",
        })
        messages.append({"role": "user", "content": user_message})

    return messages


# ─── Сжатие истории ───────────────────────────────────────────

async def _maybe_compress_history(discussion: Discussion, model: str):
    """Сжать старые сообщения в summary если их больше порога."""
    if len(discussion.messages) < DISCUSSION_SUMMARY_THRESHOLD:
        return

    # Сжимаем все кроме последних 4
    to_compress = discussion.messages[:-4]
    remaining = discussion.messages[-4:]

    history_text = "\n".join(
        f"{'Пользователь' if m.role == 'user' else 'Ассистент'}: {m.content}"
        for m in to_compress
    )

    # Если уже есть summary — включить его
    prev_summary = discussion.summary_of_old or ""
    compress_prompt = f"""Сожми следующий диалог в краткое резюме (3-5 предложений).
Сохрани ключевые выводы, решения и аргументы.

{f'Предыдущее резюме: {prev_summary}' if prev_summary else ''}

Диалог:
{history_text}

Дай только резюме, без преамбулы."""

    result = await run_llm(
        stage="discussion",
        messages=[
            {"role": "system", "content": "Ты помощник. Сожми диалог в краткое резюме."},
            {"role": "user", "content": compress_prompt},
        ],
        response_format=None,
        temperature=0.1,
        timeout=60,
        model_override=model,
    )

    if not result.is_error and result.text:
        discussion.summary_of_old = result.text.strip()
        discussion.messages = list(remaining)
        discussion.total_input_tokens += result.input_tokens
        discussion.total_output_tokens += result.output_tokens
        discussion.total_cost_usd += result.cost_usd
        if result.cost_usd > 0:
            paid_cost_tracker.add(result.cost_usd)


# ─── Claude CLI для чата ───────────────────────────────────────

async def _run_cli_chat(prompt_text: str) -> LLMResult:
    """Вызвать Claude CLI с текстовым промптом (без tool use)."""
    import time
    from webapp.services.process_runner import run_command
    from webapp.config import get_claude_model

    model = get_claude_model()
    cmd = [
        get_claude_cli(), "-p",
        "--model", model,
        "--output-format", "json",
        "--allowedTools", "WebSearch", "WebFetch",
    ]

    env_overrides = {k: None for k in os.environ if k.startswith("CLAUDE")}

    start = time.monotonic()
    exit_code, stdout, stderr = await run_command(
        cmd,
        input_text=prompt_text,
        timeout=DISCUSSION_CLI_TIMEOUT,
        env_overrides=env_overrides,
    )
    elapsed_ms = int((time.monotonic() - start) * 1000)

    if exit_code != 0 and not stdout:
        return LLMResult(
            text="", is_error=True,
            error_message=f"Claude CLI exit {exit_code}: {(stderr or '')[:500]}",
            model=model,
        )

    # Парсить JSON-вывод CLI
    from webapp.services.cli_utils import parse_cli_json_output
    cli_result = parse_cli_json_output(stdout or "")

    return LLMResult(
        text=cli_result.result_text,
        input_tokens=0,
        output_tokens=0,
        cost_usd=0.0,  # подписка — стоимость не считается
        duration_ms=elapsed_ms,
        model=model,
        is_error=cli_result.is_error and not cli_result.result_text,
        error_message="" if cli_result.result_text else "Empty CLI response",
    )


def _build_cli_prompt(
    context_text: str,
    discussion: Discussion,
    user_message: str,
) -> str:
    """Сформировать текстовый промпт для Claude CLI (контекст + история + вопрос)."""
    parts = [SYSTEM_PROMPT, "", "=== КОНТЕКСТ ===", context_text, ""]

    # Summary старых сообщений
    if discussion.summary_of_old:
        parts.append(f"[Краткое содержание предыдущего обсуждения]\n{discussion.summary_of_old}\n")

    # История (исключая последнее сообщение если оно == user_message, чтобы избежать дубля)
    history_msgs = list(discussion.messages)
    if history_msgs and history_msgs[-1].role == "user" and history_msgs[-1].content == user_message:
        history_msgs = history_msgs[:-1]

    if history_msgs:
        parts.append("=== ИСТОРИЯ ДИАЛОГА ===")
        for msg in history_msgs:
            role = "Пользователь" if msg.role == "user" else "Ассистент"
            parts.append(f"{role}: {msg.content}")
        parts.append("")

    parts.append(f"Пользователь: {user_message}")
    parts.append("")
    parts.append("Ответь на русском языке. Будь конкретен и лаконичен. Используй markdown для форматирования. Если ссылаешься на нормативный документ — проверь его актуальность через WebSearch и приведи одну ссылку на источник. Начни ответ с блока <details><summary>Ход мыслей</summary>краткий ход рассуждений</details>.")

    return "\n".join(parts)


def _estimate_tokens(text: str) -> int:
    """Грубая оценка токенов: ~3.5 символа на токен для смешанного русского/английского текста."""
    return max(1, int(len(text) / 3.5))


def _estimate_image_tokens(images: list[dict]) -> int:
    """Оценка токенов для изображений. ~1600 токенов на изображение (среднее для Claude/GPT)."""
    return len(images) * 1600


def estimate_context_tokens(project_id: str, item_id: str, item_type: str) -> dict:
    """Оценить количество токенов контекста, который будет отправлен с вопросом."""
    # Контекст
    if item_type == "finding":
        context_text, images = _build_finding_context(project_id, item_id)
    else:
        context_text, images = _build_optimization_context(project_id, item_id)

    discussion = get_discussion(project_id, item_id)

    # Токены системного промпта
    system_tokens = _estimate_tokens(SYSTEM_PROMPT)

    # Токены контекста (замечание, блоки, document_graph, нормы)
    context_tokens = _estimate_tokens(context_text)

    # Токены изображений
    image_tokens = _estimate_image_tokens(images)

    # Токены истории диалога
    history_tokens = 0
    if discussion:
        if discussion.summary_of_old:
            history_tokens += _estimate_tokens(discussion.summary_of_old) + 50  # + обёртка
        for msg in discussion.messages:
            history_tokens += _estimate_tokens(msg.content)

    total = system_tokens + context_tokens + image_tokens + history_tokens

    return {
        "total_tokens": total,
        "system_tokens": system_tokens,
        "context_tokens": context_tokens,
        "image_tokens": image_tokens,
        "image_count": len(images),
        "history_tokens": history_tokens,
        "history_messages": len(discussion.messages) if discussion else 0,
    }


# ─── Основной чат ─────────────────────────────────────────────

async def send_chat_message(
    project_id: str,
    item_id: str,
    item_type: str,
    user_message: str,
    model: str,
) -> dict:
    """Отправить сообщение в чат и получить ответ LLM.

    Returns:
        {"reply": str, "input_tokens": int, "output_tokens": int, "cost_usd": float, "total_cost_usd": float}
    """
    discussion = _ensure_discussion(project_id, item_id, item_type, model)

    # Собрать контекст
    if item_type == "finding":
        context_text, images = _build_finding_context(project_id, item_id)
    else:
        context_text, images = _build_optimization_context(project_id, item_id)

    is_first = len(discussion.messages) == 0

    # Сжатие перед новым сообщением (для CLI сжатие через OpenRouter дефолтную модель)
    compress_model = model if not _is_cli_model(model) else "google/gemini-2.5-pro"
    await _maybe_compress_history(discussion, compress_model)

    if _is_cli_model(model):
        # Claude CLI: текстовый промпт (без images — CLI не поддерживает inline images)
        prompt = _build_cli_prompt(context_text, discussion, user_message)
        result: LLMResult = await _run_cli_chat(prompt)
    else:
        # OpenRouter: multimodal messages
        messages = _build_messages(discussion, context_text, images, user_message, is_first)
        result: LLMResult = await run_llm(
            stage="discussion",
            messages=messages,
            response_format=None,  # свободный текст, не JSON
            temperature=DISCUSSION_TEMPERATURE,
            timeout=DISCUSSION_TIMEOUT,
            max_retries=2,
            model_override=model,
        )

    now = datetime.now(timezone.utc).isoformat()

    if result.is_error:
        return {
            "reply": f"Ошибка LLM: {result.error_message}",
            "is_error": True,
            "input_tokens": 0,
            "output_tokens": 0,
            "cost_usd": 0.0,
            "total_cost_usd": discussion.total_cost_usd,
        }

    # Сохранить сообщения
    discussion.messages.append(DiscussionMessage(
        role="user", content=user_message, timestamp=now,
    ))
    discussion.messages.append(DiscussionMessage(
        role="assistant",
        content=result.text,
        timestamp=now,
        input_tokens=result.input_tokens,
        output_tokens=result.output_tokens,
        cost_usd=result.cost_usd,
    ))

    discussion.total_input_tokens += result.input_tokens
    discussion.total_output_tokens += result.output_tokens
    discussion.total_cost_usd += result.cost_usd
    if result.cost_usd > 0:
        paid_cost_tracker.add(result.cost_usd)
    discussion.model = model

    _save_discussion(project_id, discussion)

    return {
        "reply": result.text,
        "is_error": False,
        "input_tokens": result.input_tokens,
        "output_tokens": result.output_tokens,
        "cost_usd": result.cost_usd,
        "total_cost_usd": discussion.total_cost_usd,
    }


# ─── Стриминг чата ─────────────────────────────────────────────

from typing import AsyncGenerator


async def _stream_cli_chat(prompt_text: str) -> AsyncGenerator[dict, None]:
    """Стриминг ответа Claude CLI через stream-json."""
    from webapp.services.process_runner import run_command_stream
    from webapp.config import get_claude_model

    model = get_claude_model()
    cmd = [
        get_claude_cli(), "-p",
        "--model", model,
        "--output-format", "stream-json",
        "--verbose",
        "--allowedTools", "WebSearch", "WebFetch",
    ]
    env_overrides = {k: None for k in os.environ if k.startswith("CLAUDE")}

    full_text = ""
    async for line in run_command_stream(
        cmd, input_text=prompt_text, env_overrides=env_overrides,
        timeout=DISCUSSION_CLI_TIMEOUT,
    ):
        if line == '[TIMEOUT]':
            yield {"type": "error", "message": "Claude CLI timeout"}
            return
        if not line.strip():
            continue
        try:
            data = json.loads(line)
        except json.JSONDecodeError:
            continue

        # Claude stream-json формат: каждая строка — JSON-объект
        # type=assistant с content_block delta, или type=result с финальным текстом
        msg_type = data.get("type", "")

        if msg_type == "assistant":
            # Дельта текста в message.content (может быть строка или массив блоков)
            raw_content = data.get("message", {}).get("content", "")
            if isinstance(raw_content, list):
                # Новый формат: [{"type": "text", "text": "..."}]
                content = "".join(
                    block.get("text", "") for block in raw_content
                    if isinstance(block, dict) and block.get("type") == "text"
                )
            else:
                content = raw_content or ""
            if content:
                full_text += content
                yield {"type": "delta", "text": content}
        elif msg_type == "content_block_delta":
            delta = data.get("delta", {}).get("text", "")
            if delta:
                full_text += delta
                yield {"type": "delta", "text": delta}
        elif msg_type == "result":
            result_text = data.get("result", "")
            if result_text and not full_text:
                full_text = result_text
            yield {
                "type": "done",
                "text": full_text or result_text,
                "input_tokens": 0,
                "output_tokens": 0,
                "cost_usd": 0.0,
            }
            return
        elif msg_type == "error":
            yield {"type": "error", "message": data.get("error", {}).get("message", "Unknown error")}
            return

    # Если не было result — отправить done с тем что накопили
    if full_text:
        yield {"type": "done", "text": full_text, "input_tokens": 0, "output_tokens": 0, "cost_usd": 0.0}


async def send_chat_message_stream(
    project_id: str,
    item_id: str,
    item_type: str,
    user_message: str,
    model: str,
    image: str = None,
) -> AsyncGenerator[dict, None]:
    """Стриминг чата — yield дельт по мере генерации, сохранение после done."""
    # Сразу отправить start-событие чтобы клиент знал что соединение установлено
    yield {"type": "start"}
    await asyncio.sleep(0)  # flush event loop

    discussion = _ensure_discussion(project_id, item_id, item_type, model)

    # Контекст
    if item_type == "finding":
        context_text, images = _build_finding_context(project_id, item_id)
    else:
        context_text, images = _build_optimization_context(project_id, item_id)

    # Если пользователь прикрепил фото — добавить в images
    if image and image.startswith("data:image/"):
        import base64
        # Извлечь base64 из data URL
        header, b64data = image.split(",", 1)
        media_type = header.split(";")[0].split(":")[1]  # e.g. "image/png"
        images.append({
            "type": "base64",
            "media_type": media_type,
            "data": b64data,
            "source": "user_upload",
        })

    is_first = len(discussion.messages) == 0

    # Сжатие
    compress_model = model if not _is_cli_model(model) else "google/gemini-2.5-pro"
    await _maybe_compress_history(discussion, compress_model)

    # Добавить user-сообщение в историю сразу
    now = datetime.now(timezone.utc).isoformat()
    discussion.messages.append(DiscussionMessage(
        role="user", content=user_message, timestamp=now,
    ))

    full_text = ""
    total_input = 0
    total_output = 0
    total_cost = 0.0

    if _is_cli_model(model):
        prompt = _build_cli_prompt(context_text, discussion, user_message)
        # Убрать последнее user-сообщение из промпта (оно уже в _build_cli_prompt)
        async for event in _stream_cli_chat(prompt):
            yield event
            await asyncio.sleep(0)  # дать event loop отправить чанк клиенту
            if event["type"] == "done":
                full_text = event["text"]
            elif event["type"] == "error":
                full_text = f"Ошибка: {event['message']}"
    else:
        # OpenRouter стриминг
        from webapp.services.llm_runner import run_llm_stream
        messages = _build_messages(discussion, context_text, images, user_message, is_first)
        # Убрать дублирующее user-сообщение (уже добавлено в discussion.messages)
        async for event in run_llm_stream(
            messages=messages,
            model_override=model,
            temperature=DISCUSSION_TEMPERATURE,
            timeout=DISCUSSION_TIMEOUT,
        ):
            yield event
            await asyncio.sleep(0)  # дать event loop отправить чанк клиенту
            if event["type"] == "done":
                full_text = event["text"]
                total_input = event.get("input_tokens", 0)
                total_output = event.get("output_tokens", 0)
                total_cost = event.get("cost_usd", 0.0)
            elif event["type"] == "error":
                full_text = f"Ошибка: {event['message']}"

    # Сохранить assistant-сообщение
    discussion.messages.append(DiscussionMessage(
        role="assistant",
        content=full_text,
        timestamp=datetime.now(timezone.utc).isoformat(),
        input_tokens=total_input,
        output_tokens=total_output,
        cost_usd=total_cost,
    ))
    discussion.total_input_tokens += total_input
    discussion.total_output_tokens += total_output
    discussion.total_cost_usd += total_cost
    if total_cost > 0:
        paid_cost_tracker.add(total_cost)
    discussion.model = model
    _save_discussion(project_id, discussion)

    # Финальное событие с total_cost (если не было done)
    yield {
        "type": "saved",
        "total_cost_usd": discussion.total_cost_usd,
    }


# ─── Резолюция ────────────────────────────────────────────────

def set_resolution(
    project_id: str,
    item_id: str,
    item_type: str,
    status: str,
    summary: str = "",
) -> dict:
    """Установить резолюцию и записать в findings/optimization JSON."""
    discussion = get_discussion(project_id, item_id)
    if discussion is None:
        discussion = Discussion(item_id=item_id, item_type=item_type)

    discussion.status = status
    discussion.resolution_summary = summary
    _save_discussion(project_id, discussion)

    # Записать статус в основной JSON
    _update_item_status(project_id, item_id, item_type, status, summary)

    return {"status": status, "summary": summary}


def _update_item_status(
    project_id: str,
    item_id: str,
    item_type: str,
    status: str,
    summary: str,
):
    """Записать discussion_status в 03_findings.json или optimization.json."""
    output_dir = resolve_project_dir(project_id) / "_output"

    if item_type == "finding":
        path = output_dir / "03_findings.json"
        items_key = "findings"
        fallback_key = "items"
    else:
        path = output_dir / "optimization.json"
        items_key = "items"
        fallback_key = "items"

    if not path.exists():
        return

    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, Exception):
        return

    items = data.get(items_key, data.get(fallback_key, []))
    for item in items:
        if item.get("id") == item_id:
            item["discussion_status"] = status
            item["resolution_summary"] = summary
            break

    try:
        path.write_text(
            json.dumps(data, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    except OSError as e:
        logger.error("Failed to write %s: %s", path, e)


# ─── Генерация изменённой версии ──────────────────────────────

async def generate_revised_version(
    project_id: str,
    item_id: str,
    item_type: str,
    model: str,
) -> dict:
    """Попросить LLM сгенерировать изменённую версию на основе диалога.

    Returns:
        {"original": dict, "revised": dict, "explanation": str}
    """
    discussion = get_discussion(project_id, item_id)
    if not discussion or not discussion.messages:
        return {"error": "Нет истории обсуждения"}

    # Загрузить оригинал
    output_dir = resolve_project_dir(project_id) / "_output"
    if item_type == "finding":
        data = _load_json(output_dir / "03_findings.json")
        items = data.get("findings", data.get("items", [])) if data else []
    else:
        data = _load_json(output_dir / "optimization.json")
        items = data.get("items", []) if data else []

    original = None
    for item in items:
        if item.get("id") == item_id:
            original = item
            break

    if not original:
        return {"error": f"Элемент {item_id} не найден"}

    # Собрать историю диалога
    history = "\n".join(
        f"{'Пользователь' if m.role == 'user' else 'Ассистент'}: {m.content}"
        for m in discussion.messages
    )
    if discussion.summary_of_old:
        history = f"[Резюме ранних сообщений]: {discussion.summary_of_old}\n\n{history}"

    prompt = f"""На основе обсуждения ниже, сгенерируй изменённую версию замечания.

Оригинал:
{json.dumps(original, ensure_ascii=False, indent=2)}

История обсуждения:
{history}

Верни JSON с двумя полями:
- "revised": полный объект замечания с внесёнными изменениями (сохрани все поля, измени только то, что обсуждалось)
- "explanation": краткое объяснение что и почему изменено (1-2 предложения)

Отвечай только JSON."""

    if _is_cli_model(model):
        cli_prompt = f"{SYSTEM_PROMPT}\n\n{prompt}"
        result = await _run_cli_chat(cli_prompt)
        # Попробовать распарсить JSON из ответа CLI
        import re
        json_data = None
        if result.text:
            try:
                json_data = json.loads(result.text)
            except json.JSONDecodeError:
                md_match = re.search(r'```(?:json)?\s*\n(.*?)\n```', result.text, re.DOTALL)
                if md_match:
                    try:
                        json_data = json.loads(md_match.group(1))
                    except json.JSONDecodeError:
                        pass
        result.json_data = json_data
    else:
        result = await run_llm(
            stage="discussion",
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": prompt},
            ],
            response_format={"type": "json_object"},
            temperature=0.2,
            timeout=DISCUSSION_TIMEOUT,
            model_override=model,
        )

    if result.is_error:
        return {"error": f"Ошибка LLM: {result.error_message}"}

    discussion.total_input_tokens += result.input_tokens
    discussion.total_output_tokens += result.output_tokens
    discussion.total_cost_usd += result.cost_usd
    if result.cost_usd > 0:
        paid_cost_tracker.add(result.cost_usd)
    _save_discussion(project_id, discussion)

    revised_data = result.json_data or {}
    return {
        "original": original,
        "revised": revised_data.get("revised", original),
        "explanation": revised_data.get("explanation", ""),
        "cost_usd": result.cost_usd,
        "total_cost_usd": discussion.total_cost_usd,
    }


def apply_revision(project_id: str, item_id: str, item_type: str, revised: dict) -> dict:
    """Применить изменённую версию замечания в основной JSON."""
    output_dir = resolve_project_dir(project_id) / "_output"

    if item_type == "finding":
        path = output_dir / "03_findings.json"
        items_key = "findings"
        fallback_key = "items"
    else:
        path = output_dir / "optimization.json"
        items_key = "items"
        fallback_key = "items"

    if not path.exists():
        return {"error": "Файл не найден"}

    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, Exception):
        return {"error": "Ошибка чтения JSON"}

    items = data.get(items_key, data.get(fallback_key, []))
    for i, item in enumerate(items):
        if item.get("id") == item_id:
            # Сохранить оригинал
            item["_original"] = {k: v for k, v in item.items() if k != "_original"}
            # Применить изменения (не трогая id и _original)
            for k, v in revised.items():
                if k not in ("id", "_original"):
                    item[k] = v
            item["discussion_status"] = "revised"
            item["revised_at"] = datetime.now(timezone.utc).isoformat()
            break

    path.write_text(
        json.dumps(data, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    # Обновить discussion
    disc = get_discussion(project_id, item_id)
    if disc:
        disc.status = "revised"
        disc.resolution_summary = "Замечание изменено по результатам обсуждения"
        _save_discussion(project_id, disc)

    return {"ok": True}


# ─── Обрезка истории (редактирование сообщения) ────────────────

def truncate_messages(project_id: str, item_id: str, keep_count: int) -> dict:
    """Обрезать историю обсуждения до keep_count сообщений."""
    disc = get_discussion(project_id, item_id)
    if disc is None:
        return {"ok": True}
    disc.messages = disc.messages[:keep_count]
    _save_discussion(project_id, disc)
    return {"ok": True, "remaining": len(disc.messages)}


# ─── Список для фронтенда ─────────────────────────────────────

def list_discussion_items(
    project_id: str,
    item_type: str = "finding",
) -> list[DiscussionListItem]:
    """Список замечаний/оптимизаций с индикатором статуса обсуждения."""
    output_dir = resolve_project_dir(project_id) / "_output"
    discussions_dir = output_dir / "discussions"

    # Загрузить существующие обсуждения
    existing: dict[str, Discussion] = {}
    if discussions_dir.exists():
        for f in discussions_dir.glob("*.json"):
            disc = get_discussion(project_id, f.stem)
            if disc:
                existing[disc.item_id] = disc

    items: list[DiscussionListItem] = []

    if item_type == "finding":
        data = _load_json(output_dir / "03_findings.json")
        if not data:
            return []
        for f in data.get("findings", data.get("items", [])):
            fid = f.get("id", "")
            disc = existing.get(fid)
            items.append(DiscussionListItem(
                item_id=fid,
                item_type="finding",
                severity=f.get("severity", ""),
                problem=f.get("description") or f.get("problem") or f.get("finding") or "",
                discussion_status=f.get("discussion_status") or (disc.status if disc else None),
                has_discussion=disc is not None and len(disc.messages) > 0,
                message_count=len(disc.messages) if disc else 0,
                sheet=f.get("sheet") or "",
                norm=f.get("norm") or f.get("norm_reference") or "",
                recommendation=f.get("solution") or f.get("recommendation") or "",
                resolution_summary=f.get("resolution_summary") or (disc.resolution_summary if disc and disc.resolution_summary else ""),
                page=f.get("page"),
                sub_findings=f.get("sub_findings"),
            ))
    else:
        data = _load_json(output_dir / "optimization.json")
        if not data:
            return []
        for item in data.get("items", []):
            oid = item.get("id", "")
            disc = existing.get(oid)
            items.append(DiscussionListItem(
                item_id=oid,
                item_type="optimization",
                severity=item.get("type", ""),
                problem=item.get("description", item.get("proposed", ""))[:300],
                discussion_status=item.get("discussion_status") or (disc.status if disc else None),
                has_discussion=disc is not None and len(disc.messages) > 0,
                message_count=len(disc.messages) if disc else 0,
                opt_type=item.get("type") or "",
                current=item.get("current") or "",
                proposed=item.get("proposed") or "",
                savings_pct=item.get("savings_pct"),
                savings_basis=item.get("savings_basis") or "",
                risks=item.get("risks") or "",
                norm=item.get("norm") or "",
                resolution_summary=item.get("resolution_summary") or (disc.resolution_summary if disc and disc.resolution_summary else ""),
                page=item.get("page"),
                sheet=item.get("sheet") or "",
                section=item.get("section") or "",
                spec_items=item.get("spec_items") or [],
            ))

    return items
