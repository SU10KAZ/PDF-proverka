"""
Формирование structured messages для OpenRouter API.

Для каждого из 10 этапов пайплайна формирует list[dict] сообщений
(system + user), пригодных для отправки в llm_runner.run_llm().

Переиспользует функции из task_builder.py (загрузка шаблонов, инъекция
дисциплин, document_graph, вендор-лист). НЕ дублирует код.

Старый Claude CLI пайплайн НЕ затрагивается.
"""
import json
import logging
import os
import re
from pathlib import Path

from backend.app.services.storage.stage_artifacts import (
    BLOCKS_ANALYSIS_FILENAME,
    BLOCKS_FOR_TEXT_FILENAME,
    TEXT_ANALYSIS_FILENAME,
    resolve_existing,
)

from backend.app.core.config import (
    BASE_DIR, PROJECTS_DIR,
    TEXT_ANALYSIS_TASK_TEMPLATE, BLOCK_ANALYSIS_TASK_TEMPLATE,
    FINDINGS_MERGE_TASK_TEMPLATE,
    NORM_VERIFY_TASK_TEMPLATE, NORM_FIX_TASK_TEMPLATE,
    OPTIMIZATION_TASK_TEMPLATE,
    OPTIMIZATION_CRITIC_TASK_TEMPLATE, OPTIMIZATION_CORRECTOR_TASK_TEMPLATE,
    get_stage_model, is_codex_model,
)
from backend.app.pipeline.stages.prepare.task_builder import (
    load_template_for_llm,
    _inject_discipline,
    _get_md_file_path,
    _get_project_paths,
    _load_document_graph,
    _build_structured_block_context,
    _load_vendor_list_for_discipline,
    _extract_page_to_sheet_map,
)
from backend.app.pipeline.stages.optimization.prescan import (
    build_optimization_prescan_section_from_text,
)
from backend.app.services.common.project_service import resolve_project_dir
from backend.app.services.llm.llm_runner import build_interleaved_content, make_image_content
from backend.app.services.common import audit_scope


def _version_output_dir(project_id: str) -> Path:
    """Папка `_output` активной версии (через bind_version() ContextVar);
    fallback — root `_output` для V1 / legacy.

    Используем для всех reads/writes runtime artefacts, чтобы V2 audit не
    читал V1 cached artifacts (02_text_analysis.json, 01_blocks_analysis.json и др.).
    Делегирует единому резолверу version_service.resolve_active_output_dir
    (reserc.md #97).
    """
    env_output_dir = audit_scope.get_output_dir()
    if env_output_dir:
        return Path(env_output_dir)

    from backend.app.services.common import version_service
    return version_service.resolve_active_output_dir(project_id)


def _version_project_dir(project_id: str) -> Path:
    """version_dir активной версии (parent of _output)."""
    env_version_dir = audit_scope.get_version_dir()
    if env_version_dir:
        return Path(env_version_dir)
    return _version_output_dir(project_id).parent

logger = logging.getLogger(__name__)

# Codex CLI отклоняет ход, чей вход превышает 1 048 576 символов
# (turn/start → input_too_large). Бюджет с запасом на служебную обёртку
# _build_json_prompt: если сборка merge-промпта превышает его, для Codex
# переключаемся на компактную проекцию 01_blocks_analysis.json.
_CODEX_INPUT_CHAR_LIMIT = 1_048_576
_CODEX_MERGE_INPUT_BUDGET = 1_000_000


# ═══════════════════════════════════════════════════════════════════════════
# Вспомогательные функции
# ═══════════════════════════════════════════════════════════════════════════

# Строки-паттерны, которые нужно убрать из шаблонов при использовании
# через API (Claude CLI-специфичные инструкции).
_CLI_PATTERNS = [
    re.compile(r"^.*Read tool.*$", re.IGNORECASE | re.MULTILINE),
    re.compile(r"^.*Write tool.*$", re.IGNORECASE | re.MULTILINE),
    re.compile(r"^.*Read file.*$", re.IGNORECASE | re.MULTILINE),
    re.compile(r"^.*WRITE via Write.*$", re.IGNORECASE | re.MULTILINE),
    re.compile(r"^.*read EACH one via Read.*$", re.IGNORECASE | re.MULTILINE),
    re.compile(r"^.*Write JSON via Write tool.*$", re.IGNORECASE | re.MULTILINE),
    re.compile(r"^.*After writing, output a brief summary.*$", re.IGNORECASE | re.MULTILINE),
    re.compile(r"^.*Do not output to chat.*$", re.IGNORECASE | re.MULTILINE),
    re.compile(r"^.*DO NOT output to chat.*$", re.MULTILINE),
]


def _clean_template_for_api(template: str) -> str:
    """Убрать Claude CLI-специфичные инструкции из шаблона.

    - Строки с Read/Write tool, Read file, WRITE via Write
    - Строки с инструкциями про вывод в чат
    - Строки с конкретными путями {OUTPUT_PATH}/filename (но сохраняет JSON-схему)
    """
    result = template

    for pattern in _CLI_PATTERNS:
        result = pattern.sub("", result)

    # Убрать строки вида "READ: `{PROJECT_PATH}/..."  и "WRITE: `{PROJECT_PATH}/..."
    # Also covers {DISCIPLINE_NORMS_FILE}, {MD_FILE_PATH} and other path placeholders
    result = re.sub(
        r"^.*(?:READ|WRITE):\s*`\{(?:OUTPUT_PATH|PROJECT_PATH|BASE_DIR|DISCIPLINE_NORMS_FILE|MD_FILE_PATH)\}.*$",
        "", result, flags=re.MULTILINE,
    )

    # Убрать пустые строки, оставшиеся после удаления (схлопнуть тройные+ переводы строк)
    result = re.sub(r"\n{3,}", "\n\n", result)

    return result.strip()


def _load_and_clean_template(
    template_path: Path,
    project_info: dict,
    project_id: str,
    **extra_placeholders: str,
) -> str:
    """Загрузить EN шаблон, инъектировать дисциплину, очистить от CLI-инструкций.

    Args:
        template_path: путь к RU-шаблону (EN загружается автоматически)
        project_info: dict с project_info.json
        project_id: ID проекта
        **extra_placeholders: дополнительные подстановки ({KEY} -> value)

    Returns:
        Готовый текст system prompt.
    """
    template = load_template_for_llm(template_path)
    template = _inject_discipline(template, project_info)
    template = _clean_template_for_api(template)

    # Стандартные подстановки
    section = (project_info or {}).get("section", "EOM")
    template = template.replace("{PROJECT_ID}", project_id)
    template = template.replace("{SECTION}", section)

    # Подстановка путей (на случай если остались после очистки)
    _, output_path = _get_project_paths(project_id)
    md_file_path = _get_md_file_path(project_info, project_id)
    project_path = str(_version_project_dir(project_id))

    template = template.replace("{OUTPUT_PATH}", output_path)
    template = template.replace("{MD_FILE_PATH}", md_file_path)
    template = template.replace("{PROJECT_PATH}", project_path)
    template = template.replace("{BASE_DIR}", str(BASE_DIR))

    # Дополнительные плейсхолдеры
    for key, value in extra_placeholders.items():
        template = template.replace(f"{{{key}}}", value)

    return template


def _read_text_analysis_for_blocks(project_id: str) -> str:
    """Прочитать 02_text_analysis.json, оставив только секции нужные для блочного анализа.

    Отправляет:
      - project_params  — параметры проекта (марки, мощности, сечения) для cross-check
      - text_findings   — текстовые замечания для межстраничной сверки
      - normative_refs_found — статусы нормативных документов

    Отрезает (экономия 15-72K chars на батч):
      - blocks_for_review   — уже отработал при формировании батчей
      - blocks_skipped      — служебная информация
      - image_block_priorities / image_blocks_priority — служебная
      - arithmetic_checks, arithmetic_verification и прочие промежуточные
    """
    file_path = resolve_existing(_version_output_dir(project_id), TEXT_ANALYSIS_FILENAME)
    if not file_path.exists():
        return "(файл 02_text_analysis.json не найден)"
    try:
        data = json.loads(file_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as e:
        return f"(ошибка чтения 02_text_analysis.json: {e})"

    KEEP_KEYS = {"project_params", "text_findings", "normative_refs_found"}
    filtered = {k: v for k, v in data.items() if k in KEEP_KEYS}
    if not filtered:
        return json.dumps(data, ensure_ascii=False, indent=2)
    return json.dumps(filtered, ensure_ascii=False, indent=2)


def _read_json_file(project_id: str, filename: str) -> str:
    """Прочитать JSON файл из _output/ и вернуть как строку.

    Returns:
        Содержимое файла или сообщение об отсутствии.
    """
    file_path = resolve_existing(_version_output_dir(project_id), filename)
    if not file_path.exists():
        return f"(файл {filename} не найден)"
    try:
        return file_path.read_text(encoding="utf-8")
    except OSError as e:
        return f"(ошибка чтения {filename}: {e})"


def _read_findings_merge_blocks(project_id: str, *, force_compact: bool = False) -> str:
    """Прочитать 01_blocks_analysis.json для merge.

    При `force_compact` (сырой payload не влезает в жёсткий лимит входа Codex
    CLI) резко уменьшаем payload: оставляем только поля, которые реально нужны,
    чтобы не терять свод замечаний из-за переполнения контекста и невалидного
    ответа.
    """
    file_path = resolve_existing(_version_output_dir(project_id), BLOCKS_ANALYSIS_FILENAME)
    if not file_path.exists():
        return "(файл 01_blocks_analysis.json не найден)"

    try:
        raw_text = file_path.read_text(encoding="utf-8")
    except OSError as e:
        return f"(ошибка чтения 01_blocks_analysis.json: {e})"

    if not force_compact:
        return raw_text

    try:
        data = json.loads(raw_text)
    except json.JSONDecodeError:
        return raw_text

    compact: dict[str, object] = {
        "stage": data.get("stage"),
        "meta": data.get("meta"),
        # Верификация текст↔блоки теперь приходит из 02_text_analysis.json
        # (items_verified_from_blocks); 01 читается целиком отдельно в build_findings_merge_messages.
        "block_analyses": [],
    }
    compact_blocks: list[dict[str, object]] = []

    for block in data.get("block_analyses", []) or []:
        findings = block.get("findings") or []
        if not findings:
            continue
        compact_blocks.append({
            "block_id": block.get("block_id"),
            "page": block.get("page"),
            "sheet": block.get("sheet"),
            "label": block.get("label"),
            "sheet_type": block.get("sheet_type"),
            "findings": [
                {
                    "id": finding.get("id"),
                    "severity": finding.get("severity"),
                    "category": finding.get("category"),
                    "finding": finding.get("finding"),
                    "norm": finding.get("norm"),
                    "value_found": finding.get("value_found"),
                    "highlight_regions": finding.get("highlight_regions"),
                }
                for finding in findings
            ],
        })

    compact["block_analyses"] = compact_blocks
    return json.dumps(compact, ensure_ascii=False, indent=2)


def _read_norms_reference(project_info: dict) -> str:
    """Прочитать нормативную базу дисциплины (norms_reference.md) inline."""
    try:
        from backend.app.services.common.discipline_service import load_discipline
        section = (project_info or {}).get("section", "EOM")
        profile = load_discipline(section)
        norms_path = profile.norms_reference_path if profile else None
        if norms_path and Path(norms_path).exists():
            return Path(norms_path).read_text(encoding="utf-8")
    except Exception:
        pass
    # Fallback: общий norms_reference.md
    from backend.app.core.config import BASE_DIR
    fallback = Path(BASE_DIR) / "norms_reference.md"
    if fallback.exists():
        try:
            return fallback.read_text(encoding="utf-8")
        except OSError:
            pass
    return ""


def _read_md_file(project_info: dict, project_id: str) -> str:
    """Прочитать обязательный MD-файл проекта и вернуть как строку."""
    md_path_raw = _get_md_file_path(project_info or {}, project_id)
    if not md_path_raw or md_path_raw == "(нет)":
        raise FileNotFoundError(
            "Markdown PDF representation is required: md_file is not set in project_info.json"
        )

    md_path = Path(md_path_raw)
    if not md_path.exists():
        md_name = (project_info or {}).get("md_file") or md_path.name
        raise FileNotFoundError(
            f"Markdown PDF representation is required: {md_name} not found"
        )
    try:
        return md_path.read_text(encoding="utf-8")
    except OSError as e:
        raise RuntimeError(f"Failed to read Markdown PDF representation: {e}") from e


def _resolve_text_analysis_source(project_info: dict, project_id: str) -> tuple[str, str, str]:
    """Выбрать источник текста для stage 01.

    Новая архитектура требует Markdown PDF representation. `document_graph`
    больше не является fallback-источником для текстового аудита.
    """
    return (
        "md",
        _read_md_file(project_info, project_id),
        "Below is the full text of the project MD file. Analyze it according to the instructions.\n\n",
    )


def _get_plan_images(project_id: str) -> list[Path]:
    """Получить PNG планов/схем для optimization (пространственный анализ).

    Фильтрует блоки по sheet_type из document_graph и 01_blocks_analysis.json.
    Поддерживает английские и русские названия типов.
    """
    project_dir = _version_project_dir(project_id)
    output_dir = _version_output_dir(project_id)
    # Резолвим через resolve_blocks_dir: хардкод "blocks" не находил кропы
    # у проектов projects_v2, где они лежат в blocks_stage02_100.
    from backend.app.pipeline.stages.block_context.contract import resolve_blocks_dir

    blocks_dir = resolve_blocks_dir(output_dir)
    if not blocks_dir.exists():
        return []

    PLAN_TYPES = {
        "floor_plan", "schematic", "axonometric", "single_line_diagram",
        "план", "схема", "аксонометрия", "однолинейная схема",
        "план этажа", "однолинейная схема", "аксонометрия",
    }

    def _matches_plan_type(sheet_type: str) -> bool:
        if not sheet_type:
            return False
        st_lower = sheet_type.lower()
        return any(pt in st_lower for pt in PLAN_TYPES)

    # Загружаем index.json для маппинга block_id -> file
    block_id_to_file: dict[str, str] = {}
    index_path = blocks_dir / "index.json"
    if index_path.exists():
        try:
            index_data = json.loads(index_path.read_text(encoding="utf-8"))
            for block in index_data.get("blocks", []):
                bid = block.get("block_id", "")
                fname = block.get("file", "")
                if bid and fname:
                    block_id_to_file[bid] = fname
        except (json.JSONDecodeError, OSError):
            pass

    def _resolve_block_path(block_id: str) -> Path | None:
        """Найти PNG файл блока по block_id."""
        # 1. Через index.json (+ восстановление эвакуированного кропа)
        from backend.app.services.common import block_crop_store

        resolved = block_crop_store.resolve_block_image(
            blocks_dir, block_id, file_name=block_id_to_file.get(block_id)
        )
        if resolved is not None:
            return resolved
        # 2. Glob fallback
        for png in blocks_dir.glob(f"*{block_id}*.png"):
            return png
        # 3. Direct name
        candidate = blocks_dir / f"{block_id}.png"
        if candidate.exists():
            return candidate
        return None

    seen: set[Path] = set()
    plan_images: list[Path] = []

    def _add(p: Path | None):
        if p and p not in seen:
            seen.add(p)
            plan_images.append(p)

    # Источник 1: document_graph image_blocks
    graph = _load_document_graph(project_id)
    if graph:
        for page in graph.get("pages", []):
            for img in page.get("image_blocks", []):
                sheet_type = img.get("type", "")
                if _matches_plan_type(sheet_type):
                    file_name = img.get("file")
                    if file_name:
                        img_path = blocks_dir / file_name
                        if img_path.exists():
                            _add(img_path)
                            continue
                    # fallback по block_id
                    bid = img.get("block_id", "")
                    if bid:
                        _add(_resolve_block_path(bid))

    # Источник 2: 01_blocks_analysis.json (sheet_type из LLM-анализа)
    analysis_path = resolve_existing(output_dir, BLOCKS_ANALYSIS_FILENAME)
    if analysis_path.exists():
        try:
            analysis = json.loads(analysis_path.read_text(encoding="utf-8"))
            for ba in analysis.get("block_analyses", []):
                st = ba.get("sheet_type", "")
                if _matches_plan_type(st):
                    bid = ba.get("block_id", "")
                    if bid:
                        _add(_resolve_block_path(bid))
        except (json.JSONDecodeError, OSError):
            pass

    return plan_images


def _build_page_contexts_from_graph(
    project_id: str,
    block_ids: list[str],
    block_pages: list[int],
) -> dict[int, str]:
    """Построить {page: context_text} для interleaved content.

    Переиспользует _build_structured_block_context из task_builder.
    Возвращает словарь для передачи в build_interleaved_content().
    """
    graph = _load_document_graph(project_id)
    if not graph:
        return {}

    # Индекс страниц
    page_contexts: dict[int, str] = {}
    target_pages = set(block_pages)

    for page_data in graph.get("pages", []):
        page_num = page_data["page"]
        if target_pages and page_num not in target_pages:
            continue

        lines = []
        sheet_no = page_data.get("sheet_no_raw") or page_data.get("sheet_no_normalized") or page_data.get("sheet_no")
        sheet_name = page_data.get("sheet_name")
        if sheet_no:
            lines.append(f"Sheet: {sheet_no}")
        if sheet_name:
            lines.append(f"Title: {sheet_name}")

        # Текстовые блоки на странице
        for tb in page_data.get("text_blocks", []):
            text = tb.get("text_norm") or tb.get("text") or ""
            if text:
                lines.append(text)

        page_contexts[page_num] = "\n".join(lines)

    return page_contexts


# ═══════════════════════════════════════════════════════════════════════════
# Этап 1: Анализ текста
# ═══════════════════════════════════════════════════════════════════════════

def build_text_analysis_messages(
    project_info: dict,
    project_id: str,
    *,
    include_norms: bool = True,
    md_override: str | None = None,
    skeleton: str | None = None,
) -> list[dict]:
    """Сформировать messages для text_analysis.

    system: шаблон с инъекцией дисциплины + (опц.) нормативная база inline
    user: полный текст MD-файла (или md_override для чанка)

    Аргументы нарезки (используются только на больших проектах, где полный
    промпт превышает лимит Codex — см. md_chunker):
        include_norms — вкладывать ли inline-базу норм в system. Tier 1 при
            переполнении ставит False (этап 04 перепроверяет нормы отдельно).
        md_override   — тело MD для конкретного чанка вместо полного файла.
        skeleton      — общий «скелет» сводных листов (ПЗ/нагрузки/спец),
            добавляемый в user reference-only ради перекрёстной сверки.
    """
    from backend.app.pipeline.stages.prepare.task_builder import _absence_guard_block
    system_prompt = _load_and_clean_template(
        TEXT_ANALYSIS_TASK_TEMPLATE, project_info, project_id,
        ABSENCE_GUARD=_absence_guard_block(),
        BLOCKS_ANALYSIS_PATH=str(resolve_existing(_version_output_dir(project_id), BLOCKS_FOR_TEXT_FILENAME)),
    )

    text_source, source_text, user_prefix = _resolve_text_analysis_source(project_info, project_id)
    if md_override is not None:
        source_text = md_override
    model = get_stage_model("text_analysis")

    # Подгрузить нормативную базу дисциплины inline там, где контекст позволяет.
    # Актуальность норм всё равно финально проверяется отдельным stage 04 через
    # MCP norms. include_norms=False — Tier 1 разгрузки промпта на больших проектах.
    norms_text = _read_norms_reference(project_info)
    if include_norms and norms_text:
        system_prompt += f"\n\n## Normative Reference (discipline norms database)\n\n{norms_text}"
    else:
        system_prompt += (
            "\n\n## Normative Reference\n\n"
            "Stage 04 will verify normative references separately. "
            "At this stage, extract only norms explicitly present in the provided source text."
        )

    try:
        from backend.app.pipeline.stages.text_analysis.md_prescan import (
            build_prescan_prompt_section,
        )
        md_file_path = _get_md_file_path(project_info, project_id)
        prescan_section = build_prescan_prompt_section(md_file_path)
        if prescan_section:
            system_prompt += "\n\n" + prescan_section
    except Exception:
        pass

    if skeleton:
        user_body = (
            "## Сводные листы для перекрёстной сверки (REFERENCE ONLY)\n\n"
            "Ниже — сводные текстовые листы проекта (ПЗ, таблицы нагрузок, "
            "спецификация). Используй их ТОЛЬКО для перекрёстной сверки значений "
            "с целевыми листами этого прохода (ПЗ↔таблицы↔спецификация). НЕ "
            "выпускай замечания, чья первичная привязка — сводный лист, если "
            "расхождение не затрагивает целевой лист этого прохода.\n\n"
            f"{skeleton}\n\n"
            "## Целевые листы этого прохода (замечания выпускай здесь):\n\n"
            f"{source_text}"
        )
    else:
        user_body = source_text

    return [
        {"role": "system", "content": system_prompt},
        {
            "role": "user",
            "content": (
                f'{user_prefix}Required output field: `"text_source": "{text_source}"`.\n\n'
                f"{user_body}"
            ),
        },
    ]


def _messages_char_len(messages: list[dict]) -> int:
    """Суммарная длина текстового содержимого messages (для оценки лимита Codex)."""
    total = 0
    for m in messages:
        content = m.get("content")
        if isinstance(content, str):
            total += len(content)
        elif isinstance(content, list):
            for part in content:
                if isinstance(part, dict) and isinstance(part.get("text"), str):
                    total += len(part["text"])
    return total


def build_text_analysis_message_sets(
    project_info: dict,
    project_id: str,
    *,
    budget: int,
) -> tuple[list[list[dict]], dict]:
    """Спланировать проходы text_analysis под лимит Codex на один ход.

    Возвращает (список наборов messages, meta). Один набор → single-pass
    (поведение как раньше); несколько → чанки, которые раннер сольёт.

    Двухступенчато (решение зафиксировано):
      single    — промпт с норм-базой влезает в бюджет (обычные проекты).
      Tier 1    — убрать inline норм-базу; если влезло — один проход.
      Tier 2    — нарезка MD по листам + общий скелет в каждый чанк.
    """
    from backend.app.pipeline.stages.text_analysis.md_chunker import (
        plan_text_analysis_chunks,
    )

    # single-pass с нормами (как сегодня).
    msgs = build_text_analysis_messages(project_info, project_id)
    size = _messages_char_len(msgs)
    if size <= budget:
        return [msgs], {"mode": "single", "chars": size}

    # Tier 1: разгрузить system, убрав inline норм-базу (этап 04 перепроверит).
    msgs_nn = build_text_analysis_messages(project_info, project_id, include_norms=False)
    size_nn = _messages_char_len(msgs_nn)
    if size_nn <= budget:
        return [msgs_nn], {"mode": "single_no_norms", "chars": size_nn}

    # Tier 2: нарезка по листам со скелетом.
    _, source_text, _ = _resolve_text_analysis_source(project_info, project_id)
    system_len = len(msgs_nn[0].get("content", ""))
    plan = plan_text_analysis_chunks(
        source_text, total_budget=budget, system_len=system_len,
    )
    if plan is None:
        # Теоретически недостижимо (size_nn > budget), но перестрахуемся.
        return [msgs_nn], {"mode": "single_no_norms", "chars": size_nn}

    sets = [
        build_text_analysis_messages(
            project_info, project_id,
            include_norms=False, md_override=chunk, skeleton=plan.skeleton,
        )
        for chunk in plan.chunks
    ]
    meta = {
        "mode": "chunked",
        "chunks": len(sets),
        "total_pages": plan.total_pages,
        "skeleton_pages": plan.skeleton_pages,
        "skeleton_chars": len(plan.skeleton),
        "skeleton_truncated": plan.skeleton_truncated,
        "chars_no_norms": size_nn,
    }
    return sets, meta


# ═══════════════════════════════════════════════════════════════════════════
# Этап 2: Анализ пакета блоков
# ═══════════════════════════════════════════════════════════════════════════

def build_block_batch_messages(
    batch_data: dict,
    project_info: dict,
    project_id: str,
    total_batches: int,
    *,
    image_scale: float = 1.0,
) -> list[dict]:
    """Сформировать messages для block_batch.

    image_scale<1.0 — ресайз PNG блоков перед base64 (fallback для локального
    GEMMA: на ошибке retry с меньшим разрешением).
    """
    batch_id = batch_data["batch_id"]
    blocks = batch_data.get("blocks", [])
    block_ids = [b["block_id"] for b in blocks]
    block_pages = [b["page"] for b in blocks if b.get("page")]

    # Контекст из document_graph
    graph = _load_document_graph(project_id)
    if graph:
        md_context = _build_structured_block_context(graph, block_ids, block_pages)
    else:
        md_context = "(document_graph.json not available)"

    # Маппинг page -> sheet
    md_file_path = _get_md_file_path(project_info, project_id)
    page_to_sheet = _extract_page_to_sheet_map(md_file_path)

    # Список блоков (текстовый)
    block_lines = []
    for block in blocks:
        pdf_page = block.get("page", "?")
        sheet_info = page_to_sheet.get(pdf_page, "")
        sheet_suffix = f", Sheet {sheet_info}" if sheet_info else ""
        block_lines.append(
            f"- block_id: {block['block_id']}, стр. {pdf_page}{sheet_suffix}, "
            f"OCR: {block.get('ocr_label', 'image')}"
        )

    system_prompt = _load_and_clean_template(
        BLOCK_ANALYSIS_TASK_TEMPLATE, project_info, project_id,
        BATCH_ID=str(batch_id),
        BATCH_ID_PADDED=f"{batch_id:03d}",
        TOTAL_BATCHES=str(total_batches),
        BLOCK_COUNT=str(len(blocks)),
        BLOCK_LIST="\n".join(block_lines),
        BLOCK_MD_CONTEXT=md_context if md_context else "(no context available)",
    )

    # 02_text_analysis.json для cross-check (только нужные секции)
    text_analysis = _read_text_analysis_for_blocks(project_id)

    # Interleaved content (text + PNG) — version-aware project_dir, чтобы PNG-блоки
    # читались из _versions/v{N}/_output/blocks для V2 audit, а не из V1.
    project_dir = _version_project_dir(project_id)
    page_contexts = _build_page_contexts_from_graph(project_id, block_ids, block_pages)

    interleaved = build_interleaved_content(
        blocks, page_contexts, project_dir, image_scale=image_scale,
    )

    # User message: text_analysis context + interleaved blocks
    user_content: list[dict] = [
        {
            "type": "text",
            "text": (
                f"## Text analysis context (02_text_analysis.json):\n\n"
                f"{text_analysis}\n\n"
                f"## Blocks to analyze ({len(blocks)} blocks):\n\n"
                f"Analyze each block image below with its page context."
            ),
        },
    ]
    user_content.extend(interleaved)

    return [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_content},
    ]


# ═══════════════════════════════════════════════════════════════════════════
# Этап 3: Свод замечаний (findings_merge)
# ═══════════════════════════════════════════════════════════════════════════

def build_findings_merge_messages(
    project_info: dict,
    project_id: str,
) -> list[dict]:
    """Сформировать messages для findings_merge.

    system: шаблон findings_merge с инъекцией дисциплины
    user: 02_text_analysis.json + 01_blocks_analysis.json inline
    """
    system_prompt = _load_and_clean_template(
        FINDINGS_MERGE_TASK_TEMPLATE, project_info, project_id,
    )

    text_analysis = _read_json_file(project_id, TEXT_ANALYSIS_FILENAME)
    merge_model = get_stage_model("findings_merge")
    blocks_analysis = _read_findings_merge_blocks(project_id)

    # Codex CLI жёстко отклоняет ход, чей вход превышает 1 048 576 символов
    # (turn/start: input_too_large). На крупных проектах сырой
    # 01_blocks_analysis.json раздувает merge-промпт за лимит, и свод падает за
    # ~1 сек после thread.started (диагностировано на 133-23-ГК-АИ1: 1.61М симв.).
    # Падаем на компактную проекцию — она сохраняет
    # findings/severity/category/norm/value_found/highlight_regions и привязку к
    # листу (для 133-23-ГК-АИ1: 1.59М → 0.49М симв., полный промпт ~0.51М).
    codex_compacted = False
    if is_codex_model(merge_model):
        approx_chars = (
            len(system_prompt) + len(str(text_analysis)) + len(blocks_analysis) + 256
        )
        if approx_chars > _CODEX_MERGE_INPUT_BUDGET:
            blocks_analysis = _read_findings_merge_blocks(
                project_id, force_compact=True,
            )
            codex_compacted = True
            logger.warning(
                "findings_merge: сырой payload ~%d симв. > бюджета Codex %d — "
                "использую компактную проекцию 01_blocks_analysis.json (%s)",
                approx_chars, _CODEX_MERGE_INPUT_BUDGET, project_id,
            )

    user_text = (
        f"## 02_text_analysis.json:\n\n{text_analysis}\n\n"
        f"## 01_blocks_analysis.json:\n\n{blocks_analysis}"
    )

    if codex_compacted:
        user_text += (
            "\n\n## Merge note:\n\n"
            "This payload is a compact projection of 01_blocks_analysis.json. "
            "Use block_id/page/sheet/findings as source of truth; do not assume omitted fields are absent in the project."
        )

    return [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_text},
    ]


# Этап 3b (Critic + Corrector) удалён — проверку замечаний делает отдельный этап
# «Верификатор» (stages/findings_verify) детерминированно, без LLM-фильтра.


# ═══════════════════════════════════════════════════════════════════════════
# Этап 4: Верификация норм
# ═══════════════════════════════════════════════════════════════════════════

def build_norm_verify_messages(
    norms_text: str,
    project_id: str,
    project_info: dict | None = None,
) -> list[dict]:
    """Сформировать messages для norm_verify.

    system: шаблон norm_verify с инъекцией дисциплины
    user: norms_text + 03_findings.json
    """
    system_prompt = _load_and_clean_template(
        NORM_VERIFY_TASK_TEMPLATE, project_info or {}, project_id,
        LLM_WORK=norms_text,
        NORMS_LIST=norms_text,
    )

    findings = _read_json_file(project_id, "03_findings.json")

    user_text = (
        f"## Norms to verify:\n\n{norms_text}\n\n"
        f"## 03_findings.json (findings with norm references):\n\n{findings}"
    )

    return [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_text},
    ]


def build_norm_fix_messages(
    findings_to_fix: str,
    project_id: str,
    project_info: dict | None = None,
) -> list[dict]:
    """Сформировать messages для norm_fix.

    system: шаблон norm_fix с инъекцией дисциплины
    user: findings_to_fix + 03_findings.json
    """
    system_prompt = _load_and_clean_template(
        NORM_FIX_TASK_TEMPLATE, project_info or {}, project_id,
        FINDINGS_TO_FIX=findings_to_fix,
    )

    findings = _read_json_file(project_id, "03_findings.json")

    user_text = (
        f"## Findings to fix (outdated norms):\n\n{findings_to_fix}\n\n"
        f"## 03_findings.json (current findings):\n\n{findings}"
    )

    return [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_text},
    ]


# ═══════════════════════════════════════════════════════════════════════════
# Этап 5: Оптимизация
# ═══════════════════════════════════════════════════════════════════════════

def build_optimization_messages(
    project_info: dict,
    project_id: str,
) -> list[dict]:
    """Сформировать messages для optimization.

    system: шаблон optimization с vendor_list и инъекцией дисциплины
    user: MD-текст + 02_text_analysis.json + 03_findings.json + PNG планов/схем
    """
    section = (project_info or {}).get("section", "EOM")
    vendor_list = _load_vendor_list_for_discipline(section)

    system_prompt = _load_and_clean_template(
        OPTIMIZATION_TASK_TEMPLATE, project_info, project_id,
        VENDOR_LIST=vendor_list,
    )

    md_text = _read_md_file(project_info, project_id)
    text_analysis = _read_json_file(project_id, TEXT_ANALYSIS_FILENAME)
    findings = _read_json_file(project_id, "03_findings.json")
    prescan_section = build_optimization_prescan_section_from_text(
        md_text,
        section=section,
        vendor_list_text=vendor_list,
        findings_data=findings,
    )

    # Multimodal: включить PNG планов/схем
    plan_images = _get_plan_images(project_id)

    if plan_images:
        # Interleaved: текст + изображения
        user_content: list[dict] = [
            {
                "type": "text",
                "text": (
                    f"## Project MD file:\n\n{md_text}\n\n"
                    f"## 02_text_analysis.json:\n\n{text_analysis}\n\n"
                    f"## 03_findings.json:\n\n{findings}\n\n"
                    f"{prescan_section}\n\n"
                    f"## Drawings (plans and schematics):\n"
                    f"Below are {len(plan_images)} drawing images for reference."
                ),
            },
        ]
        for img_path in plan_images:
            user_content.append(make_image_content(img_path))
            user_content.append({
                "type": "text",
                "text": f"[{img_path.stem}]",
            })
    else:
        # Текстовый режим
        user_content = (
            f"## Project MD file:\n\n{md_text}\n\n"
            f"## 02_text_analysis.json:\n\n{text_analysis}\n\n"
            f"## 03_findings.json:\n\n{findings}\n\n"
            f"{prescan_section}"
        )

    return [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_content},
    ]


# ═══════════════════════════════════════════════════════════════════════════
# Этап 5b: Optimization Critic + Corrector
# ═══════════════════════════════════════════════════════════════════════════

def build_optimization_critic_messages(
    project_info: dict,
    project_id: str,
) -> list[dict]:
    """Сформировать messages для optimization_critic.

    system: шаблон optimization_critic с vendor_list
    user: optimization.json + 03_findings.json
    """
    section = (project_info or {}).get("section", "EOM")
    vendor_list = _load_vendor_list_for_discipline(section)

    system_prompt = _load_and_clean_template(
        OPTIMIZATION_CRITIC_TASK_TEMPLATE, project_info, project_id,
        VENDOR_LIST=vendor_list,
    )

    optimization = _read_json_file(project_id, "optimization.json")
    findings = _read_json_file(project_id, "03_findings.json")

    user_text = (
        f"## optimization.json (proposals to review):\n\n{optimization}\n\n"
        f"## 03_findings.json (audit findings):\n\n{findings}"
    )

    return [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_text},
    ]


def build_optimization_corrector_messages(
    project_info: dict,
    project_id: str,
) -> list[dict]:
    """Сформировать messages для optimization_corrector.

    system: шаблон optimization_corrector с vendor_list
    user: optimization.json + optimization_review.json
    """
    section = (project_info or {}).get("section", "EOM")
    vendor_list = _load_vendor_list_for_discipline(section)

    system_prompt = _load_and_clean_template(
        OPTIMIZATION_CORRECTOR_TASK_TEMPLATE, project_info, project_id,
        VENDOR_LIST=vendor_list,
    )

    optimization = _read_json_file(project_id, "optimization.json")
    review = _read_json_file(project_id, "optimization_review.json")

    user_text = (
        f"## optimization.json (proposals to correct):\n\n{optimization}\n\n"
        f"## optimization_review.json (critic verdicts):\n\n{review}"
    )

    return [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_text},
    ]
