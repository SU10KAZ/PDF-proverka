"""MD enrichment pipeline для раздела «Сравнение стадий».

Подготавливает enriched MD для последующего смыслового сравнения стадий.
Ключевое отличие от старого подхода (см. `text_llm_input.prepare_text_only_markdown`):
изображения теперь не удаляются, а заменяются на улучшенное описание от
локальной VLM (Qwen 35B через LM Studio + ngrok). Сам исходный MD остаётся
неизменным — enriched-версия пишется в `comparison/sessions/<sid>/pairs/<pid>/text_enrichment/`.

Никаких внешних paid API (OpenRouter / OpenAI / Gemini / Anthropic) этот
модуль не использует — только `local_openai_compatible` provider.

Flow:
    1. Прочитать MD одной стороны (left/right).
    2. Найти text-блоки и image/imagine-блоки + их связь с реальной картинкой
       (block_id / page → result.json или render_block_crop).
    3. Для каждого image-блока:
         a. Прокатать кеш по sha256(image_bytes + model + prompt_version);
         b. Если кеш-промах и `run_model=True`, дернуть `describe_image_local`;
         c. Иначе оставить status=pending (dry-run).
    4. Собрать enriched MD: текст не трогаем, image-блоки оборачиваем
       `original_imagine_start/end` + добавляем `#### QWEN_IMAGE_DESCRIPTION`.
    5. Записать enriched MD + image_descriptions.json + сохранить prompts/raw.

Контракт enrichment(run_model=False) — dry-run:
    * никаких сетевых вызовов;
    * MD парсится; обнаруженные image/imagine-блоки попадают в counts;
    * enriched MD НЕ перезаписывается (если уже существует, его не трогаем);
    * `summary.described == summary.from_cache + успешные блоки в этой сессии`.

Контракт enrichment(run_model=True):
    * для каждого image-блока с найденной картинкой — вызвать describe_image_local;
    * результат пишется в кеш и в image_descriptions.json;
    * enriched MD пересобирается полностью.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import re
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Awaitable, Callable, Optional

from . import graphic_llm_local as graphic_local_mod
from . import paths as paths_mod

logger = logging.getLogger(__name__)


# Версия prompt'а — входит в cache-key. При изменении prompt'а — инкрементируем.
# v1                  → базовое описание + structured-поля
# v2_scheme_analysis  → + structural/single-line schemes (электро, ОВиК, гидравлика,
#                        автоматика, слаботочка, технологические процессы)
PROMPT_VERSION = "v2_scheme_analysis"


QWEN_IMAGE_DESCRIPTION_PROMPT = """Ты анализируешь изображение из проектной/рабочей документации в строительстве.

Твоя задача — не просто описать картинку, а извлечь из неё информацию, которая может быть важна для сравнения стадий проекта.

Опиши:

1. Что изображено: чертёж, узел, схема, фасад, план, таблица, спецификация, штамп, ведомость.
2. Какие проектные решения видны.
3. Какие материалы указаны.
4. Какие элементы/оборудование/системы указаны.
5. Какие числовые параметры видны:

   * размеры;
   * отметки;
   * высоты;
   * площади;
   * мощности;
   * расходы;
   * марки;
   * типы;
   * количества.
6. Какие требования, примечания или условия видны.
7. Какие таблицы или спецификации присутствуют.
8. Какие изменения может быть важно отслеживать при сравнении с другой стадией.

Не выдумывай данные.
Если текст на изображении не читается — так и напиши.
Если это штамп/титульный лист — извлеки:

* организацию;
* стадию;
* шифр;
* номер тома;
* год;
* лист;
* наименование раздела;
* разработчика/проверяющего, если читается.

ДОПОЛНИТЕЛЬНЫЙ АНАЛИЗ СХЕМЫ

Если изображение является структурной схемой, однолинейной схемой, схемой движения
воздуха, жидкости, электроэнергии, сигнала, управления, автоматики, слаботочной
системы или технологического процесса, выполни дополнительный анализ схемы.

Нужно определить:

1. Тип схемы:
   * electrical_single_line — однолинейная электрическая схема;
   * hvac_air_flow — схема движения воздуха / вентиляции;
   * water_or_liquid_flow — схема движения воды, теплоносителя или другой жидкости;
   * automation_signal — схема автоматики, управления или сигналов;
   * low_voltage_system — слаботочные системы;
   * process_scheme — технологическая схема;
   * structural_scheme — структурная схема;
   * unknown_scheme — схема есть, но тип не определён.

2. Среду / поток:
   * electricity;
   * air;
   * water;
   * liquid;
   * heat_carrier;
   * signal;
   * control;
   * data;
   * unknown.

3. Основные узлы схемы:
   источник; ввод; оборудование; щит; автомат; счётчик; насос; вентилятор;
   фильтр; калорифер; клапан; задвижка; датчик; контроллер; исполнительный
   механизм; потребитель; помещение; контур; линия; соединение; неизвестный
   элемент.

4. Связи между элементами:
   * откуда идёт поток/питание/сигнал;
   * куда он приходит;
   * направление;
   * подпись линии;
   * параметры линии;
   * уверенность.

5. Последовательность прохождения. Примеры:
   * «Ввод → ВРУ → АВР → ГРЩ → групповой автомат → нагрузка»;
   * «Приточный воздух → фильтр → калорифер → вентилятор → воздуховод → помещение»;
   * «Насос → обратный клапан → теплообменник → регулирующий клапан → потребитель»;
   * «Датчик температуры → контроллер → привод клапана».

6. Независимые контуры: если на схеме несколько независимых линий/контуров/групп,
   описать каждый отдельно.

7. Важные для сравнения факты:
   * добавлен или удалён элемент в цепочке;
   * изменён порядок элементов;
   * изменено направление;
   * изменена точка подключения;
   * появилась перемычка, байпас, резервная линия, обходная линия;
   * изменился номер линии, маркировка, группа, контур;
   * изменились параметры оборудования;
   * изменилось количество линий или контуров.

8. Неопределённости: что не читается, где направление непонятно, где подписи
   слишком мелкие, где невозможно определить соединение.

ВАЖНО:
* НЕ выдумывай связи, которых не видно. Если направление потока/питания/сигнала
  не читается — пиши «направление не определено» или «предположительно».
* Если есть несколько независимых контуров — описывай каждый отдельно.
* Если схема слишком сложная или нечитаемая — фиксируй неопределённости.
* Если изображение НЕ является схемой — поле `scheme_analysis.is_scheme=false`
  и остальные поля внутри `scheme_analysis` оставь пустыми.

Верни JSON:

{
"status": "done",
"image_kind": "drawing|table|scheme|plan|facade|section|node|stamp|specification|unknown",
"summary": "...",
"design_solutions": ["..."],
"materials": ["..."],
"equipment": ["..."],
"numeric_parameters": [
{
"name": "...",
"value": "...",
"unit": "...",
"context": "..."
}
],
"requirements": ["..."],
"tables": ["..."],
"visible_text": ["..."],
"comparison_relevant_facts": ["..."],
"uncertainties": ["..."],
"scheme_analysis": {
"is_scheme": true,
"scheme_type": "electrical_single_line|hvac_air_flow|water_or_liquid_flow|automation_signal|low_voltage_system|process_scheme|structural_scheme|unknown_scheme",
"flow_medium": "electricity|air|water|liquid|heat_carrier|signal|control|data|unknown",
"nodes": [
{
"id": "node_1",
"label": "ВРУ",
"type": "source|input|panel|breaker|meter|equipment|valve|pump|fan|filter|heater|sensor|controller|actuator|consumer|junction|line|unknown",
"visible_mark": "...",
"parameters": ["..."],
"confidence": 0.0
}
],
"connections": [
{
"from": "node_1",
"to": "node_2",
"direction": "left_to_right|right_to_left|top_to_bottom|bottom_to_top|bidirectional|unknown",
"line_label": "...",
"parameters": ["..."],
"evidence": "стрелка, линия, подпись или другое видимое основание",
"confidence": 0.0
}
],
"sequence_summary": [
"Ввод → ВРУ → АВР → ГРЩ → нагрузка"
],
"independent_circuits": [
{
"name": "Контур 1",
"sequence": "Источник → элемент → потребитель",
"notes": "..."
}
],
"comparison_relevant_scheme_facts": [
"В цепочке присутствует байпасная линия",
"Питание идёт через АВР"
],
"uncertainties": [
"Направление потока между узлами X и Y не читается"
]
},
"confidence": 0.0
}

Если изображение не является схемой:

"scheme_analysis": {
"is_scheme": false,
"scheme_type": "unknown_scheme",
"flow_medium": "unknown",
"nodes": [],
"connections": [],
"sequence_summary": [],
"independent_circuits": [],
"comparison_relevant_scheme_facts": [],
"uncertainties": []
}

Никакого markdown вне JSON.
"""


# ─── Парсер MD ────────────────────────────────────────────────────────────


_HEADING_RE = re.compile(r"^(\s{0,3})(#{1,6})\s+(.*?)\s*$")

# Image markers (см. text_llm_input.py — оставляем совместимыми с одним
# набором правил).
_IMG_TOKENS = (
    r"\bBLOCK\s*\[\s*IMAGE\s*\]",
    r"\[\s*IMAGE\s*\]",
    r"\bIMAGE\b",
    r"\bIMAGEN\b",
    r"\bIMAGINE\b",
    r"Изображени[ея]",
    r"Графический\s+блок",
    r"Графика",
    r"Иллюстрация",
)
_IMG_TOKEN_RE = re.compile(r"(?:" + r"|".join(_IMG_TOKENS) + r")", re.IGNORECASE)

_TEXT_BLOCK_HEADING_RE = re.compile(
    r"^\s{0,3}#{1,6}\s+.*?(?:BLOCK\s*\[\s*TEXT\s*\]|\[\s*TEXT\s*\])",
    re.IGNORECASE,
)

_PAGE_HEADING_RE = re.compile(
    r"^\s{0,3}#{1,6}\s+(?:СТРАНИЦА|Страница|PAGE|Лист)\b",
    re.IGNORECASE,
)

# Извлечь block_id из заголовка вида `### BLOCK [IMAGE]: <id>` или `[IMAGE] id`
_BLOCK_ID_FROM_HEADING_RE = re.compile(
    r"\[\s*(?:IMAGE|IMAGEN|IMAGINE)\s*\]\s*[:\-]?\s*([A-Za-z0-9_\-]+)",
    re.IGNORECASE,
)

# Извлечь номер страницы из заголовка-страницы Chandra
_PAGE_NUM_RE = re.compile(
    r"^\s{0,3}#{1,6}\s+(?:СТРАНИЦА|Страница|PAGE|Лист)\s*[:№#]?\s*(\d+)",
    re.IGNORECASE,
)


def _is_image_heading(stripped: str) -> bool:
    m = _HEADING_RE.match(stripped)
    if not m:
        return False
    if _TEXT_BLOCK_HEADING_RE.match(stripped):
        return False
    if _PAGE_HEADING_RE.match(stripped):
        return False
    return bool(_IMG_TOKEN_RE.search(m.group(3) or ""))


def _heading_level(stripped: str) -> Optional[int]:
    m = _HEADING_RE.match(stripped)
    if not m:
        return None
    return len(m.group(2))


def _extract_block_id(stripped_heading: str) -> Optional[str]:
    m = _BLOCK_ID_FROM_HEADING_RE.search(stripped_heading)
    if not m:
        return None
    raw = m.group(1).strip()
    return raw or None


def _extract_page_from_heading(stripped: str) -> Optional[int]:
    m = _PAGE_NUM_RE.match(stripped)
    if not m:
        return None
    try:
        return int(m.group(1))
    except ValueError:
        return None


@dataclass
class MdBlock:
    """Логический блок MD после парсинга.

    Не привязан к нумерации страниц/листов проектной документации; это
    просто отрезок исходного текста между двумя image/text boundary'ами.
    """

    kind: str               # "text" | "image"
    text: str               # исходный текст блока, как он был в MD (с переводами строк)
    page: Optional[int]     # текущая открытая `### СТРАНИЦА N`/`### Лист N`, если известна
    block_id: Optional[str] = None  # для image-блока — id из заголовка, если он там был
    order: int = 0          # порядковый номер блока этого kind среди блоков того же типа
    image_order_on_page: Optional[int] = None  # для image-блока — индекс среди image-блоков той же страницы

    @property
    def is_image(self) -> bool:
        return self.kind == "image"


def parse_md_blocks(md_text: str) -> list[MdBlock]:
    """Разбить MD на упорядоченный список text/image-блоков.

    Парсер совместим с Chandra MD-форматом (`### BLOCK [IMAGE]: ...`,
    `### СТРАНИЦА N`, `<image>...</image>`). Никаких lossy-преобразований
    с самим текстом не делается — оригинал собирается обратно конкатенацией
    `block.text`.
    """
    if not isinstance(md_text, str):
        md_text = "" if md_text is None else str(md_text)

    blocks: list[MdBlock] = []
    cur_page: Optional[int] = None
    page_image_counter: dict[int, int] = {}
    block_image_counter = 0
    block_text_counter = 0

    # Буфер для накапливания обычного текста
    text_buf: list[str] = []

    def flush_text():
        nonlocal block_text_counter
        if not text_buf:
            return
        joined = "".join(text_buf)
        # Полностью пустой буфер из одних переводов строк — оставляем для
        # точного восстановления, но не плодим лишний text-блок.
        if joined.strip() == "" and not blocks:
            text_buf.clear()
            return
        if joined.strip() == "":
            # Пустота между блоками — приклеим к последнему блоку, чтобы при
            # сборке enriched MD не терялись разделители.
            if blocks:
                blocks[-1].text += joined
            text_buf.clear()
            return
        block_text_counter += 1
        blocks.append(MdBlock(
            kind="text",
            text=joined,
            page=cur_page,
            block_id=None,
            order=block_text_counter,
        ))
        text_buf.clear()

    def append_image(text: str, block_id: Optional[str]):
        nonlocal block_image_counter
        page_image_counter[cur_page or 0] = page_image_counter.get(cur_page or 0, 0) + 1
        block_image_counter += 1
        blocks.append(MdBlock(
            kind="image",
            text=text,
            page=cur_page,
            block_id=block_id,
            order=block_image_counter,
            image_order_on_page=page_image_counter[cur_page or 0],
        ))

    lines = md_text.splitlines(keepends=True)
    i = 0
    in_image_fence = False
    fence_buf: list[str] = []

    in_image_tag = False
    tag_buf: list[str] = []

    in_image_block_by_heading = False
    image_heading_level: Optional[int] = None
    image_heading_buf: list[str] = []
    image_heading_block_id: Optional[str] = None

    while i < len(lines):
        raw_line = lines[i]
        stripped = raw_line.rstrip("\n").rstrip("\r")

        # ── Внутри ```image fence ─────────────────────────────────────
        if in_image_fence:
            fence_buf.append(raw_line)
            m_close = re.match(r"^(\s*)```\s*(\S+)?", stripped)
            if m_close and (m_close.group(2) is None or m_close.group(2) == ""):
                in_image_fence = False
                flush_text()
                append_image("".join(fence_buf), None)
                fence_buf = []
            i += 1
            continue

        # ── Внутри <image>...</image> / [IMAGE]...[/IMAGE] ─────────────
        if in_image_tag:
            tag_buf.append(raw_line)
            if re.match(
                r"^\s*(?:<\s*/\s*(?:image|imagen|imagine)\s*>|\[\s*/\s*(?:IMAGE|IMAGEN|IMAGINE)\s*\])\s*$",
                stripped, flags=re.IGNORECASE,
            ):
                in_image_tag = False
                flush_text()
                append_image("".join(tag_buf), None)
                tag_buf = []
            i += 1
            continue

        # ── Внутри image-блока, открытого заголовком ─────────────────
        if in_image_block_by_heading:
            # Что-то заканчивает блок?
            new_level = _heading_level(stripped)
            if new_level is not None:
                ends = False
                if image_heading_level is not None and new_level <= image_heading_level:
                    ends = True
                if _TEXT_BLOCK_HEADING_RE.match(stripped) or _PAGE_HEADING_RE.match(stripped):
                    ends = True
                if ends:
                    # Закрыть текущий image-блок и продолжить общую обработку строки
                    in_image_block_by_heading = False
                    image_heading_level = None
                    flush_text()
                    append_image("".join(image_heading_buf), image_heading_block_id)
                    image_heading_buf = []
                    image_heading_block_id = None
                    # не делаем i += 1 — fall-through
                else:
                    image_heading_buf.append(raw_line)
                    i += 1
                    continue
            else:
                image_heading_buf.append(raw_line)
                i += 1
                continue

        # ── Открывающий image-fence ─────────────────────────────────
        m_fence = re.match(r"^(\s*)```\s*(\S+)?", stripped)
        if m_fence:
            lang = (m_fence.group(2) or "").strip().lower()
            if lang in ("image", "imagen", "imagine"):
                in_image_fence = True
                fence_buf.append(raw_line)
                i += 1
                continue
            # обычный fence — fall-through (он попадёт в text buffer)

        # ── Открывающий image-tag ────────────────────────────────────
        if re.match(
            r"^\s*(?:<\s*(?:image|imagen|imagine)\b[^>]*>|\[\s*(?:IMAGE|IMAGEN|IMAGINE)\s*\])\s*$",
            stripped, flags=re.IGNORECASE,
        ):
            in_image_tag = True
            tag_buf.append(raw_line)
            i += 1
            continue

        # ── Page heading? ───────────────────────────────────────────
        page_num = _extract_page_from_heading(stripped)
        if page_num is not None:
            cur_page = page_num
            # Не делаем flush — page heading это часть текста, идёт в text-блок
            text_buf.append(raw_line)
            i += 1
            continue

        # ── Image heading? ──────────────────────────────────────────
        if _is_image_heading(stripped):
            in_image_block_by_heading = True
            image_heading_level = _heading_level(stripped)
            image_heading_buf.append(raw_line)
            image_heading_block_id = _extract_block_id(stripped)
            i += 1
            continue

        # ── Standalone markdown image-line (![](...)) ────────────────
        if re.match(r"^\s*(?:!\[[^\]]*\]\([^)]*\)\s*)+[\s.,;:!?]*$", stripped) and stripped.strip().startswith("!"):
            flush_text()
            append_image(raw_line, None)
            i += 1
            continue

        # ── Обычная строка → в text buffer ───────────────────────────
        text_buf.append(raw_line)
        i += 1

    # Хвосты — на случай неполных блоков в конце файла
    if in_image_fence and fence_buf:
        flush_text()
        append_image("".join(fence_buf), None)
    elif in_image_tag and tag_buf:
        flush_text()
        append_image("".join(tag_buf), None)
    elif in_image_block_by_heading and image_heading_buf:
        flush_text()
        append_image("".join(image_heading_buf), image_heading_block_id)

    flush_text()
    return blocks


# ─── Связь image-блока с реальной картинкой ──────────────────────────────


def _normalize_block_id(value: Any) -> str:
    return re.sub(r"[^A-Za-z0-9_\-]", "_", str(value or "").strip())


@dataclass
class ImageResolution:
    """Результат попытки найти реальную картинку для image-блока MD."""

    status: str                  # "ok" | "no_image" | "render_failed"
    image_path: Optional[Path] = None
    side_block_id: Optional[str] = None
    matched_by: Optional[str] = None  # "block_id" | "page_order" | "manual_crop" | ...
    note: str = ""


# ─── Кеш ──────────────────────────────────────────────────────────────────


def compute_image_cache_key(image_bytes: bytes, model: str, prompt_version: str = PROMPT_VERSION) -> str:
    h = hashlib.sha256()
    h.update(image_bytes)
    h.update(b"|")
    h.update((model or "").encode("utf-8", errors="replace"))
    h.update(b"|")
    h.update((prompt_version or "").encode("utf-8", errors="replace"))
    return h.hexdigest()


def read_cache(session_id: str, pair_id: str, key: str) -> Optional[dict]:
    p = paths_mod.text_enrichment_cache_dir(session_id, pair_id) / f"{key}.json"
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def write_cache(session_id: str, pair_id: str, key: str, payload: dict) -> None:
    p = paths_mod.text_enrichment_cache_dir(session_id, pair_id) / f"{key}.json"
    tmp = p.with_suffix(p.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(p)


# ─── Сборка enriched MD ───────────────────────────────────────────────────


def _utc_now() -> str:
    return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")


def _format_qwen_description_md(desc_payload: dict, *, model: str, page: Optional[int], block_id: Optional[str]) -> str:
    """Сформировать markdown-блок `#### QWEN_IMAGE_DESCRIPTION` для enriched MD.

    `desc_payload` — это либо `{"status": "done", ...}` (parsed JSON-ответ
    модели), либо `{"status": "error", "error": "..."}`.
    """
    lines: list[str] = []
    lines.append("#### QWEN_IMAGE_DESCRIPTION")
    status = (desc_payload.get("status") or "").strip()
    if status == "error":
        err = (desc_payload.get("error") or "unknown").strip()
        lines.append("status: error")
        lines.append(f"Описание недоступно: {err}")
        lines.append("")
        return "\n".join(lines)

    lines.append(f"Модель: {model}")
    if page is not None:
        lines.append(f"Страница: {page}")
    if block_id:
        lines.append(f"Block ID: {block_id}")
    lines.append("")

    summary = (desc_payload.get("summary") or "").strip()
    if summary:
        lines.append("Описание:")
        lines.append(summary)
        lines.append("")

    def _bullets(label: str, items: Any):
        if not isinstance(items, list) or not items:
            return
        lines.append(label)
        for it in items:
            if isinstance(it, str) and it.strip():
                lines.append(f"- {it.strip()}")
        lines.append("")

    _bullets("Проектные решения:", desc_payload.get("design_solutions"))
    _bullets("Материалы:", desc_payload.get("materials"))
    _bullets("Оборудование:", desc_payload.get("equipment"))

    nums = desc_payload.get("numeric_parameters")
    if isinstance(nums, list) and nums:
        lines.append("Числовые параметры:")
        for n in nums:
            if not isinstance(n, dict):
                continue
            name = (n.get("name") or "").strip()
            value = (n.get("value") or "").strip()
            unit = (n.get("unit") or "").strip()
            context = (n.get("context") or "").strip()
            entry = f"- {name}: {value}".rstrip(": ").rstrip()
            if unit:
                entry += f" {unit}"
            if context:
                entry += f"  ({context})"
            if entry.strip() == "-":
                continue
            lines.append(entry)
        lines.append("")

    _bullets("Требования / примечания:", desc_payload.get("requirements"))
    _bullets("Таблицы:", desc_payload.get("tables"))
    _bullets("Видимый текст:", desc_payload.get("visible_text"))
    _bullets("Существенно для сравнения стадий:", desc_payload.get("comparison_relevant_facts"))
    _bullets("Неопределённости:", desc_payload.get("uncertainties"))

    image_kind = (desc_payload.get("image_kind") or "").strip()
    if image_kind:
        lines.append(f"Тип изображения: {image_kind}")
    conf = desc_payload.get("confidence")
    if isinstance(conf, (int, float)):
        try:
            lines.append(f"Уверенность модели: {float(conf):.2f}")
        except (TypeError, ValueError):
            pass

    scheme = desc_payload.get("scheme_analysis")
    if isinstance(scheme, dict):
        is_scheme = bool(scheme.get("is_scheme"))
        if not is_scheme:
            # Кратко отметим, чтобы было видно в enriched MD, что модель проверила.
            lines.append("")
            lines.append("Схемный анализ: не применимо (изображение не является схемой)")
        else:
            lines.append("")
            lines.append("Схемный анализ:")
            scheme_type = (scheme.get("scheme_type") or "").strip()
            if scheme_type:
                lines.append(f"- Тип схемы: {scheme_type}")
            flow_medium = (scheme.get("flow_medium") or "").strip()
            if flow_medium:
                lines.append(f"- Среда / поток: {flow_medium}")
            lines.append("")

            nodes = scheme.get("nodes")
            if isinstance(nodes, list) and nodes:
                lines.append("Узлы:")
                for n in nodes:
                    if not isinstance(n, dict):
                        continue
                    nid = (n.get("id") or "").strip() or "?"
                    label = (n.get("label") or "").strip()
                    ntype = (n.get("type") or "").strip()
                    mark = (n.get("visible_mark") or "").strip()
                    parts = [f"- {nid}"]
                    if label:
                        parts.append(f": {label}")
                    if ntype:
                        parts.append(f", тип: {ntype}")
                    if mark:
                        parts.append(f", маркировка: {mark}")
                    params = n.get("parameters")
                    if isinstance(params, list) and params:
                        params_clean = [str(p).strip() for p in params if str(p).strip()]
                        if params_clean:
                            parts.append(f", параметры: {', '.join(params_clean)}")
                    nconf = n.get("confidence")
                    if isinstance(nconf, (int, float)):
                        try:
                            parts.append(f", уверенность: {float(nconf):.2f}")
                        except (TypeError, ValueError):
                            pass
                    lines.append("".join(parts))
                lines.append("")

            conns = scheme.get("connections")
            if isinstance(conns, list) and conns:
                lines.append("Связи:")
                for c in conns:
                    if not isinstance(c, dict):
                        continue
                    src = (c.get("from") or "?").strip()
                    dst = (c.get("to") or "?").strip()
                    direction = (c.get("direction") or "unknown").strip()
                    line_label = (c.get("line_label") or "").strip()
                    evidence = (c.get("evidence") or "").strip()
                    parts = [f"- {src} → {dst}", f", направление: {direction}"]
                    if line_label:
                        parts.append(f", линия: {line_label}")
                    params = c.get("parameters")
                    if isinstance(params, list) and params:
                        params_clean = [str(p).strip() for p in params if str(p).strip()]
                        if params_clean:
                            parts.append(f", параметры: {', '.join(params_clean)}")
                    if evidence:
                        parts.append(f", основание: {evidence}")
                    cconf = c.get("confidence")
                    if isinstance(cconf, (int, float)):
                        try:
                            parts.append(f", уверенность: {float(cconf):.2f}")
                        except (TypeError, ValueError):
                            pass
                    lines.append("".join(parts))
                lines.append("")

            sequence = scheme.get("sequence_summary")
            if isinstance(sequence, list) and sequence:
                lines.append("Последовательность:")
                for s in sequence:
                    if isinstance(s, str) and s.strip():
                        lines.append(f"- {s.strip()}")
                lines.append("")

            circuits = scheme.get("independent_circuits")
            if isinstance(circuits, list) and circuits:
                lines.append("Независимые контуры:")
                for circ in circuits:
                    if not isinstance(circ, dict):
                        continue
                    name = (circ.get("name") or "").strip()
                    seq = (circ.get("sequence") or "").strip()
                    notes = (circ.get("notes") or "").strip()
                    entry = "- "
                    if name:
                        entry += f"{name}: "
                    if seq:
                        entry += seq
                    if notes:
                        entry += f"  ({notes})"
                    if entry.strip() != "-":
                        lines.append(entry)
                lines.append("")

            _bullets("Существенно для сравнения (схема):", scheme.get("comparison_relevant_scheme_facts"))
            _bullets("Неопределённости (схема):", scheme.get("uncertainties"))

    return "\n".join(lines).rstrip() + "\n"


def build_enriched_md(blocks: list[MdBlock], descriptions: list[dict]) -> str:
    """Собрать enriched MD из блоков + сопоставленных описаний.

    `descriptions` — список dict'ов по индексу, соответствующему `block.order`
    для image-блоков. Каждый элемент: один блок image, со всеми полями.
    """
    desc_by_image_order: dict[int, dict] = {}
    for d in descriptions:
        order = d.get("order")
        if isinstance(order, int):
            desc_by_image_order[order] = d

    out_parts: list[str] = []
    for block in blocks:
        if block.kind == "text":
            out_parts.append("### BLOCK [TEXT]\n")
            out_parts.append(block.text)
            if not block.text.endswith("\n"):
                out_parts.append("\n")
            out_parts.append("\n")
            continue

        # image
        out_parts.append("### BLOCK [IMAGE]\n")
        out_parts.append("<!-- original_imagine_start -->\n")
        out_parts.append(block.text)
        if not block.text.endswith("\n"):
            out_parts.append("\n")
        out_parts.append("<!-- original_imagine_end -->\n\n")

        d = desc_by_image_order.get(block.order)
        if d is None:
            # block pending (dry-run) — пометим явно, чтобы было видно в enriched MD
            out_parts.append("#### QWEN_IMAGE_DESCRIPTION\n")
            out_parts.append("status: pending\n")
            out_parts.append("Описание ещё не сформировано (dry-run или модель не запущена).\n\n")
            continue

        item_status = (d.get("status") or "").lower()
        if item_status in ("pending", "no_image"):
            out_parts.append("#### QWEN_IMAGE_DESCRIPTION\n")
            out_parts.append(f"status: {item_status}\n")
            note = (d.get("error") or
                    ("Описание ещё не сформировано (dry-run)." if item_status == "pending"
                     else "Для блока не найдено изображения."))
            out_parts.append(f"{note}\n\n")
            continue

        payload = d.get("description") or {"status": "error", "error": d.get("error") or "unknown"}
        model = (d.get("model_used") or d.get("model") or "").strip()
        md_chunk = _format_qwen_description_md(
            payload,
            model=model,
            page=block.page,
            block_id=block.block_id,
        )
        out_parts.append(md_chunk)
        out_parts.append("\n")

    return "".join(out_parts)


# ─── Подготовка карты блоков из result.json ──────────────────────────────


def load_image_blocks_index_from_result_json(result_json_path: Optional[str | Path]) -> list[dict]:
    """Прочитать result.json и вернуть только image-блоки в порядке встречи.

    Используется как fallback для связи MD image-блока с реальной картинкой
    (когда block_id из MD не указан явно). Если result.json нет — возвращаем
    пустой список.
    """
    if not result_json_path:
        return []
    from . import blocks as blocks_mod
    try:
        all_blocks, _meta = blocks_mod.normalize_blocks_from_result_json(result_json_path)
    except Exception:  # noqa: BLE001
        return []
    return [b for b in all_blocks if (b.get("type") or "").lower() == "image"]


# ─── Image resolution ────────────────────────────────────────────────────


def resolve_image_for_block(
    md_block: MdBlock,
    side_image_blocks: list[dict],
    used_block_ids: set[str],
    *,
    render_crop: Optional[Callable[[str], Optional[Path]]] = None,
) -> ImageResolution:
    """Связать image/imagine-блок MD с реальной картинкой.

    Стратегия:
      1. Если md_block.block_id явно совпадает с каким-либо block_id из
         `side_image_blocks` — берём его.
      2. Если у нас есть номер страницы — пытаемся сопоставить по порядку
         image-блоков на этой странице.
      3. Если render_crop коллбэк задан и нашли side_block_id — рендерим crop.
      4. Иначе возвращаем status=no_image (резюме с warning'ом).
    """
    side_by_id = {str(b.get("id") or "").strip(): b for b in side_image_blocks if b.get("id")}
    side_by_id_norm = {_normalize_block_id(k): k for k in side_by_id.keys()}

    side_block_id: Optional[str] = None
    matched_by: Optional[str] = None

    if md_block.block_id:
        if md_block.block_id in side_by_id and md_block.block_id not in used_block_ids:
            side_block_id = md_block.block_id
            matched_by = "block_id"
        else:
            norm_id = _normalize_block_id(md_block.block_id)
            real = side_by_id_norm.get(norm_id)
            if real and real not in used_block_ids:
                side_block_id = real
                matched_by = "block_id_normalized"

    if side_block_id is None and md_block.page is not None and md_block.image_order_on_page is not None:
        same_page = [b for b in side_image_blocks if (b.get("page") or 0) == md_block.page]
        idx = md_block.image_order_on_page - 1
        if 0 <= idx < len(same_page):
            cand = same_page[idx]
            cand_id = str(cand.get("id") or "")
            if cand_id and cand_id not in used_block_ids:
                side_block_id = cand_id
                matched_by = "page_order"

    if side_block_id is None:
        return ImageResolution(status="no_image", note="no_matching_image_block_in_result_json")

    if render_crop is None:
        return ImageResolution(
            status="no_image",
            side_block_id=side_block_id,
            matched_by=matched_by,
            note="renderer_unavailable",
        )

    try:
        path = render_crop(side_block_id)
    except Exception as exc:  # noqa: BLE001
        logger.warning("md_enrichment: render_crop failed for %s: %s", side_block_id, exc)
        return ImageResolution(
            status="render_failed",
            side_block_id=side_block_id,
            matched_by=matched_by,
            note=f"render_error:{type(exc).__name__}:{exc}",
        )

    if not path or not Path(path).exists():
        return ImageResolution(
            status="render_failed",
            side_block_id=side_block_id,
            matched_by=matched_by,
            note="render_returned_empty_path",
        )

    return ImageResolution(
        status="ok",
        image_path=Path(path),
        side_block_id=side_block_id,
        matched_by=matched_by,
    )


# ─── Высокоуровневый enrich для одной стороны ────────────────────────────


@dataclass
class EnrichSideSummary:
    """Сводка одной стороны (left/right) для UI/API."""

    side: str
    status: str = "not_run"   # not_run | done | partial | error
    md_path: Optional[str] = None
    md_exists: bool = False
    enriched_md_path: Optional[str] = None
    image_blocks: int = 0
    described: int = 0
    from_cache: int = 0
    errors: int = 0
    pending: int = 0
    warnings: list[str] = field(default_factory=list)
    items: list[dict] = field(default_factory=list)


def _now_iso() -> str:
    return datetime.utcnow().isoformat(timespec="seconds") + "Z"


def _read_image_descriptions(session_id: str, pair_id: str, side: str) -> Optional[dict]:
    p = paths_mod.text_enrichment_descriptions_path(session_id, pair_id, side)
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def _write_image_descriptions(session_id: str, pair_id: str, side: str, payload: dict) -> Path:
    p = paths_mod.text_enrichment_descriptions_path(session_id, pair_id, side)
    tmp = p.with_suffix(p.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(p)
    return p


def _save_prompt_and_raw(
    session_id: str,
    pair_id: str,
    side: str,
    md_block: MdBlock,
    prompt: str,
    raw_excerpt: str,
) -> None:
    prompts_dir = paths_mod.text_enrichment_prompts_dir(session_id, pair_id)
    raw_dir = paths_mod.text_enrichment_raw_dir(session_id, pair_id)
    safe_suffix = f"{side}_{md_block.order:04d}"
    try:
        (prompts_dir / f"{safe_suffix}.txt").write_text(prompt, encoding="utf-8")
    except OSError:
        pass
    try:
        (raw_dir / f"{safe_suffix}.txt").write_text(raw_excerpt or "", encoding="utf-8")
    except OSError:
        pass


def _read_side_md(md_path: Optional[str | Path]) -> Optional[str]:
    if not md_path:
        return None
    p = Path(md_path)
    if not p.exists():
        return None
    try:
        return p.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return None


def discover_image_blocks_for_side(md_text: Optional[str]) -> tuple[list[MdBlock], list[MdBlock]]:
    """Распарсить MD и разбить на (все_блоки, image_only_блоки)."""
    if not md_text:
        return [], []
    blocks = parse_md_blocks(md_text)
    image_blocks = [b for b in blocks if b.is_image]
    return blocks, image_blocks


async def enrich_side(
    session_id: str,
    pair_id: str,
    side: str,
    *,
    md_path: Optional[str | Path],
    result_json_path: Optional[str | Path] = None,
    render_crop: Optional[Callable[[str], Optional[Path]]] = None,
    describe_fn: Optional[
        Callable[[Path, str], Awaitable[graphic_local_mod.DescribeResult]]
    ] = None,
    run_model: bool = False,
    force: bool = False,
    cfg: Optional[graphic_local_mod.LocalGraphicLLMConfig] = None,
    on_block_progress: Optional[Callable[[dict], Any]] = None,
) -> EnrichSideSummary:
    """Обработать одну сторону пары — собрать enriched MD.

    Параметры:
      md_path:           путь к исходному MD стороны;
      result_json_path:  путь к result.json (для поиска image-блоков);
      render_crop:       коллбэк side_block_id → Path с PNG. Обычно
                          functools.partial(store.render_block_crop, ...);
      describe_fn:       коллбэк (image_path, prompt) → DescribeResult.
                          Если None, используется graphic_local.describe_image_local.
      run_model:         False → dry-run, никаких сетевых вызовов;
      force:             True → перезаписать enriched MD даже если есть кеш;
      cfg:               предзагруженный config; по умолчанию читаем env.
      on_block_progress: optional sync/async callback, вызывается ПОСЛЕ обработки
                          каждого image-блока с dict
                          ``{block_index, total, block_id, page, status}``
                          (block_index — 1-based, включает текущий). Caller может
                          использовать для обновления job.json после каждого блока.
                          Исключения из коллбэка глотаются (не валят enrich_side).
    """
    if side not in ("left", "right"):
        raise ValueError("side must be 'left' or 'right'")

    cfg = cfg or graphic_local_mod.load_local_graphic_llm_config()
    if describe_fn is None:
        async def _default_describe(image_path: Path, prompt: str) -> graphic_local_mod.DescribeResult:
            return await graphic_local_mod.describe_image_local(
                image_path, prompt, cfg=cfg,
            )
        describe_fn = _default_describe

    summary = EnrichSideSummary(side=side, md_path=str(md_path) if md_path else None)

    md_text = _read_side_md(md_path)
    if md_text is None:
        summary.status = "error"
        summary.warnings.append("md_not_found")
        return summary
    summary.md_exists = True

    blocks, image_blocks = discover_image_blocks_for_side(md_text)
    summary.image_blocks = len(image_blocks)

    side_image_blocks_idx = load_image_blocks_index_from_result_json(result_json_path)

    descriptions: list[dict] = []
    used_side_block_ids: set[str] = set()

    prompt = QWEN_IMAGE_DESCRIPTION_PROMPT
    _total_blocks = len(image_blocks)

    async def _notify_progress(idx_one_based: int, mb_obj, item_obj):
        if on_block_progress is None:
            return
        try:
            payload = {
                "block_index": idx_one_based,
                "total": _total_blocks,
                "block_id": getattr(mb_obj, "block_id", None),
                "page": getattr(mb_obj, "page", None),
                "status": item_obj.get("status"),
            }
            ret = on_block_progress(payload)
            if asyncio.iscoroutine(ret):
                await ret
        except Exception:  # noqa: BLE001
            logger.debug("on_block_progress callback raised; ignored", exc_info=True)

    for _block_idx, mb in enumerate(image_blocks, start=1):
        item: dict[str, Any] = {
            "order": mb.order,
            "page": mb.page,
            "image_order_on_page": mb.image_order_on_page,
            "md_block_id": mb.block_id,
            "source": "qwen_local_openai_compatible",
            "model": cfg.model,
            "prompt_version": PROMPT_VERSION,
            "from_cache": False,
            "status": "pending",
            "side_block_id": None,
            "matched_by": None,
            "warnings": [],
            "created_at": _now_iso(),
        }

        resolution = resolve_image_for_block(
            mb, side_image_blocks_idx, used_side_block_ids,
            render_crop=render_crop,
        )
        if resolution.side_block_id and resolution.matched_by:
            used_side_block_ids.add(resolution.side_block_id)
            item["side_block_id"] = resolution.side_block_id
            item["matched_by"] = resolution.matched_by

        if resolution.status != "ok":
            item["status"] = "error" if resolution.status == "render_failed" else "no_image"
            item["error"] = resolution.note
            item["warnings"].append(resolution.note)
            descriptions.append(item)
            summary.errors += 1 if resolution.status == "render_failed" else 0
            await _notify_progress(_block_idx, mb, item)
            continue

        # Кешируем по контенту картинки
        try:
            img_bytes = Path(resolution.image_path).read_bytes()
        except OSError as exc:
            item["status"] = "error"
            item["error"] = f"read_image_failed:{type(exc).__name__}:{exc}"
            summary.errors += 1
            descriptions.append(item)
            await _notify_progress(_block_idx, mb, item)
            continue

        cache_key = compute_image_cache_key(img_bytes, cfg.model, PROMPT_VERSION)
        item["cache_key"] = cache_key

        cached = read_cache(session_id, pair_id, cache_key) if not force else None
        if cached and not force:
            item["from_cache"] = True
            item["status"] = cached.get("status") or "done"
            item["description"] = cached.get("description")
            item["model_used"] = cached.get("model_used") or cfg.model
            item["raw_response_excerpt"] = cached.get("raw_response_excerpt", "")
            descriptions.append(item)
            if item["status"] == "done":
                summary.described += 1
                summary.from_cache += 1
            else:
                summary.errors += 1
            await _notify_progress(_block_idx, mb, item)
            continue

        if not run_model:
            item["status"] = "pending"
            item["warnings"].append("dry_run_no_model_call")
            summary.pending += 1
            descriptions.append(item)
            await _notify_progress(_block_idx, mb, item)
            continue

        # ── Real call ─────────────────────────────────────────────
        started = time.monotonic()
        try:
            result = await describe_fn(Path(resolution.image_path), prompt)
        except Exception as exc:  # noqa: BLE001
            logger.exception("md_enrichment: describe_fn raised for block order=%s", mb.order)
            item["status"] = "error"
            item["error"] = f"describe_exception:{type(exc).__name__}:{exc}"
            item["duration_sec"] = round(time.monotonic() - started, 3)
            summary.errors += 1
            descriptions.append(item)
            await _notify_progress(_block_idx, mb, item)
            continue

        item["duration_sec"] = round(time.monotonic() - started, 3)
        item["model_used"] = (result.model_used or cfg.model)
        item["fallback_used"] = bool(result.fallback_used)
        item["raw_response_excerpt"] = result.raw_response_excerpt or ""
        _save_prompt_and_raw(session_id, pair_id, side, mb, prompt, result.raw_response_excerpt or "")

        if result.status == "done" and isinstance(result.parsed, dict):
            payload = dict(result.parsed)
            payload.setdefault("status", "done")
            item["status"] = "done"
            item["description"] = payload
            cache_payload = {
                "status": "done",
                "description": payload,
                "model_used": item["model_used"],
                "raw_response_excerpt": item["raw_response_excerpt"],
                "created_at": _now_iso(),
                "cache_key": cache_key,
                "prompt_version": PROMPT_VERSION,
            }
            try:
                write_cache(session_id, pair_id, cache_key, cache_payload)
            except OSError:
                item["warnings"].append("cache_write_failed")
            summary.described += 1
        else:
            item["status"] = "error"
            err_payload: dict[str, Any] = {
                "status": "error",
                "error": result.error or result.status,
            }
            item["error"] = result.error or result.status
            item["description"] = err_payload
            summary.errors += 1

        descriptions.append(item)
        await _notify_progress(_block_idx, mb, item)

    # ── Записать enriched MD + JSON ─────────────────────────────────
    enriched_md = build_enriched_md(blocks, descriptions)
    md_out = paths_mod.text_enrichment_md_path(session_id, pair_id, side)
    if force or run_model or not md_out.exists():
        try:
            md_out.write_text(enriched_md, encoding="utf-8")
            summary.enriched_md_path = str(md_out)
        except OSError as exc:
            summary.warnings.append(f"enriched_md_write_failed:{type(exc).__name__}:{exc}")
    else:
        summary.enriched_md_path = str(md_out) if md_out.exists() else None

    payload_json = {
        "version": 1,
        "session_id": session_id,
        "pair_id": pair_id,
        "side": side,
        "model": cfg.model,
        "fallback_model": cfg.fallback_model,
        "provider": cfg.provider,
        "prompt_version": PROMPT_VERSION,
        "md_path": str(md_path) if md_path else None,
        "result_json_path": str(result_json_path) if result_json_path else None,
        "enriched_md_path": str(md_out),
        "image_blocks_total": summary.image_blocks,
        "described": summary.described,
        "from_cache": summary.from_cache,
        "errors": summary.errors,
        "pending": summary.pending,
        "updated_at": _now_iso(),
        "items": descriptions,
        "run_model": bool(run_model),
        "force": bool(force),
    }
    try:
        _write_image_descriptions(session_id, pair_id, side, payload_json)
    except OSError as exc:
        summary.warnings.append(f"descriptions_json_write_failed:{type(exc).__name__}:{exc}")

    if summary.image_blocks == 0:
        summary.status = "done"
    elif summary.described == summary.image_blocks and summary.errors == 0 and summary.pending == 0:
        summary.status = "done"
    elif summary.described == 0 and summary.errors == 0 and summary.pending > 0 and not run_model:
        summary.status = "not_run"
    elif summary.errors > 0:
        summary.status = "partial" if summary.described > 0 else "error"
    else:
        summary.status = "partial"

    summary.items = descriptions
    return summary


def read_summary_only(session_id: str, pair_id: str, side: str) -> dict:
    """Лёгкое read-only представление для GET md-enrichment.

    Не запускает парсер и не читает MD — только подхватывает существующий
    JSON, чтобы быстро отрисовать статус в UI.
    """
    data = _read_image_descriptions(session_id, pair_id, side)
    if not data:
        return {
            "side": side,
            "status": "not_run",
            "image_blocks": 0,
            "described": 0,
            "from_cache": 0,
            "errors": 0,
            "pending": 0,
            "enriched_md_path": None,
        }
    return {
        "side": side,
        "status": "done" if (data.get("described") and data["described"] == data.get("image_blocks_total")) else (
            "partial" if data.get("described") else "not_run"
        ),
        "image_blocks": int(data.get("image_blocks_total") or 0),
        "described": int(data.get("described") or 0),
        "from_cache": int(data.get("from_cache") or 0),
        "errors": int(data.get("errors") or 0),
        "pending": int(data.get("pending") or 0),
        "enriched_md_path": data.get("enriched_md_path"),
        "model": data.get("model"),
        "provider": data.get("provider"),
        "updated_at": data.get("updated_at"),
    }


__all__ = [
    "PROMPT_VERSION",
    "QWEN_IMAGE_DESCRIPTION_PROMPT",
    "MdBlock",
    "ImageResolution",
    "EnrichSideSummary",
    "parse_md_blocks",
    "discover_image_blocks_for_side",
    "compute_image_cache_key",
    "read_cache",
    "write_cache",
    "build_enriched_md",
    "resolve_image_for_block",
    "load_image_blocks_index_from_result_json",
    "enrich_side",
    "read_summary_only",
]
