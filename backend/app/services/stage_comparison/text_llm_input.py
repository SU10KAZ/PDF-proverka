"""Подготовка очищенного Markdown для семантического LLM-анализа текста.

В Chandra OCR-выводе MD-файла сосуществуют два разнородных типа блоков:

  * текстовая часть документа — таблицы, спецификации, требования, описания
    решений (`### BLOCK [TEXT]: …`, обычные параграфы);
  * описания графики — `### BLOCK [IMAGE]: …`, фенсы ```image, теги
    `<image>`, маркеры `type: image`, картинки `![]()`.

Текстовый LLM-анализ (Claude Sonnet) должен видеть ТОЛЬКО первую группу:
графика сравнивается отдельным модулем по визуальным crop'ам и связям блоков,
а описания изображений — это уже OCR-производное и не должны замусоривать
смысловой text-diff.

`prepare_text_only_markdown(md_text)` принимает исходный Markdown и возвращает
очищенный текст + статистику + warnings. Удалить нужно только структурно
определённые image/imagine-блоки. Если слово «изображение» встречается внутри
обычного параграфа — текст остаётся. При сомнении — оставляем текст и пишем
warning.

Контракт:

```python
prepare_text_only_markdown(md_text: str) -> {
    "text": "...",           # очищенный Markdown
    "stats": {
        "original_chars": int,
        "filtered_chars": int,
        "removed_image_blocks": int,
        "removed_image_chars": int,
    },
    "warnings": [str, ...],
}
```
"""
from __future__ import annotations

import re
from typing import Optional

__all__ = ["prepare_text_only_markdown"]


# ─── Распознавание заголовков ───────────────────────────────────────────

# Любой markdown-заголовок: `# … …######`
_HEADING_RE = re.compile(r"^(\s{0,3})(#{1,6})\s+(.*?)\s*$")

# Внутри заголовка слова, обозначающие image-блок. Сюда же — формат Chandra
# `### BLOCK [IMAGE]: <id>`.
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

# Текстовый block-marker Chandra: `### BLOCK [TEXT]: …`
_TEXT_BLOCK_HEADING_RE = re.compile(
    r"^\s{0,3}#{1,6}\s+.*?(?:BLOCK\s*\[\s*TEXT\s*\]|\[\s*TEXT\s*\])",
    re.IGNORECASE,
)

# Страничные/листовые заголовки Chandra — это ЯВНО текстовый контекст, и они
# также служат разделителем «конец image-блока».
_PAGE_HEADING_RE = re.compile(
    r"^\s{0,3}#{1,6}\s+(?:СТРАНИЦА|Страница|PAGE|Лист)\b",
    re.IGNORECASE,
)


def _is_image_heading(stripped_line: str) -> bool:
    m = _HEADING_RE.match(stripped_line)
    if not m:
        return False
    # Текстовый block-marker раньше image-marker — это ТЕКСТОВЫЙ блок
    if _TEXT_BLOCK_HEADING_RE.match(stripped_line):
        return False
    if _PAGE_HEADING_RE.match(stripped_line):
        return False
    # Image-token в теле заголовка
    return bool(_IMG_TOKEN_RE.search(m.group(3) or ""))


def _heading_level(stripped_line: str) -> Optional[int]:
    m = _HEADING_RE.match(stripped_line)
    if not m:
        return None
    return len(m.group(2))


# ─── Распознавание KV / тегов / фенсов ───────────────────────────────────

# `type: image`, `block_type: image`, `kind: image`, `"type": "image"`, …
_KV_IMAGE_RE = re.compile(
    r"""^\s*
        [\"']?(?:type|block_type|kind|block_kind|category)[\"']?
        \s*[:=]\s*
        [\"']?(?:image|imagen|imagine)[\"']?
        \s*,?\s*$
    """,
    re.IGNORECASE | re.VERBOSE,
)

# Открывающий тег image-блока: `<image>`, `<imagine>`, `[IMAGE]`, `[IMAGINE]`,
# но строка целиком — иначе мы заденем `[IMAGE]: <id>` в заголовке Chandra
# (он уже отрезается через _is_image_heading).
_TAG_OPEN_RE = re.compile(
    r"^\s*(?:<\s*(?:image|imagen|imagine)\b[^>]*>|\[\s*(?:IMAGE|IMAGEN|IMAGINE)\s*\])\s*$",
    re.IGNORECASE,
)
_TAG_CLOSE_RE = re.compile(
    r"^\s*(?:<\s*/\s*(?:image|imagen|imagine)\s*>|\[\s*/\s*(?:IMAGE|IMAGEN|IMAGINE)\s*\])\s*$",
    re.IGNORECASE,
)

# Fenced code block — открытие/закрытие
_FENCE_RE = re.compile(r"^(\s*)```\s*(\S+)?")
_IMAGE_FENCE_LANGS = {"image", "imagen", "imagine"}

# Markdown image-line: `![…](…)`. Удаляем только строки, в которых markdown-
# картинка является основным содержимым (опционально с пробелами/знаками
# препинания вокруг). Внутри прозы оставляем — но в OCR-выводе таких случаев
# почти нет.
_MD_IMAGE_LINE_RE = re.compile(
    r"^\s*(?:!\[[^\]]*\]\([^)]*\)\s*)+[\s.,;:!?]*$"
)


# ─── Основная функция ───────────────────────────────────────────────────


def prepare_text_only_markdown(md_text: str) -> dict:
    """Удалить структурно определённые image/imagine-блоки из MD.

    Возвращает dict с очищенным Markdown, статистикой и предупреждениями.
    Не выкидывает исключения для пустых/невалидных входов: для пустой строки
    вернёт пустой результат с нулевыми счётчиками.
    """
    if md_text is None:
        md_text = ""
    if not isinstance(md_text, str):
        try:
            md_text = str(md_text)
        except Exception:  # noqa: BLE001
            md_text = ""

    original_chars = len(md_text)
    warnings: list[str] = []

    lines = md_text.splitlines(keepends=True)

    out_lines: list[str] = []
    removed_block_chars = 0
    removed_block_count = 0

    # Состояние сканирования
    in_image_block_by_heading = False
    image_block_heading_level: Optional[int] = None
    pending_image_chars = 0

    in_image_fence = False
    fence_indent = ""

    in_image_tag = False  # <image>...</image> или [IMAGE]...[/IMAGE]

    for raw_line in lines:
        line_no_nl = raw_line.rstrip("\n").rstrip("\r")
        stripped = line_no_nl

        # Внутри fenced-блока ```image — глотаем всё до закрывающего фенса.
        if in_image_fence:
            pending_image_chars += len(raw_line)
            # Закрывающий фенс — допускаем любой ``` на той же отступе
            m_close = _FENCE_RE.match(stripped)
            if m_close and (m_close.group(2) is None or m_close.group(2) == ""):
                in_image_fence = False
                fence_indent = ""
            continue

        # Внутри <image>…</image> или [IMAGE]…[/IMAGE]
        if in_image_tag:
            pending_image_chars += len(raw_line)
            if _TAG_CLOSE_RE.match(stripped):
                in_image_tag = False
                removed_block_count += 1
                removed_block_chars += pending_image_chars
                pending_image_chars = 0
            continue

        # Открывающий image-тег на отдельной строке
        if _TAG_OPEN_RE.match(stripped):
            in_image_tag = True
            pending_image_chars += len(raw_line)
            continue

        # Открывающий image-fence
        m_fence = _FENCE_RE.match(stripped)
        if m_fence:
            lang = (m_fence.group(2) or "").strip().lower()
            if lang in _IMAGE_FENCE_LANGS:
                in_image_fence = True
                fence_indent = m_fence.group(1) or ""
                pending_image_chars += len(raw_line)
                removed_block_count += 1
                continue
            # обычный фенс — мы НЕ обрабатываем его специальным образом,
            # просто проносим как обычный текст
            # (закрытие тоже пропустим через основную ветку)

        # Если внутри image-блока, открытого заголовком — проверяем, не пора
        # ли закончить.
        if in_image_block_by_heading:
            new_level = _heading_level(stripped)
            if new_level is not None:
                ends_here = False
                # Конец image-блока: заголовок того же или меньшего уровня
                if image_block_heading_level is not None and new_level <= image_block_heading_level:
                    ends_here = True
                # Любой текстовый block-marker / page-marker всегда заканчивает
                if _TEXT_BLOCK_HEADING_RE.match(stripped) or _PAGE_HEADING_RE.match(stripped):
                    ends_here = True
                if ends_here:
                    in_image_block_by_heading = False
                    image_block_heading_level = None
                    removed_block_chars += pending_image_chars
                    pending_image_chars = 0
                    # эту строку обрабатываем дальше как обычную (она —
                    # начало нового блока/раздела)
                else:
                    # Заголовок «глубже» — всё ещё часть текущего image-блока.
                    pending_image_chars += len(raw_line)
                    continue
            else:
                pending_image_chars += len(raw_line)
                continue

        # Image heading?
        if _is_image_heading(stripped):
            in_image_block_by_heading = True
            image_block_heading_level = _heading_level(stripped)
            pending_image_chars += len(raw_line)
            removed_block_count += 1
            continue

        # Standalone KV-маркер
        if _KV_IMAGE_RE.match(stripped):
            removed_block_count += 1
            removed_block_chars += len(raw_line)
            continue

        # Markdown image-line (опционально несколько подряд)
        if _MD_IMAGE_LINE_RE.match(stripped) and stripped.strip().startswith("!"):
            removed_block_count += 1
            removed_block_chars += len(raw_line)
            continue

        # Обычная строка — сохраняем.
        out_lines.append(raw_line)

    # Если входная строка закончилась внутри image-блока — учтём остаток.
    if in_image_block_by_heading and pending_image_chars:
        removed_block_chars += pending_image_chars
    elif in_image_fence and pending_image_chars:
        warnings.append("незакрытый ```image-fence — блок удалён до конца файла")
        removed_block_chars += pending_image_chars
    elif in_image_tag and pending_image_chars:
        warnings.append("незакрытый <image>/[IMAGE]-тег — блок удалён до конца файла")
        removed_block_chars += pending_image_chars

    cleaned = "".join(out_lines)
    # Сжимаем чрезмерные пустые строки, образовавшиеся после вырезания блоков.
    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned)

    filtered_chars = len(cleaned)

    if original_chars > 0 and removed_block_count == 0:
        warnings.append("image/imagine-блоки не найдены — фильтрация ничего не удалила")

    return {
        "text": cleaned,
        "stats": {
            "original_chars": original_chars,
            "filtered_chars": filtered_chars,
            "removed_image_blocks": removed_block_count,
            "removed_image_chars": max(0, original_chars - filtered_chars),
        },
        "warnings": warnings,
    }
