"""Парсинг markdown-реестра внешних замечаний.

Формат входа: документ от Chandra OCR с письмом-реестром. Тело — markdown
таблицы с разделами вида `## СТРАНИЦА N` и подсекциями `| 133/23-ГК-АРk | | |`.
Колонки строки:
  №  | Лист/Раздел | Проблема | Описание | Решение | КатегорияСУ-10 | ЧемГрозит
     | КатЗастройщик | Комментарий

На 3 страницах (7, 38, 51 — но это может варьироваться) Chandra провалила
markdown-table OCR и вместо неё дала JSON `[{"table": [...]}]`. Этот код
ловит оба варианта.

Output: list[RegisterEntry], pydantic-валидируется в service.import_register().
"""
from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Iterable, Optional

from backend.app.services.external_register.models import (
    CustomerResponse,
    RegisterEntry,
)


# ─── Нормализация ───────────────────────────────────────────────────────────

# Распознаём подсекции с тремя префиксами: 133/23-ГК-*, 1141-КИС-*, 087-РД-*.
# OCR-варианты:
#   "133/23-ГК-АР1", "133-23-ГК-АР1", "133_23-ГК-АР1", "133 23-ГК-АР1",
#   "13323-ГК-АР1", "133.23-ГК-АР1", "133.23-GK-АР1".
_SECTION_HEADER_RE = re.compile(
    r"""
    (?:^|\|\s*)                              # начало строки или после "|"
    (
        (?: 133 [\s/_\-.]* 23 [\s/_\-.]* [ГGgКKk]+ [\s\-.]* [^\s|]+ )
        |
        (?: 1141 [\s/_\-.]* [КKkКк][ИиI][СCcС] [^\s|]* )
        |
        (?: 087 [\s/_\-.]* [РRrР][ДDdД] [^\s|]* )
    )
    """,
    re.VERBOSE | re.UNICODE,
)


def normalize_section_code(raw: str) -> str:
    """Привести OCR-варианты к канонической форме `133/23-ГК-АР1`.

    Шаги:
      - убрать ведущие/хвостовые пробелы и пайпы;
      - заменить разделители (`-` или ` ` или `.`) между 133 и 23 на `/`;
      - заменить транслит ГК↔GK, РД↔RD, КИС↔KIS;
      - upper-case ASCII (но не русские буквы — они в Cyrillic).

    Возвращает «как было», если не распознали.
    """
    t = (raw or "").strip().strip("|").strip()
    if not t:
        return ""

    # Транслитерация
    t = t.replace("GK", "ГК").replace("Gk", "Гк").replace("gK", "гК")
    t = t.replace("RD", "РД").replace("Rd", "Рд")
    t = t.replace("KIS", "КИС").replace("Kis", "Кис").replace("KIs", "КИс")

    # 133/23-ГК — пробелы/подчёркивания/точки → "/"
    t = re.sub(r"^133[\s_\-.]+23([\s_\-.]+)", "133/23-", t)
    t = re.sub(r"^133[\s_\-.]*23([\s_\-.]+)", "133/23-", t)  # вариант без второго разделителя
    # «13323-ГК» — пропущенный разделитель
    t = re.sub(r"^13323([\s_\-.]+)", "133/23-", t)

    # 1141-КИС-РД-М-… — нормализуем подчёркивания на дефисы
    if t.startswith("1141"):
        t = re.sub(r"^1141[\s_\-.]+", "1141-", t)
        t = t.replace(" ", "-").replace("_", "-").replace("..", ".")
        # Сжать многократные тире
        t = re.sub(r"-{2,}", "-", t)

    # 087-РД-…
    if t.startswith("087"):
        t = re.sub(r"^087[\s_\-.]+", "087-", t)

    # Сокращаем "  " → " ", и убираем хвостовые знаки препинания
    t = re.sub(r"\s+", " ", t).strip(" .,;:|")
    # Если в коде попался хвост вида "133/23-ГК-АР1 от 19.01.2026" — обрежем
    t = re.sub(r"\s+от\s+\d.*$", "", t, flags=re.IGNORECASE)

    # Латинские "AP/AR/AI" → Кириллица "АР/АИ" в суффиксах после ГК-
    # (OCR Chandra часто путает кодировки в коротких аббревиатурах).
    def _cyrillize_suffix(m: re.Match) -> str:
        prefix = m.group(1)
        suffix = m.group(2)
        translit = (
            suffix.replace("AP", "АР")
                  .replace("AI", "АИ")
                  .replace("AR", "АР")
                  .replace("Ap", "АР")
                  .replace("Ai", "АИ")
                  .replace("BK", "ВК")
                  .replace("OB", "ОВ")
                  .replace("OV", "ОВ")
                  .replace("ITP", "ИТП")
                  .replace("CC", "СС")
                  .replace("SS", "СС")
        )
        return prefix + translit

    t = re.sub(r"(^133/23-ГК-)(.+)$", _cyrillize_suffix, t)

    # АСУДИ → АСУД.И (пропущенная точка)
    t = re.sub(r"АСУД(И|Л)\b", r"АСУД.\1", t)
    return t


def looks_like_section_header(cell0: str) -> Optional[str]:
    """Если ячейка (первый столбец) выглядит как заголовок подсекции, вернуть код.

    Параллельно нормализует. Поддерживает варианты с буллетной нумерацией
    «14. 133/23-ГК-СКС» — снимает ведущий «<int>. » перед матчингом.
    """
    candidate = cell0.strip()
    if not candidate:
        return None
    # Снять буллет "14. " префикс — но НЕ трогать "133.", это часть кода
    candidate = re.sub(r"^\d{1,2}\.\s+(?=\d{3,})", "", candidate)
    if _SECTION_HEADER_RE.search(candidate):
        return normalize_section_code(candidate)
    return None


# ─── Разбиение markdown на строки таблиц ────────────────────────────────────

_PAGE_HEADER_RE = re.compile(r"^##\s+СТРАНИЦА\s+(\d+)", re.MULTILINE)
_TABLE_LINE_RE = re.compile(r"^\|.*\|\s*$")
_TABLE_SEP_RE = re.compile(r"^\|\s*-{2,}.*\|\s*$")
_JSON_BLOCK_RE = re.compile(r"\[\s*\{.*?\}\s*\]", re.DOTALL)
_DEV_DECISION_RE = re.compile(
    r"(Отклонено|Учтено|Учитено|Учено|Учено|Внесено|По\s*согласованию[^|]*)",
    re.IGNORECASE,
)
_CATEGORY_SU10_RE = re.compile(
    r"(Критическая|Экономическая|Эксплуатационная|Рекомендательная|Проверить\s*по\s*смежным)",
    re.IGNORECASE,
)


def _split_pages(text: str) -> list[tuple[int, str]]:
    """Разбить markdown по `## СТРАНИЦА N`. Вернуть (page_no, body)."""
    out: list[tuple[int, str]] = []
    matches = list(_PAGE_HEADER_RE.finditer(text))
    if not matches:
        return [(0, text)]
    for i, m in enumerate(matches):
        page_no = int(m.group(1))
        start = m.end()
        end = matches[i + 1].start() if i + 1 < len(matches) else len(text)
        out.append((page_no, text[start:end]))
    return out


def _row_cells(line: str) -> list[str]:
    """Распилить строку `| a | b | c |` на ячейки."""
    if not line.startswith("|"):
        return []
    inner = line[1:]
    if inner.endswith("|"):
        inner = inner[:-1]
    return [c.strip() for c in inner.split("|")]


def _row_starts_with_int(cells: list[str]) -> Optional[int]:
    """Если первая ячейка вида '12' или '12 ...' — вернуть 12."""
    if not cells:
        return None
    first = cells[0].strip()
    if not first:
        return None
    m = re.match(r"^(\d+)\b", first)
    if not m:
        return None
    return int(m.group(1))


# ─── Извлечение записей ─────────────────────────────────────────────────────


def _entries_from_markdown_page(
    page_text: str,
    page_no: int,
    section_state: dict,
) -> list[RegisterEntry]:
    """Прочитать markdown-таблицу на одной странице.

    section_state: словарь {"current": str | None} — сохраняется между
    страницами, чтобы заголовок подсекции с одной страницы продолжал
    действовать на следующих, если таблица переехала.
    """
    entries: list[RegisterEntry] = []

    lines = [ln for ln in page_text.splitlines() if _TABLE_LINE_RE.match(ln) and not _TABLE_SEP_RE.match(ln)]

    for line in lines:
        cells = _row_cells(line)
        if not cells:
            continue

        # 1. Заголовок подсекции: одна непустая ячейка + остальные пустые
        first = cells[0]
        rest_empty = all(not c.strip() for c in cells[1:])
        if rest_empty:
            code = looks_like_section_header(first)
            if code:
                section_state["current"] = code
                continue

        # 2. Заголовок таблицы (Лист/Раздел | Проблема | ...) — пропустить
        if any(h in first for h in ("№", "Позиция Застроитель")):
            continue

        # 3. Строка с данными: первая ячейка содержит число
        # Допустимы форматы:
        #   "| 1 |", "| 1 AP1, л.5 |", "| 8 AP2, л. 5,6 |"
        local_no_match = re.match(r"^\s*(\d+)\b\s*(.*)$", first)
        if not local_no_match:
            continue

        local_no = int(local_no_match.group(1))
        sheet_ref_inline = local_no_match.group(2).strip()

        # У нас может быть 8 или 9 колонок (последняя бывает пустая).
        # Стандартная схема: [№/Лист, Проблема, Описание, Решение, КатСУ-10,
        # Чем грозит, КатЗастр, Коммент] либо [№, Лист, Проблема, Описание,
        # Решение, КатСУ-10, Чем грозит, КатЗастр, Коммент].
        # Различаем по тому, есть ли отдельный sheet_ref.
        if sheet_ref_inline:
            # 8-колоночная схема: первая ячейка = "№ + лист"
            sheet_ref = sheet_ref_inline
            data_cells = cells[1:]
        else:
            # 9-колоночная схема
            sheet_ref = cells[1] if len(cells) > 1 else ""
            data_cells = cells[2:]

        # Минимум 5 ячеек после shift, чтобы было что-то про проблему
        if len(data_cells) < 1:
            continue

        # Маппинг по позиции, с fallback'ом для коротких строк
        def get_cell(i: int) -> str:
            return data_cells[i].strip() if i < len(data_cells) else ""

        problem = get_cell(0)
        description = get_cell(1)
        proposed_solution = get_cell(2)
        cat_su10 = get_cell(3)
        risk = get_cell(4)
        cat_zas_raw = get_cell(5)
        customer_comment = get_cell(6)

        # Иногда OCR съезжает и category/decision лежат не на «своей» позиции —
        # ищем их по regex среди оставшихся ячеек.
        if not _CATEGORY_SU10_RE.search(cat_su10):
            for c in data_cells:
                if _CATEGORY_SU10_RE.search(c):
                    cat_su10 = c.strip()
                    break
        if not _DEV_DECISION_RE.search(cat_zas_raw):
            for c in data_cells[::-1]:
                if _DEV_DECISION_RE.search(c):
                    cat_zas_raw = c.strip()
                    break

        section_code = section_state.get("current") or ""

        # Если sheet_ref содержит подкод раздела (например "AP3 л.13"), и
        # текущая подсекция неопределена, попробуем вытащить из sheet_ref.
        # Принимаем латинские "AP/AI/AR" и кириллические "АР/АИ".
        if not section_code:
            sub = re.search(
                r"(?:^|\b)(А[РИ]\d+(?:\.\d+)?|A[PRIp]\d+(?:\.\d+)?|АИ\d+|AИ\d+)",
                sheet_ref,
            )
            if sub:
                raw_sub = sub.group(1).upper()
                raw_sub = (
                    raw_sub.replace("AP", "АР")
                           .replace("AI", "АИ")
                           .replace("AR", "АР")
                )
                section_code = f"133/23-ГК-{raw_sub}"
                # Подхватим как «текущий» — пригодится для последующих строк
                # из той же страницы, у которых может быть пустой sheet_ref.
                section_state["current"] = section_code

        key = (
            f"{section_code or 'UNKNOWN'}#{local_no}"
            if section_code
            else f"#{len(entries) + 1}"
        )

        entries.append(
            RegisterEntry(
                key=key,
                section_code=section_code,
                local_no=local_no,
                sheet_ref=sheet_ref,
                problem=problem,
                description=description,
                proposed_solution=proposed_solution,
                cat_su10=_normalize_cat_su10(cat_su10),
                risk=risk,
                customer_response_raw=cat_zas_raw,
                customer_response=CustomerResponse.from_raw(cat_zas_raw),
                customer_comment=customer_comment,
                source_page=page_no,
            )
        )

    return entries


def _normalize_cat_su10(raw: str) -> str:
    """Привести категорию СУ-10 к одной из 5 канонических."""
    if not raw:
        return ""
    m = _CATEGORY_SU10_RE.search(raw)
    if not m:
        return raw.strip()
    g = m.group(1).lower().strip()
    table = {
        "критическая": "Критическая",
        "экономическая": "Экономическая",
        "эксплуатационная": "Эксплуатационная",
        "рекомендательная": "Рекомендательная",
    }
    if g.startswith("проверить"):
        return "Проверить по смежным"
    return table.get(g, raw.strip())


def _entries_from_json_block(
    raw_block: str,
    page_no: int,
    section_state: dict,
) -> list[RegisterEntry]:
    """Распарсить JSON-fallback вида [{"analysis": ..., "table": [...]}]."""
    try:
        data = json.loads(raw_block)
    except json.JSONDecodeError:
        return []
    if not isinstance(data, list):
        return []
    entries: list[RegisterEntry] = []
    for chunk in data:
        if not isinstance(chunk, dict):
            continue
        table = chunk.get("table")
        if not isinstance(table, list):
            continue
        for item in table:
            if not isinstance(item, dict):
                continue
            # 1) Заголовки подсекций иногда «маскируются» под JSON-entry
            #    (id = "133.23-GK-COT", остальные поля пустые) — ловим их
            #    через section_state, а саму строку не превращаем в запись.
            raw_id = str(item.get("id", "")).strip()
            others_empty = all(
                not str(item.get(k, "")).strip()
                for k in ("issue", "problem", "description", "details", "remarks", "corrective", "solution")
            )
            if raw_id and others_empty and _SECTION_HEADER_RE.search(raw_id):
                section_state["current"] = normalize_section_code(raw_id)
                continue

            local_no = item.get("id")
            try:
                local_no_int = int(local_no) if local_no is not None else None
            except (TypeError, ValueError):
                local_no_int = None

            problem = (
                item.get("issue")
                or item.get("problem")
                or item.get("criticality")
                or ""
            )
            description = (
                item.get("details")
                or item.get("remarks")
                or item.get("description", "")
            )
            solution = item.get("corrective") or item.get("solution") or ""
            cat = item.get("status") or item.get("category") or ""
            risk = item.get("risk") or item.get("col5") or ""
            decision = item.get("decision") or item.get("status") or ""
            comment = item.get("remarks") or item.get("comment") or ""

            section_code = section_state.get("current") or ""
            key = (
                f"{section_code or 'UNKNOWN'}#{local_no_int or len(entries) + 1}"
            )
            entries.append(
                RegisterEntry(
                    key=key,
                    section_code=section_code,
                    local_no=local_no_int,
                    sheet_ref=str(item.get("description", "")),
                    problem=str(problem),
                    description=str(description),
                    proposed_solution=str(solution),
                    cat_su10=_normalize_cat_su10(str(cat)),
                    risk=str(risk),
                    customer_response_raw=str(decision),
                    customer_response=CustomerResponse.from_raw(str(decision)),
                    customer_comment=str(comment),
                    source_page=page_no,
                )
            )
    return entries


def parse(md_text: str) -> list[RegisterEntry]:
    """Распарсить весь документ → список RegisterEntry."""
    section_state: dict = {"current": None}
    out: list[RegisterEntry] = []
    for page_no, page_body in _split_pages(md_text):
        # Сначала пытаемся вытащить markdown-таблицу
        md_entries = _entries_from_markdown_page(page_body, page_no, section_state)
        out.extend(md_entries)
        # И параллельно ловим JSON-блоки
        for match in _JSON_BLOCK_RE.finditer(page_body):
            json_entries = _entries_from_json_block(match.group(0), page_no, section_state)
            out.extend(json_entries)
    return _dedup_entries(out)


def _dedup_entries(entries: Iterable[RegisterEntry]) -> list[RegisterEntry]:
    """Снять дубли (одна запись могла попасть и в markdown, и в json-блок)."""
    seen_keys: set[str] = set()
    out: list[RegisterEntry] = []
    for e in entries:
        k = e.key
        if k in seen_keys:
            continue
        seen_keys.add(k)
        out.append(e)
    return out


def parse_file(path: str | Path) -> list[RegisterEntry]:
    md = Path(path).read_text(encoding="utf-8")
    return parse(md)
