"""Сводная оптимизация на уровне раздела.

Сервис ничего не изменяет в проектах. Он собирает актуальные версии всех
проектов выбранного раздела в один доказательный срез:

* строки спецификаций из Markdown-представления PDF;
* оптимизации, явно принятые экспертом;
* кандидаты на тиражирование уже принятых решений в аналогичные проекты.

Каждая строка сохраняет project_id/version/page/sheet. Это принципиально:
общая таблица не должна превращаться в обезличенную ведомость, из которой
невозможно вернуться к исходному комплекту.
"""
from __future__ import annotations

import hashlib
import json
import re
import unicodedata
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Optional

from backend.app.services.common import object_service, project_service, version_service


_PAGE_RE = re.compile(r"(?m)^##\s+СТРАНИЦА\s+(\d+)\s*$")
_SHEET_RE = re.compile(r"(?m)^\*\*Лист:\*\*\s*(.+?)\s*$")
_SHEET_NAME_RE = re.compile(r"(?m)^\*\*Наименование листа:\*\*\s*(.+?)\s*$")
_DELIMITER_CELL_RE = re.compile(r"^:?-{3,}:?$")
_NUMBER_RE = re.compile(r"[-+]?\d+(?:[\s.,]\d+)*")

_CATEGORY_WORDS = (
    "оборудован", "материал", "издел", "продукц", "арматур", "кабел",
    "труб", "воздуховод", "крепеж", "светотех", "электроустанов",
)

_STOPWORDS = {
    "или", "для", "при", "без", "под", "над", "это", "как", "что",
    "заменить", "применить", "предлагается", "проект", "проекте", "раздел",
    "аналог", "аналога", "требуется", "необходимо", "оборудование", "материал",
    "текущий", "текущая", "текущее", "шт", "компл",
}

_GRAPHICS_HINTS = (
    "схем", "план", "узел", "расклад", "трасс", "габарит", "размещ",
    "подключ", "однолинейн", "координат", "привязк", "сечени",
)


def _pget(project: Any, key: str, default: Any = None) -> Any:
    if isinstance(project, dict):
        return project.get(key, default)
    return getattr(project, key, default)


def _clean_text(value: Any) -> str:
    text = str(value or "")
    text = text.replace("<br>", " ").replace("<br/>", " ").replace("<br />", " ")
    text = re.sub(r"[*_`]", "", text)
    return re.sub(r"\s+", " ", text).strip()


def _norm(value: Any) -> str:
    text = unicodedata.normalize("NFKC", _clean_text(value)).lower().replace("ё", "е")
    text = re.sub(r"\bили\s+аналог\b", " ", text)
    text = re.sub(r"[^0-9a-zа-я+./-]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _split_md_row(line: str) -> list[str]:
    r"""Разбить pipe-row, не разрезая экранированный ``\|``."""
    raw = line.strip()
    if raw.startswith("|"):
        raw = raw[1:]
    if raw.endswith("|") and not raw.endswith(r"\|"):
        raw = raw[:-1]
    cells: list[str] = []
    buf: list[str] = []
    escaped = False
    for char in raw:
        if escaped:
            buf.append(char)
            escaped = False
        elif char == "\\":
            escaped = True
        elif char == "|":
            cells.append(_clean_text("".join(buf)))
            buf = []
        else:
            buf.append(char)
    if escaped:
        buf.append("\\")
    cells.append(_clean_text("".join(buf)))
    return cells


def _is_delimiter_row(cells: Iterable[str]) -> bool:
    values = list(cells)
    return bool(values) and all(not c or _DELIMITER_CELL_RE.match(c.replace(" ", "")) for c in values)


def _column_map(headers: list[str]) -> dict[str, int]:
    result: dict[str, int] = {}
    for idx, header in enumerate(headers):
        h = _norm(header)
        field: Optional[str] = None
        if (h.startswith("поз") or "позици" in h
                or h in {"п/п", "n п/п", "no п/п", "номер п/п"}):
            field = "position"
        elif "наименование" in h or "техническая характеристика" in h:
            field = "name"
        elif "условноеобозначение" in h.replace(" ", ""):
            # В нестандартных ведомостях это ближайший аналог графы 3
            # «Тип, марка, обозначение документа, опросного листа».
            field = "type_mark"
        elif h.startswith("обозначение"):
            field = "designation"
        elif ("тип" in h and "марк" in h) or h.startswith("марка"):
            field = "type_mark"
        elif "код" in h or "артикул" in h:
            field = "code"
        elif "завод" in h or "изготовител" in h or "поставщик" in h or "производител" in h:
            field = "manufacturer"
        elif "масса" in h:
            field = "mass"
        elif ("единиц" in h or h.replace(".", "") in {"ед изм", "ед"}
              or h.startswith("ед.")):
            field = "unit"
        elif ("количество" in h or "кол-во" in h or h == "кол во"
              or h in {"кол", "кол."}):
            field = "quantity"
        elif "примеч" in h or "аксессуар" in h:
            field = "note"
        if field and field not in result:
            result[field] = idx
    return result


def _is_spec_header(headers: list[str], mapping: dict[str, int]) -> bool:
    if "name" not in mapping:
        return False
    # Одних «Обозначения» или «Массы» недостаточно: сочетание
    # «Обозначение / Наименование / Примечание» часто является ведомостью
    # документов, а не спецификацией. Полная форма 7 всё равно уверенно
    # определяется по графам «Поз.» и «Кол.».
    supporting = {"position", "type_mark", "code", "manufacturer", "unit", "quantity"}
    return len(supporting.intersection(mapping)) >= 1 and len(mapping) >= 2


def _fallback_map(width: int) -> dict[str, int]:
    # На OCR-страницах одна большая спецификация нередко разбита на несколько
    # Markdown-таблиц. У продолжений заголовком становится первая позиция.
    if width >= 9:
        return {
            "position": 0, "name": 1, "type_mark": 2, "code": 3,
            "manufacturer": 4, "unit": 5, "quantity": 6, "mass": 7,
            "note": 8,
        }
    if width == 8:
        return {
            "position": 0, "name": 1, "type_mark": 2, "code": 3,
            "manufacturer": 4, "unit": 5, "quantity": 6, "note": 7,
        }
    if width == 7:
        return {
            "position": 0, "name": 1, "type_mark": 2, "manufacturer": 3,
            "unit": 4, "quantity": 5, "note": 6,
        }
    if width == 6:
        return {"name": 0, "type_mark": 1, "manufacturer": 2, "unit": 3, "quantity": 4, "note": 5}
    if width == 5:
        return {"name": 0, "type_mark": 1, "unit": 2, "quantity": 3, "note": 4}
    return {"name": 0}


def _cell(cells: list[str], mapping: dict[str, int], field: str) -> str:
    idx = mapping.get(field)
    return cells[idx] if idx is not None and idx < len(cells) else ""


def _implicit_unit(headers: list[str], mapping: dict[str, int]) -> str:
    """Извлечь единицу из совмещённого заголовка вроде ``Кол-во, шт.``."""
    if "unit" in mapping:
        return ""
    idx = mapping.get("quantity")
    if idx is None or idx >= len(headers):
        return ""
    header = _norm(headers[idx])
    for marker, unit in (
        ("компл", "компл."), ("шт", "шт."), ("км", "км"),
        ("м2", "м²"), ("м3", "м³"), ("м", "м"),
    ):
        if re.search(rf"(?:^|[^а-яa-z]){re.escape(marker)}(?:$|[^а-яa-z])", header):
            return unit
    return ""


def _looks_like_numbering_row(cells: list[str]) -> bool:
    nonempty = [c for c in cells if c]
    return bool(nonempty) and all(re.fullmatch(r"\d+", c) for c in nonempty)


def _category_row(cells: list[str]) -> Optional[str]:
    nonempty = [c for c in cells if c]
    if len(nonempty) != 1:
        return None
    value = nonempty[0]
    low = _norm(value)
    # В форме 7 заголовок группы занимает всю строку, поэтому после OCR в
    # Markdown остаётся ровно одна заполненная ячейка. Названия вроде
    # «Корпус 1» и «Контур заземления» не содержат слова «оборудование», но
    # всё равно являются разделителями, а не позициями спецификации.
    if (any(word in low for word in _CATEGORY_WORDS)
            or (len(low) >= 3 and not re.fullmatch(r"[-+]?\d+(?:[.,]\d+)?", low))):
        return value
    return None


def _page_chunks(markdown: str) -> list[tuple[int, str]]:
    matches = list(_PAGE_RE.finditer(markdown or ""))
    if not matches:
        return [(0, markdown or "")]
    chunks: list[tuple[int, str]] = []
    for idx, match in enumerate(matches):
        end = matches[idx + 1].start() if idx + 1 < len(matches) else len(markdown)
        chunks.append((int(match.group(1)), markdown[match.start():end]))
    return chunks


def _canonical_spec_key(row: dict) -> str:
    designation = _norm(row.get("designation"))
    code = _norm(row.get("code"))
    mark = _norm(row.get("type_mark"))
    name = _norm(row.get("name"))
    if designation:
        base = f"{designation} {mark} {name}"
    elif code and mark:
        base = f"{code} {mark}"
    elif mark and len(mark) >= 4:
        base = f"{mark} {name}"
    else:
        base = name
    return base[:500]


def parse_specification_markdown(
    markdown: str,
    *,
    project_id: str,
    project_name: str,
    version_id: str,
    md_file: Optional[str] = None,
) -> list[dict]:
    """Извлечь строки спецификаций из OCR Markdown.

    Сначала ищется сильный заголовок таблицы (наименование + хотя бы одна
    профильная колонка). После него таблицы-продолжения на той же странице
    также считаются частью спецификации. Обычные ведомости и штампы на других
    страницах не попадают в выдачу.
    """
    rows: list[dict] = []
    for page, page_text in _page_chunks(markdown):
        sheet_match = _SHEET_RE.search(page_text)
        sheet_name_match = _SHEET_NAME_RE.search(page_text)
        sheet = _clean_text(sheet_match.group(1)) if sheet_match else ""
        sheet_name = _clean_text(sheet_name_match.group(1)) if sheet_name_match else ""
        lines = page_text.splitlines()
        heading = sheet_name
        strong_seen = False
        last_mapping: Optional[dict[str, int]] = None
        last_implicit_unit = ""
        table_no = 0
        i = 0
        while i < len(lines):
            line = lines[i].strip()
            if line.upper().startswith("### BLOCK"):
                # Новый OCR-блок не является продолжением предыдущей таблицы.
                # Иначе штамп и ведомости ниже спецификации ошибочно попадали
                # в общую таблицу раздела.
                heading = sheet_name
                strong_seen = False
                last_mapping = None
                last_implicit_unit = ""
            if line.startswith("#"):
                title = _clean_text(line.lstrip("#").strip())
                if title and not title.upper().startswith("BLOCK") and not title.upper().startswith("СТРАНИЦА"):
                    heading = title
            if not (line.startswith("|") and i + 1 < len(lines) and lines[i + 1].strip().startswith("|")):
                i += 1
                continue

            first = _split_md_row(line)
            delimiter = _split_md_row(lines[i + 1])
            if not _is_delimiter_row(delimiter):
                i += 1
                continue

            table_lines = [line]
            j = i + 2
            while j < len(lines) and lines[j].strip().startswith("|"):
                table_lines.append(lines[j].strip())
                j += 1

            header_map = _column_map(first)
            strong = _is_spec_header(first, header_map)
            context_hint = "спецификац" in _norm(heading) or "ведомость материалов" in _norm(heading)
            if not strong and not strong_seen and not context_hint:
                i = j
                continue

            table_no += 1
            if strong:
                strong_seen = True
                mapping = header_map
                implicit_unit = _implicit_unit(first, mapping)
                last_mapping = dict(mapping)
                last_implicit_unit = implicit_unit
                data_lines = table_lines[1:]
            else:
                max_known_index = max(last_mapping.values(), default=-1) if last_mapping else -1
                if last_mapping and max_known_index < len(first):
                    mapping = last_mapping
                    implicit_unit = last_implicit_unit
                else:
                    mapping = _fallback_map(len(first))
                    implicit_unit = _implicit_unit(first, mapping)
                data_lines = table_lines

            category = heading or sheet_name
            for row_no, row_line in enumerate(data_lines, start=1):
                cells = _split_md_row(row_line)
                if not any(cells) or _is_delimiter_row(cells) or _looks_like_numbering_row(cells):
                    continue
                category_value = _category_row(cells)
                if category_value:
                    category = category_value
                    continue

                position = _cell(cells, mapping, "position")
                designation = _cell(cells, mapping, "designation")
                name = _cell(cells, mapping, "name")
                type_mark = _cell(cells, mapping, "type_mark")
                code = _cell(cells, mapping, "code")
                manufacturer = _cell(cells, mapping, "manufacturer")
                unit = _cell(cells, mapping, "unit") or implicit_unit
                quantity = _cell(cells, mapping, "quantity")
                mass = _cell(cells, mapping, "mass")
                note = _cell(cells, mapping, "note")
                if not name:
                    meaningful = [c for c in cells if c]
                    name = max(meaningful, key=len) if meaningful else ""
                # Служебные и практически пустые строки не являются позициями.
                if not name or (_norm(name) in {"наименование", "техническая характеристика"}):
                    continue
                if not any((position, designation, type_mark, code, manufacturer, unit, quantity, mass)) and len(_norm(name)) < 8:
                    continue

                digest = hashlib.sha1(
                    f"{project_id}|{version_id}|{page}|{table_no}|{row_no}|{'|'.join(cells)}".encode("utf-8")
                ).hexdigest()[:14]
                item = {
                    "row_id": f"SPEC-{digest}",
                    "project_id": project_id,
                    "project_name": project_name,
                    "version_id": version_id,
                    "page": page,
                    "sheet": sheet,
                    "sheet_name": sheet_name,
                    "category": category,
                    "position": position,
                    "designation": designation,
                    "name": name,
                    "type_mark": type_mark,
                    "code": code,
                    "manufacturer": manufacturer,
                    "unit": unit,
                    "quantity": quantity,
                    "mass": mass,
                    "note": note,
                    "raw_cells": cells,
                    "source": {"kind": "markdown_table", "file": md_file or "", "page": page, "sheet": sheet},
                }
                item["canonical_key"] = _canonical_spec_key(item)
                rows.append(item)
            i = j
    return rows


def _quantity_number(value: str) -> Optional[float]:
    text = _clean_text(value).replace(" ", "").replace(",", ".")
    if not re.fullmatch(r"[-+]?\d+(?:\.\d+)?", text):
        return None
    try:
        return float(text)
    except ValueError:
        return None


def group_shared_specification_items(rows: list[dict]) -> list[dict]:
    """Только точные нормализованные совпадения в двух и более проектах."""
    buckets: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        key = row.get("canonical_key") or _canonical_spec_key(row)
        if len(key) >= 6:
            buckets[key].append(row)

    result: list[dict] = []
    for key, items in buckets.items():
        project_ids = sorted({str(item.get("project_id") or "") for item in items if item.get("project_id")})
        if len(project_ids) < 2:
            continue
        units = {_norm(item.get("unit")) for item in items if item.get("unit")}
        quantities = [_quantity_number(str(item.get("quantity") or "")) for item in items]
        total_quantity: Optional[float] = None
        if len(units) == 1 and quantities and all(q is not None for q in quantities):
            total_quantity = round(sum(q for q in quantities if q is not None), 6)
        representative = max(items, key=lambda item: len(_clean_text(item.get("name"))))
        result.append({
            "group_id": "COMMON-" + hashlib.sha1(key.encode("utf-8")).hexdigest()[:12],
            "canonical_key": key,
            "name": representative.get("name") or representative.get("type_mark") or key,
            "type_mark": representative.get("type_mark") or "",
            "code": representative.get("code") or "",
            "unit": next(iter(units), ""),
            "total_quantity": total_quantity,
            "project_count": len(project_ids),
            "project_ids": project_ids,
            "row_ids": [item["row_id"] for item in items],
            "rows_count": len(items),
        })
    return sorted(result, key=lambda group: (-group["project_count"], -group["rows_count"], _norm(group["name"])))


def _tokens(value: Any) -> set[str]:
    words = re.findall(r"[0-9a-zа-я][0-9a-zа-я+./-]{2,}", _norm(value))
    return {word for word in words if word not in _STOPWORDS}


def _optimization_tokens(item: dict) -> set[str]:
    return _tokens(" ".join([
        str(item.get("current") or ""),
        str(item.get("proposed") or ""),
        " ".join(str(v) for v in (item.get("spec_items") or [])),
    ]))


def _truncate(value: Any, limit: int = 150) -> str:
    text = _clean_text(value)
    if len(text) <= limit:
        return text
    return text[: max(1, limit - 1)].rstrip(" ,.;:-") + "…"


def _accepted_cluster_title(items: list[dict]) -> str:
    """Короткое предметное название вместо полной LLM-формулировки."""
    spec_items = [
        _clean_text(spec_item)
        for item in items
        for spec_item in (item.get("spec_items") or [])
        if _clean_text(spec_item)
    ]
    if spec_items:
        label = max(spec_items, key=len)
        label = re.sub(
            r"^(?:поз(?:иция)?\.?\s*)?[0-9а-яa-z./-]+(?:\s*\([^)]*\))?\s*[—–:-]\s*",
            "",
            label,
            flags=re.IGNORECASE,
        )
        return "Унифицировать: " + _truncate(label, 132)

    proposal = max(items, key=lambda item: len(_clean_text(item.get("proposed")))).get("proposed")
    return _truncate(proposal or "Общее принятое решение", 150)


def _graphics_recommended_for_items(items: list[dict]) -> bool:
    content = _norm(" ".join(
        str(value)
        for item in items
        for value in (
            item.get("current"), item.get("proposed"), item.get("risks"),
            item.get("norm"), " ".join(str(v) for v in (item.get("spec_items") or [])),
        )
    ))
    return any(hint in content for hint in _GRAPHICS_HINTS)


def cluster_accepted_optimizations(items: list[dict]) -> list[dict]:
    """Найти похожие принятые решения, не сливая их автоматически.

    Возвращаем именно кандидаты на объединение. Финальное объединение остаётся
    экспертным действием, потому что одинаковая формулировка в двух корпусах
    может иметь разные ограничения и расчётные основания.
    """
    if len(items) < 2:
        return []
    token_sets = [_optimization_tokens(item) for item in items]
    spec_token_sets = [_tokens(" ".join(str(v) for v in (item.get("spec_items") or []))) for item in items]
    parent = list(range(len(items)))
    matches: dict[tuple[int, int], dict[str, float | int | str]] = {}

    def find(x: int) -> int:
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(a: int, b: int) -> None:
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[rb] = ra

    for i in range(len(items)):
        for j in range(i + 1, len(items)):
            if items[i].get("project_id") == items[j].get("project_id"):
                continue
            a, b = token_sets[i], token_sets[j]
            if len(a) < 4 or len(b) < 4:
                continue
            common = len(a & b)
            score = common / len(a | b)
            sa, sb = spec_token_sets[i], spec_token_sets[j]
            spec_common = len(sa & sb)
            spec_overlap = spec_common / min(len(sa), len(sb)) if sa and sb else 0
            # Длинные LLM-формулировки дают низкий Jaccard даже для одного
            # изделия. Поэтому второй, более сильный путь — совпадение токенов
            # конкретных spec_items. Это всё ещё лишь кандидат на объединение.
            if (common >= 4 and score >= 0.45) or (spec_common >= 4 and spec_overlap >= 0.60):
                union(i, j)
                if spec_common >= 4 and spec_overlap >= 0.60:
                    matches[(i, j)] = {
                        "basis": "совпадение позиций спецификации",
                        "score": spec_overlap,
                        "common_tokens": spec_common,
                    }
                else:
                    matches[(i, j)] = {
                        "basis": "сходство формулировок",
                        "score": score,
                        "common_tokens": common,
                    }

    grouped: dict[int, list[tuple[int, dict]]] = defaultdict(list)
    for idx, item in enumerate(items):
        grouped[find(idx)].append((idx, item))
    result: list[dict] = []
    for indexed_items in grouped.values():
        cluster_items = [item for _, item in indexed_items]
        project_ids = sorted({str(item.get("project_id") or "") for item in cluster_items})
        if len(project_ids) < 2:
            continue
        cluster_indexes = {idx for idx, _ in indexed_items}
        cluster_matches = [
            match for (i, j), match in matches.items()
            if i in cluster_indexes and j in cluster_indexes
        ]
        strongest_match = max(cluster_matches, key=lambda match: float(match["score"]))
        representative = max(cluster_items, key=lambda item: len(_clean_text(item.get("proposed"))))
        matched_spec_items = list(dict.fromkeys(
            _clean_text(spec_item)
            for item in cluster_items
            for spec_item in (item.get("spec_items") or [])
            if _clean_text(spec_item)
        ))
        seed = min(f"{item.get('project_id')}:{item.get('id')}" for item in cluster_items)
        result.append({
            "cluster_id": "MERGE-" + hashlib.sha1(seed.encode("utf-8")).hexdigest()[:12],
            "title": _accepted_cluster_title(cluster_items),
            "representative_proposal": representative.get("proposed") or "",
            "project_count": len(project_ids),
            "project_ids": project_ids,
            "item_refs": [f"{item.get('project_id')}:{item.get('id')}" for item in cluster_items],
            "items_count": len(cluster_items),
            "match_basis": strongest_match["basis"],
            "match_score": round(float(strongest_match["score"]), 3),
            "matched_spec_items": matched_spec_items,
            "graphics_recommended": _graphics_recommended_for_items(cluster_items),
        })
    return sorted(result, key=lambda cluster: (-cluster["project_count"], -cluster["items_count"]))


def _specification_evidence_tokens(value: Any) -> set[str]:
    """Токены предметной части строки без служебных слов ссылки."""
    return _tokens(value) - {
        "поз", "позиция", "лист", "листа", "спецификация", "спецификации",
        "всего", "итого", "количество",
    }


def _specification_row_tokens(row: dict) -> set[str]:
    return _specification_evidence_tokens(" ".join(
        str(row.get(field) or "")
        for field in ("name", "type_mark", "designation", "code", "manufacturer")
    ))


def _technical_variant(value: Any) -> tuple[str, tuple[str, ...]]:
    """Выделить параметры, которые нельзя терять при поиске аналога.

    Это не доказывает взаимозаменяемость, но не позволяет переносить решение
    с НГ на HF/FRHF либо между разными степенями IP только из-за похожего
    наименования.
    """
    text = _norm(value)
    if "frhf" in text or re.search(r"\bfr\s*hf\b", text):
        fire_class = "frhf"
    elif "frls" in text or re.search(r"\bfr\s*ls\b", text):
        fire_class = "frls"
    elif re.search(r"\bhf\b", text) or "безгалоген" in text:
        fire_class = "hf"
    elif re.search(r"\bls\b", text):
        fire_class = "ls"
    elif re.search(r"(?:^|[\s(/-])нг(?:\(?[a-dа-д]\)?)?(?:$|[\s)/-])", text) or "негорюч" in text:
        fire_class = "нг"
    else:
        fire_class = ""
    ip_values = tuple(sorted(set(re.findall(r"\bip\s*([0-9]{2})\b", text))))
    return fire_class, ip_values


def _technical_variants_compatible(source: Any, target: Any) -> bool:
    source_fire, source_ip = _technical_variant(source)
    target_fire, target_ip = _technical_variant(target)
    if source_fire and target_fire and source_fire != target_fire:
        return False
    if source_ip and target_ip and not (set(source_ip) & set(target_ip)):
        return False
    return True


def _evidence_match_score(row: dict, evidence_text: Any) -> float:
    row_tokens = _specification_row_tokens(row)
    evidence_tokens = _specification_evidence_tokens(evidence_text)
    if len(row_tokens) < 3 or len(evidence_tokens) < 3:
        return 0.0
    common = len(row_tokens & evidence_tokens)
    if common < 3:
        return 0.0
    score = common / min(len(row_tokens), len(evidence_tokens))
    if score < 0.60 or not _technical_variants_compatible(evidence_text, " ".join(
        str(row.get(field) or "")
        for field in ("name", "type_mark", "designation", "code", "note")
    )):
        return 0.0
    return score


def _accepted_decision_matches_row(item: dict, row: dict) -> float:
    return max(
        (_evidence_match_score(row, evidence) for evidence in (item.get("spec_items") or [])),
        default=0.0,
    )


def build_replication_signals(
    rows: list[dict],
    accepted: list[dict],
    accepted_clusters: Optional[list[dict]] = None,
) -> list[dict]:
    """Найти проекты, куда можно перенести уже принятое решение.

    Совпадение лишь создаёт доказательного кандидата. Решение не применяется
    автоматически: проектные ограничения и графику проверяет эксперт.
    """
    clusters = accepted_clusters if accepted_clusters is not None else cluster_accepted_optimizations(accepted)
    accepted_by_ref = {str(item.get("source_ref") or f"{item.get('project_id')}:{item.get('id')}"): item for item in accepted}
    clustered_refs = {
        str(source_ref)
        for cluster in clusters
        for source_ref in (cluster.get("item_refs") or [])
    }
    decision_groups: list[list[dict]] = [
        [accepted_by_ref[source_ref] for source_ref in cluster.get("item_refs") or [] if source_ref in accepted_by_ref]
        for cluster in clusters
    ]
    decision_groups.extend(
        [item]
        for source_ref, item in accepted_by_ref.items()
        if source_ref not in clustered_refs
    )

    accepted_by_project: dict[str, list[dict]] = defaultdict(list)
    for item in accepted:
        accepted_by_project[str(item.get("project_id") or "")].append(item)

    signals: list[dict] = []
    for items in decision_groups:
        if not items or not any(item.get("spec_items") for item in items):
            continue
        source_project_ids = sorted({str(item.get("project_id") or "") for item in items if item.get("project_id")})
        target_rows: list[dict] = []
        match_scores: list[float] = []
        for row in rows:
            project_id = str(row.get("project_id") or "")
            if not project_id or project_id in source_project_ids:
                continue
            score = max((_accepted_decision_matches_row(item, row) for item in items), default=0.0)
            if score <= 0:
                continue
            # Если в целевом проекте по этой позиции уже есть принятое решение,
            # повторно предлагать его тиражирование не нужно.
            if any(_accepted_decision_matches_row(existing, row) > 0 for existing in accepted_by_project.get(project_id, [])):
                continue
            target_rows.append(row)
            match_scores.append(score)

        if not target_rows:
            continue
        target_project_ids = sorted({str(row.get("project_id") or "") for row in target_rows})
        representative = max(items, key=lambda item: len(_clean_text(item.get("proposed"))))
        label = _accepted_cluster_title(items)
        label = re.sub(r"^Унифицировать:\s*", "", label, flags=re.IGNORECASE)
        seed = min(str(item.get("source_ref") or f"{item.get('project_id')}:{item.get('id')}") for item in items)
        signals.append({
            "signal_id": "REPL-" + hashlib.sha1(seed.encode("utf-8")).hexdigest()[:12],
            "kind": "replicate_accepted_optimization",
            "priority": "high",
            "title": "Тиражировать принятое решение: " + _truncate(label, 126),
            "reason": (
                f"Решение принято в {len(source_project_ids)} проект(ах). "
                f"Аналогичные позиции найдены ещё в {len(target_project_ids)} проект(ах), "
                "где это решение пока не принято."
            ),
            "project_ids": sorted(set(source_project_ids + target_project_ids)),
            "source_project_ids": source_project_ids,
            "target_project_ids": target_project_ids,
            "evidence_refs": [str(item.get("source_ref") or f"{item.get('project_id')}:{item.get('id')}") for item in items],
            "target_row_ids": [str(row.get("row_id") or "") for row in target_rows if row.get("row_id")],
            "items_count": len(items),
            "target_rows_count": len(target_rows),
            "match_basis": "совпадение с позициями принятого решения",
            "match_score": round(sum(match_scores) / len(match_scores), 3),
            "representative_proposal": representative.get("proposed") or "",
            "status": "candidate_requires_validation",
            "next_step": "Проверить применимость ограничений и принять решение для целевых проектов раздела.",
            "graphics_recommended": _graphics_recommended_for_items(items),
        })
    return sorted(
        signals,
        key=lambda signal: (-len(signal["target_project_ids"]), -signal["target_rows_count"], _norm(signal["title"])),
    )


def _safe_json(path: Path) -> Optional[dict]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else None
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return None


def _legacy_md(version_dir: Path) -> tuple[Optional[str], Optional[str]]:
    candidates = [version_dir / "02_work" / "document.md"]
    candidates += sorted((version_dir / "01_input").glob("*_document.md")) if (version_dir / "01_input").is_dir() else []
    candidates += sorted(version_dir.glob("*_document.md"))
    candidates += sorted(version_dir.glob("*.md"))
    for path in candidates:
        if path.is_file():
            try:
                return path.read_text(encoding="utf-8"), path.name
            except (OSError, UnicodeDecodeError):
                continue
    return None, None


def _blocks_count(index: Any) -> int:
    if not isinstance(index, dict):
        return 0
    if isinstance(index.get("blocks"), list):
        return len(index["blocks"])
    try:
        return int(index.get("total_blocks") or 0)
    except (TypeError, ValueError):
        return 0


def load_project_bundle(project: Any, *, object_id: Optional[str] = None) -> dict:
    """Прочитать актуальные данные проекта из v2, затем из legacy."""
    project_id = str(_pget(project, "project_id", ""))
    requested_version = str(_pget(project, "version_id", "") or "") or None
    current_object_id = object_id or object_service.get_current_id()

    try:
        from backend.app.services.storage.projects_v2_adapter import ProjectsV2Adapter

        adapter = ProjectsV2Adapter()
        doc = adapter.find_document_by_project_id(project_id, object_id=current_object_id) if adapter.is_available() else None
        if doc is not None:
            version_id = adapter.resolve_version_id(doc, requested_version) or adapter.resolve_version_id(doc)
            if not version_id:
                raise FileNotFoundError("актуальная версия не определена")
            doc_dir = Path(doc["doc_dir"])
            md_text, md_file = adapter.md_text(doc_dir, version_id)
            return {
                "version_id": version_id,
                "md_text": md_text,
                "md_file": md_file,
                "optimization": adapter.read_optimization(doc_dir, version_id) or {},
                "expert_review": adapter.read_review(doc_dir, version_id, "expert_review.json") or {},
                "graphic_blocks": _blocks_count(adapter.read_blocks_index(doc_dir, version_id)),
                "error": None,
            }
    except Exception as exc:
        v2_error = str(exc)
    else:
        v2_error = "проект не найден в projects_v2"

    try:
        ctx = version_service.resolve_project_version_context(project_id, requested_version)
        version_dir = Path(ctx["version_dir"])
        output_dir = Path(ctx["output_dir"])
        md_text, md_file = _legacy_md(version_dir)
        optimization = _safe_json(output_dir / "optimization.json") or {}
        review = None
        for candidate in (
            version_dir / "04_review" / "expert_review.json",
            output_dir / "expert_review.json",
            version_dir / "_output" / "expert_review.json",
        ):
            review = _safe_json(candidate)
            if review is not None:
                break
        return {
            "version_id": str(ctx.get("version_id") or requested_version or "v1"),
            "md_text": md_text,
            "md_file": md_file,
            "optimization": optimization,
            "expert_review": review or {},
            "graphic_blocks": _blocks_count(
                _safe_json(output_dir / "blocks" / "index.json")
                or _safe_json(output_dir / "blocks_stage02_100" / "index.json")
            ),
            "error": None,
        }
    except Exception as exc:
        return {
            "version_id": requested_version or "",
            "md_text": None,
            "md_file": None,
            "optimization": {},
            "expert_review": {},
            "graphic_blocks": 0,
            "error": f"v2: {v2_error}; legacy: {exc}",
        }


def _accepted_optimizations(project: Any, bundle: dict) -> tuple[list[dict], int]:
    optimization = bundle.get("optimization") or {}
    raw_items = optimization.get("items") if isinstance(optimization, dict) else []
    raw_items = raw_items if isinstance(raw_items, list) else []
    decisions = (bundle.get("expert_review") or {}).get("decisions") or []
    accepted_ids = {
        str(decision.get("item_id"))
        for decision in decisions
        if isinstance(decision, dict)
        and decision.get("item_type") == "optimization"
        and decision.get("decision") == "accepted"
    }
    project_id = str(_pget(project, "project_id", ""))
    project_name = str(_pget(project, "name", project_id))
    accepted: list[dict] = []
    for item in raw_items:
        if not isinstance(item, dict) or str(item.get("id")) not in accepted_ids:
            continue
        enriched = dict(item)
        enriched.update({
            "project_id": project_id,
            "project_name": project_name,
            "version_id": bundle.get("version_id") or "",
            "expert_decision": "accepted",
            "source_ref": f"{project_id}:{item.get('id')}",
        })
        accepted.append(enriched)
    return accepted, len(raw_items)


def _current_object_projects(object_id: Optional[str] = None) -> list[Any]:
    """Тот же object scope, который видит Dashboard.

    ``project_service.list_projects`` в полном v2-cutover исторически умеет
    перечислять документы всех объектов. Основной GET /api/projects поверх
    него уже использует строгий read-canary scope; сводка раздела обязана
    повторять именно этот контракт, иначе в таблицу King&Sons попадут корпуса
    Alia с тем же кодом дисциплины.
    """
    try:
        from backend.app.services.storage.storage_read_facade import production_uses_v2
        if production_uses_v2():
            from backend.app.services.storage.read_canary import v2_projects_list
            return list(v2_projects_list(object_id=object_id).get("projects") or [])
    except Exception:
        # Legacy fallback сохраняет прежнее поведение сервиса проектов.
        pass
    if object_id:
        with project_service.pinned_object(object_id):
            return list(project_service.list_projects())
    return list(project_service.list_projects())


def collect_section_optimization_data(
    section: str,
    *,
    object_id: Optional[str] = None,
    projects: Optional[list[Any]] = None,
    loader: Optional[Callable[[Any], dict]] = None,
) -> dict:
    """Этап 1: собрать исходные данные раздела без межпроектных выводов."""
    section_code = _clean_text(section).upper()
    source_projects = projects if projects is not None else _current_object_projects(object_id)
    section_projects = [p for p in source_projects if str(_pget(p, "section", "")).upper() == section_code]
    load = loader or (lambda project: load_project_bundle(project, object_id=object_id))

    spec_rows: list[dict] = []
    accepted: list[dict] = []
    project_rows: list[dict] = []
    warnings: list[str] = []
    total_optimization_items = 0
    graphics_total = 0

    for project in section_projects:
        project_id = str(_pget(project, "project_id", ""))
        project_name = str(_pget(project, "name", project_id))
        bundle = load(project)
        project_spec: list[dict] = []
        if bundle.get("md_text"):
            project_spec = parse_specification_markdown(
                str(bundle["md_text"]),
                project_id=project_id,
                project_name=project_name,
                version_id=str(bundle.get("version_id") or _pget(project, "version_id", "")),
                md_file=bundle.get("md_file"),
            )
            spec_rows.extend(project_spec)
        elif not bundle.get("error"):
            warnings.append(f"{project_id}: Markdown актуальной версии не найден")

        project_accepted, project_opt_total = _accepted_optimizations(project, bundle)
        accepted.extend(project_accepted)
        total_optimization_items += project_opt_total
        graphics = int(bundle.get("graphic_blocks") or _pget(project, "block_count", 0) or 0)
        graphics_total += graphics
        if bundle.get("error"):
            warnings.append(f"{project_id}: {bundle['error']}")
        project_rows.append({
            "project_id": project_id,
            "project_name": project_name,
            "version_id": bundle.get("version_id") or _pget(project, "version_id", ""),
            "specification_rows": len(project_spec),
            "optimization_items": project_opt_total,
            "accepted_optimizations": len(project_accepted),
            "graphic_blocks": graphics,
            "has_markdown": bool(bundle.get("md_text")),
            "error": bundle.get("error"),
        })

    return {
        "section": section_code,
        "section_project_count": len(section_projects),
        "projects": project_rows,
        "specification_rows": spec_rows,
        "accepted_optimizations": accepted,
        "warnings": warnings,
        "optimization_items": total_optimization_items,
        "graphic_blocks_available": graphics_total,
    }


def normalize_section_optimization_data(collected: dict) -> dict:
    """Этап 2: нормализовать ключи сопоставления, не обезличивая источники.

    Парсер уже очищает большинство полей. Этот шаг всё равно выполняется
    отдельно: он гарантирует единый canonical_key для данных, которые могли
    прийти из разных форм спецификаций, и сохраняет в строках исходный проект,
    версию, лист и позицию.
    """
    normalized_rows: list[dict] = []
    text_fields = (
        "project_id", "project_name", "version_id", "sheet", "sheet_name", "category",
        "position", "designation", "name", "type_mark", "code", "manufacturer",
        "unit", "quantity", "mass", "note",
    )
    for source_row in collected.get("specification_rows") or []:
        row = dict(source_row)
        for field in text_fields:
            if field in row:
                row[field] = _clean_text(row[field])
        row["canonical_key"] = _canonical_spec_key(row)
        normalized_rows.append(row)

    normalized_accepted: list[dict] = []
    for source_item in collected.get("accepted_optimizations") or []:
        item = dict(source_item)
        for field in ("project_id", "project_name", "version_id", "id", "current", "proposed", "risks", "norm"):
            if field in item:
                item[field] = _clean_text(item[field])
        item["spec_items"] = [_clean_text(value) for value in (item.get("spec_items") or []) if _clean_text(value)]
        normalized_accepted.append(item)

    normalized = dict(collected)
    normalized["specification_rows"] = normalized_rows
    normalized["accepted_optimizations"] = normalized_accepted
    return normalized


def synthesize_section_optimization_data(normalized: dict) -> dict:
    """Этап 3: сформировать межпроектные группы и кандидаты для эксперта."""
    spec_rows = list(normalized.get("specification_rows") or [])
    accepted = list(normalized.get("accepted_optimizations") or [])
    project_rows = list(normalized.get("projects") or [])
    shared_groups = group_shared_specification_items(spec_rows)
    accepted_clusters = cluster_accepted_optimizations(accepted)
    signals = build_replication_signals(spec_rows, accepted, accepted_clusters)
    section_code = _clean_text(normalized.get("section")).upper()
    return {
        "meta": {
            "section": section_code,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "project_count": int(normalized.get("section_project_count") or len(project_rows)),
            "projects_with_specifications": sum(1 for p in project_rows if p["specification_rows"] > 0),
            "specification_rows": len(spec_rows),
            "optimization_items": int(normalized.get("optimization_items") or 0),
            "accepted_optimizations": len(accepted),
            "shared_specification_groups": len(shared_groups),
            "accepted_merge_candidates": len(accepted_clusters),
            "replication_candidates": len(signals),
            "signals": len(signals),
            "graphic_blocks_available": int(normalized.get("graphic_blocks_available") or 0),
        },
        "projects": project_rows,
        "specification_rows": spec_rows,
        "accepted_optimizations": accepted,
        "shared_specification_groups": shared_groups,
        "accepted_optimization_clusters": accepted_clusters,
        "signals": signals,
        "analysis_stages": [
            {"key": "collect", "title": "Сбор", "description": "Актуальные спецификации и экспертные решения всех проектов раздела."},
            {"key": "normalize", "title": "Нормализация", "description": "Сопоставление номенклатуры без потери проекта, версии, листа и позиции."},
            {"key": "synthesize", "title": "Синтез", "description": "Поиск проектов для тиражирования уже принятых оптимизаций."},
            {"key": "agent", "title": "Умный агент", "description": "Инженерная оценка применимости решения отдельно для каждого целевого проекта."},
            {"key": "graphics", "title": "Графика по запросу", "description": "Точечная проверка только спорных кандидатов по связанным блокам чертежей."},
            {"key": "review", "title": "Эксперт", "description": "Принятие решения уровня раздела; автоматическое применение запрещено."},
        ],
        "warnings": list(normalized.get("warnings") or []),
    }


def build_section_optimization(
    section: str,
    *,
    object_id: Optional[str] = None,
    projects: Optional[list[Any]] = None,
    loader: Optional[Callable[[Any], dict]] = None,
) -> dict:
    """Собрать готовый read-only срез раздела в порядке pipeline-этапов."""
    collected = collect_section_optimization_data(
        section,
        object_id=object_id,
        projects=projects,
        loader=loader,
    )
    normalized = normalize_section_optimization_data(collected)
    return synthesize_section_optimization_data(normalized)


__all__ = [
    "build_section_optimization",
    "collect_section_optimization_data",
    "normalize_section_optimization_data",
    "synthesize_section_optimization_data",
    "build_replication_signals",
    "cluster_accepted_optimizations",
    "group_shared_specification_items",
    "load_project_bundle",
    "parse_specification_markdown",
]
