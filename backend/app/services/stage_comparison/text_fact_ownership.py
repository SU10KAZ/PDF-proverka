"""Text Fact Ownership: какой структуре документа принадлежит текстовый фрагмент.

Ответ строится только на доказанной структуре подготовки текста (Stage 2):
блок Markdown = таблица, её первая строка = шапка, абзац перед ней = заголовок
таблицы, ячейки строки = ``location_parts``, заголовок раздела над абзацем,
дословный повтор текста на нескольких страницах той же стороны.  Ничего не
выводится из близости на странице и ни одно правило не обращается к модели.

Статусы:

* ``PROVEN`` — владелец и семантика полей доказаны (строка под шапкой той же
  ширины; двухколоночная таблица «подпись — значение»; «Подпись: значение»);
* ``PARTIAL`` — владелец доказан, семантика полей нет (строка структурно
  доказанной таблицы без шапки; абзац под заголовком; повтор по документу);
* ``AMBIGUOUS`` — таблица с несогласованными ширинами строк;
* ``UNKNOWN`` — структурного владельца нет.

Отсутствие локального владельца НЕ делает текст общедокументным: scope
``DOCUMENT_SHARED`` присваивается только доказанному повтору на ≥ 2 страницах.

Формула, TeX и чистая математическая нотация не являются семантикой поля:
подпись поля обязана быть словом, иначе значение под ней в факт не попадает.
"""
from __future__ import annotations

import re
from collections import defaultdict
from typing import Any, Mapping

from .production_artifacts import content_signature, stable_id, utc_now
from .text_region_classifier import table_context

KIND = "stage_comparison_text_fact_ownership"
SCHEMA_VERSION = "text-fact-ownership.v1"
PRODUCER_VERSION = "text-fact-ownership-v1"

OWNER_KINDS = ("ENTITY", "TABLE_ROW", "TABLE", "FUNCTION", "SHEET", "DOCUMENT", "UNKNOWN")
STATUSES = ("PROVEN", "PARTIAL", "AMBIGUOUS", "UNKNOWN")
CHANNELS = (
    "EXACT_TABLE_ROW", "EXACT_CONTAINER", "EXPLICIT_LABEL", "EXPLICIT_KEY_VALUE",
    "SECTION_SCOPE", "DOCUMENT_SHARED", "OTHER",
)
SCOPES = ("FUNCTION_LOCAL", "TABLE_LOCAL", "SHEET_SHARED", "DOCUMENT_SHARED", "UNKNOWN")

_CONFUSABLES = str.maketrans({
    "a": "а", "c": "с", "e": "е", "o": "о", "p": "р", "x": "х", "y": "у",
    "k": "к", "h": "н", "b": "в", "m": "м", "t": "т", "ё": "е",
})
_NUMBER_CELL_RE = re.compile(r"^[+-]?\d+(?:[.,]\d+)?(?:\s*%)?$")
_NUMBERS_RE = re.compile(r"\d+(?:[.,]\d+)?")
_TEX_RE = re.compile(r"[\\{}^_$]|\\text|\\cdot|\\sum|\\frac|\\times|\\sqrt")
_MATH_ONLY_RE = re.compile(r"^[\s0-9A-Za-zΑ-Ωα-ω±×÷·∙•=<>≤≥≈≠∑∆∏√∞°%()\[\]/.,:;'\"′″*+-]{0,3}$")
_KEY_VALUE_RE = re.compile(r"^\s*(?P<label>[A-Za-zА-Яа-яЁё][^:=]{1,60}?)\s*[:=]\s*(?P<value>\S.*)$")
_LEGEND_RE = re.compile(r"^\s*(?:\[[^\]]{2,80}\]\s*[-–—]?\s*|[-–—•·]\s+)(\S.*)$")


def canonical_text(value: Any) -> str:
    """Регистр, ё/е, латинские двойники кириллицы, всё кроме букв и цифр."""
    return re.sub(r"[^0-9a-zа-я]+", "", str(value or "").casefold().translate(_CONFUSABLES))


def letters_only(value: Any) -> str:
    return re.sub(r"[^a-zа-я]", "", canonical_text(value))


def is_semantic_label(value: Any) -> bool:
    """Подпись поля — это слово (≥ 3 букв), не формула, не TeX, не одна буква с индексом."""
    text = " ".join(str(value or "").split())
    if not text or _TEX_RE.search(text):
        return False
    if len(letters_only(text)) < 3:
        return False
    if len(text.split()) > 8:
        return False
    return True


def is_plain_value(value: Any) -> bool:
    """Значение поля без формул/TeX и разумной длины."""
    text = " ".join(str(value or "").split())
    return bool(text) and not _TEX_RE.search(text) and len(text) <= 80


def _levenshtein(left: str, right: str, *, limit: int = 3) -> int:
    if abs(len(left) - len(right)) > limit:
        return limit
    previous = list(range(len(right) + 1))
    for index, left_char in enumerate(left, 1):
        current = [index]
        for jndex, right_char in enumerate(right, 1):
            current.append(min(previous[jndex] + 1, current[jndex - 1] + 1, previous[jndex - 1] + (left_char != right_char)))
        previous = current
    return min(previous[-1], limit)


def material_difference(before: Any, after: Any) -> bool:
    """Существенная разница значений: другие числа или больше двух букв.

    Разница в одну-две буквы при тех же числах («п1ж» → «p1ж», «Летняя» →
    «Лётная») между двумя распознанными редакциями не отличима от ошибки OCR
    и фактом не объявляется — такое свидетельство остаётся на проверке.
    """
    if canonical_text(before) == canonical_text(after):
        return False
    if _NUMBERS_RE.findall(str(before or "")) != _NUMBERS_RE.findall(str(after or "")):
        return True
    left, right = letters_only(before), letters_only(after)
    if left == right:
        return False
    return _levenshtein(left, right) > 2


def _cells(fragment: Mapping[str, Any]) -> list[str]:
    parts = [str(value) for value in fragment.get("location_parts") or [] if str(value).strip()]
    return parts or [str(fragment.get("text") or "")]


def _record(
    fragment: Mapping[str, Any], side: str, *, owner_kind: str, owner_id: str | None,
    status: str, channel: str, scope: str, evidence: list[str], fields: list[str | None] | None = None,
    row_key: str | None = None, table_group: str | None = None, table_title: str | None = None,
    header_cells: list[str] | None = None, repeated_pages: int = 0,
) -> dict[str, Any]:
    return {
        "fragment_id": str(fragment.get("id") or ""),
        "side": side,
        "page": fragment.get("pdf_page"),
        "owner_kind": owner_kind,
        "owner_id": owner_id,
        "ownership_status": status,
        "ownership_channel": channel,
        "scope": scope,
        "fields": fields,
        "fields_known": bool(fields) and all(fields),
        "row_key": row_key,
        "table_group": table_group,
        "table_title": table_title,
        "header_cells": header_cells,
        "repeated_pages": repeated_pages,
        "evidence": list(evidence),
    }


def fragment_ownership_index(text_preparation: Mapping[str, Any] | None) -> dict[str, dict[str, Any]]:
    """Владелец каждого фрагмента подготовки текста; чистая функция над Stage 2."""
    fragments = (text_preparation or {}).get("fragments") or {}
    by_group: dict[tuple[str, str], list[Mapping[str, Any]]] = defaultdict(list)
    by_block: dict[tuple[str, str], list[Mapping[str, Any]]] = defaultdict(list)
    by_page: dict[tuple[str, int], list[Mapping[str, Any]]] = defaultdict(list)
    pages_of_text: dict[tuple[str, str], set[int]] = defaultdict(set)
    ordered: list[tuple[str, Mapping[str, Any]]] = []
    for side_key in ("left", "right"):
        side = side_key.upper()
        for fragment in fragments.get(side_key) or ():
            if not isinstance(fragment, Mapping):
                continue
            ordered.append((side, fragment))
            page = int(fragment.get("pdf_page") or 0)
            if fragment.get("source_kind") == "table_row":
                by_group[(side, str(fragment.get("source_group") or ""))].append(fragment)
            by_block[(side, str(fragment.get("source_block_id") or ""))].append(fragment)
            by_page[(side, page)].append(fragment)
            canonical = canonical_text(fragment.get("text"))
            if canonical:
                pages_of_text[(side, canonical)].add(page)
    for values in (*by_group.values(), *by_block.values(), *by_page.values()):
        values.sort(key=lambda value: (int(value.get("pdf_page") or 0), int(value.get("order") or 0)))
    index: dict[str, dict[str, Any]] = {}
    for side, fragment in ordered:
        fragment_id = str(fragment.get("id") or "")
        page = int(fragment.get("pdf_page") or 0)
        repeated = len(pages_of_text.get((side, canonical_text(fragment.get("text"))), ()))
        shared_scope = "DOCUMENT_SHARED" if repeated >= 2 else "SHEET_SHARED"
        kind = str(fragment.get("source_kind") or "")
        block = by_block[(side, str(fragment.get("source_block_id") or ""))]
        if kind == "table_row":
            group_key = str(fragment.get("source_group") or "")
            table = table_context(fragment, by_group[(side, group_key)], block)
            cells = table["cells"]
            row_key = cells[0] if cells else None
            key_ok = bool(row_key) and not (len(cells) == 1 and _NUMBER_CELL_RE.match(row_key.strip()))
            title = table.get("title")
            if (
                len(cells) == 2 and table["table_proven"] and row_key
                and is_semantic_label(row_key) and not re.search(r"\d", row_key)
            ):
                index[fragment_id] = _record(
                    fragment, side, owner_kind="TABLE_ROW",
                    owner_id=f"row:{group_key}:{canonical_text(row_key)}",
                    status="PROVEN", channel="EXPLICIT_LABEL", scope="TABLE_LOCAL",
                    evidence=["two_column_label_value_table", "repeated_row_structure"],
                    fields=[row_key], row_key=row_key, table_group=group_key, table_title=title,
                )
                continue
            header_cells = table.get("header_cells")
            if header_cells and len(header_cells) >= 3 and table["table_proven"] and key_ok:
                fields = [cell if is_semantic_label(cell) else None for cell in header_cells]
                index[fragment_id] = _record(
                    fragment, side, owner_kind="TABLE_ROW",
                    owner_id=f"row:{group_key}:{canonical_text(row_key)}",
                    status="PROVEN", channel="EXACT_TABLE_ROW", scope="TABLE_LOCAL",
                    evidence=["header_first_row_same_width", "repeated_row_structure", "row_key"],
                    fields=fields, row_key=row_key, table_group=group_key, table_title=title,
                    header_cells=list(header_cells),
                )
                continue
            if table["table_proven"] and key_ok:
                index[fragment_id] = _record(
                    fragment, side, owner_kind="TABLE_ROW",
                    owner_id=f"row:{group_key}:{canonical_text(row_key)}",
                    status="PARTIAL", channel="EXACT_CONTAINER", scope="TABLE_LOCAL",
                    evidence=["repeated_row_structure", "row_key", "no_proven_header"],
                    row_key=row_key, table_group=group_key, table_title=title,
                )
                continue
            if table["table_proven"]:
                index[fragment_id] = _record(
                    fragment, side, owner_kind="TABLE", owner_id=f"table:{group_key}",
                    status="PARTIAL", channel="EXACT_CONTAINER", scope="TABLE_LOCAL",
                    evidence=["repeated_row_structure", "no_row_key"], table_group=group_key, table_title=title,
                )
                continue
            if table["rows"] >= 2:
                index[fragment_id] = _record(
                    fragment, side, owner_kind="TABLE", owner_id=f"table:{group_key}",
                    status="AMBIGUOUS", channel="EXACT_CONTAINER", scope="TABLE_LOCAL",
                    evidence=["inconsistent_row_widths"], table_group=group_key, table_title=title,
                )
                continue
            index[fragment_id] = _record(
                fragment, side, owner_kind="UNKNOWN", owner_id=None, status="UNKNOWN",
                channel="OTHER", scope="UNKNOWN", evidence=["single_table_row_without_structure"],
            )
            continue
        text = str(fragment.get("text") or "")
        key_value = _KEY_VALUE_RE.match(text)
        if key_value and is_semantic_label(key_value.group("label")) and not re.search(r"\d", key_value.group("label")) \
                and len(key_value.group("label").split()) <= 6 and is_plain_value(key_value.group("value")):
            index[fragment_id] = _record(
                fragment, side, owner_kind="SHEET", owner_id=f"sheet:{side}:{page}",
                status="PROVEN", channel="EXPLICIT_KEY_VALUE", scope=shared_scope,
                evidence=["explicit_label_colon_value"], fields=[key_value.group("label").strip()],
                repeated_pages=repeated,
            )
            continue
        if _LEGEND_RE.match(text):
            index[fragment_id] = _record(
                fragment, side, owner_kind="TABLE", owner_id=f"legend:{fragment.get('source_block_id')}",
                status="PARTIAL", channel="EXPLICIT_LABEL", scope=shared_scope,
                evidence=["legend_marker"], repeated_pages=repeated,
            )
            continue
        heading = None
        position = next((i for i, value in enumerate(block) if str(value.get("id")) == fragment_id), 0)
        for value in reversed(block[:position]):
            if value.get("source_kind") == "heading":
                heading = str(value.get("text") or "")
                break
        if heading is None and kind != "heading":
            page_values = by_page[(side, page)]
            page_position = next((i for i, value in enumerate(page_values) if str(value.get("id")) == fragment_id), 0)
            for value in reversed(page_values[:page_position]):
                if value.get("source_kind") == "heading":
                    heading = str(value.get("text") or "")
                    break
        if heading:
            index[fragment_id] = _record(
                fragment, side, owner_kind="SHEET",
                owner_id=f"section:{side}:{page}:{canonical_text(heading)[:24]}",
                status="PARTIAL", channel="SECTION_SCOPE", scope=shared_scope,
                evidence=["heading_above"], repeated_pages=repeated,
            )
            continue
        if repeated >= 2:
            index[fragment_id] = _record(
                fragment, side, owner_kind="DOCUMENT", owner_id=f"doc:{side}",
                status="PARTIAL", channel="DOCUMENT_SHARED", scope="DOCUMENT_SHARED",
                evidence=[f"identical_text_on_{repeated}_pages"], repeated_pages=repeated,
            )
            continue
        index[fragment_id] = _record(
            fragment, side, owner_kind="UNKNOWN", owner_id=None, status="UNKNOWN",
            channel="OTHER", scope="UNKNOWN", evidence=["no_structural_owner"], repeated_pages=repeated,
        )
    return index


_STATUS_RANK = {"PROVEN": 3, "PARTIAL": 2, "AMBIGUOUS": 1, "UNKNOWN": 0}


def _combine(left: Mapping[str, Any] | None, right: Mapping[str, Any] | None) -> dict[str, Any]:
    """Владелец атома по обеим сторонам: одна сторона — её запись; две — согласованная."""
    if left is None and right is None:
        return {"owner_kind": "UNKNOWN", "owner_id": None, "ownership_status": "UNKNOWN", "ownership_channel": "OTHER", "scope": "UNKNOWN", "fields": None, "evidence": ["fragment_not_in_preparation"]}
    if left is None or right is None:
        record = left or right
        return {key: record.get(key) for key in ("owner_kind", "owner_id", "ownership_status", "ownership_channel", "scope", "fields", "evidence")}
    consistent = (
        left["owner_kind"] == right["owner_kind"]
        and left["ownership_channel"] == right["ownership_channel"]
        and canonical_text(left.get("row_key")) == canonical_text(right.get("row_key"))
        and [canonical_text(v) for v in (left.get("fields") or [])] == [canonical_text(v) for v in (right.get("fields") or [])]
    )
    weakest = min((left, right), key=lambda value: _STATUS_RANK[value["ownership_status"]])
    status = weakest["ownership_status"] if consistent else ("AMBIGUOUS" if _STATUS_RANK[weakest["ownership_status"]] > 0 else "UNKNOWN")
    return {
        "owner_kind": left["owner_kind"] if consistent else "UNKNOWN",
        "owner_id": left["owner_id"] if consistent else None,
        "ownership_status": status,
        "ownership_channel": left["ownership_channel"] if consistent else "OTHER",
        "scope": left["scope"] if consistent else "UNKNOWN",
        "fields": left.get("fields") if consistent else None,
        "evidence": sorted(set(left.get("evidence") or []) | set(right.get("evidence") or [])) + ([] if consistent else ["sides_disagree"]),
    }


def build_text_fact_ownership(
    *,
    pair_id: Any,
    atoms_artifact: Mapping[str, Any],
    text_preparation: Mapping[str, Any] | None,
    generated_at: str | None = None,
) -> dict[str, Any]:
    """Артефакт владельца для каждого текстового атома; ничего в атомах не меняет."""
    index = fragment_ownership_index(text_preparation)
    records: list[dict[str, Any]] = []
    for atom in atoms_artifact.get("atoms") or ():
        if not isinstance(atom, Mapping):
            continue
        provenance = atom.get("provenance") or {}
        locations = provenance.get("locations") or {}
        side_records = {}
        for side in ("LEFT", "RIGHT"):
            ids = [str(value.get("fragment_id") or "") for value in locations.get(side) or () if isinstance(value, Mapping)]
            found = [index[fragment_id] for fragment_id in ids if fragment_id in index]
            side_records[side] = found[0] if len(found) == 1 else None
        combined = _combine(side_records["LEFT"], side_records["RIGHT"])
        records.append({
            "ownership_id": stable_id("town_", pair_id, atom.get("atom_id")),
            "fact_id": provenance.get("semantic_fact_id"),
            "source_text_atom_id": atom.get("atom_id"),
            **combined,
            "direction": atom.get("direction"),
            "applicability": combined["scope"],
            "evidence_refs": [
                {"side": side, "fragment_id": record["fragment_id"], "page": record["page"], "evidence": record["evidence"]}
                for side, record in side_records.items() if record is not None
            ],
        })
    records.sort(key=lambda value: str(value["source_text_atom_id"]))
    counts = {
        "atoms": len(records),
        "by_status": {status: sum(1 for value in records if value["ownership_status"] == status) for status in STATUSES},
        "by_kind": {kind: sum(1 for value in records if value["owner_kind"] == kind) for kind in OWNER_KINDS},
        "by_channel": {channel: sum(1 for value in records if value["ownership_channel"] == channel) for channel in CHANNELS},
        "by_scope": {scope: sum(1 for value in records if value["scope"] == scope) for scope in SCOPES},
    }
    payload = {
        "kind": KIND,
        "schema_version": SCHEMA_VERSION,
        "version": 1,
        "pair_id": pair_id,
        "generated_at": generated_at or utc_now(),
        "atoms_signature": atoms_artifact.get("input_signature"),
        "preparation_signature": (text_preparation or {}).get("input_signature"),
        "ownership": records,
        "diagnostics": {**counts, "uses_model": False, "proximity_used": False},
        "provenance": {"producer": PRODUCER_VERSION, "uses_model": False},
    }
    payload["input_signature"] = content_signature({
        "producer": PRODUCER_VERSION,
        "atoms_signature": payload["atoms_signature"],
        "preparation_signature": payload["preparation_signature"],
        "ownership": records,
    })
    return payload


__all__ = [
    "KIND", "SCHEMA_VERSION", "PRODUCER_VERSION",
    "OWNER_KINDS", "STATUSES", "CHANNELS", "SCOPES",
    "build_text_fact_ownership",
    "canonical_text",
    "fragment_ownership_index",
    "is_plain_value",
    "is_semantic_label",
    "material_difference",
]
