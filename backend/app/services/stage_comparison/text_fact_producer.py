"""Deterministic structured facts over production Stage 3 TEXT deltas.

The producer is deliberately precision-first.  It accepts only source units
whose structure is explicit in Stage 2 preparation: a recognised electrical
load table, a complete key/value expression, or a complete list of labelled
values.  Narrative text and ambiguous table layouts remain unresolved for an
engineer.  No model, fuzzy entity match, or unstated engineering assumption is
used here.
"""
from __future__ import annotations

from collections import Counter, defaultdict
from decimal import Decimal, InvalidOperation
import re
from typing import Any, Iterable, Mapping

from .production_artifacts import content_signature, stable_id, utc_now
from .production_text_flow import PREPARATION_KIND, PREPARATION_SCHEMA_VERSION
from . import room_schedule
from .text_comparison import canonicalize_text
from .text_semantic_validation import iter_stage3_evidence, stage3_content_signature
from .unified_entity_bridge.entity_normalizer import canonical_entity_name


KIND = "stage_comparison_text_fact_production"
SCHEMA_VERSION = "text-fact-production.v1"
PRODUCER_VERSION = "deterministic-text-fact-producer-v1"

_NUMBER_RE = re.compile(r"^[+-]?\d+(?:[.,]\d+)?$")
_NUMBER_IN_VALUE_RE = re.compile(r"[+-]?\d+(?:[.,]\d+)?")
_DESIGNATION_RE = re.compile(
    r"(?<![0-9A-ZА-ЯЁ])"
    r"(?:ЩР|ШР|ВРУ|ГРЩ|SHR|VRU|MSB|PANEL|QF|QS)"
    r"(?:\s*[-./]?\s*[0-9A-ZА-ЯЁ]+(?:[-./][0-9A-ZА-ЯЁ]+)*)",
    re.IGNORECASE,
)
_LEADING_DESIGNATION_RE = re.compile(
    rf"^\s*(?P<entity>{_DESIGNATION_RE.pattern})\s*[:;]\s*(?P<body>.+)$",
    re.IGNORECASE,
)
_SUPPLY_RE = re.compile(
    rf"^\s*(?P<subject>{_DESIGNATION_RE.pattern})\s*[-–—]\s*от\s+"
    r"(?P<source>.+?)\s*$",
    re.IGNORECASE,
)

# A recognised load table has thirteen cells.  Each schema entry after the
# entity cell is one independent property, so a row never becomes one
# sentence-shaped mega-fact.
_ELECTRICAL_COLUMNS = (
    ("quantity", "QUANTITY", None),
    ("unit_installed_power_kw", "PARAMETER", "kW"),
    ("installed_power_kw", "PARAMETER", "kW"),
    ("utilization_coefficient", "PARAMETER", None),
    ("demand_coefficient", "PARAMETER", None),
    ("coincidence_coefficient", "PARAMETER", None),
    ("power_factor_cos_phi", "PARAMETER", None),
    ("reactive_factor_tan_phi", "PARAMETER", None),
    ("demand_active_power_kw", "PARAMETER", "kW"),
    ("demand_reactive_power_kvar", "PARAMETER", "kvar"),
    ("demand_apparent_power_kva", "PARAMETER", "kVA"),
    ("maximum_calculated_current_a", "PARAMETER", "A"),
)
_TOTAL_COLUMNS = (
    ("total_demand_coefficient", "PARAMETER", None),
    ("total_power_factor_cos_phi", "PARAMETER", None),
    ("total_demand_active_power_kw", "PARAMETER", "kW"),
    ("total_demand_reactive_power_kvar", "PARAMETER", "kvar"),
    ("total_demand_apparent_power_kva", "PARAMETER", "kVA"),
    ("total_maximum_calculated_current_a", "PARAMETER", "A"),
)

_VALUE_TOKEN = (
    r"(?:не\s+(?:ниже|менее|более)\s+|[≥≤<>]=?\s*)?"
    r"(?:IP\s*\d{2,3}|"
    r"[+-]?\d+(?:[.,]\d+)?"
    r"(?:\s*(?:…|\.\.|до)\s*[+-]?\d+(?:[.,]\d+)?)?"
    r"(?:\s*(?:°\s*C|°С|кВар|кВА|кВт|Вт|мА|А|В|кВ|Гц|мм2|мм²|%|шт\.?|м2|м²))?"
    r"|[A-ZА-ЯЁ]{1,8}[-./]?\d+[0-9A-ZА-ЯЁ./-]*)"
)

# Labels are closed and ordered most-specific-first.  A generic ``key: value``
# expression is handled separately, but only when its complete right-hand
# side satisfies ``_VALUE_TOKEN``.
_LABEL_SPECS = (
    ("protection_degree", "PARAMETER", r"степен(?:ь|и)\s+защиты|protection\s+degree"),
    ("temperature_range", "PARAMETER", r"температур(?:а|ы|ный\s+диапазон)|temperature"),
    ("voltage", "PARAMETER", r"напряжени(?:е|я)|voltage"),
    ("frequency", "PARAMETER", r"частот(?:а|ы)|frequency"),
    ("installed_power", "PARAMETER", r"установленн(?:ая|ой)\s+мощност(?:ь|и)"),
    ("power", "PARAMETER", r"мощност(?:ь|и)|power"),
    ("current", "PARAMETER", r"(?:расчетн(?:ый|ого)\s+)?ток|current"),
    ("cross_section", "PARAMETER", r"сечени(?:е|я)|cross[- ]?section"),
    ("quantity", "QUANTITY", r"количеств(?:о|а)|кол-во|quantity|count"),
    ("reserve_space", "SPACE", r"резерв(?:ное\s+место)?|запас\s+площади"),
    ("device_type", "TYPE", r"тип|марка|модель|type|model"),
)
_LABEL_PATTERNS = tuple(
    (
        facet,
        dimension,
        re.compile(
            rf"^\s*(?:{label})\s*(?:[:=\-]\s*)?(?P<value>{_VALUE_TOKEN})\s*$",
            re.IGNORECASE,
        ),
    )
    for facet, dimension, label in _LABEL_SPECS
)
_KEY_VALUE_RE = re.compile(
    rf"^\s*(?P<label>[A-ZА-ЯЁ][A-ZА-ЯЁ0-9 ()/._-]{{1,70}}?)\s*[:=]\s*"
    rf"(?P<value>{_VALUE_TOKEN})\s*$",
    re.IGNORECASE,
)


def _strict_decimal(value: Any) -> Decimal | None:
    raw = str(value or "").strip().replace(",", ".")
    if not _NUMBER_RE.fullmatch(raw):
        return None
    try:
        return Decimal(raw)
    except InvalidOperation:
        return None


def _normalized_value(value: Any, unit: str | None = None) -> str:
    raw = " ".join(str(value or "").strip().split())
    raw = re.sub(r"(?<=\d),(?=\d)", ".", raw)
    raw = re.sub(r"\s+", " ", raw)
    return f"{raw} {unit}" if raw and unit else raw


def _facet_from_label(label: str) -> str:
    canonical = canonical_entity_name(label).casefold()
    canonical = re.sub(r"[^0-9a-zа-я]+", "_", canonical).strip("_")
    return canonical[:80] or "value"


def _extract_designation(text: str) -> tuple[str, str] | None:
    match = _DESIGNATION_RE.search(str(text or ""))
    if not match:
        return None
    original = " ".join(match.group(0).split())
    canonical = canonical_entity_name(original)
    return (original, canonical) if canonical else None


def _scope_ref(group: Mapping[str, Any]) -> str:
    return stable_id(
        "text_scope_",
        group.get("id"),
        sorted(int(page) for page in group.get("left_pages") or []),
        sorted(int(page) for page in group.get("right_pages") or []),
    )


def _entity_refs(
    *,
    pair_id: Any,
    group: Mapping[str, Any],
    original: str | None,
    canonical: str | None,
    explicit_project_entity: bool,
    context: str | None = None,
) -> tuple[str, str | None, dict[str, Any]]:
    canonical_value = str(canonical or "").strip()
    original_value = str(original or "").strip()
    if not canonical_value:
        subject_ref = stable_id("text_scope_subject_", pair_id, group.get("id"))
        return subject_ref, None, {
            "original": original_value or None,
            "canonical": None,
            "project_identity_established": False,
        }
    identity = {
        "pair_id": pair_id,
        "group_id": group.get("id"),
        "entity": canonical_value,
        "context": context,
    }
    subject_ref = "text_entity:" + canonical_value
    if context:
        subject_ref += ":" + content_signature(context)[:10]
    project_ref = (
        stable_id("project_text_entity_", identity)
        if explicit_project_entity
        else None
    )
    return subject_ref, project_ref, {
        "original": original_value or None,
        "canonical": canonical_value,
        "context": context,
        "project_identity_established": bool(project_ref),
    }


def _relation_assessment(
    group: Mapping[str, Any],
    *,
    opposite_coverage_incomplete: bool,
) -> tuple[str, str, dict[str, Any]]:
    status = str(group.get("relation_status") or "").upper()
    relation_type = str(group.get("relation_type") or "").upper()
    reasons: list[str] = []
    missing_fields: list[str] = []
    if status == "POSSIBLE":
        # The property itself is structured and grounded.  What remains
        # unproven is the upstream sheet pairing, which is resolved by the
        # single SHEET question for this group rather than hundreds of
        # duplicate per-property CHANGE questions.
        reasons.append("sheet_relation_unconfirmed")
        missing_fields.append("sheet_relation_confirmation")
    elif status != "HIGH" and relation_type != "USER_SELECTED":
        reasons.append("sheet_relation_confidence_unknown")
        missing_fields.append("sheet_relation_confirmation")
    if opposite_coverage_incomplete:
        # A zero-fragment opposite side is not proof that a property was
        # added/removed.  OCR Markdown may contain only an IMAGE block while
        # the underlying PDF still has vector text.  Keep the property and
        # its anchor, but do not promote it after a SHEET answer.
        reasons.append("opposite_side_structured_coverage_incomplete")
        missing_fields.append("opposite_side_structured_coverage")
    if not reasons:
        return "MATERIAL_CHANGE", "HIGH", {
            "reason_codes": [],
            "missing_fields": [],
            "only_upstream_relation_blocker": False,
            "per_atom_question_actionable": True,
        }
    return "REVIEW_REQUIRED", (
        "UNKNOWN" if opposite_coverage_incomplete else "MEDIUM"
    ), {
        "reason_codes": reasons,
        "missing_fields": missing_fields,
        "only_upstream_relation_blocker": reasons == ["sheet_relation_unconfirmed"],
        # Both blockers belong to upstream scope/coverage, not to 320
        # independent engineering judgements about otherwise parsed cells.
        "per_atom_question_actionable": False,
    }


def _direction(bucket: str, before: Any, after: Any, dimension: str) -> str:
    if bucket == "added":
        return "ADDED"
    if bucket == "removed":
        return "REMOVED"
    if dimension == "TYPE":
        return "REPLACED"
    left_numbers = _NUMBER_IN_VALUE_RE.findall(str(before or ""))
    right_numbers = _NUMBER_IN_VALUE_RE.findall(str(after or ""))
    if len(left_numbers) == len(right_numbers) == 1:
        left = _strict_decimal(left_numbers[0])
        right = _strict_decimal(right_numbers[0])
        if left is not None and right is not None:
            if right > left:
                return "INCREASED"
            if right < left:
                return "DECREASED"
    return "ALTERED"


def _anchors(item: Mapping[str, Any]) -> dict[str, list[dict[str, Any]]]:
    return {
        "LEFT": [dict(value) for value in item.get("left_anchors") or [] if isinstance(value, Mapping)],
        "RIGHT": [dict(value) for value in item.get("right_anchors") or [] if isinstance(value, Mapping)],
    }


def _fragments_by_id(preparation: Mapping[str, Any]) -> dict[str, tuple[str, dict[str, Any]]]:
    fragments = preparation.get("fragments")
    if not isinstance(fragments, Mapping):
        raise ValueError("text preparation fragments required")
    output: dict[str, tuple[str, dict[str, Any]]] = {}
    for side in ("left", "right"):
        values = fragments.get(side) or []
        if not isinstance(values, list):
            raise ValueError(f"text preparation fragments.{side} must be an array")
        for raw in values:
            if not isinstance(raw, Mapping):
                raise ValueError("text preparation fragment must be an object")
            fragment = dict(raw)
            fragment_id = str(fragment.get("id") or "")
            if not fragment_id or fragment_id in output:
                raise ValueError("text preparation contains missing/duplicate fragment id")
            output[fragment_id] = (side, fragment)
    return output


def _is_electrical_header(fragment: Mapping[str, Any]) -> bool:
    value = canonicalize_text(str(fragment.get("text") or ""))
    return all(token in value for token in (
        "наименование потребителей",
        "установленная мощность",
        "потребная мощность",
    ))


def _is_electrical_units(fragment: Mapping[str, Any]) -> bool:
    value = canonicalize_text(str(fragment.get("text") or ""))
    return (
        ("cosφ" in value or "cosf" in value)
        and ("pp=" in value or "рр=" in value)
        and ("qp=" in value or "qр=" in value)
    )


# Разметку экспликации («номер | наименование | площадь | категория») читает
# room_schedule: она нужна и подготовке текста, которая режет сдвоенные строки
# ДО сравнения, и этому производителю.
_ROOM_HEADER_COLUMNS = room_schedule.HEADER_COLUMNS
_ROOM_SCHEDULE_COLUMNS = (
    ("room_name", "TYPE", None),
    # «м²», not «m2»: the unit is appended to the value, and a unit carrying an
    # ASCII digit makes ``_direction`` count two numbers in «15.71 m2» and fall
    # back to ALTERED instead of INCREASED.
    ("room_area_m2", "PARAMETER", "м²"),
    ("room_fire_category", "TYPE", None),
)
_ROOM_CODE_RE = room_schedule.ROOM_CODE_RE
_ROOM_AREA_RE = room_schedule.ROOM_AREA_RE
_ROOM_AREA_UNIT_RE = room_schedule.ROOM_AREA_UNIT_RE
_ROOM_CATEGORY_RE = room_schedule.ROOM_CATEGORY_RE

_looks_like_room_header = room_schedule.looks_like_header


room_schedule_header_units = room_schedule.header_units
room_row_units = room_schedule.row_units
_room_unit_is_valid = room_schedule.unit_is_valid


def _room_schedule_header_width(fragment: Mapping[str, Any]) -> int | None:
    """The widest unit this header proves, or None when it is not a header."""
    return room_schedule.header_width(fragment)


def _is_section_row(fragment: Mapping[str, Any]) -> bool:
    parts = list(fragment.get("location_parts") or [])
    if len(parts) != 1:
        return False
    value = canonicalize_text(str(parts[0]))
    return bool(
        _extract_designation(value)
        and (
            " от " in f" {value} "
            or "режим" in value
            or len(value.split()) <= 3
        )
    )


def _table_contexts(
    fragments: Mapping[str, tuple[str, dict[str, Any]]],
) -> tuple[
    set[tuple[str, str]],
    dict[tuple[str, str], int],
    dict[str, str | None],
    dict[str, str],
    dict[str, dict[str, str]],
]:
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for _fragment_id, (side, fragment) in fragments.items():
        if fragment.get("source_kind") != "table_row":
            continue
        source_group = str(fragment.get("source_group") or "")
        if source_group:
            grouped[(side, source_group)].append(fragment)
    valid_tables: set[tuple[str, str]] = set()
    room_schedule_widths: dict[tuple[str, str], int] = {}
    context_by_fragment: dict[str, str | None] = {}
    metadata_reason: dict[str, str] = {}
    for key, values in grouped.items():
        ordered = sorted(values, key=lambda value: (
            int(value.get("pdf_page") or 0),
            int(value.get("order") or 0),
            str(value.get("id") or ""),
        ))
        header_ids = {str(value["id"]) for value in ordered if _is_electrical_header(value)}
        unit_ids = {str(value["id"]) for value in ordered if _is_electrical_units(value)}
        if header_ids and unit_ids:
            valid_tables.add(key)
        room_header_ids: set[str] = set()
        room_widths: set[int] = set()
        room_header_seen = False
        for value in ordered:
            width = _room_schedule_header_width(value)
            if width is None:
                continue
            room_header_seen = True
            room_header_ids.add(str(value["id"]))
            room_widths.add(width)
        # Every header in the group declares its own column count, and a row is
        # then validated against the widest one — the fourth column is optional
        # and only ever missing from the tail, so accepting the wider shape
        # cannot mis-read the narrower one.  Room tables are tracked separately
        # from ``valid_tables``: that set means «proven electrical load table»
        # and nothing else.
        if room_widths and key not in valid_tables:
            room_schedule_widths[key] = max(room_widths)
        elif room_header_seen:
            # An unproven table still has header rows, and a header row is
            # never a fact whether or not its table was proven.
            room_header_ids = {
                str(value["id"]) for value in ordered
                if _looks_like_room_header(value)
            }
        current_context: str | None = None
        for fragment in ordered:
            fragment_id = str(fragment["id"])
            if fragment_id in header_ids:
                metadata_reason[fragment_id] = "electrical_table_header_not_a_fact"
            elif fragment_id in unit_ids:
                metadata_reason[fragment_id] = "electrical_table_units_not_a_fact"
            elif fragment_id in room_header_ids:
                metadata_reason[fragment_id] = "room_schedule_header_not_a_fact"
            elif _is_section_row(fragment):
                current_context = canonicalize_text(str(fragment.get("text") or ""))
                metadata_reason[fragment_id] = "table_section_label_not_a_fact"
            context_by_fragment[fragment_id] = current_context
    assembly_rows = _assembly_rows(fragments)
    for fragment_id in assembly_rows:
        # Строка состава — факт, а не метаданные, даже если её раньше приняли
        # за подпись раздела.
        metadata_reason.pop(fragment_id, None)
    return (
        valid_tables,
        room_schedule_widths,
        context_by_fragment,
        metadata_reason,
        assembly_rows,
    )


def _table_row_properties(
    fragment: Mapping[str, Any],
    *,
    side: str,
    valid_tables: set[tuple[str, str]],
    context_by_fragment: Mapping[str, str | None],
) -> dict[str, Any] | None:
    key = (side, str(fragment.get("source_group") or ""))
    if key not in valid_tables or fragment.get("source_kind") != "table_row":
        return None
    parts = [canonicalize_text(str(value)) for value in fragment.get("location_parts") or []]
    if len(parts) == 13 and parts[0] and all(_strict_decimal(value) is not None for value in parts[1:]):
        columns = _ELECTRICAL_COLUMNS
        subject = parts[0]
    elif (
        len(parts) == 7
        and parts[0] == "итого"
        and all(_strict_decimal(value) is not None for value in parts[1:])
    ):
        columns = _TOTAL_COLUMNS
        subject = "TOTAL"
    else:
        return None
    context = context_by_fragment.get(str(fragment.get("id") or ""))
    canonical_subject = canonical_entity_name(subject)
    values = {
        facet: {
            "dimension": dimension,
            "value": _normalized_value(raw, unit),
            "raw_value": raw,
            "unit": unit,
            "cell_index": index,
        }
        for index, ((facet, dimension, unit), raw) in enumerate(
            zip(columns, parts[1:]), start=1
        )
    }
    return {
        "subject_original": subject,
        "subject_canonical": canonical_subject,
        "context": context,
        "values": values,
        "rule": "recognized_electrical_load_table",
        "explicit_project_entity": True,
    }


def _room_row_properties(
    fragment: Mapping[str, Any],
    *,
    side: str,
    room_schedule_widths: Mapping[tuple[str, str], int],
    context_by_fragment: Mapping[str, str | None],
) -> dict[str, Any] | None:
    """One room, one row: «28.1 | холл | 15,71» in a table that named itself.

    Every column meaning comes from the header proven for this very
    ``source_group``, so nothing is inferred from the shape of a single row.
    A glued pair of rows («02.1 Рампа 2019,94 B2 02.42а Кладовая 6,18») has
    more cells than the header declares and is refused: taking its last number
    as the area of the first room would be wrong by a factor of 327.
    """
    key = (side, str(fragment.get("source_group") or ""))
    width = room_schedule_widths.get(key)
    if width is None or fragment.get("source_kind") != "table_row":
        return None
    parts = [canonicalize_text(str(value)) for value in fragment.get("location_parts") or []]
    # Fewer cells than the header only ever means a trailing empty category.
    if not 3 <= len(parts) <= width:
        return None
    if not _ROOM_CODE_RE.match(parts[0]):
        return None
    if not parts[1] or _NUMBER_RE.match(parts[1]):
        return None
    if not _ROOM_AREA_RE.match(parts[2]):
        return None
    if len(parts) == 4 and not _ROOM_CATEGORY_RE.match(parts[3]):
        return None
    parts[2] = _ROOM_AREA_UNIT_RE.sub("", parts[2]).strip()
    values = {
        facet: {
            "dimension": dimension,
            "value": _normalized_value(raw, unit),
            "raw_value": raw,
            "unit": unit,
            "cell_index": index,
        }
        for index, ((facet, dimension, unit), raw) in enumerate(
            zip(_ROOM_SCHEDULE_COLUMNS, parts[1:]), start=1
        )
    }
    return {
        "subject_original": parts[0],
        "subject_canonical": canonical_entity_name(parts[0]),
        "context": context_by_fragment.get(str(fragment.get("id") or "")),
        "values": values,
        "rule": "recognized_room_schedule_table",
        "explicit_project_entity": True,
    }


# ── Состав конструкции: «пирог» кровли, пола, покрытия ─────────────────────
#
# Такая таблица не подписывает свои колонки — она подписывает саму себя:
# перед каждым составом стоит абзац «Кровля тип К3 (толщ. 350-550мм)».  Этот
# заголовок и есть доказательство: он задаёт объект (К3) и границу состава,
# внутри которой строка «минераловатный утеплитель ... | -150 мм» читается как
# толщина слоя, а не как случайная пара ячеек.
#
# Один блок документа обычно содержит несколько составов подряд, и материалы в
# них повторяются с разной толщиной.  Поэтому идентичность слоя действует
# только внутри своего состава, а материал, встретившийся в составе дважды,
# отбрасывается целиком: два разных значения под одним именем — не факт.
# Обозначение состава пишут чертёжным шрифтом, и «К3» регулярно приходит из
# OCR как «КЗ», а «ПТ3» — как «ПТЗ». Кириллическая «з» на месте цифры внутри
# обозначения — это одна и та же марка, поэтому она приводится к цифре: иначе
# левый лист говорит про «КЗ», правый про «К3», и один состав раздваивается.
_ASSEMBLY_HEADING_RE = re.compile(
    r"^\s*(?P<kind>кровл\w*|покрыти\w*|пол\w*|пирог\w*|состав\w*)\s+"
    r"тип\s+(?P<head>[а-яa-z]{0,3}\s?-?)"
    r"(?P<number>[0-9зо]{1,3}(?:\.[0-9зо]{1,2})?)"
    r"(?![а-яa-z])",
    re.I,
)
_ASSEMBLY_CODE_DIGITS = str.maketrans({"з": "3", "о": "0"})
_ASSEMBLY_THICKNESS_RE = re.compile(
    r"^-?\d{1,4}(?:[.,]\d{1,2})?"
    r"(?:\s*\.{2,3}\s*-?\d{1,4}(?:[.,]\d{1,2})?)?\s*мм$",
    re.I,
)
_ASSEMBLY_THICKNESS_LEAD_RE = re.compile(r"^-\s*")
_ASSEMBLY_UNIT_RE = re.compile(r"\s*мм\s*$", re.I)
#: Сколько строк «материал | толщина» обязана иметь таблица, чтобы считаться
#: составом конструкции.  Меньше — это не таблица слоёв, а совпадение формы.
_ASSEMBLY_MIN_THICKNESS_ROWS = 4
_ASSEMBLY_MIN_THICKNESS_SHARE = 0.4
_ASSEMBLY_MIN_MATERIAL_CHARS = 4


def _assembly_heading(fragment: Mapping[str, Any]) -> str | None:
    """«Кровля тип К3 (толщ. 350-550мм)» → «кровля тип к3»."""
    if fragment.get("source_kind") not in {"paragraph", "heading"}:
        return None
    text = canonicalize_text(str(fragment.get("text") or ""))
    match = _ASSEMBLY_HEADING_RE.match(text)
    if not match:
        return None
    # Буквенная часть марки остаётся буквенной; цифровая — цифровой.
    code = match.group("head").replace(" ", "") + match.group("number").translate(
        _ASSEMBLY_CODE_DIGITS
    )
    return " ".join(f"{match.group('kind')} тип {code}".split())


def _assembly_thickness_row(parts: list[str]) -> tuple[str, str] | None:
    if len(parts) != 2 or not _ASSEMBLY_THICKNESS_RE.match(parts[1]):
        return None
    material = parts[0].strip()
    if len(material) < _ASSEMBLY_MIN_MATERIAL_CHARS or _NUMBER_RE.match(material):
        return None
    thickness = _ASSEMBLY_UNIT_RE.sub("", _ASSEMBLY_THICKNESS_LEAD_RE.sub("", parts[1])).strip()
    return material, thickness


def _assembly_material_only_row(parts: list[str]) -> str | None:
    if len(parts) != 1:
        return None
    material = parts[0].strip()
    if len(material) <= 6 or _NUMBER_RE.match(material):
        return None
    return material


def _assembly_rows(
    fragments: Mapping[str, tuple[str, dict[str, Any]]],
) -> dict[str, dict[str, str]]:
    """fragment_id → {assembly, material} для строк доказанных составов.

    Заголовок состава — это абзац, а не строка таблицы, поэтому группировка
    здесь своя: она обязана видеть и абзацы, иначе границу состава читать
    нечем.
    """
    # Группируем по БЛОКУ, а не по source_group: заголовок состава — абзац с
    # ключом «blk_…», а его строки — таблица с ключом «blk_…:table». Общий у
    # них только идентификатор блока, и именно он держит состав вместе.
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for _fragment_id, (side, fragment) in fragments.items():
        block_id = str(
            fragment.get("source_block_id")
            or str(fragment.get("source_group") or "").split(":", 1)[0]
        )
        if block_id:
            grouped[(side, block_id)].append(fragment)
    output: dict[str, dict[str, str]] = {}
    for _key, values in grouped.items():
        ordered = sorted(values, key=lambda value: (
            int(value.get("pdf_page") or 0),
            int(value.get("order") or 0),
            str(value.get("id") or ""),
        ))
        rows = [
            [canonicalize_text(str(cell)) for cell in value.get("location_parts") or []]
            for value in ordered
            if value.get("source_kind") == "table_row"
        ]
        populated = [row for row in rows if row]
        thickness_rows = [row for row in populated if _assembly_thickness_row(row)]
        if (
            len(thickness_rows) < _ASSEMBLY_MIN_THICKNESS_ROWS
            or len(thickness_rows) < _ASSEMBLY_MIN_THICKNESS_SHARE * len(populated)
        ):
            continue
        # Режем блок на составы по собственным заголовкам документа.
        segments: list[tuple[str | None, list[tuple[str, str]]]] = [(None, [])]
        for value in ordered:
            heading = _assembly_heading(value)
            if heading is not None:
                segments.append((heading, []))
                continue
            if value.get("source_kind") != "table_row":
                continue
            parts = [
                canonicalize_text(str(cell))
                for cell in value.get("location_parts") or []
            ]
            thickness = _assembly_thickness_row(parts)
            material = thickness[0] if thickness else _assembly_material_only_row(parts)
            if material is None:
                continue
            segments[-1][1].append((str(value["id"]), material))
        for assembly, members in segments:
            if assembly is None:
                # Строки до первого заголовка не принадлежат доказанному
                # составу: назвать их объект нечем.
                continue
            counts = Counter(material for _fragment_id, material in members)
            for fragment_id, material in members:
                if counts[material] != 1:
                    continue
                output[fragment_id] = {"assembly": assembly, "material": material}
    return output


def _assembly_row_properties(
    fragment: Mapping[str, Any],
    *,
    assembly_rows: Mapping[str, Mapping[str, str]],
) -> dict[str, Any] | None:
    record = assembly_rows.get(str(fragment.get("id") or ""))
    if record is None:
        return None
    parts = [canonicalize_text(str(cell)) for cell in fragment.get("location_parts") or []]
    material = record["material"]
    facet_key = content_signature(material)[:12]
    thickness = _assembly_thickness_row(parts)
    if thickness is not None:
        values = {
            f"assembly_layer_thickness_mm_{facet_key}": {
                "dimension": "PARAMETER",
                "value": _normalized_value(thickness[1], "мм"),
                "raw_value": parts[1],
                "unit": "мм",
                "cell_index": 1,
            }
        }
    else:
        # Слой без толщины: доказан сам факт его присутствия в составе.
        values = {
            f"assembly_layer_{facet_key}": {
                "dimension": "STRUCTURE",
                "value": _normalized_value(material),
                "raw_value": parts[0],
                "unit": None,
                "cell_index": 0,
            }
        }
    return {
        "subject_original": record["assembly"],
        "subject_canonical": canonical_entity_name(record["assembly"]),
        # Объект — сам состав, а слой живёт в facet_ref.  Класть материал в
        # контекст нельзя: тогда каждый слой станет отдельным объектом проекта
        # и «Кровля К3» распадётся на четырнадцать сущностей.
        "context": None,
        "values": values,
        "rule": "recognized_assembly_layer_table",
        "explicit_project_entity": True,
    }


def _strict_expression_properties(text: str) -> dict[str, Any] | None:
    """Parse only when every semicolon-delimited segment is a complete fact."""
    body = " ".join(str(text or "").strip().split())
    designation = _extract_designation(body)
    leading = _LEADING_DESIGNATION_RE.fullmatch(body)
    if leading:
        body = leading.group("body")
        designation = _extract_designation(leading.group("entity"))
    segments = [segment.strip(" .") for segment in re.split(r"\s*;\s*", body) if segment.strip(" .")]
    if not segments:
        return None
    values: dict[str, dict[str, Any]] = {}
    for index, segment in enumerate(segments):
        parsed: tuple[str, str, str] | None = None
        for facet, dimension, pattern in _LABEL_PATTERNS:
            match = pattern.fullmatch(segment)
            if match:
                parsed = (facet, dimension, match.group("value"))
                break
        if parsed is None:
            match = _KEY_VALUE_RE.fullmatch(segment)
            if match:
                parsed = (
                    _facet_from_label(match.group("label")),
                    "PARAMETER",
                    match.group("value"),
                )
        if parsed is None:
            return None
        facet, dimension, raw_value = parsed
        if facet in values:
            # Two properties with the same label in one source unit have no
            # deterministic identity without a stronger table/schema anchor.
            return None
        values[facet] = {
            "dimension": dimension,
            "value": _normalized_value(raw_value),
            "raw_value": raw_value,
            "unit": None,
            "cell_index": index,
        }
    original, canonical = designation or (None, None)
    return {
        "subject_original": original,
        "subject_canonical": canonical,
        "context": None,
        "values": values,
        "rule": "complete_labeled_value_expression",
        "explicit_project_entity": designation is not None,
    }


def _supply_property(text: str) -> dict[str, Any] | None:
    match = _SUPPLY_RE.fullmatch(" ".join(str(text or "").split()))
    if not match:
        return None
    original = " ".join(match.group("subject").split())
    canonical = canonical_entity_name(original)
    source = " ".join(match.group("source").split())
    if not canonical or not source:
        return None
    return {
        "subject_original": original,
        "subject_canonical": canonical,
        "context": None,
        "values": {
            "supply_source": {
                "dimension": "CONNECTION",
                "value": source,
                "raw_value": source,
                "unit": None,
                "cell_index": 0,
            }
        },
        "rule": "explicit_supply_from_expression",
        "explicit_project_entity": True,
    }


def _fragment_properties(
    fragment: Mapping[str, Any],
    *,
    side: str,
    valid_tables: set[tuple[str, str]],
    room_schedule_widths: Mapping[tuple[str, str], int],
    context_by_fragment: Mapping[str, str | None],
    assembly_rows: Mapping[str, Mapping[str, str]],
) -> dict[str, Any] | None:
    return (
        _table_row_properties(
            fragment,
            side=side,
            valid_tables=valid_tables,
            context_by_fragment=context_by_fragment,
        )
        or _room_row_properties(
            fragment,
            side=side,
            room_schedule_widths=room_schedule_widths,
            context_by_fragment=context_by_fragment,
        )
        or _assembly_row_properties(fragment, assembly_rows=assembly_rows)
        or _supply_property(str(fragment.get("text") or ""))
        or _strict_expression_properties(str(fragment.get("text") or ""))
    )


def _metadata_reason(
    fragment: Mapping[str, Any],
    known_reasons: Mapping[str, str],
) -> str | None:
    fragment_id = str(fragment.get("id") or "")
    if fragment_id in known_reasons:
        # An explicit supply expression is a connection fact, not just a
        # section label.  Its parser gets first refusal in the caller.
        if _supply_property(str(fragment.get("text") or "")) is not None:
            return None
        return known_reasons[fragment_id]
    source_kind = str(fragment.get("source_kind") or "")
    text = canonicalize_text(str(fragment.get("text") or ""))
    if source_kind == "heading":
        return "heading_not_a_fact"
    if text in {"примечание:", "примечания:", "notes:", "note:"}:
        return "notes_heading_not_a_fact"
    return None


def _referenced_fragments(
    item: Mapping[str, Any],
    fragments: Mapping[str, tuple[str, dict[str, Any]]],
) -> dict[str, list[dict[str, Any]]]:
    output = {"left": [], "right": []}
    for side, key in (("left", "left_fragment_ids"), ("right", "right_fragment_ids")):
        for value in item.get(key) or []:
            fragment_id = str(value)
            located = fragments.get(fragment_id)
            if located is None:
                raise ValueError("Stage 3 references a fragment outside its preparation")
            actual_side, fragment = located
            if actual_side != side:
                raise ValueError("Stage 3 fragment side does not match preparation")
            output[side].append(fragment)
    return output


def _preparation_groups(
    preparation: Mapping[str, Any],
) -> dict[str, dict[str, Any]]:
    output: dict[str, dict[str, Any]] = {}
    for value in preparation.get("comparison_groups") or []:
        if not isinstance(value, Mapping):
            raise ValueError("text preparation comparison group must be an object")
        group = dict(value)
        group_id = str(group.get("id") or "")
        if not group_id or group_id in output:
            raise ValueError("text preparation comparison group id invalid")
        output[group_id] = group
    return output


def _bind_preparation_group(
    stage3_group: Mapping[str, Any],
    prepared_groups: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    group_id = str(stage3_group.get("id") or "")
    prepared = prepared_groups.get(group_id)
    if prepared is None:
        raise ValueError("Stage 3 group is absent from its text preparation")
    for key in ("left_pages", "right_pages"):
        actual = sorted(int(value) for value in stage3_group.get(key) or [])
        expected = sorted(int(value) for value in prepared.get(key) or [])
        if actual != expected:
            raise ValueError("Stage 3 group pages differ from text preparation")
    stage3_type = str(stage3_group.get("relation_type") or "")
    prepared_type = str(prepared.get("relation_type") or "")
    if stage3_type and prepared_type and stage3_type != prepared_type:
        raise ValueError("Stage 3 relation type differs from text preparation")
    stage3_status = stage3_group.get("relation_status")
    prepared_status = prepared.get("relation_status", prepared.get("status"))
    if stage3_status and prepared_status and stage3_status != prepared_status:
        raise ValueError("Stage 3 relation status differs from text preparation")
    return {
        **dict(stage3_group),
        "relation_type": stage3_type or prepared_type,
        # Old Stage 3 artifacts could lose this field during an idempotent
        # normalization pass.  The signed Stage 2 preparation is the honest
        # upstream source and may restore a missing value, never overwrite a
        # contradictory one.
        "relation_status": stage3_status or prepared_status,
    }


def _structured_group_coverage(
    group: Mapping[str, Any],
    fragments: Mapping[str, tuple[str, dict[str, Any]]],
) -> dict[str, int]:
    pages = {
        "left": {int(value) for value in group.get("left_pages") or []},
        "right": {int(value) for value in group.get("right_pages") or []},
    }
    return {
        side.upper(): sum(
            1
            for actual_side, fragment in fragments.values()
            if actual_side == side
            and int(fragment.get("pdf_page") or 0) in pages[side]
        )
        for side in ("left", "right")
    }


def _single_side_properties(
    values: list[dict[str, Any]],
    *,
    side: str,
    valid_tables: set[tuple[str, str]],
    room_schedule_widths: Mapping[tuple[str, str], int],
    context_by_fragment: Mapping[str, str | None],
    assembly_rows: Mapping[str, Mapping[str, str]],
) -> dict[str, Any] | None:
    if len(values) != 1:
        return None
    return _fragment_properties(
        values[0],
        side=side,
        valid_tables=valid_tables,
        room_schedule_widths=room_schedule_widths,
        context_by_fragment=context_by_fragment,
        assembly_rows=assembly_rows,
    )


def _property_pairs(
    bucket: str,
    left: dict[str, Any] | None,
    right: dict[str, Any] | None,
) -> tuple[dict[str, Any] | None, list[dict[str, Any]]]:
    if bucket == "added":
        if right is None:
            return None, []
        return right, [
            {"facet": facet, "before": None, "after": value}
            for facet, value in sorted(right["values"].items())
        ]
    if bucket == "removed":
        if left is None:
            return None, []
        return left, [
            {"facet": facet, "before": value, "after": None}
            for facet, value in sorted(left["values"].items())
        ]
    if left is None or right is None:
        return None, []
    if (
        left.get("subject_canonical") != right.get("subject_canonical")
        or left.get("context") != right.get("context")
        or left.get("rule") != right.get("rule")
    ):
        return None, []
    if set(left["values"]) != set(right["values"]):
        return None, []
    pairs = []
    for facet in sorted(left["values"]):
        before, after = left["values"][facet], right["values"][facet]
        if before["value"] != after["value"]:
            pairs.append({"facet": facet, "before": before, "after": after})
    return right, pairs


def _structurally_identical(
    bucket: str,
    left: dict[str, Any] | None,
    right: dict[str, Any] | None,
) -> bool:
    """True когда обе стороны разобраны и структурно совпали до последнего поля."""
    if bucket != "changed" or left is None or right is None:
        return False
    descriptor, pairs = _property_pairs(bucket, left, right)
    return descriptor is not None and not pairs


def _facts_for_evidence(
    *,
    pair_id: Any,
    source_ref: str,
    group: Mapping[str, Any],
    bucket: str,
    item: Mapping[str, Any],
    left: dict[str, Any] | None,
    right: dict[str, Any] | None,
    structured_coverage: Mapping[str, int],
) -> list[dict[str, Any]]:
    descriptor, pairs = _property_pairs(bucket, left, right)
    if descriptor is None or not pairs:
        return []
    subject_ref, project_ref, entity = _entity_refs(
        pair_id=pair_id,
        group=group,
        original=descriptor.get("subject_original"),
        canonical=descriptor.get("subject_canonical"),
        explicit_project_entity=bool(descriptor.get("explicit_project_entity")),
        context=descriptor.get("context"),
    )
    opposite_side = "LEFT" if bucket == "added" else (
        "RIGHT" if bucket == "removed" else None
    )
    item_provenance = item.get("provenance")
    item_provenance = (
        item_provenance if isinstance(item_provenance, Mapping) else {}
    )
    explicit_coverage = item_provenance.get("structured_coverage")
    explicit_coverage = (
        explicit_coverage if isinstance(explicit_coverage, Mapping) else {}
    )
    explicit_opposite = explicit_coverage.get(opposite_side) if opposite_side else None
    explicitly_incomplete = bool(
        opposite_side
        and (
            item_provenance.get("opposite_side_structured_coverage_complete")
            is False
            or (
                isinstance(explicit_opposite, Mapping)
                and (
                    explicit_opposite.get("complete") is False
                    or str(explicit_opposite.get("status") or "").upper()
                    == "INCOMPLETE"
                )
            )
        )
    )
    opposite_coverage_incomplete = bool(
        opposite_side
        and (
            int(structured_coverage.get(opposite_side) or 0) == 0
            or explicitly_incomplete
        )
    )
    outcome, confidence, review_requirement = _relation_assessment(
        group,
        opposite_coverage_incomplete=opposite_coverage_incomplete,
    )
    output = []
    for pair in pairs:
        before = pair["before"]
        after = pair["after"]
        template = after or before
        dimension = str(template["dimension"])
        before_value = before["value"] if before else None
        after_value = after["value"] if after else None
        fact_identity = {
            "source_evidence_ref": source_ref,
            "facet_ref": pair["facet"],
            "subject_ref": subject_ref,
            "dimension": dimension,
        }
        output.append({
            "fact_id": stable_id("tfact_", fact_identity),
            "source_evidence_ref": source_ref,
            "scope_ref": _scope_ref(group),
            "subject_ref": subject_ref,
            "project_entity_ref": project_ref,
            "facet_ref": pair["facet"],
            "dimension": dimension,
            "direction": _direction(bucket, before_value, after_value, dimension),
            "outcome": outcome,
            "confidence": confidence,
            "before_value": before_value,
            "after_value": after_value,
            "provenance": {
                "producer": PRODUCER_VERSION,
                "parser_rule": descriptor["rule"],
                "source_fragment_ids": {
                    "LEFT": sorted(str(value) for value in item.get("left_fragment_ids") or []),
                    "RIGHT": sorted(str(value) for value in item.get("right_fragment_ids") or []),
                },
                "source_anchors": _anchors(item),
                "entity": entity,
                "property": {
                    "facet_ref": pair["facet"],
                    "before_cell_index": before.get("cell_index") if before else None,
                    "after_cell_index": after.get("cell_index") if after else None,
                    "unit": template.get("unit"),
                },
                "relation": {
                    "group_id": group.get("id"),
                    "type": group.get("relation_type"),
                    "status": group.get("relation_status"),
                },
                "review_requirement": review_requirement,
                "structured_coverage": {
                    "LEFT": int(structured_coverage.get("LEFT") or 0),
                    "RIGHT": int(structured_coverage.get("RIGHT") or 0),
                    "required_opposite_side": opposite_side,
                    "opposite_side_complete": not opposite_coverage_incomplete,
                },
                "uses_model": False,
            },
        })
    return output


def produce_text_facts(
    text_differences: Mapping[str, Any],
    text_preparation: Mapping[str, Any],
    *,
    generated_at: str | None = None,
) -> dict[str, Any]:
    """Return versioned structured facts and explicit non-factual evidence."""
    if (
        text_preparation.get("kind") != PREPARATION_KIND
        or text_preparation.get("schema_version") != PREPARATION_SCHEMA_VERSION
    ):
        raise ValueError("production text preparation artifact required")
    if text_differences.get("pair_id") != text_preparation.get("pair_id"):
        raise ValueError("text preparation belongs to another pair")

    fragments = _fragments_by_id(text_preparation)
    prepared_groups = _preparation_groups(text_preparation)
    (
        valid_tables,
        room_schedule_widths,
        contexts,
        known_metadata,
        assembly_rows,
    ) = _table_contexts(fragments)
    facts: list[dict[str, Any]] = []
    not_applicable: list[dict[str, Any]] = []
    rule_counts: Counter[str] = Counter()
    coverage_by_group: dict[str, dict[str, int]] = {}
    all_source_refs: list[str] = []

    for source_ref, group, bucket, item in iter_stage3_evidence(text_differences):
        all_source_refs.append(source_ref)
        bound_group = _bind_preparation_group(group, prepared_groups)
        group_id = str(bound_group.get("id") or "")
        structured_coverage = coverage_by_group.setdefault(
            group_id,
            _structured_group_coverage(bound_group, fragments),
        )
        referenced = _referenced_fragments(item, fragments)
        left = _single_side_properties(
            referenced["left"], side="left",
            valid_tables=valid_tables, room_schedule_widths=room_schedule_widths,
            context_by_fragment=contexts, assembly_rows=assembly_rows,
        )
        right = _single_side_properties(
            referenced["right"], side="right",
            valid_tables=valid_tables, room_schedule_widths=room_schedule_widths,
            context_by_fragment=contexts, assembly_rows=assembly_rows,
        )
        produced = _facts_for_evidence(
            pair_id=text_differences.get("pair_id"),
            source_ref=source_ref,
            group=bound_group,
            bucket=bucket,
            item=item,
            left=left,
            right=right,
            structured_coverage=structured_coverage,
        )
        if produced:
            facts.extend(produced)
            rule_counts.update(str(fact["provenance"]["parser_rule"]) for fact in produced)
            continue

        referenced_all = [*referenced["left"], *referenced["right"]]
        if _structurally_identical(bucket, left, right):
            # Обе стороны разобраны одним и тем же правилом, объект тот же, и
            # ни одно значение не разошлось: «185,03» против «185,03 м2» — это
            # запись единицы, а не изменение проекта.  Такое расхождение
            # Stage 3 нашёл честно, но фактом оно не является.
            not_applicable.append({
                "source_evidence_ref": source_ref,
                "reason_code": "structured_values_identical",
                "provenance": {
                    "producer": PRODUCER_VERSION,
                    "reason_codes": ["structured_values_identical"],
                    "parser_rule": str(left["rule"]),
                    "source_fragment_ids": sorted(
                        str(fragment.get("id") or "") for fragment in referenced_all
                    ),
                    "uses_model": False,
                },
            })
            continue

        reasons = [
            _metadata_reason(fragment, known_metadata)
            for fragment in referenced_all
        ]
        if referenced_all and all(reasons):
            reason_codes = sorted({str(reason) for reason in reasons if reason})
            not_applicable.append({
                "source_evidence_ref": source_ref,
                "reason_code": (
                    reason_codes[0]
                    if len(reason_codes) == 1
                    else "non_factual_document_structure"
                ),
                "provenance": {
                    "producer": PRODUCER_VERSION,
                    "reason_codes": reason_codes,
                    "source_fragment_ids": sorted(
                        str(fragment.get("id") or "") for fragment in referenced_all
                    ),
                    "uses_model": False,
                },
            })

    facts.sort(key=lambda value: value["fact_id"])
    not_applicable.sort(key=lambda value: value["source_evidence_ref"])
    fact_ids = [str(value["fact_id"]) for value in facts]
    if len(fact_ids) != len(set(fact_ids)):
        raise ValueError("Text Fact Producer produced duplicate fact_id")
    fact_source_refs = {str(value["source_evidence_ref"]) for value in facts}
    not_applicable_refs = {
        str(value["source_evidence_ref"]) for value in not_applicable
    }
    if fact_source_refs & not_applicable_refs:
        raise ValueError("source evidence cannot be both factual and not applicable")
    unresolved = sorted(set(all_source_refs) - fact_source_refs - not_applicable_refs)
    automatic = sum(value["outcome"] != "REVIEW_REQUIRED" for value in facts)
    review_required = len(facts) - automatic
    sheet_relation_blocked = sum(
        "sheet_relation_unconfirmed"
        in value["provenance"]["review_requirement"]["reason_codes"]
        for value in facts
    )
    coverage_blocked = sum(
        "opposite_side_structured_coverage_incomplete"
        in value["provenance"]["review_requirement"]["reason_codes"]
        for value in facts
    )
    stage3_signature = stage3_content_signature(text_differences)
    signature_payload = {
        "producer": PRODUCER_VERSION,
        "preparation_signature": text_preparation.get("input_signature"),
        "stage3_signature": stage3_signature,
        "facts": facts,
        "not_applicable_source_evidence": not_applicable,
    }
    input_signature = content_signature(signature_payload)
    return {
        "kind": KIND,
        "schema_version": SCHEMA_VERSION,
        "version": 1,
        "pair_id": text_differences.get("pair_id"),
        "generated_at": generated_at or utc_now(),
        "input_signature": input_signature,
        "preparation_signature": text_preparation.get("input_signature"),
        "stage3_signature": stage3_signature,
        "facts": facts,
        "not_applicable_source_evidence": not_applicable,
        "unresolved_source_evidence": unresolved,
        "diagnostics": {
            "stage3_evidence": len(all_source_refs),
            "facts": len(facts),
            "automatic_facts": automatic,
            "review_required_facts": review_required,
            "sheet_relation_blocked_facts": sheet_relation_blocked,
            "opposite_coverage_blocked_facts": coverage_blocked,
            "not_applicable_source_evidence": len(not_applicable),
            "unresolved_source_evidence": len(unresolved),
            "facts_by_rule": dict(sorted(rule_counts.items())),
            "recognized_electrical_tables": len(valid_tables),
            "recognized_room_schedule_tables": len(room_schedule_widths),
            "structured_fragment_coverage_by_group": dict(
                sorted(coverage_by_group.items())
            ),
            "one_property_per_fact": True,
            "uses_model": False,
        },
        "provenance": {
            "producer": PRODUCER_VERSION,
            "uses_model": False,
            "precision_first": True,
            "ambiguous_scopes_fail_closed": True,
        },
    }


__all__ = [
    "KIND",
    "PRODUCER_VERSION",
    "SCHEMA_VERSION",
    "produce_text_facts",
]
