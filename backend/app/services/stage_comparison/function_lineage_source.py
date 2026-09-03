"""Deterministic, provenance-ready source facts for Function Lineage.

The extractor consumes Markdown that already exists for a document version.
It does not run OCR or a model and it never returns the raw page body.  Its
output is deliberately compact so it can travel beside the production sheet
index without becoming an input to Sheet Matcher v3 (which ignores the
``function_lineage_source`` field).
"""
from __future__ import annotations

import re
from typing import Any, Iterable, Mapping, Sequence

from .production_artifacts import content_signature


SCHEMA_VERSION = "function-lineage-source.v1"
MAX_FACTS_PER_FIELD = 18

_PAGE_RE = re.compile(r"(?m)^##\s+Page\s+(\d+)\s*$")
_PAGE_META_RE = re.compile(r"^> \*\*(?:Created|Crop):\*\*.*$", re.MULTILINE)
_STAMP_RE = re.compile(r"^> \*\*Stamp:\*\* (.+)$", re.MULTILINE)
_FIELD_RE = re.compile(r"^\*\*([^*:\n]+):\*\*\s*(.+)$", re.MULTILINE)
_IMAGE_RE = re.compile(r"^\*\*\[IMAGE\]\*\*\s*\|\s*(.+)$", re.MULTILINE)
_TOKEN_RE = re.compile(r"[a-zа-я0-9]+(?:[./_-][a-zа-я0-9]+)*", re.IGNORECASE)
_SYSTEM_RE = re.compile(
    r"(?<![a-zа-я0-9])(?:\d{0,2}[авктэщб][a-zа-я0-9]*(?:[.\-/]\d+[a-zа-я0-9]*)*|"
    r"вр[ущ][a-zа-я0-9.\-/]*|грщ[a-zа-я0-9.\-/]*|щ[а-яa-z0-9.\-/]{1,12})",
    re.IGNORECASE,
)
_OBJECT_RE = re.compile(
    r"\b(?:корпус|секци(?:я|и))\s*[№#]?\s*\d+(?:[.,]\d+)?\b",
    re.IGNORECASE,
)
_CORPUS_RE = re.compile(r"\bкорпус\s*[№#]?\s*\d+(?:[.,]\d+)?\b", re.IGNORECASE)
_SECTION_RE = re.compile(r"\bсекци(?:я|и)\s*[№#]?\s*\d+(?:[.,]\d+)?\b", re.IGNORECASE)
_ZONE_RE = re.compile(
    r"\b(?:зона\s*[№#]?\s*\d+(?:[.,]\d+)?|\d+(?:[.,]\d+)?\s+зона)\b",
    re.IGNORECASE,
)
_FLOOR_RE = re.compile(
    r"(?:этаж\w*\s*)?[+\-]?\d+[,.]\d{3}|\b[-−]?\d{1,2}\s*(?:этаж\w*|эт[.])",
    re.IGNORECASE,
)
_CROSS_SHEET_RE = re.compile(
    r"\b(?:см\.|лист(?:е|у|ом|а)?\s*№?\s*\d+|раздел\s+[а-яa-z0-9.\-/]+|"
    r"продолжение|начало|окончание)\b",
    re.IGNORECASE,
)

_FUNCTION_RULES: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("ELECTRICAL_DISTRIBUTION", ("вру", "грщ", "распределительн", "однолинейн", "электроснабжен")),
    ("LOAD_CALCULATION", ("расчет нагруз", "расчёт нагруз", "потребная мощность", "расчетный ток")),
    ("LIGHTING", ("освещен", "светильник", "оздс", "lighting")),
    ("GROUNDING_LIGHTNING", ("заземлен", "молниезащит", "уравнивани")),
    ("WATER_DRAINAGE", ("водоотведен", "канализац", "водосток", "сточн", "дренаж")),
    ("WATER_SUPPLY", ("водоснабжен", "водопровод", "холодн", "в1", "в1.1", "в1.2")),
    ("HOT_WATER", ("горяч", "т3", "т4")),
    ("FIRE_WATER", ("пожар", "впв", "в2.1", "в2.2", "апт")),
    ("RISER_DISTRIBUTION", ("стояк", "квартир", "этаж")),
    ("PUMPING_PRESSURE", ("насос", "повышен", "напор", "booster")),
    ("METERING", ("водомер", "счетчик", "счётчик", "узел учета", "узел учёта")),
    ("DOMESTIC_PRESSURE_BOOST", ("насосная хвс", "хозяйственно-питьевого водоснабжения", "domestic booster")),
    ("FIRE_PRESSURE_BOOST", ("насосная хвс и впв", "насосная впв", "установка пожаротушения", "fire booster")),
    ("INCOMING_METERING", ("водомерный узел", "водомерного узла", "общедомовой водомер", "ввод в1")),
)

_EQUIPMENT_MARKERS = (
    "насос", "счетчик", "счётчик", "водомер", "автомат", "клапан", "кран",
    "щит", "вру", "грщ", "трансформатор", "фильтр", "установка", "бак",
)
_CONSUMER_MARKERS = (
    "потребител", "квартир", "помещен", "пожарн", "освещен", "нагруз",
    "венткамер", "автостоян",
)
_UPSTREAM_MARKERS = ("источник", "ввод", "от ", "питани", "исходн", "вход")
_DOWNSTREAM_MARKERS = ("далее", "к ", "потребител", "отходит", "подает", "подаёт", "выход")


def _clean(value: Any) -> str:
    return " ".join(str(value or "").replace("ё", "е").casefold().split())


def _unique(values: Iterable[Any], *, limit: int = MAX_FACTS_PER_FIELD) -> list[str]:
    output: list[str] = []
    seen: set[str] = set()
    for raw in values:
        value = " ".join(str(raw or "").split())
        key = _clean(value)
        if value and key not in seen:
            output.append(value)
            seen.add(key)
        if len(output) >= limit:
            break
    return output


def _pipe_fields(value: str) -> dict[str, str]:
    fields: dict[str, str] = {}
    for part in value.split("|"):
        key, separator, content = part.partition(":")
        if separator and key.strip():
            fields[key.strip()] = " ".join(content.split())
    return fields


def _field_values(body: str) -> dict[str, list[str]]:
    values: dict[str, list[str]] = {}
    for match in _FIELD_RE.finditer(body):
        values.setdefault(match.group(1).strip(), []).append(match.group(2).strip())
    for match in _IMAGE_RE.finditer(body):
        for key, value in _pipe_fields(match.group(1)).items():
            values.setdefault(key, []).append(value)
    return values


def _sentences(body: str, needles: Sequence[str], *, limit: int) -> list[str]:
    pieces = re.split(r"(?<=[.!?])\s+|\n+", body)
    normalized_needles = tuple(_clean(value) for value in needles)
    return _unique(
        (
            piece.strip(" -*|#")
            for piece in pieces
            if any(value in _clean(piece) for value in normalized_needles)
        ),
        limit=limit,
    )


def _function_classes(text: str) -> list[str]:
    normalized = _clean(text)
    tokens = set(_TOKEN_RE.findall(normalized))
    classes = [
        name
        for name, needles in _FUNCTION_RULES
        if any((needle in tokens if len(needle) <= 3 else needle in normalized) for needle in needles)
    ]
    if "насос" in normalized and any(value in normalized for value in ("пожар", "впв", "в2.1", "в2.2")):
        classes.append("FIRE_PRESSURE_BOOST")
    if "насос" in normalized and any(value in normalized for value in ("хозяйственно", "хвс", "domestic")):
        classes.append("DOMESTIC_PRESSURE_BOOST")
    if "водомер" in normalized and any(value in normalized for value in ("ввод", "входн", "двумя вводами", "общедомов")):
        classes.append("INCOMING_METERING")
    return list(dict.fromkeys(classes)) or ["GENERAL_DOCUMENT_FUNCTION"]


def _document_role(body: str, stamp: Mapping[str, str], fields: Mapping[str, Sequence[str]]) -> str:
    text = _clean(" ".join([stamp.get("Name", ""), *fields.get("Type", []), body[:1800]]))
    code = _clean(stamp.get("Code", ""))
    if "содержан" in text or "contents" in text:
        return "TOC"
    if (
        "изменен" in text
        or "change register" in text
        or (code.endswith(".то") and _clean(stamp.get("Name")) == "лист")
    ):
        return "CHANGE_REGISTER"
    if (
        fields.get("Summary")
        or fields.get("Description")
        or fields.get("Entities")
        or fields.get("Type")
        or _IMAGE_RE.search(body)
    ):
        return "GRAPHIC_SHEET"
    if any(value in text for value in ("спецификац", "ведомост", "таблиц", "schedule")):
        return "TABLE"
    return "OTHER"


def _stable_entities(entities: Sequence[str]) -> list[str]:
    return _unique(
        value
        for value in entities
        if any(character.isdigit() for character in value)
        or any(marker in _clean(value) for marker in _EQUIPMENT_MARKERS)
    )


def _page_source(page: int, body: str) -> dict[str, Any]:
    clean_body = _PAGE_META_RE.sub("", body)
    stamp_match = _STAMP_RE.search(clean_body)
    stamp = _pipe_fields(stamp_match.group(1)) if stamp_match else {}
    fields = _field_values(clean_body)

    def values(*names: str) -> list[str]:
        return _unique(value for name in names for value in fields.get(name, []))

    summaries = values("Summary", "Purpose", "Function")
    descriptions = values("Description")
    entity_items = _unique(
        item.strip()
        for value in values("Entities", "Equipment")
        for item in re.split(r"[,;]", value)
    )
    function_text = " ".join([stamp.get("Name", ""), *summaries, *descriptions])
    evidence_text = " ".join([function_text, *entity_items])
    functions = []
    for function_class in _function_classes(function_text or clean_body):
        needles = next((items for name, items in _FUNCTION_RULES if name == function_class), ())
        snippets = _sentences(clean_body, needles, limit=8) if needles else []
        if not snippets:
            snippets = _unique([*summaries, stamp.get("Name")], limit=4)
        functions.append({
            "function_class": function_class,
            "fragment_text": [value[:480] for value in snippets],
        })

    objects = _unique([*values("Object"), *_OBJECT_RE.findall(evidence_text)])
    corpora = _unique(value for value in objects if _CORPUS_RE.search(value))
    sections = _unique(value for value in objects if _SECTION_RE.search(value))
    zones = _unique([*values("Zone"), *_ZONE_RE.findall(evidence_text)])
    floors = _unique([*values("Level"), *_FLOOR_RE.findall(evidence_text)])
    systems = _unique(_SYSTEM_RE.findall(evidence_text))
    equipment = _unique([
        *(value for value in entity_items if any(marker in _clean(value) for marker in _EQUIPMENT_MARKERS)),
        *_sentences(" ".join([*summaries, *descriptions]), _EQUIPMENT_MARKERS, limit=8),
    ])
    consumers = _unique([
        *(value for value in entity_items if any(marker in _clean(value) for marker in _CONSUMER_MARKERS)),
        *_sentences(" ".join([*summaries, *descriptions]), _CONSUMER_MARKERS, limit=8),
    ])
    upstream = _sentences(" ".join([*summaries, *descriptions]), _UPSTREAM_MARKERS, limit=8)
    downstream = _sentences(" ".join([*summaries, *descriptions]), _DOWNSTREAM_MARKERS, limit=8)
    cross_refs = _unique(
        value
        for value in re.split(r"(?<=[.!?])\s+|\n+", " ".join([*summaries, *descriptions]))
        if _CROSS_SHEET_RE.search(value)
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "physical_page": page,
        "graphic_sheet_number": stamp.get("Sheet") or None,
        "title": stamp.get("Name") or (summaries[0] if summaries else None),
        "document_role": _document_role(clean_body, stamp, fields),
        "serviced_object": objects or None,
        "building": corpora or None,
        "corpus": corpora or None,
        "section": sections or None,
        "zone": zones or None,
        "floors": floors or None,
        "systems": systems or None,
        "consumers": consumers or None,
        "equipment_roles": equipment or None,
        "upstream": upstream or None,
        "downstream": downstream or None,
        "stable_entities": _stable_entities(entity_items) or None,
        "cross_sheet_functional_references": cross_refs or None,
        "functions": functions,
        "source_content_signature": content_signature(clean_body),
    }


def extract_page_sources(markdown: str) -> dict[int, dict[str, Any]]:
    """Return compact deterministic facts keyed by one-based physical page."""
    matches = list(_PAGE_RE.finditer(markdown or ""))
    output: dict[int, dict[str, Any]] = {}
    for index, match in enumerate(matches):
        page = int(match.group(1))
        end = matches[index + 1].start() if index + 1 < len(matches) else len(markdown)
        output[page] = _page_source(page, markdown[match.end():end])
    return output


__all__ = ["SCHEMA_VERSION", "extract_page_sources"]
