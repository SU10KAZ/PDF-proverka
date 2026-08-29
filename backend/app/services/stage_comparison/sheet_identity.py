"""Deterministic sheet identity from the drawing stamp (штамп).

A drawing sheet identifies itself in the stamp title cell: ``Корпуса 1, 2.
План 3 этажа``.  That line is present in the text layer of both sides of a
comparison pair in directly comparable form, and it is the only signal that
states *which* sheet this is.  The PDF page number is not that signal: page 28
of the old set and page 28 of the new set are unrelated facts.

Extraction is geometry-anchored on purpose.  The same words appear in the
table of contents of the very same document, in a left-hand column, listing
every plan in the volume.  A regex over the whole page therefore reads the
contents page as if it were twenty different sheets.  Only the stamp cell —
the lower-right corner of the *displayed* page, after the page rotation is
applied — carries identity.

No model, no OCR, no network: PyMuPDF text blocks plus normalization.  A sheet
whose stamp cannot be parsed simply has no identity, and the sheet matcher
falls back to its content signals for that page.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field, replace
from typing import Any, Iterable, Mapping, Sequence

from .production_artifacts import content_signature, utc_now


KIND = "stage_comparison_sheet_identities"
SCHEMA_VERSION = "sheet-identity.v1"
EXTRACTOR_VERSION = "stamp-sheet-identity.v1"

#: Sheet kinds this extractor is able to *prove* from a stamp title.  A kind is
#: only ever emitted for a literal keyword; nothing is inferred from layout.
SHEET_KINDS = frozenset({
    "PLAN",
    "ROOF",
    "BASEMENT",
    "TECHNICAL_SPACE",
    "TYPICAL_PLAN",
    "SECTION",
    "FACADE",
})

#: Kinds whose stamp title carries no other discriminator, so the elevation is
#: the only thing that tells two such sheets apart.  «План технического
#: пространства на отм. -1,800» and «... на отм. -5,400» are two different
#: sheets of the same buildings: both are TECHNICAL_SPACE, neither has floors,
#: an underground ordinal or a section axis, and without the elevation their
#: keys were literally identical.  PLAN is told apart by its floors, BASEMENT
#: by its ordinal, SECTION/FACADE by their axis — adding an elevation there
#: would only split a pair whose stamps happen to spell the level on one side.
ELEVATION_IDENTIFYING_KINDS = frozenset({"TECHNICAL_SPACE"})

#: Stamp cell of the displayed page.  Every stamp title observed on real
#: production sheets sits at ``y0 >= 0.93``; the contents page lists its titles
#: at ``y0 <= 0.61``.  The bound is deliberately loose on x because narrow
#: sheets push the title cell leftwards, and tight on y because that is what
#: actually separates a stamp from body text.
STAMP_ZONE_MIN_Y0 = 0.85
STAMP_ZONE_MIN_X1 = 0.55

_ORDINALS = {
    "первого": 1,
    "второго": 2,
    "третьего": 3,
    "четвертого": 4,
    "пятого": 5,
}

# «Корпуса 1, 2.» / «Корпус 4.» / «Корпуса 3, 3.1.»
_BUILDINGS_RE = re.compile(r"корпус\w*\s*(?P<numbers>\d+(?:\.\d+)?(?:\s*[,и]\s*\d+(?:\.\d+)?)*)", re.I)
_BUILDING_NUMBER_RE = re.compile(r"\d+(?:\.\d+)?")

# The title itself.  ``план`` must be followed by whitespace so that prose such
# as «Типовой этаж представлен планом 3 этажа» in a sheet note never matches.
_TITLE_RE = re.compile(
    r"план\s+(?P<what>"
    r"кровли"
    r"|(?P<basement_ordinal>первого|второго|третьего|четвертого|пятого)\s+подземного\s+этажа"
    r"|технического\s+пространства"
    r"|типового\s+этажа"
    r"|(?P<floors>\d+(?:\s*[-–—]\s*\d+)?)\s*этаж\w*"
    r")",
    re.I,
)
_SECTION_RE = re.compile(r"разрез\s+(?P<axis>[\w\d\-–—'ʼ.]+)", re.I)
_FACADE_RE = re.compile(r"фасад\s+(?P<axis>[\w\d\-–—'ʼ./]+)", re.I)

# «на отм. -6,000» / «на отм.-9.600»
_ELEVATION_RE = re.compile(r"на\s+отм\.?\s*(?P<value>[-−+]?\d+[.,]\d+|[-−+]?\d+)", re.I)
# «Техническое пространство -1.800» — the same level written without the «отм.»
# prefix.  Deliberately narrow: an explicit sign plus exactly three decimals is
# the Russian elevation convention, and nothing else on a stamp is written that
# way, so a scale «М 1:200» or a date «12.25» can never be read as a level.
_BARE_ELEVATION_RE = re.compile(r"(?<![\d.,])(?P<value>[-−+]\d+[.,]\d{3})(?![\d])")
# «АА/БЭ-03-ДС3-АР1» / «АА/БЭ-03-ДC3 - АР1» — the same designation is typeset
# with both Cyrillic «С» and Latin «C», so homoglyphs are folded before use.
_DESIGNATION_RE = re.compile(
    r"[А-ЯA-Z]{2,3}\s*[/\\]\s*[А-ЯA-Z]{2,3}(?:\s*-\s*[А-ЯA-Z0-9.]+)+"
)
_HOMOGLYPHS = str.maketrans({
    "A": "А", "B": "В", "C": "С", "E": "Е", "H": "Н", "K": "К", "M": "М",
    "O": "О", "P": "Р", "T": "Т", "X": "Х",
})

_MAX_FLOOR_RANGE = 200


def _normalize_text(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).replace("ё", "е").replace("Ё", "Е").strip()


def _parse_buildings(text: str) -> tuple[tuple[str, ...], int | None]:
    """Return the building numbers named before the title and where they start."""
    numbers: list[str] = []
    start: int | None = None
    for match in _BUILDINGS_RE.finditer(text):
        if start is None:
            start = match.start()
        numbers.extend(_BUILDING_NUMBER_RE.findall(match.group("numbers")))
    unique = {value.rstrip(".") for value in numbers if value}
    ordered = tuple(
        sorted(unique, key=lambda value: [int(part) for part in value.split(".")])
    )
    return ordered, (start if ordered else None)


def _parse_floors(raw: str) -> tuple[tuple[str, ...], dict[str, int] | None]:
    """Expand «3-15 этажей» into an explicit floor set; keep the range too."""
    numbers = [int(value) for value in re.findall(r"\d+", raw)]
    if not numbers:
        return (), None
    if len(numbers) == 1:
        return (str(numbers[0]),), None
    start, end = numbers[0], numbers[1]
    if end < start or end - start > _MAX_FLOOR_RANGE:
        # A malformed or absurd range is not identity; refuse to invent one.
        return (), None
    return tuple(str(value) for value in range(start, end + 1)), {"from": start, "to": end}


def _parse_elevation(text: str) -> str | None:
    match = _ELEVATION_RE.search(text) or _BARE_ELEVATION_RE.search(text)
    if not match:
        return None
    raw = match.group("value").replace("−", "-").replace(",", ".")
    try:
        value = float(raw)
    except ValueError:
        return None
    # Both sides write the same level as «-6,000» and «-6.000»; canonicalize so
    # a decimal-separator convention never splits one sheet into two.  «+0,000»
    # and «-0,000» are the same level, so the sign of zero is dropped: leaving
    # it would make «-0» and «0» two different sheets.
    if value == 0:
        value = 0.0
    return f"{value:.3f}".rstrip("0").rstrip(".") or "0"


def _parse_designation(text: str) -> str | None:
    match = _DESIGNATION_RE.search(text)
    if not match:
        return None
    value = re.sub(r"\s+", "", match.group(0)).replace("\\", "/")
    return value.translate(_HOMOGLYPHS) or None


@dataclass(frozen=True)
class SheetIdentity:
    """What a stamp proves about one drawing sheet."""

    page: int
    sheet_kind: str
    buildings: tuple[str, ...] = ()
    floors: tuple[str, ...] = ()
    floor_range: tuple[int, int] | None = None
    basement_ordinal: int | None = None
    section_axis: str | None = None
    elevation: str | None = None
    sheet_designation: str | None = None
    raw_stamp_text: str = ""
    confidence: str = "HIGH"
    evidence: Mapping[str, Any] = field(default_factory=dict)

    @property
    def stamp_key(self) -> str:
        """Canonical exact-match key.  Equal keys mean the same sheet."""
        parts = [
            "B=" + ",".join(self.buildings),
            "K=" + self.sheet_kind,
            "F=" + ",".join(self.floors),
        ]
        if self.basement_ordinal is not None:
            parts.append(f"L={self.basement_ordinal}")
        if self.section_axis:
            parts.append("A=" + self.section_axis)
        if self.sheet_kind in ELEVATION_IDENTIFYING_KINDS:
            # For these kinds the level IS the identity.  An unnamed level is
            # its own value, not a wildcard: «Техническое пространство» without
            # an elevation cannot be declared the same sheet as the one at
            # -1,800 — that claim belongs to the engineer, not to this key.
            parts.append("E=" + (self.elevation or ""))
        return "|".join(parts)

    @property
    def floor_set(self) -> frozenset[str]:
        return frozenset(self.floors)

    def matches(self, other: "SheetIdentity") -> bool:
        return self.stamp_key == other.stamp_key

    def to_dict(self) -> dict[str, Any]:
        return {
            "page": self.page,
            "stamp_key": self.stamp_key,
            "sheet_kind": self.sheet_kind,
            "buildings": list(self.buildings),
            "floors": list(self.floors),
            "floor_range": (
                {"from": self.floor_range[0], "to": self.floor_range[1]}
                if self.floor_range
                else None
            ),
            "basement_ordinal": self.basement_ordinal,
            "section_axis": self.section_axis,
            "elevation": self.elevation,
            "sheet_designation": self.sheet_designation,
            "raw_stamp_text": self.raw_stamp_text,
            "confidence": self.confidence,
            "evidence": dict(self.evidence),
            "extractor": EXTRACTOR_VERSION,
        }


def identity_from_dict(value: Mapping[str, Any] | None) -> SheetIdentity | None:
    """Rebuild an identity from a persisted artifact without re-reading PDFs."""
    if not isinstance(value, Mapping):
        return None
    kind = str(value.get("sheet_kind") or "").upper()
    page = value.get("page")
    if kind not in SHEET_KINDS or not isinstance(page, int) or isinstance(page, bool):
        return None
    raw_range = value.get("floor_range")
    floor_range = None
    if isinstance(raw_range, Mapping):
        start, end = raw_range.get("from"), raw_range.get("to")
        if isinstance(start, int) and isinstance(end, int):
            floor_range = (start, end)
    ordinal = value.get("basement_ordinal")
    return SheetIdentity(
        page=page,
        sheet_kind=kind,
        buildings=tuple(str(item) for item in value.get("buildings") or ()),
        floors=tuple(str(item) for item in value.get("floors") or ()),
        floor_range=floor_range,
        basement_ordinal=ordinal if isinstance(ordinal, int) else None,
        section_axis=(
            str(value["section_axis"]) if value.get("section_axis") else None
        ),
        elevation=str(value["elevation"]) if value.get("elevation") else None,
        sheet_designation=(
            str(value["sheet_designation"]) if value.get("sheet_designation") else None
        ),
        raw_stamp_text=str(value.get("raw_stamp_text") or ""),
        confidence=str(value.get("confidence") or "HIGH"),
        evidence=dict(value.get("evidence") or {}),
    )


def parse_stamp_title(text: str, *, page: int = 0) -> SheetIdentity | None:
    """Parse one stamp title cell.  Returns ``None`` when nothing is proven."""
    flat = _normalize_text(text)
    if not flat:
        return None
    match = _TITLE_RE.search(flat)
    kind: str
    floors: tuple[str, ...] = ()
    floor_range = None
    basement_ordinal = None
    section_axis = None
    anchor = match
    if match is not None:
        what = match.group("what").lower()
        if what.startswith("кровли"):
            kind = "ROOF"
        elif match.group("basement_ordinal"):
            kind = "BASEMENT"
            basement_ordinal = _ORDINALS[match.group("basement_ordinal").lower()]
        elif what.startswith("технического"):
            kind = "TECHNICAL_SPACE"
        elif what.startswith("типового"):
            kind = "TYPICAL_PLAN"
        else:
            kind = "PLAN"
            floors, raw_range = _parse_floors(match.group("floors") or "")
            if not floors:
                return None
            floor_range = (raw_range["from"], raw_range["to"]) if raw_range else None
        raw = match.group(0)
    else:
        section = _SECTION_RE.search(flat)
        facade = _FACADE_RE.search(flat)
        if section is not None:
            kind, section_axis, raw = "SECTION", section.group("axis"), section.group(0)
            anchor = section
        elif facade is not None:
            kind, section_axis, raw = "FACADE", facade.group("axis"), facade.group(0)
            anchor = facade
        else:
            return None

    buildings, buildings_start = _parse_buildings(flat[: anchor.start()])
    # The engineer reads «Корпуса 1, 2. План кровли», not «План кровли»: the
    # label on a question card has to be the whole identification line.
    raw = flat[buildings_start:anchor.end()] if buildings_start is not None else raw
    return SheetIdentity(
        page=page,
        sheet_kind=kind,
        buildings=buildings,
        floors=floors,
        floor_range=floor_range,
        basement_ordinal=basement_ordinal,
        section_axis=section_axis.strip(" .").casefold() if section_axis else None,
        elevation=_parse_elevation(flat),
        sheet_designation=_parse_designation(flat),
        raw_stamp_text=raw.strip(" ."),
        confidence="HIGH",
        evidence={"source": "STAMP_TEXT_LAYER", "normalized_title": flat[:300]},
    )


def _stamp_zone_blocks(page: Any) -> list[tuple[float, float, str]]:
    """Return ``(x1_fraction, y0_fraction, text)`` for the stamp cell only.

    Block coordinates come back in unrotated page space; ``rotation_matrix``
    maps them into the space the reader actually sees, which is the space the
    stamp-cell fractions are defined in.  Skipping that step reads a rotated
    sheet's stamp as if it were somewhere in the middle of the page.
    """
    import fitz  # imported lazily: parsing must stay usable without PyMuPDF

    width = float(page.rect.width) or 1.0
    height = float(page.rect.height) or 1.0
    output: list[tuple[float, float, str]] = []
    for block in page.get_text("blocks"):
        rect = fitz.Rect(block[:4]) * page.rotation_matrix
        rect.normalize()
        x1_fraction = float(rect.x1) / width
        y0_fraction = float(rect.y0) / height
        if y0_fraction < STAMP_ZONE_MIN_Y0 or x1_fraction < STAMP_ZONE_MIN_X1:
            continue
        text = _normalize_text(block[4])
        if text:
            output.append((x1_fraction, y0_fraction, text))
    output.sort(key=lambda item: (-item[1], -item[0]))
    return output


def extract_sheet_identities(
    pdf_path: str,
    pages: Iterable[int] | None = None,
) -> dict[int, SheetIdentity]:
    """Read the stamp identity of every requested page.  Read-only, no model.

    A page contributing two *different* keys is reported as no identity at all
    rather than as a guess: that is the honest answer for a sheet whose stamp
    cell was misread, and the content signals still get their chance.
    """
    import fitz

    identities: dict[int, SheetIdentity] = {}
    document = fitz.open(pdf_path)
    try:
        wanted = (
            sorted({int(page) for page in pages})
            if pages is not None
            else range(1, document.page_count + 1)
        )
        for number in wanted:
            if number < 1 or number > document.page_count:
                continue
            page = document[number - 1]
            found: dict[str, SheetIdentity] = {}
            zone = _stamp_zone_blocks(page)
            # The document designation lives in its own cell of the same stamp,
            # not inside the title line, so it is read from the whole zone.
            designation = next(
                (
                    value
                    for value in (_parse_designation(text) for _x, _y, text in zone)
                    if value
                ),
                None,
            )
            for x1_fraction, y0_fraction, text in zone:
                identity = parse_stamp_title(text, page=number)
                if identity is None:
                    continue
                identity = replace(
                    identity,
                    sheet_designation=identity.sheet_designation or designation,
                    evidence={
                        **dict(identity.evidence),
                        "zone_x1_fraction": round(x1_fraction, 4),
                        "zone_y0_fraction": round(y0_fraction, 4),
                        "page_rotation": int(page.rotation),
                    },
                )
                found.setdefault(identity.stamp_key, identity)
            if len(found) == 1:
                identities[number] = next(iter(found.values()))
    finally:
        document.close()
    return identities


def build_sheet_identity_artifact(
    left_pdf_path: str,
    right_pdf_path: str,
    *,
    left_pages: Iterable[int] | None = None,
    right_pages: Iterable[int] | None = None,
    generated_at: str | None = None,
) -> dict[str, Any]:
    """Persistable identity artifact for both sides of one comparison pair."""
    left = extract_sheet_identities(left_pdf_path, left_pages)
    right = extract_sheet_identities(right_pdf_path, right_pages)
    payload = {
        "LEFT": [left[page].to_dict() for page in sorted(left)],
        "RIGHT": [right[page].to_dict() for page in sorted(right)],
    }
    return {
        "kind": KIND,
        "schema_version": SCHEMA_VERSION,
        "extractor_version": EXTRACTOR_VERSION,
        "version": 1,
        "generated_at": generated_at or utc_now(),
        "input_signature": content_signature({
            "extractor": EXTRACTOR_VERSION,
            "identities": payload,
        }),
        "identities": payload,
        "diagnostics": {
            "left_pages_with_identity": len(left),
            "right_pages_with_identity": len(right),
            "uses_model": False,
            "uses_ocr": False,
            "page_number_used_as_identity": False,
        },
    }


def identities_by_key(
    identities: Sequence[SheetIdentity],
) -> dict[str, list[SheetIdentity]]:
    grouped: dict[str, list[SheetIdentity]] = {}
    for identity in identities:
        grouped.setdefault(identity.stamp_key, []).append(identity)
    for value in grouped.values():
        value.sort(key=lambda item: item.page)
    return grouped


def covers_floors(container: SheetIdentity, member: SheetIdentity) -> bool:
    """True when ``container`` is a floor-range sheet that includes ``member``.

    «План 3-15 этажей» genuinely covers «План 7 этажа» of the same buildings.
    That is a grouping *candidate* for the existing 1→N / N→1 machinery, never
    a 1:1 identity: the two sheets are not the same sheet.
    """
    if container.sheet_kind != "PLAN" or member.sheet_kind != "PLAN":
        return False
    if container.buildings != member.buildings:
        return False
    if container.floor_range is None or not member.floors:
        return False
    if container.floor_set == member.floor_set:
        return False
    return member.floor_set < container.floor_set


__all__ = [
    "ELEVATION_IDENTIFYING_KINDS",
    "EXTRACTOR_VERSION",
    "KIND",
    "SCHEMA_VERSION",
    "SHEET_KINDS",
    "STAMP_ZONE_MIN_X1",
    "STAMP_ZONE_MIN_Y0",
    "SheetIdentity",
    "build_sheet_identity_artifact",
    "covers_floors",
    "extract_sheet_identities",
    "identities_by_key",
    "identity_from_dict",
    "parse_stamp_title",
]
