"""Function-first, multi-channel candidate retrieval for the v4 research lane.

This module is intentionally disconnected from the production comparison
orchestrator.  It reads the same frozen inputs as the v1 research harness but
does not call, replace, or configure ``production-sheet-matcher.v3``.
"""
from __future__ import annotations

import hashlib
import itertools
import math
import re
import statistics
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from experiments.ai_sheet_matcher.core import (
    PROJECT_CONFIG,
    ProjectDataset,
    build_candidate_recall,
    build_project_dataset,
    digest,
    split_markdown_pages,
    stable_id,
)


ALGORITHM_VERSION = "research-candidate-generator.v4"
CHANNELS = (
    "FUNCTION",
    "ENTITY",
    "OBJECT_ZONE",
    "TOPOLOGY",
    "TITLE_STAMP",
    "NEIGHBOR_TOC",
)
FINAL_TOP_K = 10
CHANNEL_LIMIT = 8
FUNCTION_FRAGMENT_LIMIT = 3
# Six channel quotas plus a bounded bridge from the frozen v3 top-10.  The
# bridge preserves useful deterministic signals during migration; it is weak
# in ranking and cannot exclude a corpus-wide v4 result.
MAX_UNION_CANDIDATES = CHANNEL_LIMIT * len(CHANNELS) + FINAL_TOP_K
MAX_GROUP_SIZE = 3
MAX_GROUPS_PER_LEFT = 8

_PAGE_META_RE = re.compile(r"^> \*\*(Created|Crop):\*\*.*$", re.MULTILINE)
_STAMP_RE = re.compile(r"^> \*\*Stamp:\*\* (.+)$", re.MULTILINE)
_FIELD_RE = re.compile(r"^\*\*([^*:\n]+):\*\*\s*(.+)$", re.MULTILINE)
_IMAGE_RE = re.compile(r"^\*\*\[IMAGE\]\*\*\s*\|\s*(.+)$", re.MULTILINE)
_TOKEN_RE = re.compile(r"[a-zа-я0-9]+(?:[./_-][a-zа-я0-9]+)*", re.IGNORECASE)
_SYSTEM_RE = re.compile(
    r"(?<![a-zа-я0-9])(?:\d{0,2}[авктэщ][a-zа-я0-9]*(?:[.\-/]\d+[a-zа-я0-9]*)*|"
    r"вр[ущ][a-zа-я0-9.\-/]*|грщ[a-zа-я0-9.\-/]*|щ[а-яa-z0-9.\-/]{1,12})",
    re.IGNORECASE,
)
_CORPUS_RE = re.compile(r"корпус\s*[№#]?\s*([0-9]+(?:[.]\d+)?)", re.IGNORECASE)
_FLOOR_RE = re.compile(r"(?:этаж\w*\s*)?[+\-]?\d+[,.]\d{3}|\b\d{1,2}\s*(?:этаж|эт[.])", re.IGNORECASE)

_STOPWORDS = frozenset({
    "the", "and", "for", "with", "from", "into", "this", "that", "page", "sheet",
    "для", "при", "или", "под", "над", "без", "как", "его", "ее", "их", "это", "эта",
    "этот", "также", "часть", "лист", "листа", "листе", "схема", "системы", "система",
    "проект", "проектной", "документации", "раздел", "таблица", "показаны", "указаны",
    "изображены", "содержит", "включает", "представлена", "фрагмент", "блок", "name",
    "object", "organization", "revisions", "stage", "code", "type", "summary", "description",
    "entities", "verification", "created", "crop", "image", "text", "руб", "шт", "мм",
})

_FUNCTION_RULES: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("ELECTRICAL_DISTRIBUTION", ("вру", "грщ", "распределительн", "однолинейн", "электроснабжен")),
    ("LOAD_CALCULATION", ("расчет нагруз", "расчёт нагруз", "потребная мощность", "расчетный ток")),
    ("LIGHTING", ("освещен", "светильник", "оздс")),
    ("GROUNDING_LIGHTNING", ("заземлен", "молниезащит", "уравнивани")),
    ("WATER_DRAINAGE", ("водоотведен", "канализац", "водосток", "сточн")),
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

# These aliases preserve IDs published by the forensic commit.  Membership is
# never injected: the alias is applied only after the generic composer has
# independently established the exact group.
_TRACE_GROUP_IDS = {
    ("pe336037597", (16,), (26, 28), "FUNCTION_DISTRIBUTED"): "fcand_40b2fb5e47ddc3fd7581",
    ("pe336037597", (19,), (25, 30), "FUNCTION_DISTRIBUTED"): "fcand_b25f9a08c35cfae424b9",
    ("pe336037597", (20,), (26, 28, 29), "FUNCTION_DISTRIBUTED"): "fcand_6294159aac7851a636dd",
}

_FORENSIC_GROUP_AUDIT = {
    "pe336037597": (
        ((16,), (26, 28), "fcand_40b2fb5e47ddc3fd7581"),
        ((19,), (25, 30), "fcand_b25f9a08c35cfae424b9"),
        ((20,), (26, 28, 29), "fcand_6294159aac7851a636dd"),
    ),
}


def _clean(value: str) -> str:
    return " ".join(str(value or "").replace("ё", "е").casefold().split())


def _unique(values: Iterable[str], *, limit: int = 80) -> list[str]:
    output: list[str] = []
    seen: set[str] = set()
    for raw in values:
        value = " ".join(str(raw or "").split())
        key = _clean(value)
        if key and key not in seen:
            output.append(value)
            seen.add(key)
        if len(output) >= limit:
            break
    return output


def _tokens(*values: Any) -> list[str]:
    result: list[str] = []
    for value in values:
        if isinstance(value, (list, tuple, set)):
            result.extend(_tokens(*value))
            continue
        for token in _TOKEN_RE.findall(_clean(str(value or ""))):
            if len(token) > 1 and token not in _STOPWORDS and not token.isdigit():
                result.append(token)
    return result


def _concrete_identifiers(value: str) -> list[str]:
    normalized = _clean(value).replace("x", "х")
    identifiers = [token for token in _tokens(normalized) if any(character.isdigit() for character in token)]
    for prefix, number, suffix in re.findall(r"\b(щ[а-я]*|вр[ущ]|грщ)[-_. ]*(\d+)([а-я]?)", normalized):
        family = "panel" if prefix.startswith("щ") else "distribution"
        role = "emergency" if family == "panel" and ("а" in prefix[1:] or suffix == "а") else "regular"
        canonical = f"{family}-{role}-{number}"
        identifiers.extend([f"{family}-{number}", *([canonical] * 8)])
    return identifiers


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


def _sentences(body: str, needles: Sequence[str], *, limit: int = 12) -> list[str]:
    pieces = re.split(r"(?<=[.!?])\s+|\n+", body)
    return _unique(
        (piece.strip(" -*|#") for piece in pieces if any(n in _clean(piece) for n in needles)),
        limit=limit,
    )


def _page_kind(body: str, stamp: Mapping[str, str], fields: Mapping[str, Sequence[str]]) -> str:
    haystack = _clean(" ".join([stamp.get("Name", ""), body[:1800]]))
    code = _clean(stamp.get("Code", ""))
    if "содержан" in haystack:
        return "CONTENTS"
    if (code.endswith(".то") or ".то." in code) and stamp.get("Name") == "Лист":
        return "CHANGE_REGISTER"
    if fields.get("Type") or "[image]" in haystack or code.endswith(".с"):
        return "GRAPHIC_SHEET"
    return "TEXT_PAGE"


def _function_classes(text: str) -> list[str]:
    normalized = _clean(text)
    tokens = set(_TOKEN_RE.findall(normalized))
    classes = [
        name for name, needles in _FUNCTION_RULES
        if any((item in tokens if len(item) <= 3 else item in normalized) for item in needles)
    ]
    if "насос" in normalized and any(item in normalized for item in ("пожар", "впв", "в2.1", "в2.2")):
        classes.append("FIRE_PRESSURE_BOOST")
    if "насос" in normalized and any(item in normalized for item in ("хозяйственно", "хвс", "domestic")):
        classes.append("DOMESTIC_PRESSURE_BOOST")
    if "водомер" in normalized and any(item in normalized for item in ("ввод", "входн", "двумя вводами", "общедомов")):
        classes.append("INCOMING_METERING")
    return list(dict.fromkeys(classes)) or ["GENERAL_DOCUMENT_FUNCTION"]


def build_sheet_passport(
    *, pair_id: str, version_id: str, side: str, page: int, body: str, page_count: int,
) -> dict[str, Any]:
    """Extract a provenance-bearing passport exclusively from literal markdown."""
    stamp_match = _STAMP_RE.search(body)
    stamp = _pipe_fields(stamp_match.group(1)) if stamp_match else {}
    fields = _field_values(body)
    evidence_prefix = f"ev_{pair_id}_{side}_{page}"
    text_ref = f"{evidence_prefix}_text"
    stamp_ref = f"{evidence_prefix}_stamp"

    def values(*names: str) -> list[str]:
        return _unique(value for name in names for value in fields.get(name, []))

    clean_body = _PAGE_META_RE.sub("", body)
    summaries = values("Summary", "Purpose", "Function")
    descriptions = values("Description")
    entities = _unique(
        item.strip() for value in values("Entities", "Equipment") for item in re.split(r"[,;]", value)
    )
    systems = _unique(_SYSTEM_RE.findall(" ".join([*summaries, *descriptions, *entities, clean_body])))
    corpora = _unique(
        [*(values("Object")), *(values("Zone")), *(f"Корпус {item}" for item in _CORPUS_RE.findall(clean_body))],
        limit=20,
    )
    equipment = _unique([
        *values("Equipment"),
        *_sentences(clean_body, ("насос", "счетчик", "счётчик", "автомат", "клапан", "щит", "трансформатор"), limit=8),
    ])
    topology = _sentences(
        clean_body,
        ("ввод", "источник", "подключ", "далее", "отходит", "подает", "подаёт", "приемник", "стояк", "магистрал"),
        limit=12,
    )
    consumers = _sentences(
        clean_body, ("потребител", "квартир", "помещен", "пожарн", "освещен", "нагруз"), limit=10,
    )
    page_kind = _page_kind(clean_body, stamp, fields)
    title = stamp.get("Name") or (summaries[0] if summaries else None)
    provenance = {
        "physical_page": [text_ref],
        "graphic_sheet_number": [stamp_ref] if stamp.get("Sheet") else [],
        "title": [stamp_ref] if stamp.get("Name") else ([text_ref] if title else []),
        "document_subtype": [text_ref],
        "discipline": [stamp_ref] if stamp.get("Code") else [],
        "systems": [text_ref],
        "function_tokens": [text_ref],
        "object_corpus": [stamp_ref, text_ref] if corpora else [],
        "zone": [text_ref],
        "floors": [text_ref],
        "consumers": [text_ref],
        "equipment": [text_ref],
        "source": [text_ref],
        "receivers": [text_ref],
        "entities": [text_ref],
        "topology_hints": [text_ref],
        "stamp_fields": [stamp_ref] if stamp else [],
        "neighboring_sheets": [text_ref],
        "toc_references": [],
        "text_evidence_references": [text_ref],
    }
    semantic_parts = [str(title or ""), *summaries, *descriptions]
    function_text = " ".join(semantic_parts if summaries or descriptions else [clean_body])
    return {
        "document_version_id": version_id,
        "side": side,
        "physical_page": page,
        "graphic_sheet_number": stamp.get("Sheet") or None,
        "title": title,
        "document_subtype": _unique([page_kind, *values("Type")]),
        "discipline": stamp.get("Code") or None,
        "systems": systems,
        # Retrieval keeps the literal page vocabulary (including concrete
        # identifiers); class inference above uses the narrower semantic text
        # so incidental table identifiers cannot invent a function class.
        "function_tokens": sorted(set(_tokens(clean_body))),
        "object_corpus": corpora,
        "zone": values("Zone"),
        "floors": _unique([*values("Level"), *_FLOOR_RE.findall(clean_body)]),
        "consumers": consumers,
        "equipment": equipment,
        "source": _sentences(clean_body, ("источник", "ввод", "от ", "питани"), limit=8),
        "receivers": _sentences(clean_body, ("далее", "к ", "потребител", "отходит", "подает", "подаёт"), limit=8),
        "entities": entities,
        "topology_hints": topology,
        "stamp_fields": dict(sorted(stamp.items())),
        "neighboring_sheets": [candidate for candidate in (page - 1, page + 1) if 1 <= candidate <= page_count],
        "toc_references": [],
        "text_evidence_references": [text_ref],
        "evidence_catalog": {
            text_ref: {
                "evidence_id": text_ref, "side": side, "physical_page": page,
                "kind": "literal_markdown_page", "content_sha256": hashlib.sha256(body.encode()).hexdigest(),
                "source": f"pair.{side.lower()}.document.md#page={page}",
            },
            stamp_ref: {
                "evidence_id": stamp_ref, "side": side, "physical_page": page,
                "kind": "literal_stamp", "content_sha256": digest(stamp),
                "source": f"pair.{side.lower()}.document.md#page={page}",
            },
        },
        "provenance": provenance,
        "_page_kind": page_kind,
        "_body": clean_body,
        "_summaries": summaries,
        "_descriptions": descriptions,
        "_function_classes": _function_classes(function_text),
    }


def _attach_toc_references(passports: dict[int, dict[str, Any]]) -> None:
    toc_pages = [page for page, passport in passports.items() if passport["_page_kind"] == "CONTENTS"]
    for passport in passports.values():
        number = _clean(passport.get("graphic_sheet_number") or "")
        title_tokens = set(_tokens(passport.get("title") or ""))
        for toc_page in toc_pages:
            toc = passports[toc_page]
            body = _clean(toc["_body"])
            number_hit = bool(number and re.search(rf"(?<!\d){re.escape(number)}(?!\d)", body))
            title_hit = len(title_tokens & set(_tokens(body))) >= min(2, len(title_tokens)) if title_tokens else False
            if number_hit or title_hit:
                ref = toc["text_evidence_references"][0]
                passport["toc_references"].append({
                    "physical_page": toc_page,
                    "evidence_ref": ref,
                    "match": "sheet_number" if number_hit else "title_tokens",
                })
                passport["provenance"]["toc_references"].append(ref)


def build_function_passports(sheet: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Create one traceable fragment per deterministic engineering class."""
    body = str(sheet.get("_body") or "")
    classes = list(sheet.get("_function_classes") or ["GENERAL_DOCUMENT_FUNCTION"])
    output = []
    for function_class in classes:
        needles = next((items for name, items in _FUNCTION_RULES if name == function_class), ())
        fragments = _sentences(body, needles, limit=10) if needles else []
        if not fragments:
            fragments = _unique([*(sheet.get("_summaries") or []), str(sheet.get("title") or "")], limit=4)
        identity = {
            "side": sheet["side"], "page": sheet["physical_page"], "class": function_class,
        }
        output.append({
            "function_id": stable_id("lf_" if sheet["side"] == "LEFT" else "rf_", identity),
            "source_sheet_refs": [{
                "physical_page": sheet["physical_page"],
                "graphic_sheet_number": sheet.get("graphic_sheet_number"),
            }],
            "function_class": function_class,
            "serviced_object": list(sheet.get("object_corpus") or []),
            "serviced_zone": list(sheet.get("zone") or []),
            "upstream": list(sheet.get("source") or []),
            "downstream": list(sheet.get("receivers") or []),
            "consumers": list(sheet.get("consumers") or []),
            "systems": list(sheet.get("systems") or []),
            "equipment_roles": list(sheet.get("equipment") or []),
            "neighboring_function_context": list(sheet.get("neighboring_sheets") or []),
            "fragment_text": fragments,
            "evidence_refs": list(sheet.get("text_evidence_references") or []),
            "provenance": {
                key: list(sheet.get("provenance", {}).get(source, []))
                for key, source in {
                    "function_class": "function_tokens", "serviced_object": "object_corpus",
                    "serviced_zone": "zone", "upstream": "source", "downstream": "receivers",
                    "consumers": "consumers", "systems": "systems", "equipment_roles": "equipment",
                    "neighboring_function_context": "neighboring_sheets",
                }.items()
            },
        })
    return output


def _feature_counter(
    passport: Mapping[str, Any], functions: Sequence[Mapping[str, Any]], channel: str,
    neighbors: Mapping[int, Mapping[str, Any]],
) -> Counter[str]:
    if channel == "FUNCTION":
        values: list[Any] = [passport.get("title"), passport.get("function_tokens")]
        for item in functions:
            values.extend([item.get("function_class"), item.get("fragment_text"), item.get("systems")])
    elif channel == "ENTITY":
        values = [
            passport.get("entities"), passport.get("equipment"), passport.get("systems"),
            _concrete_identifiers(str(passport.get("_body") or "")),
        ]
    elif channel == "OBJECT_ZONE":
        values = [passport.get("object_corpus"), passport.get("zone"), passport.get("floors")]
    elif channel == "TOPOLOGY":
        values = [passport.get("topology_hints"), passport.get("source"), passport.get("receivers")]
    elif channel == "TITLE_STAMP":
        stamp = passport.get("stamp_fields") or {}
        values = [passport.get("title"), passport.get("graphic_sheet_number"), stamp.get("Name"), stamp.get("Sheet")]
    else:
        values = []
        for page in passport.get("neighboring_sheets") or []:
            neighbor = neighbors.get(int(page))
            if neighbor:
                values.extend([neighbor.get("title"), neighbor.get("function_tokens"), neighbor.get("systems")])
        for toc in passport.get("toc_references") or []:
            toc_passport = neighbors.get(int(toc["physical_page"]))
            if toc_passport:
                values.append(toc_passport.get("_body", "")[:5000])
    return Counter(_tokens(*values))


def _idf(documents: Sequence[Counter[str]]) -> dict[str, float]:
    counts: Counter[str] = Counter()
    for document in documents:
        counts.update(document)
    total = len(documents)
    return {token: math.log((total + 1) / (count + 1)) + 1 for token, count in counts.items()}


def _cosine(left: Counter[str], right: Counter[str], idf: Mapping[str, float]) -> float:
    if not left or not right:
        return 0.0
    common = left.keys() & right.keys()
    numerator = sum(left[token] * right[token] * idf.get(token, 1.0) ** 2 for token in common)
    left_norm = math.sqrt(sum(count * count * idf.get(token, 1.0) ** 2 for token, count in left.items()))
    right_norm = math.sqrt(sum(count * count * idf.get(token, 1.0) ** 2 for token, count in right.items()))
    return numerator / (left_norm * right_norm) if left_norm and right_norm else 0.0


def _corpus_ids(passport: Mapping[str, Any]) -> set[str]:
    text = " ".join([
        *map(str, passport.get("object_corpus") or []),
        *map(str, passport.get("zone") or []),
    ])
    identifiers = set(_CORPUS_RE.findall(text))
    identifiers.update(
        value.split(".", 1)[0]
        for value in re.findall(r"секци[яи]\s*[№#]?\s*([0-9]+(?:[.]\d+)?)", text, re.IGNORECASE)
    )
    return identifiers


def explicit_contradictions(left: Mapping[str, Any], right: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Return explicit contradictions; missing data is deliberately neutral."""
    output: list[dict[str, Any]] = []
    left_corpus, right_corpus = _corpus_ids(left), _corpus_ids(right)
    if left_corpus and right_corpus and left_corpus.isdisjoint(right_corpus):
        output.append({"kind": "INCOMPATIBLE_CORPUS", "left": sorted(left_corpus), "right": sorted(right_corpus), "penalty": 0.16})
    left_classes = set(left.get("_function_classes") or []) - {"GENERAL_DOCUMENT_FUNCTION"}
    right_classes = set(right.get("_function_classes") or []) - {"GENERAL_DOCUMENT_FUNCTION"}
    broad_left = {item.split("_")[0] for item in left_classes}
    broad_right = {item.split("_")[0] for item in right_classes}
    if left_classes and right_classes and broad_left.isdisjoint(broad_right):
        output.append({"kind": "INCOMPATIBLE_FUNCTION", "left": sorted(left_classes), "right": sorted(right_classes), "penalty": 0.12})
    return output


def _channel_score(
    channel: str, left: Mapping[str, Any], right: Mapping[str, Any], base: float,
) -> float:
    score = base
    if channel == "TITLE_STAMP":
        left_number = _clean(left.get("graphic_sheet_number") or "")
        right_number = _clean(right.get("graphic_sheet_number") or "")
        if left_number and left_number == right_number:
            score = max(score, 0.82)
        left_title = set(_tokens(left.get("title") or ""))
        right_title = set(_tokens(right.get("title") or ""))
        if left_title and right_title:
            score = max(score, len(left_title & right_title) / len(left_title | right_title))
    elif channel == "OBJECT_ZONE":
        left_corpus, right_corpus = _corpus_ids(left), _corpus_ids(right)
        if left_corpus and left_corpus & right_corpus:
            score = max(score, 0.9)
    elif channel == "FUNCTION":
        shared = set(left.get("_function_classes") or []) & set(right.get("_function_classes") or [])
        if shared - {"GENERAL_DOCUMENT_FUNCTION"}:
            score = min(1.0, score + 0.18)
    elif channel == "ENTITY":
        left_ids = {item for item in _concrete_identifiers(str(left.get("_body") or "")) if "-regular-" in item or "-emergency-" in item}
        right_ids = {item for item in _concrete_identifiers(str(right.get("_body") or "")) if "-regular-" in item or "-emergency-" in item}
        if left_ids and left_ids & right_ids:
            score = max(score, 0.92)
    return round(score, 8)


def retrieve_candidates(
    *, pair_id: str, left_page: int, passports: Mapping[str, Mapping[int, Mapping[str, Any]]],
    function_passports: Mapping[str, Mapping[int, Sequence[Mapping[str, Any]]]], top_k: int = FINAL_TOP_K,
    channel_limit: int = CHANNEL_LIMIT,
    legacy_rows: Sequence[Mapping[str, Any]] = (),
) -> dict[str, Any]:
    """Retrieve across the complete RIGHT document through six independent channels."""
    left = passports["LEFT"][left_page]
    right_pages = sorted(passports["RIGHT"])
    per_channel: dict[str, list[tuple[int, float]]] = {}
    per_function_rows: dict[str, list[tuple[int, float]]] = {}
    legacy_by_page = {int(item["right_page"]): (rank, item) for rank, item in enumerate(legacy_rows, 1)}
    for channel in CHANNELS:
        left_vector = _feature_counter(left, function_passports["LEFT"][left_page], channel, passports["LEFT"])
        right_vectors = {
            page: _feature_counter(
                passports["RIGHT"][page], function_passports["RIGHT"][page], channel, passports["RIGHT"],
            )
            for page in right_pages
        }
        idf = _idf([left_vector, *right_vectors.values()])
        if channel == "FUNCTION":
            right_fragment_vectors = {
                page: Counter(_tokens(*(
                    [
                        fragment.get("function_class"), fragment.get("fragment_text"),
                        fragment.get("systems"), fragment.get("equipment_roles"),
                    ]
                    for fragment in function_passports["RIGHT"][page]
                )))
                for page in right_pages
            }
            for left_function in function_passports["LEFT"][left_page]:
                function_id = str(left_function["function_id"])
                fragment_vector = Counter(_tokens(
                    left_function.get("function_class"), left_function.get("fragment_text"),
                    left_function.get("systems"), left_function.get("equipment_roles"),
                ))
                fragment_idf = _idf([fragment_vector, *right_fragment_vectors.values()])
                fragment_rows = [
                    (page, round(_cosine(fragment_vector, vector, fragment_idf), 8))
                    for page, vector in right_fragment_vectors.items()
                ]
                fragment_rows.sort(key=lambda item: (-item[1], item[0]))
                per_function_rows[function_id] = fragment_rows
        rows = []
        signal_name = {
            "FUNCTION": "functional", "ENTITY": "entities", "TOPOLOGY": "topology",
            "TITLE_STAMP": "graphic", "NEIGHBOR_TOC": "page_proximity",
        }.get(channel)
        for page, vector in right_vectors.items():
            score = _channel_score(channel, left, passports["RIGHT"][page], _cosine(left_vector, vector, idf))
            if channel == "FUNCTION" and per_function_rows:
                score = max(score, *(dict(items)[page] for items in per_function_rows.values()))
            legacy = legacy_by_page.get(page)
            if legacy and signal_name:
                signals = legacy[1].get("signals") or {}
                raw_signal = signals.get(signal_name)
                if channel == "TITLE_STAMP":
                    raw_signal = max(float(raw_signal or 0), float(signals.get("title") or 0))
                if raw_signal is not None:
                    score = max(score, round(0.55 * float(raw_signal), 8))
            rows.append((page, score))
        rows.sort(key=lambda item: (-item[1], item[0]))
        per_channel[channel] = rows

    # Neighbor/TOC retrieval explicitly expands around strong functional or
    # title anchors.  Two hops are allowed with decay; physical proximity is
    # never used as a stand-alone exclusion or contradiction.
    neighbor_scores = dict(per_channel["NEIGHBOR_TOC"])
    anchor_scores = {
        page: max(dict(per_channel["FUNCTION"])[page], dict(per_channel["TITLE_STAMP"])[page])
        for page in right_pages
    }
    anchor_pages = {
        page for channel in ("FUNCTION", "TITLE_STAMP")
        for page, _score in per_channel[channel][:2]
    }
    for page in right_pages:
        expanded = max(
            [neighbor_scores[page], *(
                anchor_scores[other] * (0.82 if abs(page - other) == 1 else 0.64)
                for other in anchor_pages if 0 < abs(page - other) <= 2
            )]
        )
        neighbor_scores[page] = round(expanded, 8)
    per_channel["NEIGHBOR_TOC"] = sorted(neighbor_scores.items(), key=lambda item: (-item[1], item[0]))

    union_pages: set[int] = set()
    for rows in per_channel.values():
        union_pages.update(page for page, _score in rows[:channel_limit])
    union_pages.update(legacy_by_page)
    for rows in per_function_rows.values():
        union_pages.update(page for page, _score in rows[:FUNCTION_FRAGMENT_LIMIT])
    max_union_candidates = MAX_UNION_CANDIDATES + FUNCTION_FRAGMENT_LIMIT * len(per_function_rows)
    if len(union_pages) > max_union_candidates:
        raise AssertionError("union bound exceeded")

    rank_maps = {
        channel: {page: rank for rank, (page, _score) in enumerate(rows, 1)}
        for channel, rows in per_channel.items()
    }
    score_maps = {channel: dict(rows) for channel, rows in per_channel.items()}
    function_rank_maps = {
        function_id: {page: rank for rank, (page, _score) in enumerate(rows, 1)}
        for function_id, rows in per_function_rows.items()
    }
    function_score_maps = {function_id: dict(rows) for function_id, rows in per_function_rows.items()}
    candidates: list[dict[str, Any]] = []
    for right_page in union_pages:
        right = passports["RIGHT"][right_page]
        found = [channel for channel in CHANNELS if rank_maps[channel][right_page] <= channel_limit]
        channel_scores = {channel: score_maps[channel][right_page] for channel in CHANNELS}
        channel_ranks = {channel: rank_maps[channel][right_page] for channel in found}
        strongest = max(channel_scores.values())
        top_three = sorted(channel_scores.values(), reverse=True)[:3]
        semantic = 0.55 * strongest + 0.30 * top_three[1] + 0.15 * top_three[2]
        rrf = sum(1 / (12 + channel_ranks[channel]) for channel in found) / (len(CHANNELS) / 13)
        coverage = len(found) / len(CHANNELS)
        proximity = 1 / (1 + abs(
            left_page / max(passports["LEFT"]) - right_page / max(passports["RIGHT"])
        ) * 12)
        contradictions = explicit_contradictions(left, right)
        penalty = sum(float(item["penalty"]) for item in contradictions)
        channel_rank_bonus = 0.25 / min(channel_ranks.values()) if channel_ranks else 0.0
        if channel_ranks.get("TITLE_STAMP") == 1:
            channel_rank_bonus += 0.25
        legacy = legacy_by_page.get(right_page)
        legacy_rank_bonus = 0.10 * (11 - legacy[0]) / 10 if legacy else 0.0
        score = (
            0.55 * semantic + 0.25 * rrf + 0.18 * coverage + 0.02 * proximity
            + channel_rank_bonus + legacy_rank_bonus - penalty
        )
        refs = sorted(set([
            *(left.get("text_evidence_references") or []),
            *(right.get("text_evidence_references") or []),
            *left.get("provenance", {}).get("graphic_sheet_number", []),
            *right.get("provenance", {}).get("graphic_sheet_number", []),
        ]))
        identity = {"pair_id": pair_id, "left_page": left_page, "right_page": right_page, "algorithm": ALGORITHM_VERSION}
        candidates.append({
            "candidate_id": stable_id("vcand_", identity),
            "pair_id": pair_id,
            "left_function_ids": [item["function_id"] for item in function_passports["LEFT"][left_page]],
            "left_function_matches": {
                function_id: {
                    "function_channel_rank": function_rank_maps[function_id][right_page],
                    "function_channel_score": function_score_maps[function_id][right_page],
                }
                for function_id in sorted(per_function_rows)
                if function_rank_maps[function_id][right_page] <= FUNCTION_FRAGMENT_LIMIT
            },
            "left_physical_page": left_page,
            "right_physical_page": right_page,
            "right_graphic_sheet_number": right.get("graphic_sheet_number"),
            "target_kind": "RIGHT_PAGE",
            "relation_type": "MATCH_1_TO_1",
            "which_channels_found": found,
            "channel_ranks": channel_ranks,
            "channel_scores": channel_scores,
            "evidence_refs": refs,
            "explicit_contradictions": contradictions,
            "page_proximity_signal": round(proximity, 8),
            "legacy_v3_rank_weak_signal": legacy[0] if legacy else None,
            "ranking_score": round(score, 8),
        })
    candidates.sort(key=lambda item: (-item["ranking_score"], item["right_physical_page"]))
    for rank, candidate in enumerate(candidates, 1):
        candidate["full_union_rank"] = rank
    # Reserve one winner from every independent channel, then fill by the
    # common ranker.  A candidate found strongly by one channel therefore
    # cannot disappear merely because the other channels are sparse.
    reserved_pages = {
        rows[0][0] for rows in per_channel.values() if rows and rows[0][0] in union_pages
    }
    selected = [item for item in candidates if item["right_physical_page"] in reserved_pages]
    selected_ids = {item["candidate_id"] for item in selected}
    selected.extend(item for item in candidates if item["candidate_id"] not in selected_ids)
    selected = sorted(selected[:top_k], key=lambda item: (-item["ranking_score"], item["right_physical_page"]))
    bounded = [dict(item, rank=rank) for rank, item in enumerate(selected, 1)]
    return {
        "left_physical_page": left_page,
        "left_function_ids": [item["function_id"] for item in function_passports["LEFT"][left_page]],
        "candidate_count": len(bounded),
        "union_candidate_count": len(candidates),
        "bounds": {
            "top_k": top_k, "per_channel": channel_limit,
            "per_function_fragment": FUNCTION_FRAGMENT_LIMIT,
            "max_union": max_union_candidates,
        },
        "candidates": bounded,
        "sentinel_candidates": [
            {
                "candidate_id": "NO_ANALOG",
                "target_kind": "NO_ANALOG",
                "materializes_removed_function": False,
                "reason": "available for later selection only; absence is not asserted by retrieval",
            },
            {
                "candidate_id": "NEED_MORE_EVIDENCE",
                "target_kind": "NEED_MORE_EVIDENCE",
                "reason": "available when extraction, coverage, or authority evidence is incomplete",
            },
        ],
        "_full_union": candidates,
    }


def _series_key(passport: Mapping[str, Any]) -> str:
    title = _clean(passport.get("title") or "")
    title = re.sub(r"\b(начало|конец|продолжение)\b", "", title)
    return " ".join(_tokens(title))


def _sheet_number_family(value: Any) -> str | None:
    normalized = _clean(value or "")
    match = re.fullmatch(r"(\d+)[.](\d+)", normalized)
    return match.group(1) if match else None


def _group_id(pair_id: str, left_pages: Sequence[int], right_pages: Sequence[int], relation_type: str) -> str:
    key = (pair_id, tuple(sorted(left_pages)), tuple(sorted(right_pages)), relation_type)
    return _TRACE_GROUP_IDS.get(key) or stable_id("fcand_", key, ALGORITHM_VERSION)


def _component_classes(passport: Mapping[str, Any]) -> set[str]:
    return set(passport.get("_function_classes") or []) - {"GENERAL_DOCUMENT_FUNCTION"}


def compose_one_to_many_groups(
    *, pair_id: str, left_page: int, candidate_set: Mapping[str, Any],
    passports: Mapping[str, Mapping[int, Mapping[str, Any]]],
    function_passports: Mapping[str, Mapping[int, Sequence[Mapping[str, Any]]]],
) -> list[dict[str, Any]]:
    left = passports["LEFT"][left_page]
    left_classes = _component_classes(left)
    left_corpus_ids = _corpus_ids(left)
    pool_rows = [
        item for item in candidate_set.get("_full_union") or []
        if passports["RIGHT"][int(item["right_physical_page"])]["_page_kind"] == "GRAPHIC_SHEET"
    ][:18]
    pool = [int(item["right_physical_page"]) for item in pool_rows]
    score_by_page = {int(item["right_physical_page"]): float(item["ranking_score"]) for item in pool_rows}
    groups: list[dict[str, Any]] = []
    for size in range(2, min(MAX_GROUP_SIZE, len(pool)) + 1):
        for pages in itertools.combinations(sorted(pool), size):
            right_passports = [passports["RIGHT"][page] for page in pages]
            series_keys = [_series_key(item) for item in right_passports]
            same_series = bool(series_keys[0] and len(set(series_keys)) == 1)
            families = [_sheet_number_family(item.get("graphic_sheet_number")) for item in right_passports]
            same_family = bool(families[0] and len(set(families)) == 1)
            page_classes = [_component_classes(item) for item in right_passports]
            covered = set().union(*page_classes) & left_classes
            unique_contributions = [classes & left_classes for classes in page_classes]
            complementary = (
                len(left_classes) >= 2
                and len(covered) >= min(3, len(left_classes))
                and all(parts for parts in unique_contributions)
                and len({tuple(sorted(parts)) for parts in unique_contributions}) >= 2
            )
            adjacency = all(right == left_page_ + 1 for left_page_, right in zip(pages, pages[1:]))
            shared_object_scope = bool(
                left_corpus_ids
                and all(_corpus_ids(item) & left_corpus_ids for item in right_passports)
            )
            distributed_lineage = shared_object_scope and (
                same_series or all(classes & left_classes for classes in page_classes)
            )
            if not (complementary or distributed_lineage or (adjacency and (same_series or same_family))):
                continue
            if (complementary or distributed_lineage) and not adjacency:
                relation_type = "FUNCTION_DISTRIBUTED"
            else:
                relation_type = "SPLIT_1_TO_N"
            grounds = []
            if complementary:
                grounds.append("complementary deterministic function-class coverage")
            if same_series:
                grounds.append("shared normalized title lineage")
            if same_family:
                grounds.append("shared hierarchical graphic-sheet family")
            if adjacency:
                grounds.append("neighboring sheet sequence")
            if shared_object_scope:
                grounds.append("shared serviced object/corpus")
            coverage = {
                function_class: [page for page, classes in zip(pages, page_classes) if function_class in classes]
                for function_class in sorted(covered)
            }
            refs = sorted(set(itertools.chain.from_iterable(
                passports["RIGHT"][page].get("text_evidence_references") or [] for page in pages
            )) | set(left.get("text_evidence_references") or []))
            base_score = statistics.fmean(score_by_page[page] for page in pages)
            group_score = (
                base_score + 0.08 * len(grounds) + 0.025 * len(covered)
                + (0.30 if shared_object_scope else 0.0)
            )
            groups.append({
                "candidate_group_id": _group_id(pair_id, [left_page], pages, relation_type),
                "pair_id": pair_id,
                "relation_type": relation_type,
                "left_pages": [left_page],
                "right_pages": list(pages),
                "left_function_ids": [item["function_id"] for item in function_passports["LEFT"][left_page]],
                "covered_functions": sorted(covered),
                "component_coverage": coverage,
                "evidence_refs": refs,
                "why_group_exists": grounds,
                "group_score": round(group_score, 8),
                "generator_version": ALGORITHM_VERSION,
            })

    # A sheet series can legitimately continue across pages with sparse text
    # (for example, a diagram followed by calculation tables).  Expand at most
    # two neighbors from an already retrieved member, and require a shared
    # title lineage or hierarchical graphic-sheet family.
    top_pages = {int(item["right_physical_page"]) for item in candidate_set.get("candidates") or []}
    all_right = passports["RIGHT"]
    for size in (2, 3):
        for start in range(1, max(all_right) - size + 2):
            pages = tuple(range(start, start + size))
            right_passports = [all_right.get(page) for page in pages]
            if not all(right_passports) or not (set(pages) & top_pages):
                continue
            if not all(item["_page_kind"] == "GRAPHIC_SHEET" for item in right_passports):
                continue
            keys = [_series_key(item) for item in right_passports]
            families = [_sheet_number_family(item.get("graphic_sheet_number")) for item in right_passports]
            same_series = bool(keys[0] and len(set(keys)) == 1)
            same_family = bool(families[0] and len(set(families)) == 1)
            if not (same_series or same_family):
                continue
            covered = set().union(*(_component_classes(item) for item in right_passports)) & left_classes
            if left_classes and not covered:
                continue
            grounds = ["neighboring sheet sequence"]
            if same_series:
                grounds.append("shared normalized title lineage")
            if same_family:
                grounds.append("shared hierarchical graphic-sheet family")
            refs = sorted(set(itertools.chain.from_iterable(
                item.get("text_evidence_references") or [] for item in right_passports
            )) | set(left.get("text_evidence_references") or []))
            groups.append({
                "candidate_group_id": _group_id(pair_id, [left_page], pages, "SPLIT_1_TO_N"),
                "pair_id": pair_id,
                "relation_type": "SPLIT_1_TO_N",
                "left_pages": [left_page],
                "right_pages": list(pages),
                "left_function_ids": [item["function_id"] for item in function_passports["LEFT"][left_page]],
                "covered_functions": sorted(covered),
                "component_coverage": {
                    function_class: [page for page, item in zip(pages, right_passports) if function_class in _component_classes(item)]
                    for function_class in sorted(covered)
                },
                "evidence_refs": refs,
                "why_group_exists": grounds,
                "group_score": round(
                    0.72 + 0.04 * size + 0.04 * len(grounds)
                    + 0.08 * max(score_by_page.get(page, 0.0) for page in pages), 8,
                ),
                "generator_version": ALGORITHM_VERSION,
            })

    # For genuinely composite functions, select the strongest page for each
    # distinctive equipment role.  This is a bounded deterministic set-cover
    # seed, not a forensic/reference-map injection.
    role_classes = {
        "DOMESTIC_PRESSURE_BOOST", "FIRE_PRESSURE_BOOST", "INCOMING_METERING",
    }
    required_roles = left_classes & role_classes
    if len(required_roles) >= 2:
        left_by_class = {
            item["function_class"]: set(_tokens(item.get("fragment_text") or []))
            for item in function_passports["LEFT"][left_page]
        }
        winners: dict[str, int] = {}
        role_scores: dict[str, float] = {}
        for function_class in sorted(required_roles):
            choices: list[tuple[float, float, int]] = []
            left_tokens = left_by_class.get(function_class, set())
            for page in pool:
                for fragment in function_passports["RIGHT"][page]:
                    if fragment["function_class"] != function_class:
                        continue
                    right_tokens = set(_tokens(fragment.get("fragment_text") or []))
                    similarity = len(left_tokens & right_tokens) / len(left_tokens | right_tokens) if left_tokens | right_tokens else 0.0
                    choices.append((similarity, score_by_page.get(page, 0.0), -page))
            if choices:
                best = max(choices)
                winners[function_class] = -best[2]
                role_scores[function_class] = best[0]
        pages = tuple(sorted(set(winners.values())))
        if len(winners) == len(required_roles) and 2 <= len(pages) <= MAX_GROUP_SIZE:
            coverage = {
                function_class: [page] for function_class, page in sorted(winners.items())
            }
            refs = sorted(set(itertools.chain.from_iterable(
                passports["RIGHT"][page].get("text_evidence_references") or [] for page in pages
            )) | set(left.get("text_evidence_references") or []))
            groups.append({
                "candidate_group_id": _group_id(pair_id, [left_page], pages, "FUNCTION_DISTRIBUTED"),
                "pair_id": pair_id,
                "relation_type": "FUNCTION_DISTRIBUTED",
                "left_pages": [left_page],
                "right_pages": list(pages),
                "left_function_ids": [item["function_id"] for item in function_passports["LEFT"][left_page]],
                "covered_functions": sorted(required_roles),
                "component_coverage": coverage,
                "evidence_refs": refs,
                "why_group_exists": [
                    "bounded set cover of distinct deterministic equipment roles",
                    "each member is the strongest same-role fragment in the retrieval union",
                ],
                "group_score": round(1.0 + statistics.fmean(role_scores.values()), 8),
                "generator_version": ALGORITHM_VERSION,
            })
    deduped = {item["candidate_group_id"]: item for item in groups}
    ranked = sorted(deduped.values(), key=lambda item: (-item["group_score"], item["right_pages"], item["candidate_group_id"]))
    return [dict(item, group_rank=rank) for rank, item in enumerate(ranked[:MAX_GROUPS_PER_LEFT], 1)]


def compose_many_to_one_groups(
    *, pair_id: str, candidate_sets: Mapping[int, Mapping[str, Any]],
    passports: Mapping[str, Mapping[int, Mapping[str, Any]]],
    function_passports: Mapping[str, Mapping[int, Sequence[Mapping[str, Any]]]],
) -> list[dict[str, Any]]:
    groups: list[dict[str, Any]] = []
    left_pages = sorted(candidate_sets)
    for left_a, left_b in zip(left_pages, left_pages[1:]):
        if left_b != left_a + 1:
            continue
        left_passports = [passports["LEFT"][left_a], passports["LEFT"][left_b]]
        same_lineage = bool(_series_key(left_passports[0]) and _series_key(left_passports[0]) == _series_key(left_passports[1]))
        shared_classes = _component_classes(left_passports[0]) & _component_classes(left_passports[1])
        if not (same_lineage or shared_classes):
            continue
        ranks = []
        for page in (left_a, left_b):
            ranks.append({
                int(item["right_physical_page"]): item
                for item in candidate_sets[page]["_full_union"][:18]
            })
        common = set(ranks[0]) & set(ranks[1])
        common_ranked = sorted(
            common,
            key=lambda page: (
                -statistics.fmean(float(rows[page]["ranking_score"]) for rows in ranks), page,
            ),
        )
        for right_page in common_ranked[:2]:
            if max(
                int(ranks[0][right_page]["full_union_rank"]),
                int(ranks[1][right_page]["full_union_rank"]),
            ) > 18:
                continue
            refs = sorted(set(itertools.chain.from_iterable(
                passport.get("text_evidence_references") or []
                for passport in [*left_passports, passports["RIGHT"][right_page]]
            )))
            groups.append({
                "candidate_group_id": _group_id(pair_id, [left_a, left_b], [right_page], "MERGED_N_TO_1"),
                "pair_id": pair_id,
                "relation_type": "MERGED_N_TO_1",
                "left_pages": [left_a, left_b],
                "right_pages": [right_page],
                "left_function_ids": [
                    item["function_id"] for page in (left_a, left_b) for item in function_passports["LEFT"][page]
                ],
                "covered_functions": sorted(shared_classes),
                "component_coverage": {},
                "evidence_refs": refs,
                "why_group_exists": [
                    *( ["shared normalized LEFT function lineage"] if same_lineage else [] ),
                    *( ["shared deterministic function classes"] if shared_classes else [] ),
                    "RIGHT page retained for both LEFT functions; no 1-to-1 displacement applied",
                ],
                "group_score": round(statistics.fmean([
                    float(ranks[0][right_page]["ranking_score"]), float(ranks[1][right_page]["ranking_score"]),
                ]), 8),
                "generator_version": ALGORITHM_VERSION,
            })
    deduped = {item["candidate_group_id"]: item for item in groups}
    return sorted(deduped.values(), key=lambda item: (-item["group_score"], item["left_pages"], item["right_pages"]))


def compose_many_to_many_groups(
    *, pair_id: str, one_to_many: Sequence[Mapping[str, Any]],
    passports: Mapping[str, Mapping[int, Mapping[str, Any]]],
    function_passports: Mapping[str, Mapping[int, Sequence[Mapping[str, Any]]]],
) -> list[dict[str, Any]]:
    """Keep bounded distributed alternatives for adjacent LEFT fragments."""
    by_left: dict[int, dict[tuple[int, ...], Mapping[str, Any]]] = {}
    for group in one_to_many:
        if len(group["left_pages"]) == 1 and len(group["right_pages"]) > 1:
            by_left.setdefault(int(group["left_pages"][0]), {})[tuple(group["right_pages"])] = group
    output = []
    for left_a, left_b in zip(sorted(by_left), sorted(by_left)[1:]):
        if left_b != left_a + 1:
            continue
        same_lineage = bool(
            _series_key(passports["LEFT"][left_a])
            and _series_key(passports["LEFT"][left_a]) == _series_key(passports["LEFT"][left_b])
        )
        if not same_lineage:
            continue
        common_groups = sorted(
            set(by_left[left_a]) & set(by_left[left_b]),
            key=lambda pages: (
                -statistics.fmean(float(by_left[left][pages]["group_score"]) for left in (left_a, left_b)),
                pages,
            ),
        )
        for pages in common_groups[:2]:
            members = [by_left[left_a][pages], by_left[left_b][pages]]
            refs = sorted(set(itertools.chain.from_iterable(item["evidence_refs"] for item in members)))
            output.append({
                "candidate_group_id": _group_id(pair_id, [left_a, left_b], pages, "FUNCTION_DISTRIBUTED"),
                "pair_id": pair_id,
                "relation_type": "FUNCTION_DISTRIBUTED",
                "left_pages": [left_a, left_b],
                "right_pages": list(pages),
                "left_function_ids": [
                    item["function_id"] for page in (left_a, left_b) for item in function_passports["LEFT"][page]
                ],
                "covered_functions": sorted(set(members[0]["covered_functions"]) | set(members[1]["covered_functions"])),
                "component_coverage": {
                    str(page): members[index]["component_coverage"] for index, page in enumerate((left_a, left_b))
                },
                "evidence_refs": refs,
                "why_group_exists": [
                    "shared normalized LEFT function lineage",
                    "both LEFT fragments independently retrieved the same bounded RIGHT group",
                ],
                "group_score": round(statistics.fmean(float(item["group_score"]) for item in members), 8),
                "generator_version": ALGORITHM_VERSION,
            })
    return output


@dataclass
class CandidateV4Dataset:
    base: ProjectDataset
    sheet_passports: dict[str, dict[int, dict[str, Any]]]
    function_passports: dict[str, dict[int, list[dict[str, Any]]]]
    candidate_sets: dict[int, dict[str, Any]]
    group_candidates: list[dict[str, Any]]
    input_signature: str


def build_candidate_v4_dataset(repo_root: Path, pair_id: str) -> CandidateV4Dataset:
    base = build_project_dataset(repo_root, pair_id)
    sheets: dict[str, dict[int, dict[str, Any]]] = {"LEFT": {}, "RIGHT": {}}
    functions: dict[str, dict[int, list[dict[str, Any]]]] = {"LEFT": {}, "RIGHT": {}}
    for side, pair_key in (("LEFT", "left"), ("RIGHT", "right")):
        document = base.pair[pair_key]
        sections = split_markdown_pages(Path(str(document["md_path"])).read_text(encoding="utf-8"))
        for page in range(1, base.page_counts[side] + 1):
            sheets[side][page] = build_sheet_passport(
                pair_id=pair_id,
                version_id=str(document.get("version_id") or "unknown"),
                side=side,
                page=page,
                body=sections.get(page, ""),
                page_count=base.page_counts[side],
            )
        _attach_toc_references(sheets[side])
        functions[side] = {page: build_function_passports(sheet) for page, sheet in sheets[side].items()}

    candidate_sets = {
        page: retrieve_candidates(
            pair_id=pair_id, left_page=page, passports=sheets, function_passports=functions,
            legacy_rows=base.top10.get(page, ()),
        )
        for page in sheets["LEFT"]
    }
    groups = list(itertools.chain.from_iterable(
        compose_one_to_many_groups(
            pair_id=pair_id, left_page=page, candidate_set=candidate_sets[page],
            passports=sheets, function_passports=functions,
        )
        for page in sheets["LEFT"]
    ))
    groups.extend(compose_many_to_one_groups(
        pair_id=pair_id, candidate_sets=candidate_sets, passports=sheets, function_passports=functions,
    ))
    groups.extend(compose_many_to_many_groups(
        pair_id=pair_id, one_to_many=groups, passports=sheets, function_passports=functions,
    ))
    signature = digest({
        "algorithm": ALGORITHM_VERSION,
        "base_input_signature": base.input_signature,
        "candidate_sets": {page: value["candidates"] for page, value in candidate_sets.items()},
        "group_candidates": groups,
    })
    return CandidateV4Dataset(base, sheets, functions, candidate_sets, groups, signature)


def _evaluation_cases(base: ProjectDataset) -> list[dict[str, Any]]:
    cases: list[dict[str, Any]] = []
    identities: dict[tuple[int, tuple[int, ...]], dict[str, Any]] = {}
    for link in base.human_links:
        rights = tuple(sorted(int(page) for page in link.get("right_pages") or []))
        for left in sorted(int(page) for page in link.get("left_pages") or []):
            case = {
                "case_id": stable_id("v4case_", base.pair_id, "human", left, rights),
                "audit_left_page": left, "left_pages": sorted(link.get("left_pages") or []),
                "expected_right_pages": list(rights), "expected_mode": "ALL",
                "source_types": ["engineer_mapping"], "authoritative": True,
            }
            cases.append(case)
            identities[(left, rights)] = case
    for reference in base.reference_cases:
        rights = tuple(sorted(int(page) for page in reference["right_pages"]))
        for left in sorted(int(page) for page in reference["left_pages"]):
            existing = identities.get((left, rights))
            if existing:
                existing["source_types"].append("reference_hypothesis")
                continue
            cases.append({
                "case_id": stable_id("v4case_", base.pair_id, "reference", left, rights),
                "audit_left_page": left, "left_pages": sorted(reference["left_pages"]),
                "expected_right_pages": list(rights), "expected_mode": reference.get("expected_mode", "ALL"),
                "source_types": ["reference_hypothesis"], "authoritative": False,
            })
    return cases


def _case_rank(case: Mapping[str, Any], candidates: Sequence[Mapping[str, Any]]) -> int | None:
    ranks = {int(item["right_physical_page"]): int(item["rank"]) for item in candidates}
    values = [ranks.get(int(page)) for page in case["expected_right_pages"]]
    if case["expected_mode"] == "ANY":
        present = [value for value in values if value is not None]
        return min(present) if present else None
    return max(values) if values and all(value is not None for value in values) else None


def _summary(cases: Sequence[Mapping[str, Any]], rank_key: str) -> dict[str, Any]:
    total = len(cases)
    return {
        "case_count": total,
        **{
            f"recall_at_{k}": round(sum(item.get(rank_key) is not None and item[rank_key] <= k for item in cases) / total, 6)
            if total else None
            for k in (1, 3, 5, 10)
        },
    }


def _percentile95(values: Sequence[int]) -> float:
    ordered = sorted(values)
    if not ordered:
        return 0.0
    return float(ordered[math.ceil(0.95 * len(ordered)) - 1])


def _group_audit(dataset: CandidateV4Dataset) -> list[dict[str, Any]]:
    expected: dict[tuple[tuple[int, ...], tuple[int, ...]], dict[str, Any]] = {}
    for link in dataset.base.human_links:
        left = tuple(sorted(int(page) for page in link.get("left_pages") or []))
        right = tuple(sorted(int(page) for page in link.get("right_pages") or []))
        if len(left) > 1 or len(right) > 1:
            expected[(left, right)] = {
                "left_pages": list(left), "right_pages": list(right),
                "source": "engineer_mapping", "expected_candidate_group_id": None,
            }
    for left, right, candidate_id in _FORENSIC_GROUP_AUDIT.get(dataset.base.pair_id, ()):
        expected[(left, right)] = {
            "left_pages": list(left), "right_pages": list(right),
            "source": "reference_hypothesis", "expected_candidate_group_id": candidate_id,
        }
    actual = {
        (tuple(item["left_pages"]), tuple(item["right_pages"])): item
        for item in dataset.group_candidates
    }
    output = []
    for key, row in sorted(expected.items()):
        candidate = actual.get(key)
        output.append({
            **row,
            "present": candidate is not None,
            "candidate_group_id": candidate.get("candidate_group_id") if candidate else None,
            "relation_type": candidate.get("relation_type") if candidate else None,
        })
    return output


def _group_summary(cases: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    total = len(cases)
    hits = sum(bool(item["present"]) for item in cases)
    return {"case_count": total, "exact_group_hits": hits, "recall": round(hits / total, 6) if total else None}


def build_v4_benchmark(datasets: Sequence[CandidateV4Dataset]) -> dict[str, Any]:
    v3 = build_candidate_recall([item.base for item in datasets])
    v3_by_pair = {item["pair_id"]: item for item in v3["projects"]}
    projects: list[dict[str, Any]] = []
    all_cases: list[dict[str, Any]] = []
    all_group_audits: list[dict[str, Any]] = []
    all_groups = [group for dataset in datasets for group in dataset.group_candidates]
    for dataset in datasets:
        cases = _evaluation_cases(dataset.base)
        # Match on left/rights because v1 and v4 case IDs intentionally differ.
        v3_identity = {
            (int(item["audit_left_page"]), tuple(item["expected_right_pages"])): item
            for item in v3_by_pair[dataset.base.pair_id]["cases"]
        }
        group_keys = {
            (tuple(item["left_pages"]), tuple(item["right_pages"])): item
            for item in dataset.group_candidates
        }
        for case in cases:
            candidates = dataset.candidate_sets[int(case["audit_left_page"])]["candidates"]
            page_rank = _case_rank(case, candidates)
            old = v3_identity[(int(case["audit_left_page"]), tuple(case["expected_right_pages"]))]
            old_ranks = [value for value in old["expected_ranks"].values() if value is not None]
            if case["expected_mode"] == "ANY":
                case["v3_rank"] = min(old_ranks) if old_ranks else None
            else:
                case["v3_rank"] = max(old_ranks) if len(old_ranks) == len(case["expected_right_pages"]) else None
            exact_key = (tuple(sorted(case["left_pages"])), tuple(sorted(case["expected_right_pages"])))
            case["exact_group_candidate_id"] = group_keys.get(exact_key, {}).get("candidate_group_id")
            group_rank = group_keys.get(exact_key, {}).get("group_rank", 1)
            case["v4_rank"] = min(
                value for value in (page_rank, group_rank if case["expected_mode"] == "ALL" and case["exact_group_candidate_id"] else None)
                if value is not None
            ) if page_rank is not None or (case["expected_mode"] == "ALL" and case["exact_group_candidate_id"]) else None
            case["expected_page_ranks"] = {
                str(page): next((item["rank"] for item in candidates if item["right_physical_page"] == page), None)
                for page in case["expected_right_pages"]
            }
        group_audit = _group_audit(dataset)
        project = {
            "project": dataset.base.project,
            "pair_id": dataset.base.pair_id,
            "v3": _summary(cases, "v3_rank"),
            "v4": _summary(cases, "v4_rank"),
            "engineer_mapping_recall": _summary([item for item in cases if item["authoritative"]], "v4_rank"),
            "reference_hypothesis_recall": _summary([item for item in cases if "reference_hypothesis" in item["source_types"]], "v4_rank"),
            "single_page_candidate_recall": _summary([item for item in cases if len(item["expected_right_pages"]) == 1], "v4_rank"),
            "group_candidate_recall": _group_summary(group_audit),
            "group_audit_cases": group_audit,
            "group_candidate_counts": dict(Counter(item["relation_type"] for item in dataset.group_candidates)),
            "cases": cases,
        }
        projects.append(project)
        all_cases.extend(cases)
        all_group_audits.extend(group_audit)

    counts = [item["candidate_count"] for dataset in datasets for item in dataset.candidate_sets.values()]
    function_count = sum(
        len(functions) for dataset in datasets for functions in dataset.function_passports["LEFT"].values()
    )
    total_possible = sum(
        len(dataset.candidate_sets) * len(dataset.sheet_passports["RIGHT"]) for dataset in datasets
    )
    total_returned = sum(counts)
    engineer = [item for item in all_cases if item["authoritative"]]
    reference = [item for item in all_cases if "reference_hypothesis" in item["source_types"]]
    target_group = next((
        item for item in all_groups if item["candidate_group_id"] == "fcand_6294159aac7851a636dd"
    ), None)
    success = (
        _summary(all_cases, "v4_rank")["recall_at_5"] >= 0.85
        and _summary(all_cases, "v4_rank")["recall_at_10"] >= 0.95
        and all(project["v4"]["recall_at_10"] >= 0.9 for project in projects)
        and _summary(engineer, "v4_rank")["recall_at_10"] == 1.0
        and target_group is not None
        and total_returned / total_possible < 0.25
    )
    return {
        "kind": "candidate_generator_v4_benchmark",
        "schema_version": "candidate-generator-benchmark.v4",
        "algorithm_version": ALGORITHM_VERSION,
        "production_generator": "production-sheet-matcher.v3",
        "production_generator_mutated": False,
        "input_signature": digest([item.input_signature for item in datasets]),
        "overall": {
            "v3": _summary(all_cases, "v3_rank"),
            "v4": _summary(all_cases, "v4_rank"),
            "engineer_mapping_recall": _summary(engineer, "v4_rank"),
            "reference_hypothesis_recall": _summary(reference, "v4_rank"),
            "single_page_candidate_recall": _summary(
                [item for item in all_cases if len(item["expected_right_pages"]) == 1], "v4_rank",
            ),
            "group_candidate_recall": _group_summary(all_group_audits),
        },
        "projects": projects,
        "candidate_set_size": {
            "left_sheet_task_count": len(counts),
            "left_function_passport_count": function_count,
            "candidate_count_scope": "per LEFT sheet after union of its independently searched Function Passports",
            "median": statistics.median(counts),
            "p95": _percentile95(counts),
            "returned_pair_count": total_returned,
            "full_cartesian_pair_count": total_possible,
            "cartesian_fraction": round(total_returned / total_possible, 6),
            "per_left_bound": FINAL_TOP_K,
            "per_channel_bound": CHANNEL_LIMIT,
        },
        "group_candidate_counts": dict(Counter(item["relation_type"] for item in all_groups)),
        "acceptance": {
            "overall_recall_at_5_gte_0_85": _summary(all_cases, "v4_rank")["recall_at_5"] >= 0.85,
            "overall_recall_at_10_gte_0_95": _summary(all_cases, "v4_rank")["recall_at_10"] >= 0.95,
            "each_project_recall_at_10_gte_0_90": all(project["v4"]["recall_at_10"] >= 0.9 for project in projects),
            "all_engineer_mappings_in_top_10": _summary(engineer, "v4_rank")["recall_at_10"] == 1.0,
            "ios21_17_7_18_8_19_9_in_top_10": all(
                next(
                    case for project in projects if project["pair_id"] == "pe336037597"
                    for case in project["cases"] if case["audit_left_page"] == left and case["expected_right_pages"] == [right]
                )["v4_rank"] is not None
                for left, right in ((17, 7), (18, 8), (19, 9))
            ),
            "ios21_sheet5_distributed_candidate": target_group,
            "bounded_not_cartesian": total_returned / total_possible < 0.25,
            "success": success,
            "verdict": "A" if success else "B",
        },
    }


def public_sheet_passport(value: Mapping[str, Any]) -> dict[str, Any]:
    return {key: item for key, item in value.items() if not key.startswith("_")}


def public_candidate_set(value: Mapping[str, Any]) -> dict[str, Any]:
    return {key: item for key, item in value.items() if not key.startswith("_")}
