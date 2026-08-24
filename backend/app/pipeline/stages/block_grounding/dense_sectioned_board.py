"""Production profile for dense sectioned switchboards (ГРЩ / ВРУ).

The profile consumes :class:`VectorEvidence`; it never opens or parses a PDF.
All rules below describe recognizers, roles, layout and normalization for the
``dense_sectioned_board`` dialect.  There are no block ids, page ids, equipment
lists, or expected section counts in production code.
"""
from __future__ import annotations

import collections
import itertools
import re
from dataclasses import dataclass
from typing import Optional

from .system_graph import (
    SCHEMA_VERSION,
    make_edge,
    make_node,
    union_bbox,
    validate_system_graph,
)
from .vector_evidence import (
    VectorEvidence,
    _cluster_by_gap,
    _median,
    bind_offset_columns,
)


PROFILE_ID = "dense_sectioned_board"
PROFILE_VERSION = "dense-sectioned-board-v1"
UNKNOWN_PROFILE = "UNKNOWN"


@dataclass(frozen=True)
class Token:
    x0: float
    y0: float
    x1: float
    y1: float
    text: str

    @property
    def cx(self) -> float:
        return (self.x0 + self.x1) / 2.0

    @property
    def cy(self) -> float:
        return (self.y0 + self.y1) / 2.0

    @property
    def height(self) -> float:
        return max(0.1, self.y1 - self.y0)

    def bbox(self) -> list[float]:
        return [round(self.x0, 3), round(self.y0, 3), round(self.x1, 3), round(self.y1, 3)]


DEVICE_RE = re.compile(
    r"^(?P<section>\d{0,2})(?P<kind>QF|QS|SF|FU|FV|HL|KM|KA|ВР|ВН|РН|ППН|УЗИП|ОПН)"
    r"(?P<number>\d*(?:\.\d+)*)$",
    re.IGNORECASE,
)
SWITCHING_KINDS = frozenset({"QF", "QS", "SF", "ВР", "ВН"})
DEVICE_TYPES = {
    "QF": "CIRCUIT_BREAKER",
    "SF": "CIRCUIT_BREAKER",
    "QS": "SWITCH_DISCONNECTOR",
    "ВР": "SWITCH_DISCONNECTOR",
    "ВН": "LOAD_BREAK_SWITCH",
    "FU": "FUSE",
    "ППН": "FUSE",
    "РН": "VOLTAGE_RELAY",
    "УЗИП": "SURGE_PROTECTION",
    "ОПН": "SURGE_PROTECTION",
    "FV": "SURGE_PROTECTION",
    "HL": "INDICATOR",
    "KM": "CONTACTOR",
    "KA": "RELAY",
}

RATING_RE = re.compile(r"^(\d{2,5})\s*[АA]$", re.IGNORECASE)
BUS_MARK_RE = re.compile(r"^L1[,\-]\s*L2[,\-]\s*L3$|^PEN$", re.IGNORECASE)
SECTION_NAME_RE = re.compile(r"^(РП\d+|с\.ш\.?\d*|секц\w*)$", re.IGNORECASE)
SOURCE_TP_RE = re.compile(r"^ТП\d*$", re.IGNORECASE)
SOURCE_TRANSFORMER_RE = re.compile(r"^\d?[ТT]\d$", re.IGNORECASE)
SOURCE_INPUT_RE = re.compile(r"^Ввод$", re.IGNORECASE)
BUSWAY_RE = re.compile(r"^Шинопровод|^Шинопр", re.IGNORECASE)
METER_RE = re.compile(
    r"^\d?[ТT][ТTA]\d|^Wh\d?$|^PW\d?$|Меркур|НАРТИС|Мультиметр|Анализатор|^МТ-7",
    re.IGNORECASE,
)
COMPENSATION_RE = re.compile(r"^(АУКРМ|УКРМ|КРМ)[\w\-№.]*$", re.IGNORECASE)
SERVICE_RE = re.compile(r"УЗИП|ОПН|^FV\d|^FU\d|^ППН|разрядник", re.IGNORECASE)
RESERVE_RE = re.compile(r"^Резерв", re.IGNORECASE)
CABLE_RE = re.compile(r"ППГ|ВВГ|КППГ|NYM|КПС|ПуГП", re.IGNORECASE)
AVR_RE = re.compile(r"^(АВР|SA|SF/SA|Секц\.?)$", re.IGNORECASE)

DESTINATION_RE = re.compile(r"^\d{0,2}ГРЩ\d{0,2}[-–][\wА-Яа-я.\-]{2,}$")
POSITIONAL_RE = re.compile(r"^ГРЩ\d+[-–]РП\d+[-–]\d+$")
CONSUMER_RE = re.compile(
    r"^(?:ВРУ|ШУ|ЩУ|ШР|ЩР|ШК|ЩК|ШН|ЩН|ХМ|ДР|АУКРМ|УКРМ|КРМ|ЭБ|ЯСН|ЯТП|ГРЩ|Щ|Ш|Я)"
    r"(?:[0-9OОа-я]|[.\-–_][0-9A-ZА-Яа-я]|[A-ZА-Я][A-ZА-Я0-9OО.\-–_]*)?"
    r"[A-ZА-Яa-zа-я0-9.\-–_№]*$"
)

LATIN_TO_CYR = str.maketrans(
    {
        "A": "А",
        "B": "В",
        "C": "С",
        "E": "Е",
        "H": "Н",
        "K": "К",
        "M": "М",
        "O": "О",
        "P": "Р",
        "T": "Т",
        "X": "Х",
        "Y": "У",
    }
)
PANEL_FAMILY_RE = re.compile(r"^(ВРУ|ШУ|ШР|ЩУ|ЩР|ШК|ЩК)[.\-–_]?(?P<rest>.*)$")
STAGE_PREFIX_RE = re.compile(r"^\d{0,2}ГРЩ\d{0,2}[-–](?P<rest>.+)$")
ORDINARY_WORD_RE = re.compile(r"^[А-ЯA-Z][а-яa-z]{2,}$")


def _tokens(evidence: VectorEvidence) -> list[Token]:
    output = []
    for word in evidence.visual_words:
        try:
            text = str(word[4]).strip()
            if text:
                output.append(
                    Token(
                        float(word[0]),
                        float(word[1]),
                        float(word[2]),
                        float(word[3]),
                        text,
                    )
                )
        except (IndexError, TypeError, ValueError):
            continue
    return output


def _token_evidence(token: Token, role: str) -> dict:
    return {
        "kind": "token",
        "role": role,
        "value": token.text,
        "bbox": token.bbox(),
        "source_tokens": [token.text],
    }


def _geometry_evidence(role: str, bbox, value, source_tokens=()) -> dict:
    return {
        "kind": "geometry",
        "role": role,
        "value": value,
        "bbox": [round(float(item), 3) for item in bbox],
        "source_tokens": [str(token) for token in source_tokens],
    }


def _bbox_for_tokens(tokens: list[Token]) -> list[float]:
    return union_bbox(*(token.bbox() for token in tokens))


def extract_device_candidates(evidence: VectorEvidence) -> list[dict]:
    """Recognize device-like tokens without assigning topology roles."""
    candidates = []
    for token in _tokens(evidence):
        match = DEVICE_RE.fullmatch(token.text)
        if not match:
            continue
        kind = match.group("kind").upper()
        number = match.group("number") or ""
        section = match.group("section") or None
        confidence = 0.96 if number else 0.78
        candidates.append(
            {
                "type_candidate": DEVICE_TYPES.get(kind, "UNKNOWN_DEVICE"),
                "kind": kind,
                "label": token.text,
                "label_section": section,
                "number": number,
                "bbox": token.bbox(),
                "cx": round(token.cx, 3),
                "cy": round(token.cy, 3),
                "height": round(token.height, 3),
                "confidence": confidence,
                "evidence": [_token_evidence(token, "device_designation")],
                "source_tokens": [token.text],
                "token": token,
            }
        )
    return candidates


def _device_rows(candidates: list[dict]) -> list[dict]:
    switching = [candidate for candidate in candidates if candidate["kind"] in SWITCHING_KINDS]
    if not switching:
        return []
    height = _median([candidate["height"] for candidate in switching]) or 8.0
    tolerance = max(6.0, 2.5 * height)
    y_clusters = _cluster_by_gap(
        [candidate["cy"] for candidate in switching], gap=tolerance
    )
    rows = []
    for index, cluster in enumerate(y_clusters, 1):
        low, high = min(cluster) - tolerance, max(cluster) + tolerance
        members = sorted(
            [candidate for candidate in switching if low <= candidate["cy"] <= high],
            key=lambda candidate: candidate["cx"],
        )
        xs = [candidate["cx"] for candidate in members]
        gaps = [xs[position + 1] - xs[position] for position in range(len(xs) - 1)]
        step = _median([gap for gap in gaps if gap > 1]) or 0.0
        regular = (
            sum(1 for gap in gaps if step and 0.45 * step <= gap <= 1.8 * step)
            / max(len(gaps), 1)
        )
        prefixed = sum(bool(candidate["label_section"]) for candidate in members)
        rows.append(
            {
                "id": f"ROW{index}",
                "y": round(_median([candidate["cy"] for candidate in members]), 3),
                "count": len(members),
                "step": round(step, 3),
                "regularity": round(regular, 3),
                "prefixed_fraction": round(prefixed / max(len(members), 1), 3),
                "bbox": _bbox_for_tokens([candidate["token"] for candidate in members]),
                "labels": [candidate["label"] for candidate in members],
                "members": members,
            }
        )
    return rows


def _device_columns(candidates: list[dict], step: float) -> list[dict]:
    if not candidates:
        return []
    tolerance = max(5.0, 0.35 * (step or 20.0))
    clusters = _cluster_by_gap([candidate["cx"] for candidate in candidates], gap=tolerance)
    columns = []
    for index, cluster in enumerate(clusters, 1):
        low, high = min(cluster) - tolerance, max(cluster) + tolerance
        members = [candidate for candidate in candidates if low <= candidate["cx"] <= high]
        columns.append(
            {
                "id": f"COL{index}",
                "x": round(_median([candidate["cx"] for candidate in members]), 3),
                "count": len(members),
                "labels": [candidate["label"] for candidate in members],
                "bbox": union_bbox(*(candidate["bbox"] for candidate in members)),
            }
        )
    return columns


def _horizontal_bus_lines(evidence: VectorEvidence, *, min_length: float) -> list[dict]:
    output, seen = [], set()
    for line in evidence.lines:
        if len(line) != 4:
            continue
        x0, y0, x1, y1 = (float(value) for value in line)
        length = abs(x1 - x0)
        if length < min_length or abs(y1 - y0) > max(2.0, 0.05 * length):
            continue
        bbox = [min(x0, x1), min(y0, y1), max(x0, x1), max(y0, y1)]
        key = tuple(round(value / 2.0) for value in bbox)
        if key in seen:
            continue
        seen.add(key)
        output.append({"bbox": bbox, "length": round(length, 3)})
    return sorted(output, key=lambda item: item["length"], reverse=True)


def detect_dense_sectioned_board(evidence: VectorEvidence) -> dict:
    """Return dense profile detection or explicit ``UNKNOWN`` with signals."""
    candidates = extract_device_candidates(evidence)
    rows = _device_rows(candidates)
    outgoing = max(rows, key=lambda row: (row["count"], row["regularity"]), default=None)
    switching = [candidate for candidate in candidates if candidate["kind"] in SWITCHING_KINDS]
    qf_like = [candidate for candidate in switching if candidate["kind"] in {"QF", "QS", "SF"}]
    prefix_groups = sorted(
        {candidate["label_section"] for candidate in qf_like if candidate["label_section"]}
    )
    prefixed_fraction = sum(bool(candidate["label_section"]) for candidate in qf_like) / max(
        len(qf_like), 1
    )
    undotted_fraction = sum("." not in candidate["number"] for candidate in qf_like) / max(
        len(qf_like), 1
    )
    step = float((outgoing or {}).get("step") or 0.0)
    row_members = (outgoing or {}).get("members") or []
    xs = [candidate["cx"] for candidate in row_members]
    gaps = [xs[index + 1] - xs[index] for index in range(len(xs) - 1)]
    median_gap = _median([gap for gap in gaps if gap > 1]) or 0.0
    largest_gap = max(gaps, default=0.0)
    central_gap_ratio = largest_gap / max(median_gap, 1.0)
    page_width = float((evidence.page_size or [0.0, 0.0])[0])
    bus_lines = _horizontal_bus_lines(
        evidence, min_length=max(100.0, 0.08 * page_width, 3.0 * step)
    )
    tokens = _tokens(evidence)
    bus_markers = sum(bool(BUS_MARK_RE.fullmatch(token.text)) for token in tokens)
    tie_candidates = sum(
        candidate["kind"] in {"QS", "ВР", "ВН"} for candidate in switching
    )
    row_count = int((outgoing or {}).get("count") or 0)
    columns = _device_columns(switching, step)

    score_parts = {
        "device_density": min(len(switching) / 20.0, 1.0) * 0.18,
        "dense_row": min(row_count / 15.0, 1.0) * 0.16,
        "section_prefixes": (
            min(prefixed_fraction, 1.0) * 0.22 if 2 <= len(prefix_groups) <= 4 else 0.0
        ),
        "undotted_numbering": min(undotted_fraction, 1.0) * 0.10,
        "repeated_columns": min(len(columns) / 12.0, 1.0) * 0.10,
        "bus_evidence": min((len(bus_lines) > 0) + (bus_markers > 0), 2) / 2.0 * 0.14,
        "section_gap": min(central_gap_ratio / 2.0, 1.0) * 0.06,
        "tie_candidate": min(tie_candidates, 1) * 0.04,
    }
    confidence = round(sum(score_parts.values()), 3)
    semantic_section_signal = (
        2 <= len(prefix_groups) <= 4 and prefixed_fraction >= 0.55
    )
    geometric_section_signal = (
        undotted_fraction >= 0.8
        and central_gap_ratio >= 1.6
        and bool(bus_lines)
        and bool(tie_candidates)
    )
    detected = bool(
        evidence.extraction_ok
        and len(switching) >= 10
        and row_count >= 8
        and confidence >= 0.62
        and (semantic_section_signal or geometric_section_signal)
    )
    reasons = []
    if detected:
        reasons.append("dense repeated device row recovered")
        if semantic_section_signal:
            reasons.append("section prefixes agree with repeated columns")
        if bus_lines or bus_markers:
            reasons.append("bus geometry or bus labels present")
        if central_gap_ratio >= 1.6 or tie_candidates:
            reasons.append("section gap or switching tie candidate present")
    else:
        if len(switching) < 10:
            reasons.append("insufficient_device_density")
        if row_count < 8:
            reasons.append("no_dense_device_row")
        if not (semantic_section_signal or geometric_section_signal):
            reasons.append("section_evidence_insufficient")
        if confidence < 0.62:
            reasons.append("profile_confidence_below_threshold")
    return {
        "id": PROFILE_ID if detected else UNKNOWN_PROFILE,
        "detected": detected,
        "profile_confidence": confidence,
        "threshold": 0.62,
        "reasons": reasons,
        "signals": {
            "device_candidates": len(candidates),
            "switching_devices": len(switching),
            "dense_row_devices": row_count,
            "device_rows": len(rows),
            "device_columns": len(columns),
            "prefixed_fraction": round(prefixed_fraction, 3),
            "undotted_fraction": round(undotted_fraction, 3),
            "prefix_groups": prefix_groups,
            "long_horizontal_lines": len(bus_lines),
            "bus_markers": bus_markers,
            "tie_candidates": tie_candidates,
            "central_gap_ratio": round(central_gap_ratio, 3),
            "score_parts": {key: round(value, 3) for key, value in score_parts.items()},
        },
        "layout": {
            "rows": [_serializable_row(row) for row in rows],
            "columns": columns,
            "selected_row_id": (outgoing or {}).get("id"),
        },
    }


def _serializable_row(row: dict) -> dict:
    return {key: value for key, value in row.items() if key != "members"}


def _partition_sections(row: dict, evidence: VectorEvidence):
    members = list(row["members"])
    xs = [candidate["cx"] for candidate in members]
    gaps = [xs[index + 1] - xs[index] for index in range(len(xs) - 1)]
    step = _median([gap for gap in gaps if gap > 1]) or 60.0
    prefixes = [candidate["label_section"] for candidate in members]
    distinct = sorted({prefix for prefix in prefixes if prefix})
    use_prefixes = (
        len([prefix for prefix in prefixes if prefix]) >= 0.7 * len(members)
        and 2 <= len(distinct) <= 4
    )
    conflicts = []

    if use_prefixes:
        count = len(distinct)
        best = None
        best_score = -1.0
        max_gap = max(gaps, default=1.0)
        for cuts in itertools.combinations(range(1, len(members)), count - 1):
            bounds = (0,) + cuts + (len(members),)
            for order in itertools.permutations(distinct):
                agreement = 0
                for group_index in range(count):
                    for item_index in range(bounds[group_index], bounds[group_index + 1]):
                        agreement += prefixes[item_index] == order[group_index]
                gap_bonus = sum(gaps[cut - 1] for cut in cuts) / (
                    max(max_gap, 1.0) * max(count - 1, 1)
                )
                score = agreement / max(len(members), 1) + 0.5 * gap_bonus
                if score > best_score:
                    best_score = score
                    best = bounds, order, agreement
        bounds, order, agreement = best
        clusters, keys = [], []
        for group_index, section_prefix in enumerate(order):
            cluster = members[bounds[group_index] : bounds[group_index + 1]]
            if not cluster:
                continue
            clusters.append(cluster)
            keys.append(section_prefix)
            for candidate in cluster:
                if candidate["label_section"] and candidate["label_section"] != section_prefix:
                    conflicts.append(
                        {
                            "label": candidate["label"],
                            "bbox": candidate["bbox"],
                            "label_section": candidate["label_section"],
                            "geometric_section": section_prefix,
                            "reason": "label prefix conflicts with contiguous X partition",
                        }
                    )
        prefix_agreement = agreement / max(len([prefix for prefix in prefixes if prefix]), 1)
        partition_source = "label_prefix+x_gap+bus_geometry"
    else:
        if len(members) < 2:
            return [], conflicts, step
        largest_gap_index = max(range(len(gaps)), key=lambda index: gaps[index])
        if gaps[largest_gap_index] < max(1.6 * step, step + 25.0):
            clusters = [members]
        else:
            clusters = [members[: largest_gap_index + 1], members[largest_gap_index + 1 :]]
        keys = [None] * len(clusters)
        prefix_agreement = None
        partition_source = "x_gap+bus_geometry"

    tokens = _tokens(evidence)
    section_names = [token for token in tokens if SECTION_NAME_RE.fullmatch(token.text)]
    bus_lines = _horizontal_bus_lines(
        evidence, min_length=max(3.0 * step, 0.05 * float(evidence.page_size[0]))
    )
    sections = []
    row_y = float(row["y"])
    for index, (cluster, prefix) in enumerate(zip(clusters, keys), 1):
        x0, x1 = cluster[0]["cx"], cluster[-1]["cx"]
        expanded = [x0 - 0.6 * step, x1 + 0.6 * step]
        names = [
            token
            for token in section_names
            if expanded[0] - 2 * step <= token.cx <= expanded[1] + 2 * step
        ]
        names.sort(key=lambda token: abs(token.cy - row_y))
        name_token = names[0] if names else None
        supported_lines = []
        for line in bus_lines:
            line_x0, _, line_x1, _ = line["bbox"]
            overlap = max(0.0, min(expanded[1], line_x1) - max(expanded[0], line_x0))
            if overlap >= 0.35 * max(expanded[1] - expanded[0], 1.0):
                supported_lines.append(line)
            if len(supported_lines) >= 3:
                break
        devices_bbox = union_bbox(*(candidate["bbox"] for candidate in cluster))
        section_bbox = union_bbox(
            devices_bbox,
            *(line["bbox"] for line in supported_lines),
            name_token.bbox() if name_token else None,
        )
        confidence = 0.45
        confidence += 0.20 if prefix is not None else 0.0
        confidence += 0.15 if supported_lines else 0.0
        confidence += 0.10 if name_token else 0.0
        confidence += 0.10 if prefix_agreement is None or prefix_agreement >= 0.8 else 0.0
        sections.append(
            {
                "id": f"BUS{index}",
                "index": index,
                "label_prefix": prefix,
                "name": name_token.text if name_token else f"секция {index}",
                "name_token": name_token,
                "x_range": [round(x0, 3), round(x1, 3)],
                "step": round(step, 3),
                "devices": cluster,
                "connected_devices": [candidate["label"] for candidate in cluster],
                "bus_lines": supported_lines,
                "bbox": section_bbox,
                "partition_source": partition_source,
                "prefix_agreement": (
                    round(prefix_agreement, 3) if prefix_agreement is not None else None
                ),
                "confidence": round(min(confidence, 1.0), 3),
            }
        )
    return sections, conflicts, step


def _rating_near(candidate: dict, tokens: list[Token], step: float):
    best = None
    token = candidate["token"]
    for other in tokens:
        match = RATING_RE.fullmatch(other.text)
        if not match:
            continue
        if abs(other.cx - token.cx) < 0.45 * step and 0 <= other.cy - token.cy < 6 * token.height:
            rating = int(match.group(1))
            if best is None or rating > best[0]:
                best = rating, other
    return best


def _classify_incoming(candidates, row, sections, step, evidence):
    row_y = float(row["y"])
    row_height = _median([candidate["height"] for candidate in row["members"]]) or 8.0
    below = [
        candidate
        for candidate in candidates
        if candidate["cy"] > row_y + 1.5 * row_height
    ]
    gaps = [
        (left["x_range"][1], right["x_range"][0], left["id"], right["id"])
        for left, right in zip(sections, sections[1:])
    ]
    tokens = _tokens(evidence)
    output = []
    for candidate in below:
        rating = _rating_near(candidate, tokens, step)
        in_gap = next(
            (gap for gap in gaps if gap[0] < candidate["cx"] < gap[1]), None
        )
        host = None
        if in_gap is None and sections:
            host = min(
                sections,
                key=lambda section: min(
                    abs(candidate["cx"] - section["x_range"][0]),
                    abs(candidate["cx"] - section["x_range"][1]),
                    0.0
                    if section["x_range"][0] <= candidate["cx"] <= section["x_range"][1]
                    else 1e9,
                ),
            )
        controls = [
            token
            for token in tokens
            if AVR_RE.fullmatch(token.text)
            and abs(token.cx - candidate["cx"]) < 0.8 * step
            and abs(token.cy - candidate["cy"]) < 14 * candidate["height"]
        ]
        output.append(
            {
                **candidate,
                "rating_a": rating[0] if rating else None,
                "rating_token": rating[1] if rating else None,
                "in_gap": in_gap[2:] if in_gap else None,
                "host_section": host["id"] if host else None,
                "control_tokens": controls,
                "role": None,
            }
        )

    for gap in gaps:
        pool = [item for item in output if item["in_gap"] == gap[2:]]
        if not pool:
            continue
        center = (gap[0] + gap[1]) / 2.0
        pool.sort(key=lambda item: (item["cy"], abs(item["cx"] - center)))
        pool[0]["role"] = "SECTION_DEVICE"

    # An input can sit just inside the visual break between two bus sections.
    # Once the uppermost commutation device has claimed the tie role, return
    # the remaining gap candidates to the nearest section.  Leaving them
    # sectionless lets a neighbouring fuse win the input-device rating contest.
    for item in output:
        if item["in_gap"] and item["role"] is None and item["host_section"] is None:
            item["host_section"] = min(
                sections,
                key=lambda section: min(
                    abs(item["cx"] - section["x_range"][0]),
                    abs(item["cx"] - section["x_range"][1]),
                ),
            )["id"]

    source_tokens = [
        token
        for token in tokens
        if SOURCE_TP_RE.fullmatch(token.text)
        or SOURCE_TRANSFORMER_RE.fullmatch(token.text)
        or SOURCE_INPUT_RE.fullmatch(token.text)
    ]
    for section in sections:
        pool = [
            item
            for item in output
            if item["role"] is None
            and item["host_section"] == section["id"]
        ]
        if not pool:
            continue

        def input_score(item):
            aligned_source = any(
                token.cy > item["cy"] and abs(token.cx - item["cx"]) < 3 * step
                for token in source_tokens
            )
            return (item["rating_a"] or 0, int(aligned_source), -item["cy"])

        selected = max(pool, key=input_score)
        if selected["rating_a"] is not None or input_score(selected)[1]:
            selected["role"] = "INPUT_DEVICE"
            selected["host_section"] = section["id"]

    for item in output:
        if item["role"] is None and item["type_candidate"] in {
            "FUSE",
            "VOLTAGE_RELAY",
            "SURGE_PROTECTION",
            "INDICATOR",
        }:
            item["role"] = "SERVICE_GROUP"
        if item["role"] is None:
            item["role"] = "UNKNOWN_NODE"
            if item["host_section"] is None and sections:
                item["host_section"] = min(
                    sections,
                    key=lambda section: min(
                        abs(item["cx"] - section["x_range"][0]),
                        abs(item["cx"] - section["x_range"][1]),
                    ),
                )["id"]
    return output


def _build_source_paths(incoming, evidence, step):
    tokens = _tokens(evidence)
    inputs = [item for item in incoming if item["role"] == "INPUT_DEVICE"]
    source_tp = [token for token in tokens if SOURCE_TP_RE.fullmatch(token.text)]
    transformers = [
        token for token in tokens if SOURCE_TRANSFORMER_RE.fullmatch(token.text)
    ]
    external = [token for token in tokens if SOURCE_INPUT_RE.fullmatch(token.text)]
    busways = [token for token in tokens if BUSWAY_RE.search(token.text)]
    paths = []
    for item in inputs:
        device = item["token"]

        def below(pool):
            return [
                token
                for token in pool
                if token.cy > device.cy and abs(token.cx - device.cx) < 3 * step
            ]

        anchor, representation = None, "UNKNOWN_SOURCE"
        if below(transformers):
            anchor = min(below(transformers), key=lambda token: abs(token.cx - device.cx))
            representation = "TRANSFORMER_EXPLICIT"
        elif below(source_tp):
            anchor = min(below(source_tp), key=lambda token: abs(token.cx - device.cx))
            representation = "UPSTREAM_TP_CONNECTION"
        elif below(external):
            anchor = min(below(external), key=lambda token: abs(token.cx - device.cx))
            representation = "EXTERNAL_FEEDER"
        intermediates = []
        for token in below(busways):
            if anchor and token.cy > anchor.cy:
                continue
            owner = min(inputs, key=lambda other: abs(other["cx"] - token.cx))
            if owner is item:
                intermediates.append(token)
        paths.append(
            {
                "input": item,
                "section": item["host_section"],
                "source_token": anchor,
                "source_role": "UPSTREAM_SUPPLY",
                "source_representation": representation,
                "intermediates": intermediates,
            }
        )
    return paths


def _looks_like_designation(value: str) -> bool:
    return not bool(ORDINARY_WORD_RE.fullmatch((value or "").strip()))


def normalize_token(value: str) -> str:
    normalized = (value or "").strip().strip(",;:()[]«»\"'")
    normalized = normalized.upper().replace("Ё", "Е").translate(LATIN_TO_CYR)
    normalized = normalized.replace(".", "-").replace("–", "-").replace("_", "-")
    return re.sub(r"-{2,}", "-", normalized).strip("-")


def canonical_identity(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    normalized = normalize_token(value)
    if POSITIONAL_RE.fullmatch(normalized):
        return None
    stage = STAGE_PREFIX_RE.fullmatch(normalized)
    if stage:
        normalized = stage.group("rest")
    if re.fullmatch(r"РП\d+-\d+", normalized):
        return None
    family = PANEL_FAMILY_RE.fullmatch(normalized)
    if family:
        rest = family.group("rest")
        if rest and not re.fullmatch(r"\d+[А-Я]?|[А-Я]", rest):
            normalized = rest
    if normalized.startswith("Щ"):
        normalized = "Ш" + normalized[1:]
    return normalized or None


def _bind_labels(row, evidence, y_low, y_high, pattern, reject=None):
    members = row["members"]
    if not members:
        return {}, {}
    step = float(row["step"] or 60.0)
    x_low = members[0]["cx"] - 1.5 * step
    x_high = members[-1]["cx"] + 1.5 * step
    tokens = [
        token
        for token in _tokens(evidence)
        if pattern.fullmatch(token.text)
        and y_low <= token.cy <= y_high
        and x_low <= token.cx <= x_high
        and not (reject and reject(token.text))
    ]
    if not tokens:
        return {}, {}
    height = _median([token.height for token in tokens]) or 8.0
    tolerance = max(4.0, 1.2 * height)
    label_rows = _cluster_by_gap([token.cy for token in tokens], gap=tolerance)
    label_rows.sort(key=lambda values: abs(_median(values) - row["y"]))
    devices = [
        (candidate["cx"], candidate["cy"], _device_key(candidate))
        for candidate in members
    ]
    result, conflicts = {}, {}
    for label_row in label_rows:
        low, high = min(label_row) - tolerance, max(label_row) + tolerance
        labels = sorted(
            (token.cx, token.cy, token.text)
            for token in tokens
            if low <= token.cy <= high
        )
        assigned, row_conflicts = bind_offset_columns(devices, labels)
        for key, value in assigned.items():
            if value and key not in result:
                result[key] = value
        for key, notes in row_conflicts.items():
            conflicts.setdefault(key, []).extend(notes)
    return result, conflicts


def _device_key(candidate: dict) -> str:
    return f"{candidate['label']}@{round(candidate['cx'])}"


def _extract_outgoing(row, sections, evidence):
    row_y = float(row["y"])
    step = float(row["step"] or 60.0)
    y_low = min((token.cy for token in _tokens(evidence)), default=row_y) - 1.0
    y_high = row_y - 8.0
    destination, destination_conflicts = _bind_labels(
        row, evidence, y_low, y_high, DESTINATION_RE
    )
    positional, _ = _bind_labels(row, evidence, y_low, y_high, POSITIONAL_RE)
    consumer, consumer_conflicts = _bind_labels(
        row,
        evidence,
        y_low,
        y_high,
        CONSUMER_RE,
        reject=lambda value: (
            canonical_identity(value) is None
            or bool(DESTINATION_RE.fullmatch(value))
            or not _looks_like_designation(value)
        ),
    )
    reserve, _ = _bind_labels(row, evidence, y_low, y_high, RESERVE_RE)
    tokens = _tokens(evidence)
    outgoing = []
    section_by_label = {
        candidate["label"] + f"@{round(candidate['cx'])}": section
        for section in sections
        for candidate in section["devices"]
    }
    for candidate in row["members"]:
        key = _device_key(candidate)
        section = section_by_label[key]
        raw_candidates = [
            ("destination_code", destination.get(key)),
            ("consumer_label", consumer.get(key)),
            ("positional_code", positional.get(key)),
        ]
        identities = []
        evidence_items = list(candidate["evidence"])
        for role, value in raw_candidates:
            if not value:
                continue
            identity = canonical_identity(value)
            evidence_items.append(
                {
                    "kind": "token",
                    "role": role,
                    "value": value,
                    "canonical": identity,
                    "bbox": candidate["bbox"],
                    "source_tokens": [value],
                }
            )
            if identity:
                identities.append((role, value, identity))
        identity_set = sorted({identity for _, _, identity in identities})
        identity = identities[0][2] if identities else None
        display = consumer.get(key) or destination.get(key) or positional.get(key)
        column_tokens = [
            token
            for token in tokens
            if abs(token.cx - candidate["cx"]) < 0.55 * step
            and y_low <= token.cy <= row_y + 80.0
        ]
        ratings = [
            int(RATING_RE.fullmatch(token.text).group(1))
            for token in column_tokens
            if RATING_RE.fullmatch(token.text)
        ]
        cables = [token.text for token in column_tokens if CABLE_RE.search(token.text)]
        is_reserve = bool(reserve.get(key)) or any(
            RESERVE_RE.fullmatch(token.text) for token in column_tokens
        )
        conflicts = list(destination_conflicts.get(key) or []) + list(
            consumer_conflicts.get(key) or []
        )
        if len(identity_set) > 1:
            conflicts.append("identity evidence conflicts: " + ", ".join(identity_set))
        confidence = 0.35
        if len(identity_set) == 1:
            confidence = 0.72 if len(identities) == 1 else 0.92
        if len(identity_set) > 1:
            confidence = 0.35
        status = "RESERVE" if is_reserve and not identity else "ACTIVE" if identity else "UNKNOWN"
        outgoing.append(
            {
                **candidate,
                "device_key": key,
                "section": section["id"],
                "display_label": display,
                "canonical_identity": identity,
                "identity_set": identity_set,
                "identity_confidence": confidence,
                "status": status,
                "rating_a": max(ratings) if ratings else None,
                "cables": cables[:2],
                "evidence": evidence_items,
                "conflicts": conflicts,
            }
        )
    return outgoing


def _functional_groups(evidence, sections, step):
    tokens = _tokens(evidence)
    definitions = (
        ("METERING_GROUP", [token for token in tokens if METER_RE.search(token.text)]),
        (
            "COMPENSATION_GROUP",
            [token for token in tokens if COMPENSATION_RE.fullmatch(token.text)],
        ),
        ("SERVICE_GROUP", [token for token in tokens if SERVICE_RE.search(token.text)]),
    )
    groups = []
    for group_type, members in definitions:
        by_section = collections.defaultdict(list)
        for token in members:
            inside = next(
                (
                    section
                    for section in sections
                    if section["x_range"][0] - 2 * step
                    <= token.cx
                    <= section["x_range"][1] + 2 * step
                ),
                None,
            )
            if inside:
                by_section[inside["id"]].append(token)
        for section_id, section_tokens in sorted(by_section.items()):
            groups.append(
                {
                    "id": f"{group_type}:{section_id}",
                    "type": group_type,
                    "section": section_id,
                    "tokens": section_tokens,
                    "bbox": _bbox_for_tokens(section_tokens),
                    "confidence": min(0.95, 0.55 + 0.05 * len(section_tokens)),
                }
            )
    return groups


def build_dense_sectioned_board_graph(
    evidence: VectorEvidence,
    *,
    detection: Optional[dict] = None,
    discipline: str = "ЭОМ",
) -> dict:
    """Build one SYSTEM_GRAPH from already extracted vector evidence."""
    detection = detection or detect_dense_sectioned_board(evidence)
    base = {
        "schema_version": SCHEMA_VERSION,
        "profile_id": detection["id"],
        "block": {
            "block_id": str(evidence.provenance.get("block_id") or ""),
            "page_index": evidence.page_index,
            "rotation": evidence.provenance.get("rotation_degrees"),
            "bbox_visual_pt": evidence.block_bbox,
        },
        "discipline": discipline,
        "profile": detection,
        "nodes": [],
        "edges": [],
        "quality": {
            "backbone_recovered": False,
            "source_confidence": None,
            "bus_confidence": None,
            "section_confidence": None,
            "device_coverage": 0.0,
            "feeder_coverage": 0.0,
            "identity_coverage": 0.0,
            "unknown_nodes": 0,
            "unknown_edges": 0,
        },
        "quality_gates": {
            "extraction": evidence.extraction_gate,
            "detection": {
                "detected": detection["detected"],
                "profile_confidence": detection["profile_confidence"],
                "reasons": detection["reasons"],
            },
            "structure": {"backbone_recovered": False, "reasons": []},
        },
        "provenance": {
            "profile": PROFILE_ID,
            "profile_version": PROFILE_VERSION,
            "manual_cases": False,
            "vector_evidence": dict(evidence.provenance),
        },
        "analysis": {
            "device_candidates": [],
            "device_rows": detection["layout"]["rows"],
            "device_columns": detection["layout"]["columns"],
            "section_candidates": [],
        },
        "warnings": [],
    }
    if not evidence.extraction_ok or not detection["detected"]:
        base["quality_gates"]["structure"]["reasons"] = [
            "profile_not_detected" if evidence.extraction_ok else "extraction_failed"
        ]
        base["validation"] = validate_system_graph(base)
        return base

    candidates = extract_device_candidates(evidence)
    rows = _device_rows(candidates)
    row = next(
        (item for item in rows if item["id"] == detection["layout"]["selected_row_id"]),
        None,
    )
    if row is None:
        base["quality_gates"]["structure"]["reasons"] = ["device_row_not_found"]
        base["validation"] = validate_system_graph(base)
        return base
    sections, section_conflicts, step = _partition_sections(row, evidence)
    incoming = _classify_incoming(candidates, row, sections, step, evidence)
    source_paths = _build_source_paths(incoming, evidence, step)
    outgoing = _extract_outgoing(row, sections, evidence)
    groups = _functional_groups(evidence, sections, step)

    serializable_candidates = [
        {key: value for key, value in candidate.items() if key != "token"}
        for candidate in candidates
    ]
    base["analysis"] = {
        "device_candidates": serializable_candidates,
        "device_rows": [_serializable_row(item) for item in rows],
        "device_columns": _device_columns(candidates, step),
        "section_candidates": [
            {
                key: value
                for key, value in section.items()
                if key not in {"devices", "name_token", "bus_lines"}
            }
            | {
                "bus_line_evidence": section["bus_lines"],
                "device_labels": [device["label"] for device in section["devices"]],
            }
            for section in sections
        ],
        "incoming_roles": [
            {
                "label": item["label"],
                "bbox": item["bbox"],
                "role": item["role"],
                "host_section": item["host_section"],
                "in_gap": item["in_gap"],
                "rating_a": item["rating_a"],
            }
            for item in incoming
        ],
        "section_label_conflicts": section_conflicts,
    }

    nodes, edges, node_index = [], [], {}

    def add_node(node):
        nodes.append(node)
        node_index[node["id"]] = node
        return node["id"]

    def add_edge(edge_type, source, target, confidence, evidence_items):
        edge_id = f"{edge_type}:{source}->{target}"
        edges.append(
            make_edge(
                edge_id,
                edge_type,
                source,
                target,
                confidence=confidence,
                evidence=evidence_items,
                source_bbox=node_index[source]["bbox"],
                target_bbox=node_index[target]["bbox"],
                source_tokens=(
                    node_index[source]["source_tokens"] + node_index[target]["source_tokens"]
                ),
            )
        )

    for section in sections:
        evidence_items = [
            _geometry_evidence(
                "device_cluster",
                section["bbox"],
                {
                    "x_range": section["x_range"],
                    "connected_devices": section["connected_devices"],
                    "partition_source": section["partition_source"],
                },
                section["connected_devices"],
            )
        ]
        if section["name_token"]:
            evidence_items.append(_token_evidence(section["name_token"], "section_name"))
        for line in section["bus_lines"]:
            evidence_items.append(
                _geometry_evidence("horizontal_bus_line", line["bbox"], line)
            )
        add_node(
            make_node(
                section["id"],
                "BUS_SECTION",
                confidence=section["confidence"],
                evidence=evidence_items,
                bbox=section["bbox"],
                source_tokens=section["connected_devices"],
                label=section["name"],
                canonical_identity=f"SECTION#{section['index']}",
                attrs={
                    "x_range": section["x_range"],
                    "device_count": len(section["devices"]),
                    "label_prefix": section["label_prefix"],
                },
            )
        )

    for index, path in enumerate(source_paths, 1):
        input_item = path["input"]
        source_token = path["source_token"]
        source_bbox = source_token.bbox() if source_token else input_item["bbox"]
        source_evidence = (
            [_token_evidence(source_token, "source_anchor")]
            if source_token
            else [
                _geometry_evidence(
                    "source_path_unresolved",
                    input_item["bbox"],
                    {"aligned_input": input_item["label"]},
                    [input_item["label"]],
                )
            ]
        )
        source_id = f"SOURCE{index}"
        add_node(
            make_node(
                source_id,
                "SOURCE",
                confidence=0.95 if source_token else 0.3,
                evidence=source_evidence,
                bbox=source_bbox,
                source_tokens=[source_token.text] if source_token else [input_item["label"]],
                label=source_token.text if source_token else None,
                canonical_identity=f"SOURCE_PATH#{path['section']}",
                source_role=path["source_role"],
                source_representation=path["source_representation"],
                attrs={"section": path["section"]},
            )
        )
        previous = source_id
        for intermediate_index, token in enumerate(path["intermediates"], 1):
            intermediate_id = f"SOURCE{index}:PATH{intermediate_index}"
            add_node(
                make_node(
                    intermediate_id,
                    "SERVICE_GROUP",
                    confidence=0.75,
                    evidence=[_token_evidence(token, "source_path_element")],
                    bbox=token.bbox(),
                    source_tokens=[token.text],
                    label=token.text,
                    canonical_identity=f"SOURCE_PATH_ELEMENT#{path['section']}",
                    section=path["section"],
                    attrs={"subclass": "BUSWAY"},
                )
            )
            add_edge(
                "FEEDS",
                previous,
                intermediate_id,
                0.75,
                [_geometry_evidence("source_path_order", union_bbox(node_index[previous]["bbox"], token.bbox()), {})],
            )
            previous = intermediate_id
        input_id = f"INPUT{index}"
        input_evidence = list(input_item["evidence"])
        if input_item["rating_token"]:
            input_evidence.append(_token_evidence(input_item["rating_token"], "rating"))
        input_evidence.extend(
            _token_evidence(token, "control") for token in input_item["control_tokens"]
        )
        add_node(
            make_node(
                input_id,
                "INPUT_DEVICE",
                confidence=0.95 if input_item["rating_a"] else 0.72,
                evidence=input_evidence,
                bbox=union_bbox(
                    input_item["bbox"],
                    input_item["rating_token"].bbox() if input_item["rating_token"] else None,
                ),
                source_tokens=[input_item["label"]],
                label=input_item["label"],
                canonical_identity=f"INPUT#{path['section']}",
                section=path["section"],
                attrs={
                    "rating_a": input_item["rating_a"],
                    "control": [token.text for token in input_item["control_tokens"]],
                },
            )
        )
        add_edge(
            "FEEDS",
            previous,
            input_id,
            0.9 if source_token else 0.55,
            [
                _geometry_evidence(
                    "source_to_input_alignment",
                    union_bbox(node_index[previous]["bbox"], input_item["bbox"]),
                    {},
                )
            ],
        )
        if path["section"] in node_index:
            add_edge(
                "FEEDS",
                input_id,
                path["section"],
                0.9,
                [
                    _geometry_evidence(
                        "input_to_bus_section",
                        union_bbox(
                            input_item["bbox"], node_index[path["section"]]["bbox"]
                        ),
                        {},
                    )
                ],
            )

    for item in incoming:
        if item["role"] == "SECTION_DEVICE" and item["in_gap"]:
            left, right = item["in_gap"]
            node_id = f"SECTION_DEVICE:{left}-{right}"
            evidence_items = list(item["evidence"])
            evidence_items.append(
                _geometry_evidence(
                    "between_sections",
                    item["bbox"],
                    {"left": left, "right": right, "x": item["cx"]},
                    [item["label"]],
                )
            )
            add_node(
                make_node(
                    node_id,
                    "SECTION_DEVICE",
                    confidence=0.92,
                    evidence=evidence_items,
                    bbox=item["bbox"],
                    source_tokens=[item["label"]],
                    label=item["label"],
                    canonical_identity=f"SECTION_TIE#{left}-{right}",
                    attrs={
                        "type_candidate": item["type_candidate"],
                        "rating_a": item["rating_a"],
                        "control": [token.text for token in item["control_tokens"]],
                    },
                )
            )
            add_edge(
                "TIES_SECTIONS",
                node_id,
                left,
                0.92,
                [_geometry_evidence("gap_left", union_bbox(item["bbox"], node_index[left]["bbox"]), {})],
            )
            add_edge(
                "TIES_SECTIONS",
                node_id,
                right,
                0.92,
                [_geometry_evidence("gap_right", union_bbox(item["bbox"], node_index[right]["bbox"]), {})],
            )
        elif item["role"] in {"SERVICE_GROUP", "UNKNOWN_NODE"}:
            node_type = item["role"]
            node_id = f"{node_type}:{_device_key(item)}"
            add_node(
                make_node(
                    node_id,
                    node_type,
                    confidence=0.7 if node_type == "SERVICE_GROUP" else 0.3,
                    evidence=item["evidence"],
                    bbox=item["bbox"],
                    source_tokens=[item["label"]],
                    label=item["label"],
                    canonical_identity=None,
                    section=item["host_section"],
                    attrs={"type_candidate": item["type_candidate"], "rating_a": item["rating_a"]},
                )
            )
            if item["host_section"] in node_index:
                edge_type = (
                    "PROTECTS_OR_SWITCHES"
                    if node_type == "SERVICE_GROUP"
                    else "BELONGS_TO_SECTION"
                )
                add_edge(
                    edge_type,
                    node_id,
                    item["host_section"],
                    0.65 if node_type == "SERVICE_GROUP" else 0.35,
                    [
                        _geometry_evidence(
                            "x_near_section",
                            union_bbox(
                                item["bbox"],
                                node_index[item["host_section"]]["bbox"],
                            ),
                            {},
                        )
                    ],
                )

    for item in outgoing:
        outgoing_id = f"OUT:{item['device_key']}"
        add_node(
            make_node(
                outgoing_id,
                "OUTGOING_DEVICE",
                confidence=item["identity_confidence"],
                evidence=item["evidence"],
                bbox=item["bbox"],
                source_tokens=[item["label"]] + [
                    value
                    for value in (item["display_label"], item["canonical_identity"])
                    if value
                ],
                label=item["label"],
                display_label=item["display_label"],
                canonical_identity=item["canonical_identity"],
                section=item["section"],
                attrs={
                    "type_candidate": item["type_candidate"],
                    "column": item["cx"],
                    "rating_a": item["rating_a"],
                    "cables": item["cables"],
                    "status": item["status"],
                    "identity_set": item["identity_set"],
                    "nearby_text": [
                        evidence_item["value"]
                        for evidence_item in item["evidence"]
                        if evidence_item.get("kind") == "token"
                    ],
                    "connected_geometry": {"section": item["section"], "x": item["cx"]},
                },
                conflicts=item["conflicts"],
            )
        )
        add_edge(
            "FEEDS",
            item["section"],
            outgoing_id,
            0.9,
            [
                _geometry_evidence(
                    "device_column_on_bus",
                    union_bbox(node_index[item["section"]]["bbox"], item["bbox"]),
                    {"x": item["cx"]},
                    [item["label"]],
                )
            ],
        )
        add_edge(
            "BELONGS_TO_SECTION",
            outgoing_id,
            item["section"],
            0.95,
            [_geometry_evidence("column_partition", item["bbox"], {"section": item["section"]}, [item["label"]])],
        )
        if item["canonical_identity"]:
            terminal_id = f"LOAD:{item['device_key']}"
            terminal_type = "LOAD"
            terminal_confidence = item["identity_confidence"]
            terminal_label = item["display_label"] or item["canonical_identity"]
        else:
            terminal_id = f"UNKNOWN_TERMINAL:{item['device_key']}"
            terminal_type = "UNKNOWN_NODE"
            terminal_confidence = 0.25
            terminal_label = item["display_label"]
        add_node(
            make_node(
                terminal_id,
                terminal_type,
                confidence=terminal_confidence,
                evidence=item["evidence"],
                bbox=item["bbox"],
                source_tokens=node_index[outgoing_id]["source_tokens"],
                label=terminal_label,
                canonical_identity=item["canonical_identity"],
                section=item["section"],
                attrs={"status": item["status"]},
            )
        )
        add_edge(
            "TERMINATES_AT",
            outgoing_id,
            terminal_id,
            terminal_confidence,
            [
                _geometry_evidence(
                    "same_feeder_column",
                    item["bbox"],
                    {"x": item["cx"]},
                    node_index[outgoing_id]["source_tokens"],
                )
            ],
        )

    for group in groups:
        evidence_items = [
            _token_evidence(token, "functional_group_member") for token in group["tokens"][:20]
        ]
        add_node(
            make_node(
                group["id"],
                group["type"],
                confidence=group["confidence"],
                evidence=evidence_items,
                bbox=group["bbox"],
                source_tokens=[token.text for token in group["tokens"]],
                label=group["id"],
                canonical_identity=group["id"],
                section=group["section"],
                attrs={"member_count": len(group["tokens"])},
            )
        )
        relation = {
            "METERING_GROUP": "MEASURES",
            "COMPENSATION_GROUP": "BELONGS_TO_SECTION",
            "SERVICE_GROUP": "PROTECTS_OR_SWITCHES",
        }[group["type"]]
        add_edge(
            relation,
            group["id"],
            group["section"],
            group["confidence"],
            [
                _geometry_evidence(
                    "functional_group_section",
                    union_bbox(group["bbox"], node_index[group["section"]]["bbox"]),
                    {},
                )
            ],
        )

    base["nodes"] = nodes
    base["edges"] = edges
    source_nodes = [node for node in nodes if node["type"] == "SOURCE"]
    bus_nodes = [node for node in nodes if node["type"] == "BUS_SECTION"]
    section_nodes = [node for node in nodes if node["type"] == "SECTION_DEVICE"]
    outgoing_nodes = [node for node in nodes if node["type"] == "OUTGOING_DEVICE"]
    unknown_nodes = [node for node in nodes if node["type"] == "UNKNOWN_NODE"]
    identity_count = sum(bool(node.get("canonical_identity")) for node in outgoing_nodes)
    unknown_edges = sum(edge["confidence"] < 0.5 for edge in edges)
    quality = {
        "backbone_recovered": bool(len(bus_nodes) >= 2 and len(outgoing_nodes) >= 8),
        "source_confidence": round(
            sum(node["confidence"] for node in source_nodes) / max(len(source_nodes), 1), 3
        ),
        "bus_confidence": round(
            sum(node["confidence"] for node in bus_nodes) / max(len(bus_nodes), 1), 3
        ),
        "section_confidence": (
            round(max((node["confidence"] for node in section_nodes), default=0.0), 3)
            if len(bus_nodes) > 1
            else None
        ),
        "device_coverage": round(
            min(1.0, (len(outgoing) + len(incoming)) / max(len(candidates), 1)),
            3,
        ),
        "feeder_coverage": round(len(outgoing_nodes) / max(len(row["members"]), 1), 3),
        "identity_coverage": round(identity_count / max(len(outgoing_nodes), 1), 3),
        "unknown_nodes": len(unknown_nodes),
        "unknown_edges": unknown_edges,
        "sections": len(bus_nodes),
        "inputs": len([node for node in nodes if node["type"] == "INPUT_DEVICE"]),
        "section_devices": len(section_nodes),
        "outgoing_devices": len(outgoing_nodes),
        "section_label_conflicts": len(section_conflicts),
    }
    base["quality"] = quality
    structure_reasons = []
    if len(bus_nodes) < 2:
        structure_reasons.append("fewer_than_two_bus_sections")
    if len(outgoing_nodes) < 8:
        structure_reasons.append("insufficient_outgoing_devices")
    if len(source_nodes) < len(bus_nodes):
        structure_reasons.append("source_paths_incomplete")
    if len(bus_nodes) > 1 and not section_nodes:
        structure_reasons.append("section_device_not_found")
    base["quality_gates"]["structure"] = {
        "backbone_recovered": quality["backbone_recovered"],
        "reasons": structure_reasons,
        "metrics": quality,
    }
    if section_conflicts:
        base["warnings"].append(
            f"{len(section_conflicts)} device labels conflict with geometric section partition"
        )
    if quality["identity_coverage"] < 0.8:
        base["warnings"].append(
            f"feeder identity recovered for {quality['identity_coverage']:.0%} of devices"
        )
    if quality["section_confidence"] == 0.0:
        base["warnings"].append("multiple sections found but section device is unknown")
    base["validation"] = validate_system_graph(base)
    return base


def evaluate_dense_sectioned_board_gate(graph: Optional[dict]) -> dict:
    if not graph:
        return {
            "use": False,
            "reason": "graph_not_built",
            "reasons": ["graph_not_built"],
            "extraction": {},
            "detection": {},
            "structure": {},
        }
    gates = graph.get("quality_gates") or {}
    extraction = gates.get("extraction") or {}
    detection = gates.get("detection") or {}
    structure = gates.get("structure") or {}
    reasons = []
    if not extraction.get("extraction_ok"):
        reasons.extend(extraction.get("reasons") or ["extraction_failed"])
    if not detection.get("detected"):
        reasons.extend(detection.get("reasons") or ["profile_not_detected"])
    if not structure.get("backbone_recovered"):
        reasons.extend(structure.get("reasons") or ["backbone_not_recovered"])
    validation = graph.get("validation") or {}
    if not validation.get("valid"):
        reasons.append("system_graph_contract_invalid")
    return {
        "use": not reasons,
        "reason": reasons[0] if reasons else None,
        "reasons": reasons,
        "extraction": extraction,
        "detection": detection,
        "structure": structure,
        "quality": graph.get("quality") or {},
        "validation": validation,
    }


def render_dense_sectioned_board_markdown(graph: dict) -> str:
    quality = graph.get("quality") or {}
    nodes = graph.get("nodes") or []
    by_type = collections.defaultdict(list)
    for node in nodes:
        by_type[node.get("type")].append(node)
    lines = [
        "## SYSTEM_GRAPH: плотный секционированный щит",
        "",
        f"Профиль: `{(graph.get('profile') or {}).get('id')}`; confidence "
        f"{(graph.get('profile') or {}).get('profile_confidence')}.",
        f"Секции: {quality.get('sections', 0)}; вводы: {quality.get('inputs', 0)}; "
        f"секционные аппараты: {quality.get('section_devices', 0)}; "
        f"отходящие: {quality.get('outgoing_devices', 0)}.",
        "",
        "### Источники и вводы",
    ]
    for node in by_type["SOURCE"]:
        lines.append(
            f"- {node.get('label') or 'UNKNOWN'} → "
            f"{node.get('canonical_identity')} "
            f"({node.get('source_representation')}, confidence={node.get('confidence')})"
        )
    for node in by_type["INPUT_DEVICE"]:
        lines.append(
            f"- {node.get('label')} → {node.get('section')}; "
            f"номинал {(node.get('attrs') or {}).get('rating_a') or 'не установлен'} А"
        )
    lines.extend(["", "### Секции и секционный аппарат"])
    for node in by_type["BUS_SECTION"]:
        lines.append(
            f"- {node['id']}: {node.get('label')} — "
            f"{(node.get('attrs') or {}).get('device_count')} отходящих колонок"
        )
    for node in by_type["SECTION_DEVICE"]:
        lines.append(f"- {node.get('label')} ({node.get('canonical_identity')})")
    lines.extend(["", "### Отходящие ветви"])
    for node in by_type["OUTGOING_DEVICE"]:
        lines.append(
            f"- {node.get('section')} / {node.get('label')} → "
            f"{node.get('display_label') or 'UNKNOWN'}; "
            f"confidence={node.get('confidence')}"
        )
    lines.extend(
        [
            "",
            "### Honesty metrics",
            f"- source_confidence: {quality.get('source_confidence')}",
            f"- bus_confidence: {quality.get('bus_confidence')}",
            f"- section_confidence: {quality.get('section_confidence')}",
            f"- device_coverage: {quality.get('device_coverage')}",
            f"- feeder_coverage: {quality.get('feeder_coverage')}",
            f"- identity_coverage: {quality.get('identity_coverage')}",
            f"- unknown_nodes: {quality.get('unknown_nodes')}",
            f"- unknown_edges: {quality.get('unknown_edges')}",
        ]
    )
    return "\n".join(lines)


__all__ = [
    "PROFILE_ID",
    "PROFILE_VERSION",
    "UNKNOWN_PROFILE",
    "build_dense_sectioned_board_graph",
    "canonical_identity",
    "detect_dense_sectioned_board",
    "evaluate_dense_sectioned_board_gate",
    "extract_device_candidates",
    "render_dense_sectioned_board_markdown",
]
