"""Deterministic protection checks for vector electrical single-line diagrams.

The detector deliberately does not use OCR or an LLM.  It reads exact values
from the canonical block package and, for outgoing feeder tables, binds the
``Imax`` and protection-setting rows by their PDF X coordinate.

Fail-soft is part of the public contract: an inapplicable block or a table that
cannot be bound unambiguously produces no detector leg/findings.
"""
from __future__ import annotations

import json
import math
import re
import time
from pathlib import Path
from statistics import median
from typing import Any, Mapping, Sequence


DETECTOR_MODEL = "deterministic/protection"

_VECTOR_SOURCE_KINDS = frozenset({
    "raw_vector",
    "structured_singleline",
    "structured_electrical",
})
_NUMBER = r"\d{1,6}(?:[.,]\d+)?"
_NUMBER_ONLY_RE = re.compile(rf"^(?P<value>{_NUMBER})$")
_EMERGENCY_RE = re.compile(r"послеаварийн", re.IGNORECASE)
_CURRENT_RE = re.compile(
    rf"(?P<quote>I\s*[рp]\s*(?:\.[^\d=\n]{{0,16}})?=\s*"
    rf"(?P<value>{_NUMBER})\s*А)",
    re.IGNORECASE,
)
_TT_RE = re.compile(
    r"(?<![\d/])(?P<primary>\d{3,5})\s*/\s*5(?:\s*А)?"
    r"(?:\s*,?\s*кл\.?\s*\d(?:[.,]\d+)?)?",
    re.IGNORECASE,
)
_QF_INLINE_RE = re.compile(
    rf"^(?P<position>\d+\s*QF(?:[.\d]*)?)\s*"
    rf"(?:[·xх*]\s*)?(?P<rating>{_NUMBER})\s*А$",
    re.IGNORECASE,
)
_QF_ONLY_RE = re.compile(r"^(?P<position>\d+\s*QF(?:[.\d]*)?)$", re.IGNORECASE)
_AMP_ONLY_RE = re.compile(rf"^(?P<rating>{_NUMBER})\s*А$", re.IGNORECASE)


def _as_float(value: Any) -> float | None:
    try:
        parsed = float(str(value).strip().replace(" ", "").replace(",", "."))
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def _source_number(value: float) -> str:
    if float(value).is_integer():
        return str(int(value))
    return (f"{value:.6f}".rstrip("0").rstrip(".")).replace(".", ",")


def _block_text(package: Mapping[str, Any]) -> str:
    # ``markdown`` is the exact, bounded block representation.  ``user_text``
    # repeats it plus an instruction tail, so concatenating both would duplicate
    # every measurement and weaken ambiguity checks.
    return str(package.get("markdown") or package.get("user_text") or "").strip()


def _is_applicable(package: Mapping[str, Any], text: str) -> bool:
    if str(package.get("source_kind") or "") not in _VECTOR_SOURCE_KINDS:
        return False
    if not text:
        return False
    # A raw-vector package can belong to any discipline.  Require direct drawing
    # tokens rather than trusting an image description/classifier.
    has_board = re.search(r"\b(?:ГРЩ|ВРУ|РУНН)\b|СЕКЦИЯ\s+ШИН", text, re.IGNORECASE)
    has_protection = re.search(r"\b\d+\s*QF\b|ток\s+аппарата\s+защиты", text, re.IGNORECASE)
    return bool(has_board and has_protection)


def _finding(
    *,
    category: str,
    affected_entity: str,
    finding: str,
    value_found: str,
    recommendation: str,
) -> dict[str, Any]:
    """Return a fully grounded finding accepted by the evidence-first gate."""
    return {
        "severity": "КРИТИЧЕСКОЕ",
        "category": category,
        "finding": finding,
        "norm_quote": None,
        "value_found": value_found,
        "recommendation": recommendation,
        "claim_type": "direct_violation",
        "affected_entity": affected_entity,
        "evidence_quote": value_found,
        "evidence_kind": "vector_text",
        "context_status": "sufficient",
        "confidence": 1.0,
        "counterevidence_checked": True,
        "required_context": [],
    }


def _emergency_current(text: str) -> tuple[float, str] | None:
    candidates: list[tuple[float, str]] = []
    for marker in _EMERGENCY_RE.finditer(text):
        window = text[marker.start(): marker.start() + 800]
        for match in _CURRENT_RE.finditer(window):
            value = _as_float(match.group("value"))
            if value is not None:
                candidates.append((value, re.sub(r"\s+", " ", match.group("quote")).strip()))
    distinct = {value for value, _ in candidates}
    if len(distinct) != 1:
        return None
    value = next(iter(distinct))
    quote = next(quote for candidate, quote in candidates if candidate == value)
    return value, quote


def _input_preamble(text: str) -> str:
    cutoffs = [
        match.start()
        for pattern in (
            r"\b[12]\s*СЕКЦИЯ\s+ШИН",
            r"№\s*фидера",
            r"\bОТХОДЯЩ(?:АЯ|ИЕ)\b",
        )
        if (match := re.search(pattern, text, re.IGNORECASE))
    ]
    return text[: min(cutoffs)] if cutoffs else text


def _input_findings(text: str) -> list[dict[str, Any]]:
    current = _emergency_current(text)
    if current is None:
        return []
    emergency_a, current_quote = current
    preamble = _input_preamble(text)
    findings: list[dict[str, Any]] = []

    seen_tt: set[tuple[float, str]] = set()
    for match in _TT_RE.finditer(preamble):
        primary_a = _as_float(match.group("primary"))
        ratio_quote = re.sub(r"\s+", " ", match.group(0)).strip(" ,")
        key = (primary_a or -1.0, ratio_quote.casefold())
        if primary_a is None or key in seen_tt:
            continue
        seen_tt.add(key)
        if primary_a >= emergency_a:
            continue
        primary = _source_number(primary_a)
        emergency = _source_number(emergency_a)
        evidence = f"{ratio_quote}; {current_quote}"
        findings.append(_finding(
            category="protection_tt_ratio",
            affected_entity=f"ТТ {ratio_quote}",
            finding=(
                f"Первичный номинал ТТ {ratio_quote} недостаточен для послеаварийного "
                f"тока: {primary} А < {emergency} А."
            ),
            value_found=evidence,
            recommendation=(
                "Выбрать коэффициент трансформации ТТ с первичным номиналом не ниже "
                f"{emergency} А и скорректировать схему/расчёт."
            ),
        ))

    lines = [line.strip() for line in preamble.splitlines() if line.strip()]
    seen_breakers: set[tuple[str, float]] = set()
    for index, line in enumerate(lines):
        inline = _QF_INLINE_RE.fullmatch(line)
        position = rating_raw = None
        if inline:
            position = re.sub(r"\s+", "", inline.group("position"))
            rating_raw = inline.group("rating")
        else:
            position_match = _QF_ONLY_RE.fullmatch(line)
            if position_match and index + 1 < len(lines):
                rating_match = _AMP_ONLY_RE.fullmatch(lines[index + 1])
                if rating_match:
                    position = re.sub(r"\s+", "", position_match.group("position"))
                    rating_raw = rating_match.group("rating")
        if not position or rating_raw is None:
            continue
        # A section/incomer role must be stated locally by the vector text.
        role_context = " ".join(lines[max(0, index - 5): index + 13])
        if not re.search(r"секци|ввод|луч|шина|\bВП\b", role_context, re.IGNORECASE):
            continue
        rating_a = _as_float(rating_raw)
        if rating_a is None:
            continue
        key = (position.upper(), rating_a)
        if key in seen_breakers:
            continue
        seen_breakers.add(key)
        if rating_a >= emergency_a:
            continue
        rating = _source_number(rating_a)
        emergency = _source_number(emergency_a)
        breaker_quote = f"{position} · {rating_raw} А"
        evidence = f"{breaker_quote}; {current_quote}"
        findings.append(_finding(
            category="protection_bus_breaker",
            affected_entity=position,
            finding=(
                f"Номинал секционного/вводного аппарата {position} недостаточен для "
                f"послеаварийного тока шины: {rating} А < {emergency} А."
            ),
            value_found=evidence,
            recommendation=(
                f"Выбрать аппарат {position} с номиналом не ниже {emergency} А либо "
                "исправить расчётный послеаварийный режим."
            ),
        ))
    return findings


def _word_text(word: Sequence[Any]) -> str:
    return str(word[4]) if len(word) > 4 else ""


def _word_x(word: Sequence[Any]) -> float:
    return (float(word[0]) + float(word[2])) / 2.0


def _word_y(word: Sequence[Any]) -> float:
    return (float(word[1]) + float(word[3])) / 2.0


def _word_height(word: Sequence[Any]) -> float:
    return max(0.1, float(word[3]) - float(word[1]))


def _line_groups(words: Sequence[Sequence[Any]]) -> list[list[Sequence[Any]]]:
    grouped: dict[tuple[Any, ...], list[Sequence[Any]]] = {}
    for index, word in enumerate(words):
        if len(word) > 6:
            key: tuple[Any, ...] = ("pdf", word[5], word[6])
        else:
            # Test/custom tuples without PyMuPDF block/line ids.
            key = ("y", round(_word_y(word), 1), index)
        grouped.setdefault(key, []).append(word)
    return [sorted(group, key=_word_x) for group in grouped.values()]


def _normalized_line(group: Sequence[Sequence[Any]]) -> str:
    return re.sub(r"\s+", " ", " ".join(_word_text(word) for word in group)).casefold()


def _header_kind(line: str) -> str | None:
    if "максималь" in line and "ток" in line and "линии" in line:
        return "imax"
    if "устав" in line and ("ав" in line or "защит" in line):
        return "setting"
    if "ток" in line and "защит" in line and (
        "аппарат" in line or "срабатыван" in line
    ):
        return "setting"
    if "фидер" in line and ("№" in line or "номер" in line):
        return "feeder"
    return None


def _header_anchors(words: Sequence[Sequence[Any]]) -> dict[str, list[dict[str, float]]]:
    result: dict[str, list[dict[str, float]]] = {"imax": [], "setting": [], "feeder": []}
    for group in _line_groups(words):
        kind = _header_kind(_normalized_line(group))
        if kind is None:
            continue
        phrase_words: list[Sequence[Any]] = []
        for word in group:
            token = _word_text(word).strip()
            if _NUMBER_ONLY_RE.fullmatch(token) or token == "-":
                break
            phrase_words.append(word)
        if not phrase_words:
            phrase_words = list(group)
        result[kind].append({
            "x0": min(float(word[0]) for word in phrase_words),
            "x1": max(float(word[2]) for word in phrase_words),
            "y": median(_word_y(word) for word in phrase_words),
            "height": median(_word_height(word) for word in phrase_words),
        })
    for anchors in result.values():
        anchors.sort(key=lambda item: (item["x0"], item["y"]))
    return result


def _row_values(
    words: Sequence[Sequence[Any]],
    anchor: Mapping[str, float],
    *,
    right: float,
    integer_only: bool = False,
) -> list[dict[str, Any]]:
    y_tolerance = max(2.5, float(anchor["height"]) * 0.75)
    values: list[dict[str, Any]] = []
    for word in words:
        text = _word_text(word).strip()
        match = _NUMBER_ONLY_RE.fullmatch(text)
        if not match or (integer_only and ("," in text or "." in text)):
            continue
        x = _word_x(word)
        if x <= float(anchor["x1"]) or x >= right:
            continue
        if abs(_word_y(word) - float(anchor["y"])) > y_tolerance:
            continue
        value = _as_float(match.group("value"))
        if value is not None:
            values.append({"x": x, "value": value, "quote": text, "height": _word_height(word)})
    values.sort(key=lambda item: item["x"])
    return values


def _feeder_number(
    words: Sequence[Sequence[Any]],
    feeder_anchors: Sequence[Mapping[str, float]],
    imax_anchor: Mapping[str, float],
    *,
    x: float,
    right: float,
) -> str | None:
    if not feeder_anchors:
        return None
    anchor = min(
        feeder_anchors,
        key=lambda item: abs(float(item["x0"]) - float(imax_anchor["x0"])),
    )
    values = _row_values(words, anchor, right=right, integer_only=True)
    if not values:
        return None
    nearest = min(values, key=lambda item: abs(float(item["x"]) - x))
    tolerance = max(5.0, float(anchor["height"]) * 1.75)
    if abs(float(nearest["x"]) - x) > tolerance:
        return None
    return str(int(nearest["value"]))


def detect_outgoing_setting_findings(
    vector_words: Sequence[Sequence[Any]],
) -> list[dict[str, Any]]:
    """Bind outgoing ``setting`` and ``Imax`` values by exact PDF X columns."""
    words = [word for word in vector_words if len(word) >= 5]
    if not words:
        return []
    anchors = _header_anchors(words)
    imax_anchors = anchors["imax"]
    setting_anchors = anchors["setting"]
    if not imax_anchors or not setting_anchors:
        return []

    findings: list[dict[str, Any]] = []
    used_setting_anchors: set[int] = set()
    for imax_index, imax_anchor in enumerate(imax_anchors):
        # Repeated tables are normally side by side.  The next Imax header is
        # the exact right boundary; a midpoint would cut the first table in half.
        right = (
            float(imax_anchors[imax_index + 1]["x0"])
            if imax_index + 1 < len(imax_anchors)
            else float("inf")
        )
        candidates = [
            (index, anchor)
            for index, anchor in enumerate(setting_anchors)
            if index not in used_setting_anchors
            and float(imax_anchor["x0"]) - 30.0 <= float(anchor["x0"]) < right
        ]
        if not candidates:
            continue
        setting_index, setting_anchor = min(
            candidates,
            key=lambda item: (
                abs(float(item[1]["x0"]) - float(imax_anchor["x0"])),
                abs(float(item[1]["y"]) - float(imax_anchor["y"])),
            ),
        )
        used_setting_anchors.add(setting_index)
        imax_values = _row_values(words, imax_anchor, right=right)
        setting_values = _row_values(words, setting_anchor, right=right)
        if not imax_values or not setting_values:
            continue

        typical_height = median(
            [item["height"] for item in imax_values + setting_values]
        )
        x_tolerance = max(5.0, typical_height * 1.75)
        available = set(range(len(imax_values)))
        for setting in setting_values:
            if not available:
                break
            nearest_index = min(
                available,
                key=lambda index: abs(float(imax_values[index]["x"]) - float(setting["x"])),
            )
            imax = imax_values[nearest_index]
            if abs(float(imax["x"]) - float(setting["x"])) > x_tolerance:
                continue
            available.remove(nearest_index)
            if float(setting["value"]) >= float(imax["value"]):
                continue
            feeder = _feeder_number(
                words,
                anchors["feeder"],
                imax_anchor,
                x=float(setting["x"]),
                right=right,
            )
            entity = f"фидер {feeder}" if feeder else "отходящая линия"
            entity_dative = f"фидера {feeder}" if feeder else "отходящей линии"
            setting_value = _source_number(float(setting["value"]))
            imax_value = _source_number(float(imax["value"]))
            evidence = (
                (f"фидер {feeder}; " if feeder else "")
                + f"Ток аппарата защиты = {setting['quote']} А; "
                + f"Максимальный ток линии = {imax['quote']} А"
            )
            findings.append(_finding(
                category="protection_outgoing_setting",
                affected_entity=entity,
                finding=(
                    f"Уставка аппарата защиты для {entity_dative} ниже максимального расчётного "
                    f"тока линии: {setting_value} А < {imax_value} А."
                ),
                value_found=evidence,
                recommendation=(
                    f"Назначить уставку защиты для {entity_dative} не ниже {imax_value} А с проверкой "
                    "допустимого тока кабеля и селективности."
                ),
            ))
    return findings


def _find_block_record(document_graph: Mapping[str, Any], block_id: str):
    for page in document_graph.get("pages") or []:
        if not isinstance(page, Mapping):
            continue
        for key in ("image_blocks", "text_blocks", "blocks"):
            for block in page.get(key) or []:
                if not isinstance(block, Mapping):
                    continue
                current_id = block.get("id") or block.get("block_id")
                if str(current_id or "") == str(block_id):
                    return page, block
    return None, None


def _locate_pdf(output_dir: Path) -> Path | None:
    roots = [output_dir, *list(output_dir.parents)[:6]]
    seen: set[Path] = set()
    for root in roots:
        for candidate in (root / "02_work" / "document.pdf", root / "document.pdf"):
            if candidate in seen:
                continue
            seen.add(candidate)
            if candidate.is_file():
                return candidate
    return None


def extract_block_vector_words(output_dir: Path, block_id: str) -> list[Sequence[Any]]:
    """Load exact words for one document-graph block; return ``[]`` on any problem."""
    try:
        output_dir = Path(output_dir)
        graph_path = output_dir / "document_graph.json"
        if not graph_path.is_file():
            return []
        document_graph = json.loads(graph_path.read_text(encoding="utf-8"))
        page_record, block_record = _find_block_record(document_graph, str(block_id))
        pdf_path = _locate_pdf(output_dir)
        if page_record is None or block_record is None or pdf_path is None:
            return []

        import fitz

        page_index = page_record.get("page_index")
        if page_index is None:
            page_no = page_record.get("page") or page_record.get("source_page_number")
            page_index = int(page_no) - 1
        with fitz.open(pdf_path) as document:
            page = document[int(page_index)]
            words = page.get_text("words")
            page_w, page_h = float(page.rect.width), float(page.rect.height)

        from backend.app.pipeline.stages.block_grounding.singleline_graph_geometry import (
            _clip_words_to_bbox,
            _clip_words_to_polygon,
        )

        polygon = (
            block_record.get("polygon_points_norm")
            or block_record.get("polygon_norm")
        )
        if polygon:
            clipped = _clip_words_to_polygon(words, polygon, page_w, page_h)
            if clipped:
                return clipped
        return _clip_words_to_bbox(
            words,
            block_record.get("coords_norm"),
            page_w,
            page_h,
        )
    except Exception:
        return []


def run_protection_table_detector(
    package: Mapping[str, Any] | None,
    *,
    output_dir: Path | None = None,
    block_id: str = "",
    vector_words: Sequence[Sequence[Any]] | None = None,
) -> dict[str, Any] | None:
    """Build a ``combine_detector_results`` leg, or ``None`` when inapplicable."""
    if not isinstance(package, Mapping):
        return None
    text = _block_text(package)
    if not _is_applicable(package, text):
        return None

    started = time.monotonic()
    findings: list[dict[str, Any]] = []
    try:
        findings.extend(_input_findings(text))
    except Exception:
        # One parser must never suppress independent checks or fail the stage.
        pass
    try:
        words = (
            list(vector_words)
            if vector_words is not None
            else extract_block_vector_words(Path(output_dir), block_id)
            if output_dir is not None and block_id
            else []
        )
        findings.extend(detect_outgoing_setting_findings(words))
    except Exception:
        pass

    return {
        "ok": True,
        "parsed": {"findings": findings},
        "context_source": str(package.get("source_kind") or "vector"),
        "input_tokens": 0,
        "output_tokens": 0,
        "reasoning_tokens": 0,
        "elapsed_ms": int((time.monotonic() - started) * 1000),
    }


__all__ = [
    "DETECTOR_MODEL",
    "detect_outgoing_setting_findings",
    "extract_block_vector_words",
    "run_protection_table_detector",
]
