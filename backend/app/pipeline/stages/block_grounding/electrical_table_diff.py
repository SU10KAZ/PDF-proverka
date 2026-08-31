"""Сравнение строк таблиц нагрузок двух редакций одного листа.

Модуль получает таблицы, собранные ``electrical_load_table``, сопоставляет их
строки между редакциями и выдаёт по одному атомарному изменению на КАЖДОЕ
доказанное свойство. Мощность и ток холодильной машины — два разных изменения
одного потребителя; склеивать их в одно «параметры изменились» нельзя, иначе
инженер не может согласиться с одним и отклонить другое.

Что модуль отказывается делать
------------------------------
* сравнивать значения, снятые в РАЗНЫХ режимах: «рабочий» и «аварийный» — не
  одна и та же величина, и стрелка «X → Y» между ними была бы выдумкой;
* сравнивать нагрузку фидера с суммарной нагрузкой потребителя, когда лист
  показывает обе;
* выбирать между двумя одинаково подходящими строками — такая пара уходит
  человеку;
* объявлять «значение убрали», если на другой стороне строка просто не
  распозналась: отсутствие прочитанного доказательства не есть доказательство
  отсутствия.
"""
from __future__ import annotations

import hashlib
from typing import Any, Mapping, Optional, Sequence

from backend.app.pipeline.stages.block_grounding.electrical_load_table import (
    BOUND,
    FACET_TITLES,
    MODE_SCOPE_LOCAL,
    MODE_SCOPE_TABLE,
    MODE_SENSITIVE_FACETS,
    MODE_STATUS_NOT_APPLICABLE,
    MODE_STATUS_PROVEN,
    MODE_STATUS_UNKNOWN,
    mode_header,
    normalized_mode_key,
)

CONTRACT_VERSION = "electrical-table-diff.v1"
PRODUCER = "electrical-table-diff-v1"

#: Порог относительного различия. Ниже него числа считаются одним значением:
#: «157.5» и «157.50» — не изменение проекта, а разное округление.
_RELATIVE_EPSILON = 1e-9

MATCH_EXACT = "EXACT"
MATCH_SECTION = "SECTION"
MATCH_DESIGNATION = "DESIGNATION"

#: Причины, по которым пара строк не даёт изменения.
REASON_MODE_MISMATCH = "mode_label_mismatch"
REASON_MODE_UNKNOWN = "mode_label_unknown"
REASON_MODE_SCOPE_MISMATCH = "mode_scope_mismatch"
REASON_SHAPE_MISMATCH = "value_shape_mismatch"
REASON_AMBIGUOUS_MATCH = "ambiguous_row_match"
REASON_INPUT_CONFLICT = "input_number_conflicts_with_section"
REASON_UNMATCHED = "row_has_no_counterpart"


def _stable_id(prefix: str, *parts: Any) -> str:
    payload = "|".join(str(part) for part in parts)
    return f"{prefix}_{hashlib.sha1(payload.encode('utf-8')).hexdigest()[:12]}"


def _values_equal(left: Sequence[float], right: Sequence[float]) -> bool:
    if len(left) != len(right):
        return False
    for a, b in zip(left, right):
        scale = max(abs(a), abs(b), 1.0)
        if abs(a - b) > _RELATIVE_EPSILON * scale:
            return False
    return True


def _input_agrees_with_section(row: Mapping[str, Any]) -> bool:
    """Номер ввода не противоречит секции.

    «ВРУ1 ввод 2» в панели РП1 — противоречие самого листа, и такую строку
    нельзя молча отнести ни к первой секции, ни ко второй.
    """
    number = row.get("input_number")
    section = row.get("section_ref")
    if number is None or not section:
        return True
    return str(section).upper() == f"РП{number}"


def _match_keys(row: Mapping[str, Any]) -> tuple[tuple, tuple, tuple]:
    designation = row.get("consumer_designation")
    section = row.get("section_ref")
    kind = row.get("row_kind")
    return (
        (designation, section, kind),
        (designation, section),
        (designation,),
    )


def _index(rows: Sequence[Mapping[str, Any]], level: int) -> dict[tuple, list[Mapping[str, Any]]]:
    buckets: dict[tuple, list[Mapping[str, Any]]] = {}
    for row in rows:
        buckets.setdefault(_match_keys(row)[level], []).append(row)
    return buckets


def match_rows(
    left_rows: Sequence[Mapping[str, Any]],
    right_rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """Сопоставляет строки двух редакций по убыванию строгости ключа.

    Сначала совпадение по обозначению, секции и виду строки; затем — без вида
    (одна редакция может показывать суммарную нагрузку там, где другая
    показывает фидерную); затем — по одному обозначению. Каждый уровень
    принимает пару, только если кандидат единственный с обеих сторон: две
    одинаково подходящие строки — это вопрос человеку, а не повод выбрать
    первую попавшуюся.
    """
    left_pool = [row for row in left_rows if row.get("binding_status") == BOUND]
    right_pool = [row for row in right_rows if row.get("binding_status") == BOUND]

    matches: list[dict[str, Any]] = []
    ambiguous: list[dict[str, Any]] = []
    used_left: set[str] = set()
    used_right: set[str] = set()

    for level, method in enumerate((MATCH_EXACT, MATCH_SECTION, MATCH_DESIGNATION)):
        remaining_left = [row for row in left_pool if row["row_id"] not in used_left]
        remaining_right = [row for row in right_pool if row["row_id"] not in used_right]
        left_index = _index(remaining_left, level)
        right_index = _index(remaining_right, level)
        for key, left_group in sorted(left_index.items(), key=lambda item: str(item[0])):
            if not key[0]:
                continue
            right_group = right_index.get(key) or []
            if not right_group:
                continue
            if len(left_group) > 1 or len(right_group) > 1:
                ambiguous.append(
                    {
                        "key": [str(part) if part is not None else None for part in key],
                        "method": method,
                        "reason": REASON_AMBIGUOUS_MATCH,
                        "left_row_ids": [row["row_id"] for row in left_group],
                        "right_row_ids": [row["row_id"] for row in right_group],
                        "summary": (
                            f"Обозначению «{key[0]}» на одном из листов отвечает "
                            f"{max(len(left_group), len(right_group))} строк(и) таблицы — "
                            "выбрать пару без человека нельзя."
                        ),
                    }
                )
                for row in left_group:
                    used_left.add(row["row_id"])
                for row in right_group:
                    used_right.add(row["row_id"])
                continue
            left_row, right_row = left_group[0], right_group[0]
            used_left.add(left_row["row_id"])
            used_right.add(right_row["row_id"])
            matches.append(
                {
                    "match_id": _stable_id("etm", left_row["row_id"], right_row["row_id"]),
                    "method": method,
                    "designation": key[0],
                    "left": left_row,
                    "right": right_row,
                }
            )

    unmatched_left = [row for row in left_pool if row["row_id"] not in used_left]
    unmatched_right = [row for row in right_pool if row["row_id"] not in used_right]
    return {
        "matches": matches,
        "ambiguous": ambiguous,
        "unmatched_left": unmatched_left,
        "unmatched_right": unmatched_right,
    }


def _facet_values(row: Mapping[str, Any]) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for value in row.get("values") or ():
        facet = value.get("facet_ref")
        if not facet:
            continue
        grouped.setdefault(facet, []).append(value)
    return grouped


def _mode_context(
    value: Mapping[str, Any],
    row: Mapping[str, Any],
    facet: str,
) -> dict[str, Any]:
    """Режим конкретного значения с совместимостью старого row-контракта."""
    if facet not in MODE_SENSITIVE_FACETS:
        return {"scope": MODE_SCOPE_LOCAL, "status": MODE_STATUS_NOT_APPLICABLE}
    status = value.get("mode_status")
    scope = value.get("mode_scope")
    if status in {MODE_STATUS_PROVEN, MODE_STATUS_UNKNOWN, MODE_STATUS_NOT_APPLICABLE}:
        return {
            "scope": scope or (
                MODE_SCOPE_TABLE if status != MODE_STATUS_NOT_APPLICABLE else MODE_SCOPE_LOCAL
            ),
            "status": status,
            "label": value.get("mode_label"),
            "key": value.get("mode_key"),
            "candidates": list(value.get("mode_candidates") or ()),
        }

    # Старые/синтетические строки хранили режим только на уровне строки.
    # Короткий заголовок даёт положительное доказательство; составной — UNKNOWN.
    legacy_label = row.get("mode_label")
    if legacy_label:
        parsed = mode_header(str(legacy_label))
        if parsed and parsed.get("status") == MODE_STATUS_PROVEN:
            return {
                "scope": MODE_SCOPE_TABLE,
                "status": MODE_STATUS_PROVEN,
                "label": parsed.get("label"),
                "key": parsed.get("key"),
                "candidates": list(parsed.get("candidates") or ()),
            }
        return {
            "scope": MODE_SCOPE_TABLE,
            "status": MODE_STATUS_UNKNOWN,
            "label": None,
            "key": None,
            "candidates": list((parsed or {}).get("candidates") or ()),
        }
    # Фидерная величина физически вне сводной таблицы — не «неизвестный
    # режим», а локальная характеристика линии. Для старого контракта вид
    # строки остаётся единственным доказательством этого различия.
    if row.get("row_kind") == "FEEDER":
        return {"scope": MODE_SCOPE_LOCAL, "status": MODE_STATUS_NOT_APPLICABLE}
    return {"scope": MODE_SCOPE_TABLE, "status": MODE_STATUS_UNKNOWN}


def _mode_labels(
    items: Sequence[Mapping[str, Any]], row: Mapping[str, Any], facet: str
) -> list[str]:
    labels: list[str] = []
    for item in items:
        context = _mode_context(item, row, facet)
        label = context.get("label")
        if label and label not in labels:
            labels.append(str(label))
        for candidate in (() if label else context.get("candidates") or ()):
            candidate_label = str(candidate.get("label") or "")
            if candidate_label and candidate_label not in labels:
                labels.append(candidate_label)
    return labels


def _mode_evidence(items: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "values": list(item.get("values") or ()),
            "raw": item.get("raw"),
            "bbox": item.get("bbox"),
            "mode_label": item.get("mode_label"),
            "mode_key": item.get("mode_key"),
            "mode_scope": item.get("mode_scope"),
            "mode_status": item.get("mode_status"),
            "mode_candidates": list(item.get("mode_candidates") or ()),
            "mode_provenance": item.get("mode_provenance"),
        }
        for item in items
    ]


def _blocked_mode_record(
    match: Mapping[str, Any],
    facet: str,
    left_items: Sequence[Mapping[str, Any]],
    right_items: Sequence[Mapping[str, Any]],
    *,
    reason: str,
) -> dict[str, Any]:
    left, right = match["left"], match["right"]
    title = FACET_TITLES.get(facet, (facet, ""))[0]
    left_labels = _mode_labels(left_items, left, facet)
    right_labels = _mode_labels(right_items, right, facet)
    if reason == REASON_MODE_MISMATCH:
        summary = (
            f"У «{match['designation']}» значения свойства «{title}» относятся "
            f"к разным расчётным режимам: слева — "
            f"«{', '.join(left_labels) or 'режим не назван'}», справа — "
            f"«{', '.join(right_labels) or 'режим не назван'}». Прямое изменение "
            "автоматически не подтверждено; требуется проверка инженера."
        )
    elif reason == REASON_MODE_SCOPE_MISMATCH:
        summary = (
            f"У «{match['designation']}» значение свойства «{title}» на одном "
            "листе приведено в сводной таблице расчётных режимов, а на другом — "
            "непосредственно у фидера. Сопоставимость значений не доказана; "
            "требуется проверка инженера."
        )
    else:
        summary = (
            f"У «{match['designation']}» не удалось определить расчётный режим "
            f"значения свойства «{title}». Прямое изменение автоматически не "
            "подтверждено; требуется проверка инженера."
        )
    return {
        "reason": reason,
        "facet_ref": facet,
        "facet_title": title,
        "left_modes": left_labels,
        "right_modes": right_labels,
        "left_values": [list(item.get("values") or ()) for item in left_items],
        "right_values": [list(item.get("values") or ()) for item in right_items],
        "summary": summary,
        "evidence": {
            "LEFT": _mode_evidence(left_items),
            "RIGHT": _mode_evidence(right_items),
        },
    }


def _append_value_comparison(
    match: Mapping[str, Any],
    facet: str,
    left_item: Mapping[str, Any],
    right_item: Mapping[str, Any],
    *,
    changes: list[dict[str, Any]],
    unchanged: list[dict[str, Any]],
    blocked: list[dict[str, Any]],
    mode_key: Optional[str] = None,
    mode_label: Optional[str] = None,
) -> None:
    before, after = left_item["values"], right_item["values"]
    if len(before) != len(after):
        blocked.append(
            {
                "reason": REASON_SHAPE_MISMATCH,
                "facet_ref": facet,
                "summary": (
                    f"У «{match['designation']}» свойство "
                    f"«{FACET_TITLES.get(facet, (facet, ''))[0]}» записано слева "
                    f"{len(before)} значением(ями), справа — {len(after)}."
                ),
            }
        )
        return
    record = _facet_record(
        match, facet, left_item, right_item, mode_key=mode_key, mode_label=mode_label
    )
    (unchanged if _values_equal(before, after) else changes).append(record)


def compare_match(match: Mapping[str, Any]) -> dict[str, Any]:
    """Сравнивает одну пару строк по каждому свойству отдельно."""
    left, right = match["left"], match["right"]
    changes: list[dict[str, Any]] = []
    unchanged: list[dict[str, Any]] = []
    blocked: list[dict[str, Any]] = []

    for row, side in ((left, "LEFT"), (right, "RIGHT")):
        if not _input_agrees_with_section(row):
            blocked.append(
                {
                    "reason": REASON_INPUT_CONFLICT,
                    "side": side,
                    "summary": (
                        f"У «{match['designation']}» номер ввода "
                        f"{row.get('input_number')} не отвечает секции "
                        f"{row.get('section_ref')}."
                    ),
                }
            )
            return {"changes": changes, "unchanged": unchanged, "blocked": blocked}

    left_facets, right_facets = _facet_values(left), _facet_values(right)
    for facet in sorted(set(left_facets) & set(right_facets)):
        left_items, right_items = left_facets[facet], right_facets[facet]
        if facet not in MODE_SENSITIVE_FACETS:
            if len(left_items) != 1 or len(right_items) != 1:
                blocked.append(
                    {
                        "reason": REASON_SHAPE_MISMATCH,
                        "facet_ref": facet,
                        "summary": (
                            f"Свойство «{FACET_TITLES.get(facet, (facet, ''))[0]}» у "
                            f"«{match['designation']}» указано на листе несколько раз — "
                            "какое значение с каким сравнивать, лист не говорит."
                        ),
                    }
                )
                continue
            _append_value_comparison(
                match, facet, left_items[0], right_items[0],
                changes=changes, unchanged=unchanged, blocked=blocked,
            )
            continue

        left_contexts = [_mode_context(item, left, facet) for item in left_items]
        right_contexts = [_mode_context(item, right, facet) for item in right_items]
        if any(context.get("status") == MODE_STATUS_UNKNOWN for context in [*left_contexts, *right_contexts]):
            def possible_keys(contexts: Sequence[Mapping[str, Any]]) -> set[str]:
                keys = {str(context.get("key")) for context in contexts if context.get("key")}
                for context in contexts:
                    keys.update(
                        str(candidate.get("key"))
                        for candidate in context.get("candidates") or ()
                        if candidate.get("key")
                    )
                return keys

            left_possible = possible_keys(left_contexts)
            right_possible = possible_keys(right_contexts)
            reason = (
                REASON_MODE_MISMATCH
                if left_possible and right_possible and left_possible.isdisjoint(right_possible)
                else REASON_MODE_UNKNOWN
            )
            blocked.append(
                _blocked_mode_record(
                    match, facet, left_items, right_items, reason=reason
                )
            )
            continue

        left_scopes = {context.get("scope") for context in left_contexts}
        right_scopes = {context.get("scope") for context in right_contexts}
        if left_scopes != right_scopes or len(left_scopes | right_scopes) != 1:
            blocked.append(
                _blocked_mode_record(
                    match, facet, left_items, right_items,
                    reason=REASON_MODE_SCOPE_MISMATCH,
                )
            )
            continue

        scope = next(iter(left_scopes | right_scopes), MODE_SCOPE_LOCAL)
        if scope == MODE_SCOPE_LOCAL:
            if len(left_items) != 1 or len(right_items) != 1:
                blocked.append(
                    {
                        "reason": REASON_SHAPE_MISMATCH,
                        "facet_ref": facet,
                        "summary": (
                            f"Свойство «{FACET_TITLES.get(facet, (facet, ''))[0]}» у "
                            f"«{match['designation']}» указано на листе несколько раз — "
                            "какое значение с каким сравнивать, лист не говорит."
                        ),
                    }
                )
                continue
            _append_value_comparison(
                match, facet, left_items[0], right_items[0],
                changes=changes, unchanged=unchanged, blocked=blocked,
            )
            continue

        left_by_mode: dict[str, list[tuple[Mapping[str, Any], Mapping[str, Any]]]] = {}
        right_by_mode: dict[str, list[tuple[Mapping[str, Any], Mapping[str, Any]]]] = {}
        for item, context in zip(left_items, left_contexts):
            key = str(context.get("key") or "")
            if key:
                left_by_mode.setdefault(key, []).append((item, context))
        for item, context in zip(right_items, right_contexts):
            key = str(context.get("key") or "")
            if key:
                right_by_mode.setdefault(key, []).append((item, context))

        common_modes = sorted(set(left_by_mode) & set(right_by_mode))
        for mode_key in common_modes:
            left_group, right_group = left_by_mode[mode_key], right_by_mode[mode_key]
            if len(left_group) != 1 or len(right_group) != 1:
                blocked.append(
                    {
                        "reason": REASON_SHAPE_MISMATCH,
                        "facet_ref": facet,
                        "mode_key": mode_key,
                        "summary": (
                            f"Свойство «{FACET_TITLES.get(facet, (facet, ''))[0]}» у "
                            f"«{match['designation']}» в одном расчётном режиме "
                            "указано несколько раз — выбрать пару нельзя."
                        ),
                    }
                )
                continue
            label = str(right_group[0][1].get("label") or left_group[0][1].get("label") or "")
            _append_value_comparison(
                match, facet, left_group[0][0], right_group[0][0],
                changes=changes, unchanged=unchanged, blocked=blocked,
                mode_key=mode_key, mode_label=label or None,
            )

        if set(left_by_mode) != set(right_by_mode):
            blocked.append(
                _blocked_mode_record(
                    match, facet, left_items, right_items, reason=REASON_MODE_MISMATCH
                )
            )
    return {"changes": changes, "unchanged": unchanged, "blocked": blocked}


def _direction(before: Sequence[float], after: Sequence[float]) -> str:
    if all(b > a for a, b in zip(before, after)):
        return "INCREASED"
    if all(b < a for a, b in zip(before, after)):
        return "DECREASED"
    return "ALTERED"


def _confidence(
    match: Mapping[str, Any],
    left_row: Mapping[str, Any],
    right_row: Mapping[str, Any],
) -> tuple[str, list[str]]:
    """Уверенность находки и оговорки, которые обязан увидеть инженер.

    Совпадение по обозначению И секции не оставляет выбора — таких пар быть
    больше одной не может. Совпадение по одному обозначению слабее; а если при
    этом одна редакция приводит суммарную величину потребителя, а другая —
    величину фидера, инженеру надо об этом сказать прямо, а не прятать различие
    за общей стрелкой.
    """
    notes: list[str] = []
    if match["method"] == MATCH_EXACT:
        confidence = "HIGH"
    elif match["method"] == MATCH_SECTION:
        confidence = "HIGH"
    else:
        confidence = "MEDIUM"
        notes.append("Строки сопоставлены только по обозначению потребителя.")
    if left_row.get("row_kind") != right_row.get("row_kind"):
        confidence = "MEDIUM"
        left_kind = "по потребителю целиком" if left_row.get("row_kind") == "CONSUMER_TOTAL" else "по фидеру"
        right_kind = "по потребителю целиком" if right_row.get("row_kind") == "CONSUMER_TOTAL" else "по фидеру"
        notes.append(
            f"На левом листе величина приведена {left_kind}, на правом — {right_kind}."
        )
    signals = len(left_row.get("designation_sources") or {}) + len(
        right_row.get("designation_sources") or {}
    )
    if signals >= 4 and confidence == "HIGH":
        confidence = "HIGH"
    return confidence, notes


def _facet_record(
    match: Mapping[str, Any],
    facet: str,
    left_value: Mapping[str, Any],
    right_value: Mapping[str, Any],
    *,
    mode_key: Optional[str] = None,
    mode_label: Optional[str] = None,
) -> dict[str, Any]:
    title, unit = FACET_TITLES.get(facet, (facet, ""))
    before, after = left_value["values"], right_value["values"]
    left_row, right_row = match["left"], match["right"]
    confidence, notes = _confidence(match, left_row, right_row)
    normalized_key = normalized_mode_key(mode_key) if mode_key else None
    facet_identity = f"{facet}@mode={normalized_key}" if normalized_key else facet
    return {
        "change_id": _stable_id("etchg", match["match_id"], facet_identity),
        "match_id": match["match_id"],
        "subject": match["designation"],
        "section_ref": right_row.get("section_ref") or left_row.get("section_ref"),
        "input_number": right_row.get("input_number") or left_row.get("input_number"),
        "row_kind": right_row.get("row_kind"),
        "mode_label": mode_label,
        "mode_key": normalized_key,
        "facet_ref": facet_identity,
        "base_facet_ref": facet,
        "facet_title": title,
        "unit": unit,
        "before_value": before[0] if len(before) == 1 else list(before),
        "after_value": after[0] if len(after) == 1 else list(after),
        "direction": _direction(before, after),
        "match_method": match["method"],
        "confidence": confidence,
        "notes": notes,
        "evidence": {
            "LEFT": {
                "row_id": left_row["row_id"],
                "page_index": left_row.get("page"),
                "bbox": left_value.get("bbox") or left_row.get("bbox"),
                "raw": left_value.get("raw"),
                "raw_run": left_value.get("raw_run"),
                "consumer_label": left_row.get("consumer_label"),
                "binding_signals": sorted(left_row.get("designation_sources") or {}),
                "order_proof": left_value.get("order_proof"),
                "mode_label": left_value.get("mode_label"),
                "mode_key": left_value.get("mode_key"),
                "mode_scope": left_value.get("mode_scope"),
                "mode_status": left_value.get("mode_status"),
                "mode_provenance": left_value.get("mode_provenance"),
            },
            "RIGHT": {
                "row_id": right_row["row_id"],
                "page_index": right_row.get("page"),
                "bbox": right_value.get("bbox") or right_row.get("bbox"),
                "raw": right_value.get("raw"),
                "raw_run": right_value.get("raw_run"),
                "consumer_label": right_row.get("consumer_label"),
                "binding_signals": sorted(right_row.get("designation_sources") or {}),
                "order_proof": right_value.get("order_proof"),
                "mode_label": right_value.get("mode_label"),
                "mode_key": right_value.get("mode_key"),
                "mode_scope": right_value.get("mode_scope"),
                "mode_status": right_value.get("mode_status"),
                "mode_provenance": right_value.get("mode_provenance"),
            },
        },
    }


def compare_load_tables(
    left_table: Mapping[str, Any],
    right_table: Mapping[str, Any],
) -> dict[str, Any]:
    """Полное сравнение таблиц нагрузок двух редакций листа."""
    matched = match_rows(left_table.get("rows") or (), right_table.get("rows") or ())
    changes: list[dict[str, Any]] = []
    unchanged: list[dict[str, Any]] = []
    blocked: list[dict[str, Any]] = []
    for match in matched["matches"]:
        result = compare_match(match)
        changes.extend(result["changes"])
        unchanged.extend(result["unchanged"])
        blocked.extend(
            {**item, "subject": match["designation"], "match_id": match["match_id"]}
            for item in result["blocked"]
        )

    # Вид строки обязан прозвучать. У ХМ1 сопоставлена фидерная строка, а без
    # пары осталась суммарная — и без этого различия отчёт выглядит так, будто
    # ХМ1 вовсе не сравнили, хотя изменение мощности по нему доказано.
    matched_subjects = {
        (match["designation"], match["left"].get("section_ref")) for match in matched["matches"]
    } | {
        (match["designation"], match["right"].get("section_ref")) for match in matched["matches"]
    }
    unproven = []
    for side, rows in (
        ("LEFT", matched["unmatched_left"]),
        ("RIGHT", matched["unmatched_right"]),
    ):
        for row in rows:
            subject = row.get("consumer_designation")
            kind = (
                "суммарная строка потребителя"
                if row.get("row_kind") == "CONSUMER_TOTAL"
                else "фидерная строка"
            )
            sheet = "левого" if side == "LEFT" else "правого"
            elsewhere = any(
                key[0] == subject for key in matched_subjects
            )
            tail = (
                " Другие строки этого потребителя сопоставлены — см. изменения выше."
                if elsewhere
                else " Сравнить её не с чем."
            )
            unproven.append(
                {
                    "reason": REASON_UNMATCHED,
                    "side": side,
                    "row_id": row["row_id"],
                    "subject": subject,
                    "section_ref": row.get("section_ref"),
                    "row_kind": row.get("row_kind"),
                    "summary": (
                        f"«{subject}»: {kind} {sheet} листа не имеет доказанной пары "
                        f"на другом листе.{tail}"
                    ),
                }
            )

    return {
        "contract_version": CONTRACT_VERSION,
        "producer": PRODUCER,
        "changes": changes,
        "unchanged": unchanged,
        "blocked": blocked + list(matched["ambiguous"]),
        "unproven": unproven,
        "counts": {
            "matches": len(matched["matches"]),
            "ambiguous_matches": len(matched["ambiguous"]),
            "changes": len(changes),
            "unchanged": len(unchanged),
            "blocked": len(blocked) + len(matched["ambiguous"]),
            "unproven": len(unproven),
        },
        "diagnostics": {
            "left_rows": len(left_table.get("rows") or ()),
            "right_rows": len(right_table.get("rows") or ()),
            "left_bound": (left_table.get("counts") or {}).get("bound", 0),
            "right_bound": (right_table.get("counts") or {}).get("bound", 0),
            "uses_model": False,
            "uses_ocr": False,
        },
    }


__all__ = [
    "CONTRACT_VERSION",
    "MATCH_DESIGNATION",
    "MATCH_EXACT",
    "MATCH_SECTION",
    "PRODUCER",
    "REASON_AMBIGUOUS_MATCH",
    "REASON_INPUT_CONFLICT",
    "REASON_MODE_MISMATCH",
    "REASON_MODE_SCOPE_MISMATCH",
    "REASON_MODE_UNKNOWN",
    "REASON_SHAPE_MISMATCH",
    "REASON_UNMATCHED",
    "compare_load_tables",
    "compare_match",
    "match_rows",
]
