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
from typing import Any, Iterable, Mapping, Optional, Sequence

from backend.app.pipeline.stages.block_grounding.electrical_load_table import (
    BOUND,
    FACET_TITLES,
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


def _normalized_mode(row: Mapping[str, Any]) -> Optional[str]:
    label = row.get("mode_label")
    if not label:
        return None
    return " ".join(str(label).split()).lower()


def compare_match(match: Mapping[str, Any]) -> dict[str, Any]:
    """Сравнивает одну пару строк по каждому свойству отдельно."""
    left, right = match["left"], match["right"]
    changes: list[dict[str, Any]] = []
    unchanged: list[dict[str, Any]] = []
    blocked: list[dict[str, Any]] = []

    left_mode, right_mode = _normalized_mode(left), _normalized_mode(right)
    if left_mode != right_mode:
        # Режимы объявлены по-разному: числа относятся к разным расчётным
        # состояниям щита, и стрелка между ними ничего бы не значила.
        blocked.append(
            {
                "reason": REASON_MODE_MISMATCH,
                "left_mode": left.get("mode_label"),
                "right_mode": right.get("mode_label"),
                "summary": (
                    f"Для «{match['designation']}» слева приведён режим "
                    f"«{left.get('mode_label') or 'без указания режима'}», справа — "
                    f"«{right.get('mode_label') or 'без указания режима'}». "
                    "Прямое сопоставление значений требует проверки связанных листов."
                ),
            }
        )
        return {"changes": changes, "unchanged": unchanged, "blocked": blocked}

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
        before, after = left_items[0]["values"], right_items[0]["values"]
        if len(before) != len(after):
            # «233,6/284,7» против одного числа — разная форма записи: у одной
            # стороны два режима, у другой один. Это не изменение величины.
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
            continue
        record = _facet_record(match, facet, left_items[0], right_items[0])
        if _values_equal(before, after):
            unchanged.append(record)
        else:
            changes.append(record)
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
) -> dict[str, Any]:
    title, unit = FACET_TITLES.get(facet, (facet, ""))
    before, after = left_value["values"], right_value["values"]
    left_row, right_row = match["left"], match["right"]
    confidence, notes = _confidence(match, left_row, right_row)
    return {
        "change_id": _stable_id("etchg", match["match_id"], facet),
        "match_id": match["match_id"],
        "subject": match["designation"],
        "section_ref": right_row.get("section_ref") or left_row.get("section_ref"),
        "input_number": right_row.get("input_number") or left_row.get("input_number"),
        "row_kind": right_row.get("row_kind"),
        "mode_label": right_row.get("mode_label"),
        "facet_ref": facet,
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
    "REASON_SHAPE_MISMATCH",
    "REASON_UNMATCHED",
    "compare_load_tables",
    "compare_match",
    "match_rows",
]
