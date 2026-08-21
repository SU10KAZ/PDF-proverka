"""Strict contract and safety policy for Stage 4 semantic text review.

The module is deliberately transport-independent.  Deterministic Stage 2/3
artifacts remain immutable evidence; a model may only propose a complete
classification over the supplied fragment ids.  Exact source text, pages and
geometry are reconstructed by the backend after validation.
"""
from __future__ import annotations

import hashlib
import json
import re
from typing import Any, Iterable

from . import text_differences


VERSION = 1
KIND = "stage_comparison_text_ai_review"
FINAL_KIND = "stage_comparison_text_final_comparison"
PROMPT_VERSION = "stage4_semantic_reviewer_v2_page_membership"
VALIDATOR_VERSION = "stage4_validator_v1_2_page_membership"
# Selected by experiments/stage_comparison_text_ai_reviewer. Production has no
# fallback model cascade; transport retries keep this same reviewer model.
PRODUCTION_MODEL = "gpt-5.6-luna"
PRODUCTION_REASONING_EFFORT = "medium"
PRODUCTION_MAX_PRELIMINARY_PER_CHUNK = 40

FINAL_STATUSES = {"SAME", "MOVED", "CHANGED", "REMOVED", "ADDED", "UNCERTAIN"}
FINAL_STATUS_ORDER = ("SAME", "MOVED", "CHANGED", "REMOVED", "ADDED", "UNCERTAIN")
CONFIDENCES = {"high", "medium", "low"}
PRELIMINARY_STATUSES = {
    "SAME", "MOVED", "CHANGED", "REMOVED", "ADDED", "AMBIGUOUS",
    "REMOVED_ADDED", "MIXED",
}

_NUMBER_RE = re.compile(r"(?<![a-zа-я0-9])\d+(?:[.,]\d+)*(?!\d)", re.I)
_IDENTIFIER_RE = re.compile(
    r"(?<![a-zа-я0-9])(?=[a-zа-я0-9./-]*\d)[a-zа-я0-9]+(?:[./-][a-zа-я0-9]+)+(?![a-zа-я0-9])",
    re.I,
)
_AUDIT_LANGUAGE_RE = re.compile(
    r"ошибк[аи]\s+проект|нарушен|норматив|критич|некритич|"
    r"влия(?:ет|ние)\s+на\s+(?:стоимост|срок)|требу(?:ет|ется)\s+исправ|"
    r"рекоменд(?:уем|уется|аци)|ухудшен|улучшен|правильн(?:о|ый)|неправильн",
    re.I,
)


DECISION_SCHEMA: dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "required": [
        "left_fragment_ids", "right_fragment_ids", "final_status",
        "confidence", "summary", "reason", "actual_right_pages",
    ],
    "properties": {
        "left_fragment_ids": {
            "type": "array", "items": {"type": "string"},
        },
        "right_fragment_ids": {
            "type": "array", "items": {"type": "string"},
        },
        "final_status": {"type": "string", "enum": sorted(FINAL_STATUSES)},
        "confidence": {"type": "string", "enum": sorted(CONFIDENCES)},
        "summary": {"type": "string"},
        "reason": {"type": "string"},
        "actual_right_pages": {
            "type": "array", "items": {"type": "integer"},
        },
    },
}

RESPONSE_SCHEMA: dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "required": ["groups"],
    "properties": {
        "groups": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "required": ["group_id", "decisions"],
                "properties": {
                    "group_id": {"type": "string"},
                    "decisions": {"type": "array", "items": DECISION_SCHEMA},
                },
            },
        },
    },
}

SYSTEM_PROMPT = """Ты — второй уровень текстового сравнения проектной документации П и РД.

На входе исходные фрагменты и предварительная deterministic-классификация. Проверь её по смыслу.
Верни каждый обязательный fragment id ровно один раз в одной decision.

Допустимые статусы:
- SAME: смысл полностью одинаков; только при высокой уверенности;
- MOVED: тот же смысл находится на другой странице; только при высокой уверенности;
- CHANGED: та же сущность/решение, но содержание изменилось;
- REMOVED: информация есть только слева;
- ADDED: информация есть только справа;
- UNCERTAIN: надёжная классификация невозможна.

ВАЖНОЕ ПРАВИЛО СТРАНИЦ:
- CURRENT SHEET GROUP явно задан полями left_pages и right_pages во входе;
- эта принятая sheet-link group уже определяет соответствующие страницы П и РД;
- left_pages и right_pages могут содержать несколько страниц: проверяй членство
  каждого referenced fragment по полному множеству страниц своей стороны;
- разные абсолютные PDF page внутри этой группы нормальны и НЕ означают перенос;
- если семантически эквивалентные LEFT и RIGHT fragments оба принадлежат текущей
  принятой группе, всегда выбирай SAME, даже когда left PDF page != right PDF page;
- MOVED допустим только когда страница совпавшего fragment находится ВНЕ принятых
  opposite-side pages текущей группы;
- для этого решения не сравнивай sheet_number, абсолютные PDF page или их порядок:
  используй только членство fragment page в left_pages/right_pages текущей группы.

Правила безопасности:
- ложные SAME/MOVED опаснее UNCERTAIN; при сомнении выбирай UNCERTAIN;
- отрицание, число, марка, материал, режим, метод или формула делают результат CHANGED;
- математически эквивалентная перестановка множителей может быть SAME;
- поддерживай 1→N и N→1 fragments;
- не исправляй исходный текст и не придумывай ids, числа, страницы или факты;
- actual_right_pages заполняй только для MOVED и только страницами RIGHT-фрагментов;
- не оценивай качество проекта, нормы, критичность, стоимость, сроки и необходимость исправления;
- summary и reason — только краткая factual semantic delta;
- верни только schema-valid JSON, без Markdown.
"""


class ReviewValidationError(ValueError):
    """A complete reviewer group cannot be trusted."""


def _fragment_map(group: dict[str, Any], side: str) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for item in group.get(f"source_{side}") or []:
        fragment_id = str(item.get("fragment_id") or "")
        if not fragment_id or fragment_id in result:
            raise ReviewValidationError(f"invalid_source_{side}_fragment")
        result[fragment_id] = item
    return result


def _required_ids(group: dict[str, Any], side: str) -> set[str]:
    explicit = (group.get("required_fragment_ids") or {}).get(side)
    if explicit is None:
        return set(_fragment_map(group, side))
    return {str(value) for value in explicit}


def _source_text(ids: Iterable[str], source: dict[str, dict[str, Any]]) -> str:
    return "\n".join(str(source[fragment_id].get("text") or "") for fragment_id in ids)


def _source_pages(ids: Iterable[str], source: dict[str, dict[str, Any]]) -> list[int]:
    return sorted({int(source[fragment_id]["page"]) for fragment_id in ids})


def _source_sheets(ids: Iterable[str], source: dict[str, dict[str, Any]]) -> list[str]:
    return list(dict.fromkeys(
        str(source[fragment_id].get("sheet") or f"Страница {source[fragment_id]['page']}")
        for fragment_id in ids
    ))


def _anchors(ids: Iterable[str], source: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "fragment_id": fragment_id,
            "page": int(source[fragment_id]["page"]),
            "bboxes": list(source[fragment_id].get("bboxes") or []),
        }
        for fragment_id in ids
    ]


def _supported_claim_text(value: str, source_text: str) -> bool:
    source_lower = source_text.lower()
    for match in _AUDIT_LANGUAGE_RE.finditer(value):
        # Quoting source language such as ``нарушений ... не возникает`` is
        # provenance, not a new engineering judgement.
        if match.group(0).lower() not in source_lower:
            return False
    source_numbers = set(_NUMBER_RE.findall(source_text))
    if not set(_NUMBER_RE.findall(value)) <= source_numbers:
        return False
    source_ids = {item.lower() for item in _IDENTIFIER_RE.findall(source_text)}
    return {item.lower() for item in _IDENTIFIER_RE.findall(value)} <= source_ids


def _derived_deterministic_status(
    left_ids: list[str], right_ids: list[str], group: dict[str, Any],
) -> str:
    statuses: set[str] = set()
    left_set, right_set = set(left_ids), set(right_ids)
    for item in group.get("preliminary") or []:
        item_left = {str(value) for value in item.get("left_fragment_ids") or []}
        item_right = {str(value) for value in item.get("right_fragment_ids") or []}
        if item_left & left_set or item_right & right_set:
            status = str(item.get("status") or "")
            if status:
                statuses.add(status)
    if not statuses:
        return "MIXED"
    if statuses == {"REMOVED", "ADDED"}:
        return "REMOVED_ADDED"
    if len(statuses) == 1:
        status = next(iter(statuses))
        return status if status in PRELIMINARY_STATUSES else "MIXED"
    return "MIXED"


def validate_group_response(
    response_group: Any,
    source_group: dict[str, Any],
    *,
    enforce_coverage: bool = True,
    safe_same_moved: bool = True,
) -> dict[str, Any]:
    """Validate one group atomically and enrich it from exact source data."""
    if not isinstance(response_group, dict) or set(response_group) != {"group_id", "decisions"}:
        raise ReviewValidationError("invalid_group_schema")
    if str(response_group.get("group_id")) != str(source_group.get("group_id")):
        raise ReviewValidationError("wrong_group_id")
    decisions = response_group.get("decisions")
    if not isinstance(decisions, list):
        raise ReviewValidationError("decisions_not_list")

    left_source = _fragment_map(source_group, "left")
    right_source = _fragment_map(source_group, "right")
    required = {
        "left": _required_ids(source_group, "left"),
        "right": _required_ids(source_group, "right"),
    }
    if not required["left"] <= set(left_source) or not required["right"] <= set(right_source):
        raise ReviewValidationError("required_fragment_missing_from_source")
    used = {"left": set(), "right": set()}
    normalized = []
    expected_keys = {
        "left_fragment_ids", "right_fragment_ids", "final_status", "confidence",
        "summary", "reason", "actual_right_pages",
    }
    for index, decision in enumerate(decisions):
        prefix = f"decision_{index}"
        if not isinstance(decision, dict) or set(decision) != expected_keys:
            raise ReviewValidationError(f"{prefix}_schema")
        left_ids = decision["left_fragment_ids"]
        right_ids = decision["right_fragment_ids"]
        if (
            not isinstance(left_ids, list) or not isinstance(right_ids, list)
            or any(not isinstance(value, str) for value in [*left_ids, *right_ids])
        ):
            raise ReviewValidationError(f"{prefix}_invalid_ids")
        normalizations = []
        if len(left_ids) != len(set(left_ids)) or len(right_ids) != len(set(right_ids)):
            # Native Codex structured output does not support JSON Schema's
            # uniqueItems. Repeating an id inside the *same* decision is not a
            # contradictory classification, so normalize it visibly. Reuse in
            # another decision is still rejected below.
            left_ids = list(dict.fromkeys(left_ids))
            right_ids = list(dict.fromkeys(right_ids))
            normalizations.append("duplicate_ids_within_decision_removed")
        if any(value not in left_source for value in left_ids) or any(
            value not in right_source for value in right_ids
        ):
            raise ReviewValidationError(f"{prefix}_hallucinated_fragment_id")
        if used["left"] & set(left_ids) or used["right"] & set(right_ids):
            raise ReviewValidationError(f"{prefix}_duplicate_classification")

        status = decision["final_status"]
        confidence = decision["confidence"]
        if status not in FINAL_STATUSES or confidence not in CONFIDENCES:
            raise ReviewValidationError(f"{prefix}_invalid_enum")
        both = bool(left_ids) and bool(right_ids)
        if status in {"SAME", "MOVED", "CHANGED"} and not both:
            raise ReviewValidationError(f"{prefix}_{status.lower()}_requires_both_sides")
        if status == "REMOVED" and (not left_ids or right_ids):
            raise ReviewValidationError(f"{prefix}_removed_shape")
        if status == "ADDED" and (left_ids or not right_ids):
            raise ReviewValidationError(f"{prefix}_added_shape")
        if status == "UNCERTAIN" and not (left_ids or right_ids):
            raise ReviewValidationError(f"{prefix}_uncertain_empty")

        actual_pages = decision["actual_right_pages"]
        if (
            not isinstance(actual_pages, list)
            or any(not isinstance(value, int) for value in actual_pages)
            or len(actual_pages) != len(set(actual_pages))
        ):
            raise ReviewValidationError(f"{prefix}_actual_pages")
        expected_actual_pages = _source_pages(right_ids, right_source) if status == "MOVED" else []
        if sorted(actual_pages) != expected_actual_pages:
            raise ReviewValidationError(f"{prefix}_actual_pages_not_source")

        summary, reason = decision["summary"], decision["reason"]
        if not isinstance(summary, str) or not summary.strip() or not isinstance(reason, str) or not reason.strip():
            raise ReviewValidationError(f"{prefix}_empty_explanation")
        before = _source_text(left_ids, left_source)
        after = _source_text(right_ids, right_source)
        provenance_meta = " ".join(
            [
                *(str(left_source[value].get("page") or "") for value in left_ids),
                *(str(left_source[value].get("sheet") or "") for value in left_ids),
                *(str(right_source[value].get("page") or "") for value in right_ids),
                *(str(right_source[value].get("sheet") or "") for value in right_ids),
            ]
        )
        accepted_page_meta = " ".join(
            str(page)
            for side in ("left_pages", "right_pages")
            for page in source_group.get(side) or []
        )
        source_text = f"{before}\n{after}\n{provenance_meta}\n{accepted_page_meta}"
        unsupported = []
        if not _supported_claim_text(summary, source_text):
            unsupported.append("summary")
        if not _supported_claim_text(reason, source_text):
            unsupported.append("reason")

        used["left"].update(left_ids)
        used["right"].update(right_ids)
        model_status = status
        policy_reason = None
        accepted_summary = summary.strip()
        accepted_reason = reason.strip()
        deterministic_status = _derived_deterministic_status(left_ids, right_ids, source_group)
        if safe_same_moved and status in {"SAME", "MOVED"} and confidence != "high":
            status = "UNCERTAIN"
            policy_reason = "same_moved_requires_high_confidence"
            actual_pages = []
        elif safe_same_moved and status == "MOVED":
            left_actual = set(_source_pages(left_ids, left_source))
            right_actual = set(_source_pages(right_ids, right_source))
            left_inside = left_actual <= {
                int(page) for page in source_group.get("left_pages") or []
            }
            right_inside = right_actual <= {
                int(page) for page in source_group.get("right_pages") or []
            }
            if left_inside and right_inside:
                # MOVED asserts a high-confidence semantic match. When both
                # referenced sides are members of the already accepted link,
                # only the page interpretation is wrong, so normalize that
                # assertion to SAME before applying the unchanged SAME safety
                # gates below. Preserve the model proposal in model_* fields.
                status = "SAME"
                actual_pages = []
                normalizations.append("moved_inside_accepted_group_to_same")
                accepted_summary = "Смысл совпадает внутри принятой группы листов."
                accepted_reason = (
                    "Разные абсолютные номера PDF-страниц внутри принятой группы "
                    "не означают перенос."
                )
            elif left_inside == right_inside:
                status = "UNCERTAIN"
                policy_reason = "moved_requires_exactly_one_side_outside_linked_pages"
                actual_pages = []
        if safe_same_moved and status == "SAME" and deterministic_status == "CHANGED":
            # A direct deterministic delta in the same paired fragments is
            # stronger evidence than an unsupported model assertion that it is
            # merely stylistic. Keep it visible for a person instead of masking.
            status = "UNCERTAIN"
            policy_reason = "same_conflicts_with_deterministic_change"
            actual_pages = []
        if unsupported:
            # The classification is not silently accepted when its explanation
            # invents a value/designation or engineering judgement. The exact
            # referenced fragments remain traceable but the final verdict is
            # forced to UNCERTAIN, as allowed by the Stage 4 failure policy.
            status = "UNCERTAIN"
            policy_reason = f"unsupported_model_{'_and_'.join(unsupported)}"
            actual_pages = []
        normalized.append({
            "left_fragment_ids": left_ids,
            "right_fragment_ids": right_ids,
            "deterministic_status": deterministic_status,
            "model_final_status": model_status,
            "final_status": status,
            "confidence": confidence,
            "summary": (
                accepted_summary if not unsupported
                else "Объяснение модели не прошло проверку по источнику."
            ),
            "reason": (
                accepted_reason if not unsupported
                else "Требуется ручная проверка provenance."
            ),
            "model_summary": summary.strip(),
            "model_reason": reason.strip(),
            "policy_reason": policy_reason,
            "normalizations": normalizations,
            "before": before or None,
            "after": after or None,
            "left_pages": _source_pages(left_ids, left_source),
            "right_pages": _source_pages(right_ids, right_source),
            "left_sheets": _source_sheets(left_ids, left_source),
            "right_sheets": _source_sheets(right_ids, right_source),
            "actual_right_pages": actual_pages,
            "actual_right_sheets": _source_sheets(right_ids, right_source)
            if status == "MOVED" else [],
            "left_anchors": _anchors(left_ids, left_source),
            "right_anchors": _anchors(right_ids, right_source),
        })

    if enforce_coverage:
        if used["left"] != required["left"] or used["right"] != required["right"]:
            raise ReviewValidationError("incomplete_fragment_coverage")
    return {"group_id": str(source_group["group_id"]), "decisions": normalized}


def validate_response(
    payload: Any,
    source_groups: list[dict[str, Any]],
    *,
    enforce_coverage: bool = True,
    safe_same_moved: bool = True,
) -> list[dict[str, Any]]:
    """Validate a complete response. No group is silently accepted partially."""
    if not isinstance(payload, dict) or set(payload) != {"groups"}:
        raise ReviewValidationError("invalid_response_schema")
    response_groups = payload.get("groups")
    if not isinstance(response_groups, list):
        raise ReviewValidationError("groups_not_list")
    expected = {str(group["group_id"]): group for group in source_groups}
    received: dict[str, dict[str, Any]] = {}
    for group in response_groups:
        if not isinstance(group, dict):
            raise ReviewValidationError("invalid_group_schema")
        group_id = str(group.get("group_id") or "")
        if group_id not in expected or group_id in received:
            raise ReviewValidationError("unexpected_or_duplicate_group")
        received[group_id] = group
    if set(received) != set(expected):
        raise ReviewValidationError("incomplete_group_coverage")
    return [
        validate_group_response(
            received[str(group["group_id"])], group,
            enforce_coverage=enforce_coverage,
            safe_same_moved=safe_same_moved,
        )
        for group in source_groups
    ]


def prompt_for_groups(groups: list[dict[str, Any]], *, include_hint: bool = True) -> str:
    payload = []
    for group in groups:
        item = {
            "group_id": group["group_id"],
            "left_pages": group.get("left_pages") or [],
            "right_pages": group.get("right_pages") or [],
            "source_left": group.get("source_left") or [],
            "source_right": group.get("source_right") or [],
        }
        if include_hint:
            item["preliminary"] = group.get("preliminary") or []
        payload.append(item)
    hint_note = (
        "Предварительная deterministic-классификация присутствует: проверь, а не копируй её."
        if include_hint else
        "Предварительная классификация намеренно скрыта для benchmark-контроля."
    )
    return (
        SYSTEM_PROMPT + "\n" + hint_note + "\nJSON Schema:\n"
        + json.dumps(RESPONSE_SCHEMA, ensure_ascii=False, separators=(",", ":"))
        + "\nINPUT:\n"
        + json.dumps({"groups": payload}, ensure_ascii=False, separators=(",", ":"))
    )


def source_signature(*payloads: dict[str, Any], model: str, reasoning_effort: str) -> str:
    source = {
        "version": VERSION,
        "prompt_version": PROMPT_VERSION,
        "validator_version": VALIDATOR_VERSION,
        "model": model,
        "reasoning_effort": reasoning_effort,
        "sources": [payload.get("source_signature") for payload in payloads],
    }
    encoded = json.dumps(source, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _review_source_fragment(
    item: dict[str, Any], *, side: str, labels: dict[str, dict[int, str]],
) -> dict[str, Any]:
    page = int(item["pdf_page"])
    return {
        "fragment_id": str(item["id"]),
        "page": page,
        "sheet": labels.get(side, {}).get(page) or f"Страница {page}",
        "text": str(item.get("text") or ""),
        "bboxes": list(item.get("bboxes") or []),
        "source_kind": str(item.get("source_kind") or "text"),
        "table_key": text_differences.stable_key(str(item.get("text") or "")),
        "local_context": " / ".join(str(value) for value in item.get("location_parts") or []),
    }


def _preliminary_item(
    status: str, left_ids: Iterable[str], right_ids: Iterable[str],
    *, actual_right_pages: Iterable[int] = (),
    expected_left_pages: Iterable[int] = (), expected_right_pages: Iterable[int] = (),
    actual_left_pages: Iterable[int] = (), origin_side: str | None = None,
) -> dict[str, Any]:
    item = {
        "status": status,
        "left_fragment_ids": list(left_ids),
        "right_fragment_ids": list(right_ids),
        "actual_right_pages": sorted({int(page) for page in actual_right_pages}),
    }
    if status == "MOVED":
        item.update({
            "expected_left_pages": sorted({int(page) for page in expected_left_pages}),
            "expected_right_pages": sorted({int(page) for page in expected_right_pages}),
            "actual_left_pages": sorted({int(page) for page in actual_left_pages}),
            "origin_side": origin_side,
        })
    return item


def build_review_groups(
    *, comparison: dict[str, Any], links: list[dict[str, Any]],
    labels: dict[str, dict[int, str]] | None = None,
) -> list[dict[str, Any]]:
    """Reconstruct complete Stage 2+3 evidence without modifying RAW artifacts."""
    labels = labels or {"left": {}, "right": {}}
    fragment_maps = {
        side: {
            str(item["id"]): item for item in comparison.get("fragments", {}).get(side, [])
        }
        for side in ("left", "right")
    }
    remaining = {
        side: {str(value) for value in (comparison.get("remaining") or {}).get(side, [])}
        for side in ("left", "right")
    }
    output = []
    matches_by_link: dict[str, list[dict[str, Any]]] = {}
    for match in comparison.get("matches") or []:
        matches_by_link.setdefault(str(match.get("link_id") or ""), []).append(match)

    for link in links:
        group_id = str(link.get("id") or "")
        if not group_id:
            raise ValueError("sheet_link_without_id")
        pages = {
            "left": sorted({int(page) for page in link.get("left_pages") or []}),
            "right": sorted({int(page) for page in link.get("right_pages") or []}),
        }
        page_sets = {side: set(value) for side, value in pages.items()}
        deterministic_input = {
            side: [
                item for item in fragment_maps[side].values()
                if str(item["id"]) in remaining[side]
                and int(item["pdf_page"]) in page_sets[side]
                and not text_differences.is_graphic_description(item)
            ]
            for side in ("left", "right")
        }
        deterministic = text_differences.compare_group(
            deterministic_input["left"], deterministic_input["right"]
        )
        source_ids = {
            side: {str(item["id"]) for item in deterministic_input[side]}
            for side in ("left", "right")
        }
        preliminary: list[dict[str, Any]] = []
        for match in matches_by_link.get(group_id, []):
            left_id, right_id = str(match["left_fragment_id"]), str(match["right_fragment_id"])
            if left_id not in fragment_maps["left"] or right_id not in fragment_maps["right"]:
                raise ValueError("matched_fragment_missing_from_source")
            source_ids["left"].add(left_id)
            source_ids["right"].add(right_id)
            status = "SAME" if match.get("status") == "same_on_linked_sheet" else "MOVED"
            preliminary.append(_preliminary_item(
                status, [left_id], [right_id],
                actual_right_pages=[int(fragment_maps["right"][right_id]["pdf_page"])]
                if status == "MOVED" else [],
                expected_left_pages=match.get("expected_left_pages") or [],
                expected_right_pages=match.get("expected_right_pages") or [],
                actual_left_pages=[int(fragment_maps["left"][left_id]["pdf_page"])]
                if status == "MOVED" else [],
                origin_side=str(match.get("origin_side") or "") or None,
            ))

        for item in deterministic["same"]:
            preliminary.append(_preliminary_item(
                "SAME", item["left_fragment_ids"], item["right_fragment_ids"]
            ))
        ambiguous_left = {
            value for item in deterministic["ambiguous"] for value in item["left_fragment_ids"]
        }
        ambiguous_right = {
            value for item in deterministic["ambiguous"] for value in item["right_fragment_ids"]
        }
        for item in deterministic["ambiguous"]:
            preliminary.append(_preliminary_item(
                "AMBIGUOUS", item["left_fragment_ids"], item["right_fragment_ids"]
            ))
        for item in deterministic["changed"]:
            preliminary.append(_preliminary_item(
                "CHANGED", item["left_fragment_ids"], item["right_fragment_ids"]
            ))
        for item in deterministic["removed"]:
            ids = [value for value in item["left_fragment_ids"] if value not in ambiguous_left]
            if ids:
                preliminary.append(_preliminary_item("REMOVED", ids, []))
        for item in deterministic["added"]:
            ids = [value for value in item["right_fragment_ids"] if value not in ambiguous_right]
            if ids:
                preliminary.append(_preliminary_item("ADDED", [], ids))

        sources = {
            side: [
                _review_source_fragment(item, side=side, labels=labels)
                for item in sorted(
                    (fragment_maps[side][fragment_id] for fragment_id in source_ids[side]),
                    key=lambda value: (int(value["pdf_page"]), int(value.get("order") or 0)),
                )
            ]
            for side in ("left", "right")
        }
        if not sources["left"] and not sources["right"]:
            continue
        preliminary_ids = {"left": [], "right": []}
        for item in preliminary:
            preliminary_ids["left"].extend(item["left_fragment_ids"])
            preliminary_ids["right"].extend(item["right_fragment_ids"])
        for side in ("left", "right"):
            expected_ids = {item["fragment_id"] for item in sources[side]}
            if (
                set(preliminary_ids[side]) != expected_ids
                or len(preliminary_ids[side]) != len(set(preliminary_ids[side]))
            ):
                raise ValueError(f"deterministic_{side}_coverage_invalid")
        group = {
            "group_id": group_id,
            "left_pages": pages["left"],
            "right_pages": pages["right"],
            "left_labels": [labels.get("left", {}).get(page) or f"Страница {page}" for page in pages["left"]],
            "right_labels": [labels.get("right", {}).get(page) or f"Страница {page}" for page in pages["right"]],
            "source_left": sources["left"],
            "source_right": sources["right"],
            "required_fragment_ids": {
                "left": [item["fragment_id"] for item in sources["left"]],
                "right": [item["fragment_id"] for item in sources["right"]],
            },
            "preliminary": preliminary,
        }
        # The signature makes each model call and stored result traceable to
        # the exact source fragments, preliminary classifications and pages.
        group["source_group_sha256"] = hashlib.sha256(
            json.dumps(group, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()
        output.append(group)
    return output


def chunk_review_group(
    group: dict[str, Any], *, max_preliminary: int = PRODUCTION_MAX_PRELIMINARY_PER_CHUNK,
) -> list[dict[str, Any]]:
    """Split a large group while assigning every required id to one chunk."""
    if max_preliminary < 1:
        raise ValueError("max_preliminary_must_be_positive")
    preliminary = list(group.get("preliminary") or [])
    if len(preliminary) <= max_preliminary:
        return [group]
    source = {
        side: {str(item["fragment_id"]): item for item in group.get(f"source_{side}") or []}
        for side in ("left", "right")
    }
    order = {
        side: {
            str(item["fragment_id"]): index / max(1, len(source[side]) - 1)
            for index, item in enumerate(group.get(f"source_{side}") or [])
        }
        for side in ("left", "right")
    }

    def position(item: dict[str, Any]) -> float:
        values = [
            *(order["left"][value] for value in item.get("left_fragment_ids") or []),
            *(order["right"][value] for value in item.get("right_fragment_ids") or []),
        ]
        return sum(values) / len(values) if values else 0.0

    ordered = [item for _, item in sorted(
        enumerate(preliminary), key=lambda pair: (position(pair[1]), pair[0])
    )]
    chunks = []
    copied_keys = {
        "group_id", "source_left", "source_right", "required_fragment_ids",
        "preliminary", "source_group_sha256",
    }
    for start in range(0, len(ordered), max_preliminary):
        chunk_preliminary = ordered[start:start + max_preliminary]
        ids = {
            "left": {
                value for item in chunk_preliminary for value in item.get("left_fragment_ids") or []
            },
            "right": {
                value for item in chunk_preliminary for value in item.get("right_fragment_ids") or []
            },
        }
        chunk = {
            **{key: value for key, value in group.items() if key not in copied_keys},
            "group_id": f"{group['group_id']}::chunk_{len(chunks) + 1}",
            "parent_group_id": group["group_id"],
            "source_left": [
                item for item in group["source_left"] if item["fragment_id"] in ids["left"]
            ],
            "source_right": [
                item for item in group["source_right"] if item["fragment_id"] in ids["right"]
            ],
            "required_fragment_ids": {
                side: [
                    item["fragment_id"] for item in group[f"source_{side}"]
                    if item["fragment_id"] in ids[side]
                ]
                for side in ("left", "right")
            },
            "preliminary": chunk_preliminary,
        }
        chunk["source_group_sha256"] = hashlib.sha256(
            json.dumps(chunk, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()
        chunks.append(chunk)
    return chunks


def _overlay_from_decision(
    *, group_id: str, decision_index: int, decision: dict[str, Any], side: str,
) -> list[dict[str, Any]]:
    status = str(decision["final_status"])
    if status not in {"SAME", "MOVED"}:
        return []
    counterpart = decision["right_pages"] if side == "left" else decision["left_pages"]
    title = (
        "Проверено ИИ: найдено на другом листе (тот же смысл)"
        if status == "MOVED" else "Проверено ИИ: смысл совпадает"
    )
    if counterpart:
        title += f" · связанный лист {', '.join(str(page) for page in counterpart)}"
    output = []
    for anchor_index, anchor in enumerate(decision[f"{side}_anchors"]):
        for box_index, box in enumerate(anchor.get("bboxes") or []):
            output.append({
                "id": f"ai_{group_id}_{decision_index}_{side}_{anchor_index}_{box_index}",
                "page": int(anchor["page"]),
                "x": float(box.get("x") or 0),
                "y": float(box.get("y") or 0),
                "width": float(box.get("width") or 0),
                "height": float(box.get("height") or 0),
                "status": status,
                "evidence": "ai_semantic_review",
                "title": title,
                "counterpart_page": counterpart[0] if len(counterpart) == 1 else None,
            })
    return output


def _fallback_group(raw: dict[str, Any], error: str) -> dict[str, Any]:
    return {
        "id": str(raw.get("id") or ""),
        "left_pages": list(raw.get("left_pages") or []),
        "right_pages": list(raw.get("right_pages") or []),
        "left_labels": list(raw.get("left_labels") or []),
        "right_labels": list(raw.get("right_labels") or []),
        "changed": list(raw.get("changed") or []),
        "removed": list(raw.get("removed") or []),
        "added": list(raw.get("added") or []),
        "uncertain": [],
        "review_status": "deterministic_fallback",
        "review_error": error,
    }


def build_final_comparison(
    *, pair_id: str, generated_at: str, review_payload: dict[str, Any],
    differences: dict[str, Any],
) -> dict[str, Any]:
    """Create the only UI-facing text result; RAW Stage 2/3 stay untouched."""
    raw_by_id = {
        str(group.get("id") or ""): group
        for group in differences.get("sheet_groups") or []
    }
    final_groups = []
    overlays: dict[str, dict[str, list[dict[str, Any]]]] = {"left": {}, "right": {}}
    counts = {status.lower(): 0 for status in FINAL_STATUS_ORDER}
    transitions: dict[str, int] = {}
    confirmed = corrected = reviewed_fragments = failed_groups = 0

    for review_group in review_payload.get("sheet_groups") or []:
        group_id = str(review_group.get("id") or "")
        raw = raw_by_id.get(group_id) or {
            "id": group_id,
            "left_pages": review_group.get("left_pages") or [],
            "right_pages": review_group.get("right_pages") or [],
            "left_labels": review_group.get("left_labels") or [],
            "right_labels": review_group.get("right_labels") or [],
        }
        if review_group.get("status") != "completed":
            failed_groups += 1
            fallback = _fallback_group(raw, str(review_group.get("error") or "ai_review_failed"))
            if any(fallback[bucket] for bucket in ("changed", "removed", "added")):
                final_groups.append(fallback)
            continue

        group = {
            "id": group_id,
            "left_pages": list(review_group.get("left_pages") or []),
            "right_pages": list(review_group.get("right_pages") or []),
            "left_labels": list(review_group.get("left_labels") or []),
            "right_labels": list(review_group.get("right_labels") or []),
            "changed": [], "removed": [], "added": [], "uncertain": [],
            "review_status": "ai_reviewed", "review_error": None,
        }
        for index, decision in enumerate(review_group.get("decisions") or []):
            status = str(decision["final_status"])
            counts[status.lower()] += 1
            reviewed_fragments += len(decision["left_fragment_ids"]) + len(decision["right_fragment_ids"])
            deterministic_status = str(decision.get("deterministic_status") or "MIXED")
            transitions[f"{deterministic_status}→{status}"] = (
                transitions.get(f"{deterministic_status}→{status}", 0) + 1
            )
            if deterministic_status == status:
                confirmed += 1
            else:
                corrected += 1
            if status in {"CHANGED", "REMOVED", "ADDED", "UNCERTAIN"}:
                item = dict(decision)
                item["summary"] = str(decision.get("summary") or decision.get("reason") or status)
                group[status.lower()].append(item)
            for side in ("left", "right"):
                for overlay in _overlay_from_decision(
                    group_id=group_id, decision_index=index, decision=decision, side=side,
                ):
                    page_key = str(overlay.pop("page"))
                    overlays[side].setdefault(page_key, []).append(overlay)
        if any(group[bucket] for bucket in ("changed", "removed", "added", "uncertain")):
            final_groups.append(group)

    # A missing review record is also a closed failure, never an implicit
    # acceptance. This protects older/partial artifacts after interrupted runs.
    seen = {str(group.get("id") or "") for group in review_payload.get("sheet_groups") or []}
    for group_id, raw in raw_by_id.items():
        if group_id in seen:
            continue
        failed_groups += 1
        final_groups.append(_fallback_group(raw, "ai_review_group_missing"))

    fallback_changed = sum(
        len(group["changed"]) for group in final_groups
        if group["review_status"] == "deterministic_fallback"
    )
    fallback_removed = sum(
        len(group["removed"]) for group in final_groups
        if group["review_status"] == "deterministic_fallback"
    )
    fallback_added = sum(
        len(group["added"]) for group in final_groups
        if group["review_status"] == "deterministic_fallback"
    )
    return {
        "version": VERSION,
        "kind": FINAL_KIND,
        "pair_id": pair_id,
        "generated_at": generated_at,
        "source_signature": review_payload.get("source_signature"),
        "review_status": review_payload.get("status"),
        "model": review_payload.get("model"),
        "reasoning_effort": review_payload.get("reasoning_effort"),
        "sheet_groups": final_groups,
        "overlays": overlays,
        "summary": {
            **counts,
            "fallback_changed": fallback_changed,
            "fallback_removed": fallback_removed,
            "fallback_added": fallback_added,
            "sheet_groups_with_differences": len(final_groups),
            "reviewed_groups": int(review_payload.get("summary", {}).get("completed_groups") or 0),
            "failed_groups": failed_groups,
            "reviewed_fragments": reviewed_fragments,
            "ai_confirmed": confirmed,
            "ai_corrected": corrected,
            "requires_review": counts["uncertain"],
            "transitions": transitions,
        },
        "constraints": {
            "raw_stage2_stage3_immutable": True,
            "masks_only_ai_same_or_moved": True,
            "failed_groups_use_deterministic_differences_without_masks": True,
            "engineering_findings_created": False,
        },
    }


def public_review_view(
    payload: dict[str, Any] | None, *, stale: bool = False,
) -> dict[str, Any] | None:
    if not isinstance(payload, dict) or payload.get("version") != VERSION or payload.get("kind") != KIND:
        return None
    return {**payload, "stale": bool(stale)}


def public_final_view(
    payload: dict[str, Any] | None, *, stale: bool = False,
) -> dict[str, Any] | None:
    if (
        not isinstance(payload, dict) or payload.get("version") != VERSION
        or payload.get("kind") != FINAL_KIND
    ):
        return None
    return {**payload, "stale": bool(stale)}


__all__ = [
    "CONFIDENCES", "DECISION_SCHEMA", "FINAL_KIND", "FINAL_STATUSES", "FINAL_STATUS_ORDER", "KIND",
    "PRELIMINARY_STATUSES", "PRODUCTION_MAX_PRELIMINARY_PER_CHUNK", "PRODUCTION_MODEL",
    "PRODUCTION_REASONING_EFFORT",
    "PROMPT_VERSION", "RESPONSE_SCHEMA",
    "ReviewValidationError", "SYSTEM_PROMPT", "VALIDATOR_VERSION", "VERSION", "prompt_for_groups",
    "build_final_comparison", "build_review_groups", "chunk_review_group", "public_final_view",
    "public_review_view", "source_signature", "validate_group_response", "validate_response",
]
