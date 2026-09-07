"""Presentation Grouping: одна строка вместо N одинаковых, атомарность нетронута.

Инженер видит 722 отдельные строки «не удалось сравнить» там, где различных
неопределённостей заметно меньше: один и тот же текст легенды повторяется на
26 листах, а десяток строк одной таблицы приходит по одной причине.  Этот
модуль строит ТОЛЬКО представление: он ничего не удаляет, не объединяет
решения и не трогает ни изменения, ни атомарные записи проверки.

Правила группировки — точные, порядок фиксирован:

1. ``IDENTICAL_CONTENT`` — побайтово одно и то же семантическое содержание
   (направление, канонические значения, причины, измерение, классификация)
   внутри одной пары документов;
2. ``SAME_OWNER_UNCERTAINTY`` — один доказанный владелец (строка/таблица/
   раздел из ``text_fact_ownership``), одна причина, одно направление;
3. ``SHEET_UNCERTAINTY`` — одна пара листов, одна причина, одно направление,
   одна структура.

Запрещены нечёткое сходство, близость на странице, «та же сущность» и «та же
функция» сами по себе.  Никаких обращений к модели.

Идентичность группы строится из СОДЕРЖАНИЯ и стабильных идентификаторов
документа (коды документов и версии, номера страниц, идентификаторы блоков
Markdown) и не зависит от ``pair_id``/сессии: та же пара документов в новой
сессии даёт те же ``group_id``.

Решение остаётся атомарным.  ``decision_policy`` по умолчанию
``DISPLAY_ONLY_GROUP``; ``UNIFORM_DECISION_GROUP`` требует доказательства, что
семантика решения у всех детей одна и та же, и здесь не выдаётся никогда.
"""
from __future__ import annotations

import os
import re
from collections import Counter, defaultdict
from typing import Any, Iterable, Mapping

from .production_artifacts import content_signature, stable_id, utc_now

KIND = "stage_comparison_presentation_groups"
SCHEMA_VERSION = "presentation-groups.v1"
BUILDER_VERSION = "review-presentation-groups-v1"
FEATURE_FLAG = "STAGE_COMPARISON_PRESENTATION_GROUPING_ENABLED"

IDENTICAL_CONTENT = "IDENTICAL_CONTENT"
SAME_OWNER_UNCERTAINTY = "SAME_OWNER_UNCERTAINTY"
SHEET_UNCERTAINTY = "SHEET_UNCERTAINTY"
QUESTION_SAME_REASON = "SHEET_UNCERTAINTY_SAME_REASON"
REVIEW_GROUP_KINDS = (IDENTICAL_CONTENT, SAME_OWNER_UNCERTAINTY, SHEET_UNCERTAINTY)

DISPLAY_ONLY_GROUP = "DISPLAY_ONLY_GROUP"
UNIFORM_DECISION_GROUP = "UNIFORM_DECISION_GROUP"

_CONFUSABLES = str.maketrans({
    "a": "а", "c": "с", "e": "е", "o": "о", "p": "р", "x": "х", "y": "у",
    "k": "к", "h": "н", "b": "в", "m": "м", "t": "т", "ё": "е",
})
_STRUCTURE_RU = {
    "TABLE": "таблицы", "EXPLICATION": "экспликации", "EQUIPMENT_TABLE": "таблицы оборудования",
    "TEXT_SECTION": "текстового раздела", "OTHER": "текста", "UNKNOWN": "фрагмента без структуры",
    "STAMP": "штампа", "TITLE_BLOCK": "штампа",
}
_DIRECTION_RU = {
    "REMOVED": "нет справа", "ADDED": "нет слева", "ALTERED": "изменены", "REPLACED": "заменены",
}


def enabled() -> bool:
    """Флаг читается в момент вызова; по умолчанию выключен."""
    raw = (os.environ.get(FEATURE_FLAG) or "").strip().lower()
    return raw in {"1", "true", "yes", "on"}


def _canonical(value: Any) -> str:
    return re.sub(r"[^0-9a-zа-я]+", "", str(value or "").casefold().translate(_CONFUSABLES))


def pair_identity(pair: Mapping[str, Any] | None) -> str:
    """Идентичность пары из кодов документов и версий — без ``pair_id``."""
    def side(name: str) -> str:
        value = (pair or {}).get(name)
        value = value if isinstance(value, Mapping) else {}
        code = str(value.get("document_code") or value.get("filename") or "")
        version = str(value.get("version_id") or "")
        return f"{code}@{version}" if version else code
    return f"{side('left')}|{side('right')}"


def _locations(item: Mapping[str, Any]) -> tuple[list[int], list[int]]:
    provenance = item.get("provenance")
    source_atom = provenance.get("source_atom") if isinstance(provenance, Mapping) else None
    raw = source_atom.get("locations") if isinstance(source_atom, Mapping) else None
    raw = raw if isinstance(raw, Mapping) else {}
    pages: dict[str, list[int]] = {}
    for side in ("LEFT", "RIGHT"):
        pages[side] = sorted({
            int(value["page"]) for value in raw.get(side) or ()
            if isinstance(value, Mapping) and isinstance(value.get("page"), int)
            and not isinstance(value.get("page"), bool)
        })
    return pages["LEFT"], pages["RIGHT"]


def _owner_label(owner_id: Any) -> str | None:
    if not owner_id:
        return None
    parts = str(owner_id).split(":")
    if parts[0] == "row":
        return f"строка «{parts[-1][:40]}» таблицы"
    if parts[0] == "table":
        return "таблица"
    if parts[0] == "section":
        return f"раздел «{parts[-1][:40]}»"
    if parts[0] == "legend":
        return "легенда"
    if parts[0] == "doc":
        return "общий текст документа"
    return parts[0]


def _pages_text(children: list[Mapping[str, Any]], key: str) -> str:
    pages = sorted({page for child in children for page in child[key]})
    return ", ".join(str(page) for page in pages[:6]) + ("…" if len(pages) > 6 else "")


def _prepare(
    synthesis: Mapping[str, Any],
    human_review_plan: Mapping[str, Any] | None,
    ownership: Mapping[str, Any] | None,
) -> list[dict[str, Any]]:
    plan = {
        str(row.get("target_id")): row
        for row in (human_review_plan or {}).get("atomic_target_mapping") or ()
        if isinstance(row, Mapping)
    }
    owners = {
        str(row.get("source_text_atom_id")): row
        for row in (ownership or {}).get("ownership") or ()
        if isinstance(row, Mapping)
    }
    prepared: list[dict[str, Any]] = []
    for item in synthesis.get("review_items") or ():
        if not isinstance(item, Mapping) or not item.get("review_evidence_id"):
            continue
        row = plan.get(str(item["review_evidence_id"])) or {}
        owner = owners.get(str(item.get("atom_id"))) or {}
        region = row.get("source_region") if isinstance(row.get("source_region"), Mapping) else {}
        left_pages, right_pages = _locations(item)
        prepared.append({
            "review_evidence_id": str(item["review_evidence_id"]),
            "atom_id": item.get("atom_id"),
            "direction": item.get("direction"),
            "dimension": item.get("dimension"),
            "before_value": item.get("before_value"),
            "after_value": item.get("after_value"),
            "canonical_before": _canonical(item.get("before_value")),
            "canonical_after": _canonical(item.get("after_value")),
            "reason_codes": sorted(str(code) for code in item.get("reason_codes") or ()),
            "project_entity_ref": item.get("project_entity_ref"),
            "evidence_refs": sorted({
                str(ref.get("evidence_ref")) for ref in item.get("evidence_refs") or ()
                if isinstance(ref, Mapping) and ref.get("evidence_ref")
            }),
            "left_pages": left_pages,
            "right_pages": right_pages,
            "classification": row.get("new_category"),
            "subtype": row.get("subtype"),
            "human_action_required": row.get("human_action_required"),
            "structure": region.get("structure"),
            "owner_id": owner.get("owner_id"),
            "owner_kind": owner.get("owner_kind"),
            "owner_status": owner.get("ownership_status"),
        })
    return prepared


def _key_identical(item: Mapping[str, Any], identity: str) -> tuple:
    return (
        IDENTICAL_CONTENT, identity, item["direction"], item["canonical_before"],
        item["canonical_after"], tuple(item["reason_codes"]), item["dimension"],
        item["classification"], item["subtype"],
    )


def _key_owner(item: Mapping[str, Any], identity: str) -> tuple | None:
    if not item["owner_id"] or item["owner_kind"] in (None, "UNKNOWN"):
        return None
    if item["owner_status"] in (None, "UNKNOWN"):
        return None
    return (
        SAME_OWNER_UNCERTAINTY, identity, str(item["owner_id"]), item["direction"],
        tuple(item["reason_codes"]), item["classification"], item["subtype"], item["dimension"],
    )


def _key_sheet(item: Mapping[str, Any], identity: str) -> tuple:
    return (
        SHEET_UNCERTAINTY, identity, tuple(item["left_pages"]), tuple(item["right_pages"]),
        item["direction"], tuple(item["reason_codes"]), item["classification"],
        item["subtype"], item["structure"],
    )


_REVIEW_RULES = (_key_identical, _key_owner, _key_sheet)
_REVIEW_REASON = {
    IDENTICAL_CONTENT: "identical_semantic_content_across_sheets",
    SAME_OWNER_UNCERTAINTY: "same_proven_owner_same_uncertainty",
    SHEET_UNCERTAINTY: "same_sheet_pair_same_uncertainty",
}


def _decision_policy(children: list[Mapping[str, Any]]) -> tuple[str, str]:
    """Групповое решение допускается только при доказанной идентичности семантики.

    У находок класса ``MISSING_EVIDENCE`` кнопки решения нет вовсе, поэтому
    группа для них — исключительно способ показа.
    """
    if children and all(child["classification"] == "MISSING_EVIDENCE" for child in children):
        return DISPLAY_ONLY_GROUP, "children_have_no_decision_button"
    if children and all(child["human_action_required"] is False for child in children):
        return DISPLAY_ONLY_GROUP, "no_child_requires_human_action"
    return DISPLAY_ONLY_GROUP, "uniform_decision_not_proven"


def _summary(kind: str, children: list[Mapping[str, Any]]) -> str:
    first = children[0]
    count = len(children)
    direction = _DIRECTION_RU.get(str(first["direction"]), str(first["direction"] or ""))
    if kind == IDENTICAL_CONTENT:
        text = str(first["after_value"] or first["before_value"] or "").strip()[:100]
        pages = _pages_text(children, "left_pages") or _pages_text(children, "right_pages")
        return f"«{text}» — {count} листов ({direction}), стр. {pages}"
    if kind == SAME_OWNER_UNCERTAINTY:
        return f"{_owner_label(first['owner_id']) or 'владелец'}: {count} фрагментов {direction}"
    pages = _pages_text(children, "left_pages") or _pages_text(children, "right_pages")
    structure = _STRUCTURE_RU.get(str(first["structure"]), "текста")
    return f"{count} фрагментов {structure} {direction}" + (f", стр. {pages}" if pages else "")


def _question_key(question: Mapping[str, Any], identity: str) -> tuple:
    context = question.get("context") if isinstance(question.get("context"), Mapping) else {}
    return (
        QUESTION_SAME_REASON, identity, str(question.get("question_type")),
        tuple(sorted(str(value) for value in context.get("why_proposed") or ())),
        str(context.get("relation_type")), str(context.get("automatic_status")),
        tuple(sorted(str(option.get("code")) for option in question.get("answer_options") or ()
                     if isinstance(option, Mapping))),
    )


def build_presentation_groups(
    *,
    pair_id: str,
    identity: str,
    synthesis: Mapping[str, Any],
    review_questions: Mapping[str, Any] | None = None,
    human_review_plan: Mapping[str, Any] | None = None,
    ownership: Mapping[str, Any] | None = None,
    generated_at: str | None = None,
) -> dict[str, Any]:
    """Построить группы показа.  Атомарные записи только читаются."""
    items = _prepare(synthesis, human_review_plan, ownership)
    keyed = [[rule(item, identity) for rule in _REVIEW_RULES] for item in items]
    counters = [Counter(keys[index] for keys in keyed if keys[index] is not None)
                for index in range(len(_REVIEW_RULES))]
    buckets: dict[tuple, list[dict[str, Any]]] = defaultdict(list)
    atomic: list[dict[str, Any]] = []
    for item, keys in zip(items, keyed):
        for index, key in enumerate(keys):
            if key is not None and counters[index][key] > 1:
                buckets[key].append(item)
                break
        else:
            atomic.append(item)

    review_groups = []
    for key, children in buckets.items():
        policy, basis = _decision_policy(children)
        first = children[0]
        review_groups.append({
            "group_id": stable_id("rgroup_", list(str(part) for part in key), length=20),
            "kind": key[0],
            "reason": _REVIEW_REASON[key[0]],
            "pair_identity": identity,
            "direction": first["direction"],
            "dimension": first["dimension"],
            "reason_codes": first["reason_codes"],
            "classification": first["classification"],
            "subtype": first["subtype"],
            "structure": first["structure"],
            "owner_id": first["owner_id"] if key[0] == SAME_OWNER_UNCERTAINTY else None,
            "affected_pages": {
                "left": sorted({page for child in children for page in child["left_pages"]}),
                "right": sorted({page for child in children for page in child["right_pages"]}),
            },
            "affected_entities": sorted({
                str(child["project_entity_ref"]) for child in children
                if child["project_entity_ref"]
            }) or None,
            "display_summary": _summary(key[0], children),
            "decision_policy": policy,
            "decision_policy_basis": basis,
            "group_status": "PENDING",
            "children_count": len(children),
            "atomic_child_ids": sorted(child["review_evidence_id"] for child in children),
            "evidence_refs": sorted({ref for child in children for ref in child["evidence_refs"]}),
        })
    review_groups.sort(key=lambda group: group["group_id"])

    questions = [
        question for question in (review_questions or {}).get("questions") or ()
        if isinstance(question, Mapping) and question.get("question_id")
    ]
    question_keys = [_question_key(question, identity) for question in questions]
    question_counter = Counter(question_keys)
    question_buckets: dict[tuple, list[Mapping[str, Any]]] = defaultdict(list)
    question_atomic: list[Mapping[str, Any]] = []
    for question, key in zip(questions, question_keys):
        (question_buckets[key] if question_counter[key] > 1 else question_atomic).append(question)
    question_groups = []
    for key, children in question_buckets.items():
        pages = {"left": [], "right": []}
        for child in children:
            context = child.get("context") if isinstance(child.get("context"), Mapping) else {}
            pages["left"].extend(int(page) for page in context.get("left_pages") or ())
            pages["right"].extend(int(page) for page in context.get("right_pages") or ())
        question_groups.append({
            "group_id": stable_id("qgroup_", list(str(part) for part in key), length=20),
            "kind": key[0],
            "reason": "same_question_type_same_evidence_reasons",
            "pair_identity": identity,
            "question_type": key[2],
            "shared_reasons": list(key[3]),
            "relation_type": key[4],
            "automatic_status": key[5],
            "affected_pages": {"left": sorted(set(pages["left"])), "right": sorted(set(pages["right"]))},
            "display_summary": (
                f"{len(children)} пар листов, основание: " + (", ".join(key[3]) or "не указано")
            ),
            # Каждый вопрос — о СВОЕЙ паре листов: ответ по одной паре ничего
            # не доказывает про другую, поэтому решение остаётся у детей.
            "decision_policy": DISPLAY_ONLY_GROUP,
            "decision_policy_basis": "each_question_concerns_a_distinct_sheet_pair",
            "group_status": "PENDING",
            "children_count": len(children),
            "atomic_child_ids": sorted(str(child["question_id"]) for child in children),
        })
    question_groups.sort(key=lambda group: group["group_id"])

    grouped_ids = [child for group in review_groups for child in group["atomic_child_ids"]]
    atomic_ids = [item["review_evidence_id"] for item in atomic]
    if len(grouped_ids) != len(set(grouped_ids)):
        raise AssertionError("presentation grouping assigned a review item to two groups")
    if len(grouped_ids) + len(atomic_ids) != len(items):
        raise AssertionError("presentation grouping lost a review item")
    q_grouped = [child for group in question_groups for child in group["atomic_child_ids"]]
    if len(q_grouped) + len(question_atomic) != len(questions):
        raise AssertionError("presentation grouping lost a question")
    if any(group["decision_policy"] != DISPLAY_ONLY_GROUP for group in review_groups + question_groups):
        raise AssertionError("uniform decision policy is not proven")

    payload = {
        "kind": KIND,
        "schema_version": SCHEMA_VERSION,
        "version": 1,
        "pair_id": pair_id,
        "pair_identity": identity,
        "generated_at": generated_at or utc_now(),
        "synthesis_signature": (synthesis.get("provenance") or {}).get("input_signature"),
        "review_groups": review_groups,
        "atomic_review_rows": sorted(atomic_ids),
        "question_groups": question_groups,
        "atomic_question_rows": sorted(str(question["question_id"]) for question in question_atomic),
        "diagnostics": {
            "atomic_review_items": len(items),
            "review_groups": len(review_groups),
            "review_group_children": len(grouped_ids),
            "review_atomic_rows": len(atomic_ids),
            "shown_review_rows": len(review_groups) + len(atomic_ids),
            "atomic_questions": len(questions),
            "question_groups": len(question_groups),
            "question_group_children": len(q_grouped),
            "question_atomic_rows": len(question_atomic),
            "shown_question_rows": len(question_groups) + len(question_atomic),
            "group_kinds": dict(sorted(Counter(group["kind"] for group in review_groups).items())),
            "atomic_changes_untouched": len(synthesis.get("changes") or ()),
            "uses_model": False,
            "fuzzy_or_proximity_keys": False,
            "atomic_records_mutated": False,
        },
        "constraints": {
            "presentation_only": True,
            "atomic_backend_preserved": True,
            "decision_stays_atomic": True,
            "group_identity_session_independent": True,
        },
        "provenance": {"producer": BUILDER_VERSION, "uses_model": False},
    }
    payload["input_signature"] = content_signature({
        "builder": BUILDER_VERSION,
        "identity": identity,
        "review_groups": review_groups,
        "atomic_review_rows": payload["atomic_review_rows"],
        "question_groups": question_groups,
        "atomic_question_rows": payload["atomic_question_rows"],
    })
    return payload


__all__ = [
    "BUILDER_VERSION", "FEATURE_FLAG", "KIND", "SCHEMA_VERSION",
    "DISPLAY_ONLY_GROUP", "UNIFORM_DECISION_GROUP", "REVIEW_GROUP_KINDS",
    "build_presentation_groups", "enabled", "pair_identity",
]
