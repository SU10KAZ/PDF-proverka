"""Deterministic separation of atomic audit targets and human questions.

The synthesis remains atomic.  This read model decides which unresolved
records are real engineering choices, which share one document-level root
cause, and which are information that must stay visible without an approval
button.  It never invents a comparison or silently removes source evidence.
"""
from __future__ import annotations

import hashlib
import re
from difflib import SequenceMatcher
from typing import Any, Iterable, Mapping, Sequence

from .production_artifacts import content_signature, stable_id, utc_now

KIND = "stage_comparison_human_review_plan"
SCHEMA_VERSION = "human-review-plan.v1"
PRODUCER = "human-review-orchestrator-v1"

ACTIONABLE_ENGINEERING_DECISION = "ACTIONABLE_ENGINEERING_DECISION"
INFORMATIONAL_LIMITATION = "INFORMATIONAL_LIMITATION"
DOCUMENT_METADATA_CHANGE = "DOCUMENT_METADATA_CHANGE"
TEXT_REQUIREMENT_CHANGE = "TEXT_REQUIREMENT_CHANGE"
MODE_NOT_COMPARABLE = "MODE_NOT_COMPARABLE"
MISSING_EVIDENCE = "MISSING_EVIDENCE"
ROOT_CAUSE_GROUP_MEMBER = "ROOT_CAUSE_GROUP_MEMBER"
SYSTEM_DIAGNOSTIC = "SYSTEM_DIAGNOSTIC"
PROVEN_CHANGE = "PROVEN_CHANGE"
FINAL_APPROVAL_ONLY = "FINAL_APPROVAL_ONLY"

UNRESOLVED_CLASSES = frozenset({
    ACTIONABLE_ENGINEERING_DECISION,
    INFORMATIONAL_LIMITATION,
    DOCUMENT_METADATA_CHANGE,
    TEXT_REQUIREMENT_CHANGE,
    MODE_NOT_COMPARABLE,
    MISSING_EVIDENCE,
    ROOT_CAUSE_GROUP_MEMBER,
    SYSTEM_DIAGNOSTIC,
    FINAL_APPROVAL_ONLY,
})

_MODE_REASONS = frozenset({
    "mode_label_mismatch", "mode_label_unknown", "mode_scope_mismatch",
})
_WORD = re.compile(r"[0-9A-Za-zА-Яа-яЁё]+", re.UNICODE)
_ADMIN_TEXT = re.compile(
    r"(?:^|\b)(?:гип|разраб\.?|проверил|н\.?\s*контр\.?|формат|лист|"
    r"подп\.?|дата|изм\.?|кол\.?\s*уч\.?)(?:\b|$)",
    re.IGNORECASE,
)
_REQUIREMENT_GRAMMAR = re.compile(
    r"(?:\bдолжн|\bвыполнить\b|\bпредусмотр|\bприменить\b|\bдопускается\b|"
    r"\bсоответств|\bпромаркировать\b|\bсм\.\s*листы?\b)",
    re.IGNORECASE,
)
_MEASUREMENT_REQUIREMENT = re.compile(
    r"(?:контрол\w*\s+качеств\w*\s+электроэнерг|измерительн\w*\s+прибор)",
    re.IGNORECASE,
)
_MEASUREMENT_CANDIDATE = re.compile(
    r"(?:мультиметр|счетчик\w*\s+(?:эл\.?\s*)?энерг)",
    re.IGNORECASE,
)
_NEUTRAL_PROTECTIVE_BUS = re.compile(
    r"\bшин\w*\b.*(?:\bn\b|\bн\b).*(?:\bpe\b|\bре\b)",
    re.IGNORECASE,
)
_NEUTRAL_PROTECTIVE_CANDIDATE = re.compile(
    r"(?:^|\W)(?:n|н|pe|ре)[-\s]?(?:шин\w*)",
    re.IGNORECASE,
)
_FORM_CHROME = re.compile(
    r"^\s*(?:стадия\s+лист|лист\s+листов|изм\.?\s+лист)\s*$",
    re.IGNORECASE,
)


def _report_id(prefix: str, *parts: Any) -> str:
    payload = "|".join(str(part) for part in parts)
    return f"{prefix}_{hashlib.sha1(payload.encode('utf-8')).hexdigest()[:12]}"


def mode_target_id(record: Mapping[str, Any]) -> str:
    """Stable identity for a report-only mode atom."""
    return stable_id(
        "hmode_",
        record.get("match_id"),
        record.get("facet_ref"),
        record.get("subject"),
        record.get("reason"),
        length=24,
    )


def unproven_target_id(record: Mapping[str, Any]) -> str:
    return stable_id(
        "hunproven_",
        record.get("side"),
        record.get("row_id"),
        record.get("subject"),
        record.get("section_ref"),
        record.get("summary"),
        length=24,
    )


def blocked_target_id(record: Mapping[str, Any]) -> str:
    return stable_id(
        "hblocked_",
        record.get("reason"),
        record.get("match_id"),
        record.get("facet_ref"),
        record.get("summary"),
        length=24,
    )


def _normalized_text(value: Any) -> str:
    return " ".join(_WORD.findall(str(value or "").casefold().replace("ё", "е")))


def _tokens(value: Any) -> set[str]:
    return {token for token in _normalized_text(value).split() if len(token) >= 3}


def _strong_semantic_candidate(needle: str, candidates: Iterable[str]) -> str | None:
    candidates = [str(candidate) for candidate in candidates]
    needle_normalized = _normalized_text(needle)
    needle_tokens = _tokens(needle)
    for candidate in candidates:
        candidate_normalized = _normalized_text(candidate)
        if not candidate_normalized:
            continue
        ratio = SequenceMatcher(None, needle_normalized, candidate_normalized).ratio()
        candidate_tokens = _tokens(candidate)
        overlap = (
            len(needle_tokens & candidate_tokens) / max(1, min(len(needle_tokens), len(candidate_tokens)))
        )
        if ratio >= 0.78 or (len(needle_tokens) >= 4 and overlap >= 0.72):
            return str(candidate)
    # Some electrical concepts are represented by terse labels spread across
    # the drawing rather than by a sentence similar to the note.  These rules
    # only identify a concrete candidate that blocks an absence claim; source
    # region and searchable-page gates still decide the target classification.
    page_text = "\n".join(candidates)
    if _MEASUREMENT_REQUIREMENT.search(needle) and _MEASUREMENT_CANDIDATE.search(page_text):
        return next(
            candidate for candidate in candidates
            if _MEASUREMENT_CANDIDATE.search(candidate)
        )
    if (
        _NEUTRAL_PROTECTIVE_BUS.search(needle_normalized)
        and _NEUTRAL_PROTECTIVE_CANDIDATE.search(page_text)
    ):
        matching = [
            candidate for candidate in candidates
            if _NEUTRAL_PROTECTIVE_CANDIDATE.search(candidate)
        ]
        return " | ".join(matching[:4])
    return None


def _fragment_indexes(
    text_preparation: Mapping[str, Any] | None,
) -> tuple[dict[str, Mapping[str, Any]], dict[tuple[str, str], list[Mapping[str, Any]]]]:
    by_id: dict[str, Mapping[str, Any]] = {}
    by_block: dict[tuple[str, str], list[Mapping[str, Any]]] = {}
    for side, values in ((text_preparation or {}).get("fragments") or {}).items():
        side_name = str(side).upper()
        for value in values or ():
            if not isinstance(value, Mapping):
                continue
            fragment_id = str(value.get("id") or value.get("fragment_id") or "")
            if fragment_id:
                by_id[fragment_id] = {**value, "_side": side_name}
            block_id = str(value.get("source_block_id") or "")
            if block_id:
                by_block.setdefault((side_name, block_id), []).append(value)
    return by_id, by_block


def _target_fragments(
    target: Mapping[str, Any],
    text_preparation: Mapping[str, Any] | None,
) -> tuple[list[Mapping[str, Any]], list[Mapping[str, Any]]]:
    by_id, by_block = _fragment_indexes(text_preparation)
    source_atom = (target.get("provenance") or {}).get("source_atom") or {}
    locations = source_atom.get("locations") or {}
    direct = []
    for side_values in locations.values():
        for location in side_values or ():
            fragment = by_id.get(str(location.get("fragment_id") or ""))
            if fragment is not None:
                direct.append(fragment)
    if not direct:
        value = target.get("after_value") if target.get("after_value") is not None else target.get("before_value")
        normalized = _normalized_text(value)
        direct = [
            fragment for fragment in by_id.values()
            if _normalized_text(fragment.get("text")) == normalized
        ]
    block = []
    for fragment in direct:
        key = (str(fragment.get("_side") or ""), str(fragment.get("source_block_id") or ""))
        block.extend(by_block.get(key) or ())
    return direct, block


def _boxes(values: Iterable[Mapping[str, Any]]) -> list[Mapping[str, Any]]:
    return [
        box
        for value in values
        for box in value.get("bboxes") or ()
        if isinstance(box, Mapping)
    ]


def _source_region(
    target: Mapping[str, Any],
    text_preparation: Mapping[str, Any] | None,
) -> dict[str, Any]:
    direct, block = _target_fragments(target, text_preparation)
    direct_boxes = _boxes(direct)
    block_boxes = _boxes(block)
    evidence = direct_boxes or block_boxes
    if evidence:
        centers = [float(box.get("x") or 0) + float(box.get("width") or 0) / 2 for box in evidence]
        center_x = sum(centers) / len(centers)
        if center_x >= 0.72:
            region = "TITLE_BLOCK"
        elif center_x <= 0.35:
            region = "NOTE_BLOCK"
        else:
            region = "ENGINEERING_TEXT"
    else:
        region = "UNKNOWN"
        center_x = None
    return {
        "region": region,
        "center_x": center_x,
        "fragment_ids": sorted({str(value.get("id") or "") for value in direct if value.get("id")}),
        "source_block_ids": sorted({str(value.get("source_block_id") or "") for value in direct if value.get("source_block_id")}),
        "source_kinds": sorted({str(value.get("source_kind") or "") for value in direct if value.get("source_kind")}),
        "basis": "DIRECT_BBOX" if direct_boxes else "SOURCE_BLOCK_BBOX" if block_boxes else "NO_LAYOUT",
    }


def _side_and_page(
    target: Mapping[str, Any],
    text_preparation: Mapping[str, Any] | None,
) -> tuple[str, int | None]:
    source_atom = (target.get("provenance") or {}).get("source_atom") or {}
    for side in ("RIGHT", "LEFT"):
        values = (source_atom.get("locations") or {}).get(side) or ()
        if values:
            page = values[0].get("page")
            return side, int(page) if isinstance(page, int) else None
    direct, _ = _target_fragments(target, text_preparation)
    if direct:
        page = direct[0].get("pdf_page")
        return str(direct[0].get("_side") or ""), int(page) if isinstance(page, int) else None
    return ("RIGHT", None) if target.get("after_value") is not None else ("LEFT", None)


def _bounded_absence(
    target: Mapping[str, Any],
    text_preparation: Mapping[str, Any] | None,
) -> dict[str, Any]:
    side, page = _side_and_page(target, text_preparation)
    opposite = "LEFT" if side == "RIGHT" else "RIGHT"
    recognition = ((text_preparation or {}).get("recognition_index") or {}).get(opposite) or {}
    selected = ((text_preparation or {}).get("extraction") or {}).get("selected_pages") or {}
    selected_pages = {int(value) for value in selected.get(opposite) or () if isinstance(value, int)}
    page_record = recognition.get(str(page)) if page is not None else None
    coverage_high = bool(
        isinstance(page_record, Mapping)
        and page_record.get("has_text_layer") is True
        and page_record.get("truncated") is False
        and int(page_record.get("char_count") or 0) >= 500
    )
    page_scoped = page is not None and page in selected_pages
    candidates = [
        str(fragment.get("text") or "")
        for fragment in ((text_preparation or {}).get("fragments") or {}).get(opposite.lower()) or ()
        if page is None or fragment.get("pdf_page") == page
    ]
    value = str(
        target.get("after_value")
        if target.get("after_value") is not None
        else target.get("before_value") or ""
    )
    normalized = _normalized_text(value)
    searchable_page = " ".join(candidates)
    normalized_page = _normalized_text(searchable_page)
    exact = value if value.strip() and value.strip() in searchable_page else None
    normalized_match = (
        value if normalized and normalized in normalized_page else None
    )
    semantic = _strong_semantic_candidate(value, candidates)
    proven = bool(
        value and coverage_high and page_scoped
        and exact is None and normalized_match is None and semantic is None
    )
    return {
        "proven": proven,
        "source_side": side,
        "opposite_side": opposite,
        "page": page,
        "full_searchable_text": coverage_high,
        "recognition_coverage": "HIGH" if coverage_high else "INSUFFICIENT",
        "page_scope_correct": page_scoped,
        "exact_match": exact,
        "normalized_match": normalized_match,
        "strong_semantic_candidate": semantic,
        "rule": "absence_of_evidence_is_not_evidence_of_absence",
    }


def _source_text_evidence(target: Mapping[str, Any]) -> dict[str, Any]:
    """Public source-side proof for one-sided text requirements."""
    source_atom = (target.get("provenance") or {}).get("source_atom") or {}
    locations = source_atom.get("locations") or {}
    side = "RIGHT" if locations.get("RIGHT") else "LEFT"
    side_locations = [
        dict(value) for value in locations.get(side) or ()
        if isinstance(value, Mapping)
    ]
    raw_text = str(
        target.get("after_value")
        if target.get("after_value") is not None
        else target.get("before_value") or ""
    )
    return {
        "side": side,
        "page": next(
            (value.get("page") for value in side_locations if value.get("page")),
            None,
        ),
        "raw_text": raw_text,
        "text_spans": side_locations,
        "navigation": {
            "kind": "REVIEW_EVIDENCE",
            "target_id": str(target.get("review_evidence_id") or ""),
        },
    }


def _structured_subject_kind(change: Mapping[str, Any]) -> str:
    atoms = ((change.get("provenance") or {}).get("source_atoms") or ())
    for atom in atoms:
        structured = ((atom or {}).get("provenance") or {}).get("structured") or {}
        subject = structured.get("subject") or {}
        if subject.get("kind"):
            return str(subject["kind"])
    return ""


def _change_clarification(change: Mapping[str, Any]) -> dict[str, Any] | None:
    """Describe an unresolved finding as clarification, never final approval."""
    subject_kind = _structured_subject_kind(change)
    if subject_kind == "reserve_function":
        return {
            "title": "Изменение резервных линий",
            "question": (
                "Две линии на правом листе являются новыми резервными линиями "
                "или это существующие линии, которые система не сопоставила?"
            ),
            "decision_type": "RESERVE_LINE_INTERPRETATION",
            "allowed_answers": [
                {"answer_id": "NEW_RESERVE_LINES", "label": "Новые резервные линии"},
                {"answer_id": "EXISTING_LINES_NOT_MATCHED", "label": "Существующие линии"},
                {"answer_id": "ADDITIONAL_EVIDENCE_REQUIRED", "label": "Нужен другой документ"},
            ],
        }
    if subject_kind == "unresolved_correspondence":
        return {
            "title": "Неоднозначное соответствие элементов схемы",
            "question": (
                "Несопоставленные элементы на двух листах относятся к одним и "
                "тем же цепям или это разные элементы?"
            ),
            "decision_type": "GRAPH_CORRESPONDENCE_CLARIFICATION",
            "allowed_answers": [
                {"answer_id": "SAME_GRAPH_ELEMENTS", "label": "Те же элементы"},
                {"answer_id": "DIFFERENT_GRAPH_ELEMENTS", "label": "Разные элементы"},
                {"answer_id": "ADDITIONAL_EVIDENCE_REQUIRED", "label": "Нужен другой документ"},
            ],
        }
    # A pending finding is already handled by final Engineer Review.  Asking
    # a generic “is this right?” here would duplicate approval and make model
    # variance change the number of clarification interactions.  Only a
    # concrete typed ambiguity earns a separate pre-approval question.
    return None


def _classify_text_target(
    target: Mapping[str, Any],
    text_preparation: Mapping[str, Any] | None,
) -> dict[str, Any]:
    value = str(
        target.get("after_value")
        if target.get("after_value") is not None
        else target.get("before_value") or ""
    )
    region = _source_region(target, text_preparation)
    administrative = bool(_ADMIN_TEXT.search(value))
    if _FORM_CHROME.fullmatch(value):
        return {
            "classification": SYSTEM_DIAGNOSTIC,
            "subtype": "EMPTY_FORM_FIELD_LABELS",
            "human_action_required": False,
            "reason": (
                "Подпись пустых полей формы не является самостоятельным "
                "изменением оформления или техническим требованием."
            ),
            "source_region": region,
            "bounded_absence": None,
        }
    if region["region"] == "TITLE_BLOCK" or (
        administrative
        and "table_row" in region["source_kinds"]
        and region["source_block_ids"]
    ):
        return {
            "classification": DOCUMENT_METADATA_CHANGE,
            "subtype": "FORMATTING_ADMINISTRATIVE_TEXT",
            "human_action_required": False,
            "reason": "Фрагмент привязан к штампу/заголовочному блоку и не задаёт инженерный выбор.",
            "source_region": region,
            "bounded_absence": None,
        }
    requirement = region["region"] == "NOTE_BLOCK" and bool(
        _REQUIREMENT_GRAMMAR.search(value)
    )
    if requirement:
        absence = _bounded_absence(target, text_preparation)
        if absence["proven"]:
            direction = "ADDED" if target.get("after_value") is not None else "REMOVED"
            return {
                "classification": TEXT_REQUIREMENT_CHANGE,
                "subtype": f"TEXT_REQUIREMENT_{direction}",
                "human_action_required": False,
                "reason": "Техническое требование находится в блоке примечаний; отсутствие эквивалента ограниченно доказано по выбранной странице.",
                "source_region": region,
                "bounded_absence": absence,
            }
        has_candidate = any(absence.get(key) for key in (
            "exact_match", "normalized_match", "strong_semantic_candidate",
        ))
        return {
            "classification": (
                ACTIONABLE_ENGINEERING_DECISION if has_candidate else MISSING_EVIDENCE
            ),
            "subtype": "TEXT_REQUIREMENT_EQUIVALENT_UNPROVEN",
            "human_action_required": has_candidate,
            "reason": (
                "На другой стороне есть конкретный кандидат эквивалентного требования; инженер должен решить, описывает ли он то же требование."
                if has_candidate
                else "Технический текст виден, но bounded absence на другой стороне не доказан."
            ),
            "source_region": region,
            "bounded_absence": absence,
        }
    return {
        "classification": MISSING_EVIDENCE,
        "subtype": "UNCLASSIFIED_ONE_SIDED_TEXT",
        "human_action_required": False,
        "reason": "Односторонний текст не имеет доказанного инженерного измерения или конкретного варианта выбора.",
        "source_region": region,
        "bounded_absence": _bounded_absence(target, text_preparation),
    }


def _evidence_refs(value: Mapping[str, Any]) -> list[Any]:
    evidence = value.get("evidence_refs")
    if isinstance(evidence, list):
        return list(evidence)
    mode_evidence = value.get("evidence")
    return [dict(mode_evidence)] if isinstance(mode_evidence, Mapping) else []


def _atomic_mapping(
    *,
    target_id: str,
    target_kind: str,
    origin: str,
    classification: str,
    current_category: str,
    human_action_required: bool,
    group_id: str | None,
    reason: str,
    value: Mapping[str, Any],
    subtype: str | None = None,
    final_approval_required: bool = False,
    source_region: Mapping[str, Any] | None = None,
    bounded_absence: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "target_id": target_id,
        "target_kind": target_kind,
        "origin": origin,
        "current_category": current_category,
        "new_category": classification,
        "subtype": subtype,
        "human_action_required": human_action_required,
        "group_id": group_id,
        "reason": reason,
        "subject": value.get("subject") or value.get("subject_ref"),
        "before_value": value.get("before_value"),
        "after_value": value.get("after_value"),
        "evidence_refs": _evidence_refs(value),
        "final_approval_required": final_approval_required,
        "source_region": dict(source_region or {}) or None,
        "bounded_absence": dict(bounded_absence or {}) if bounded_absence else None,
    }


def _mode_group(
    pair_id: str,
    records: Sequence[Mapping[str, Any]],
) -> dict[str, Any] | None:
    if not records:
        return None
    left_modes = sorted({str(mode) for row in records for mode in row.get("left_modes") or () if mode})
    right_modes = sorted({str(mode) for row in records for mode in row.get("right_modes") or () if mode})
    group_id = stable_id("hrg_mode_", pair_id, left_modes, right_modes, length=24)
    return {
        "group_id": group_id,
        "title": "Сопоставимость расчётных режимов",
        "question": "Можно ли считать перечисленные расчётные режимы сопоставимыми между редакциями?",
        "decision_type": "MODE_RELATION",
        "affected_target_ids": [mode_target_id(record) for record in records],
        "affected_subjects": sorted({str(record.get("subject") or "") for record in records if record.get("subject")}),
        "evidence_refs": [
            {"target_id": mode_target_id(record), "evidence": dict(record.get("evidence") or {})}
            for record in records
        ],
        "root_cause": "DOCUMENT_LEVEL_MODE_VOCABULARY_MISMATCH",
        "mode_sets": {"LEFT": left_modes, "RIGHT": right_modes},
        "allowed_answers": [
            {
                "answer_id": "NOT_COMPARABLE",
                "label": "Не сопоставимы",
                "requires_mapping": False,
            },
            {
                "answer_id": "DECLARE_MODE_MAPPING",
                "label": "Задать соответствие режимов",
                "requires_mapping": True,
                "mapping_fields": [
                    {"left_mode": mode, "right_mode_choices": right_modes, "required": False}
                    for mode in left_modes
                ],
            },
            {
                "answer_id": "ADDITIONAL_DOCUMENT_REQUIRED",
                "label": "Требуется дополнительный документ",
                "requires_mapping": False,
            },
        ],
        "materialization_policy": {
            "type": "APPLY_MODE_RELATION_TO_ALL_MEMBERS",
            "preserve_atomic_targets": True,
            "recompute_comparisons": True,
            "per_atom_override_allowed": True,
            "invent_mapping": False,
        },
    }


def build_human_review_plan(
    *,
    pair_id: str,
    synthesis: Mapping[str, Any],
    engineer_decisions: Mapping[str, Any] | None = None,
    electrical_table_changes: Mapping[str, Any] | None = None,
    text_preparation: Mapping[str, Any] | None = None,
    document_inconsistencies: Mapping[str, Any] | None = None,
    resolved_row_ids: Iterable[Any] = (),
    generated_at: str | None = None,
) -> dict[str, Any]:
    """Build a complete, exactly-accounted human review plan."""
    del document_inconsistencies  # Kept separate; never duplicated as A→B.
    mapping: list[dict[str, Any]] = []
    standalone: list[dict[str, Any]] = []
    metadata: list[dict[str, Any]] = []
    requirements: list[dict[str, Any]] = []
    missing: list[dict[str, Any]] = []
    review_classification: list[dict[str, Any]] = []

    decision_by_target = {
        str(row.get("target_id") or ""): row
        for row in (engineer_decisions or {}).get("decisions") or ()
        if isinstance(row, Mapping) and row.get("target_id")
    }
    stage7_ids: set[str] = set()
    review_change_ids: set[str] = set()
    for change in synthesis.get("changes") or ():
        if not isinstance(change, Mapping) or not change.get("change_id"):
            continue
        target_id = str(change["change_id"])
        stage7_ids.add(target_id)
        review = str(change.get("review_status") or "") == "REVIEW_REQUIRED"
        if review:
            review_change_ids.add(target_id)
            clarification = _change_clarification(change)
            question_required = clarification is not None
            reason = (
                "Атом содержит конкретную типизированную неоднозначность, которую можно уточнить до финальной проверки."
                if question_required else
                "Отдельного уточняющего вопроса нет: атом остаётся в финальной инженерной проверке findings."
            )
            row = _atomic_mapping(
                target_id=target_id,
                target_kind="CHANGE",
                origin="STAGE7",
                classification=(
                    ACTIONABLE_ENGINEERING_DECISION
                    if question_required else FINAL_APPROVAL_ONLY
                ),
                current_category=ACTIONABLE_ENGINEERING_DECISION,
                human_action_required=question_required,
                group_id=None,
                reason=reason,
                value=change,
                final_approval_required=True,
            )
            if clarification is not None:
                standalone.append({
                    "question_id": stable_id("hquestion_", target_id, length=24),
                    "title": clarification["title"],
                    "question": clarification["question"],
                    "decision_type": clarification["decision_type"],
                    "affected_target_ids": [target_id],
                    "evidence_refs": _evidence_refs(change),
                    "allowed_answers": clarification["allowed_answers"],
                    "materialization_policy": {
                        "type": "RECORD_FINDING_CLARIFICATION",
                        "preserve_atomic_targets": True,
                        "does_not_approve_finding": True,
                    },
                })
            review_classification.append({
                "item": _report_id("pritem", target_id),
                "target_id": target_id,
                "current_category": ACTIONABLE_ENGINEERING_DECISION,
                "new_category": row["new_category"],
                "human_action_required": "YES" if question_required else "NO",
                "group_id": None,
                "reason": reason,
            })
        else:
            row = _atomic_mapping(
                target_id=target_id,
                target_kind="CHANGE",
                origin="STAGE7",
                classification=PROVEN_CHANGE,
                current_category=PROVEN_CHANGE,
                human_action_required=False,
                group_id=None,
                reason="Доказанное изменение остаётся атомарным finding и доступно для финального approval/override, но не является нерешённым вопросом.",
                value=change,
                final_approval_required=True,
            )
        row["current_engineer_decision"] = (decision_by_target.get(target_id) or {}).get("decision")
        mapping.append(row)

    for target in synthesis.get("review_items") or ():
        if not isinstance(target, Mapping) or not target.get("review_evidence_id"):
            continue
        target_id = str(target["review_evidence_id"])
        stage7_ids.add(target_id)
        classified = _classify_text_target(target, text_preparation)
        row = _atomic_mapping(
            target_id=target_id,
            target_kind="REVIEW_EVIDENCE",
            origin="STAGE7",
            classification=classified["classification"],
            current_category=ACTIONABLE_ENGINEERING_DECISION,
            human_action_required=bool(classified["human_action_required"]),
            group_id=None,
            reason=classified["reason"],
            value=target,
            subtype=classified["subtype"],
            final_approval_required=False,
            source_region=classified["source_region"],
            bounded_absence=classified["bounded_absence"],
        )
        row["current_engineer_decision"] = (decision_by_target.get(target_id) or {}).get("decision")
        mapping.append(row)
        item = {
            "target_id": target_id,
            "text": str(target.get("after_value") or target.get("before_value") or ""),
            "classification": classified["classification"],
            "subtype": classified["subtype"],
            "reason": classified["reason"],
            "source_region": classified["source_region"],
            "bounded_absence": classified["bounded_absence"],
            "source_evidence": _source_text_evidence(target),
            "evidence_refs": _evidence_refs(target),
        }
        if classified["classification"] == DOCUMENT_METADATA_CHANGE:
            metadata.append(item)
        elif classified["classification"] == TEXT_REQUIREMENT_CHANGE:
            requirements.append(item)
        elif classified["classification"] == SYSTEM_DIAGNOSTIC:
            pass
        elif classified["classification"] == ACTIONABLE_ENGINEERING_DECISION:
            requirement_text = item["text"]
            if _NEUTRAL_PROTECTIVE_BUS.search(
                requirement_text.casefold().replace("ё", "е")
            ):
                title = "Эквивалентность требования по шинам N и PE"
                question = (
                    "Подписи PE-шин на левом листе означают то же требование, "
                    "что и указание предусмотреть в панелях шины N и PE?"
                )
            elif _MEASUREMENT_REQUIREMENT.search(requirement_text):
                title = "Эквивалентность требования по измерительным приборам"
                question = (
                    "Обозначение «Мультиметр» на левом листе выполняет то же "
                    "требование к многофункциональным измерительным приборам?"
                )
            else:
                title = "Эквивалентность технического требования"
                question = (
                    "Найденный текст на левом листе означает то же техническое "
                    "требование, что и примечание на правом листе?"
                )
            standalone.append({
                "question_id": stable_id("hquestion_", target_id, length=24),
                "title": title,
                "question": question,
                "decision_type": "TEXT_REQUIREMENT_EQUIVALENCE",
                "affected_target_ids": [target_id],
                "evidence_refs": _evidence_refs(target),
                "candidate_evidence": classified["bounded_absence"],
                "allowed_answers": [
                    {"answer_id": "SAME_REQUIREMENT", "label": "То же требование"},
                    {"answer_id": "DISTINCT_REQUIREMENT", "label": "Разные требования"},
                    {"answer_id": "ADDITIONAL_EVIDENCE_REQUIRED", "label": "Нужен другой документ"},
                ],
                "materialization_policy": {
                    "type": "APPLY_TEXT_REQUIREMENT_EQUIVALENCE",
                    "preserve_atomic_targets": True,
                },
            })
        else:
            missing.append({
                **item,
                "missing": "Доказанный эквивалент или bounded absence на другой стороне",
                "additional_evidence": "Полный searchable text выбранной страницы с достаточным recognition coverage",
            })
        review_classification.append({
            "item": _report_id("pritem", target_id),
            "target_id": target_id,
            "current_category": "UNCLASSIFIED_TEXT_REVIEW",
            "new_category": classified["classification"],
            "human_action_required": (
                "YES" if classified["human_action_required"] else "NO"
            ),
            "group_id": None,
            "reason": classified["reason"],
        })

    mode_records = [
        record for record in (electrical_table_changes or {}).get("blocked") or ()
        if isinstance(record, Mapping) and record.get("reason") in _MODE_REASONS
    ]
    group = _mode_group(pair_id, mode_records)
    groups = [group] if group else []
    mode_group_id = group["group_id"] if group else None
    for record in mode_records:
        target_id = mode_target_id(record)
        reason = "Значения нельзя сравнивать до одного явного решения о соответствии режимов."
        mapping.append(_atomic_mapping(
            target_id=target_id,
            target_kind="MODE_COMPARISON",
            origin="PRELIMINARY_REPORT",
            classification=ROOT_CAUSE_GROUP_MEMBER,
            current_category=ACTIONABLE_ENGINEERING_DECISION,
            human_action_required=False,
            group_id=mode_group_id,
            reason=reason,
            value=record,
            subtype=MODE_NOT_COMPARABLE,
        ))
        review_classification.append({
            "item": _report_id("pritem", "mode", record.get("match_id"), record.get("facet_ref")),
            "target_id": target_id,
            "current_category": MODE_NOT_COMPARABLE,
            "new_category": ROOT_CAUSE_GROUP_MEMBER,
            "human_action_required": "NO",
            "group_id": mode_group_id,
            "reason": reason,
        })

    non_mode_blocked = [
        record for record in (electrical_table_changes or {}).get("blocked") or ()
        if isinstance(record, Mapping) and record.get("reason") not in _MODE_REASONS
    ]
    for record in non_mode_blocked:
        target_id = blocked_target_id(record)
        candidate_choice = bool(
            record.get("reason") == "ambiguous_row_match"
            and (record.get("left_row_ids") or record.get("right_row_ids"))
        )
        if candidate_choice:
            reason = "Существуют конкретные кандидаты строк, поэтому инженер может сделать содержательный выбор."
            mapping.append(_atomic_mapping(
                target_id=target_id,
                target_kind="BLOCKED_TABLE_COMPARISON",
                origin="PRELIMINARY_REPORT",
                classification=ACTIONABLE_ENGINEERING_DECISION,
                current_category=MISSING_EVIDENCE,
                human_action_required=True,
                group_id=None,
                reason=reason,
                value=record,
            ))
            standalone.append({
                "question_id": stable_id("hquestion_", target_id, length=24),
                "title": "Выберите строку ВРУ3",
                "question": (
                    "Какая из двух строк ВРУ3 на левом листе соответствует "
                    "строке ВРУ3 на правом листе?"
                    if "вру3" in str(record.get("summary") or "").casefold()
                    else str(record.get("summary") or "Какие строки соответствуют друг другу?")
                ),
                "decision_type": "TABLE_ROW_IDENTITY",
                "affected_target_ids": [target_id],
                "evidence_refs": _evidence_refs(record),
                "allowed_answers": [{
                    "answer_id": "SELECT_ROW_PAIR",
                    "left_row_ids": list(record.get("left_row_ids") or ()),
                    "right_row_ids": list(record.get("right_row_ids") or ()),
                }, {
                    "answer_id": "ADDITIONAL_EVIDENCE_REQUIRED",
                    "label": "Нужен другой документ",
                }],
                "materialization_policy": {
                    "type": "APPLY_TABLE_ROW_IDENTITY",
                    "preserve_atomic_targets": True,
                },
            })
        else:
            reason = str(record.get("summary") or "Сравнение заблокировано неполным evidence.")
            information_id = stable_id("hinfo_", target_id, length=24)
            mapping.append(_atomic_mapping(
                target_id=target_id,
                target_kind="BLOCKED_TABLE_COMPARISON",
                origin="PRELIMINARY_REPORT",
                classification=MISSING_EVIDENCE,
                current_category=INFORMATIONAL_LIMITATION,
                human_action_required=False,
                group_id=information_id,
                reason=reason,
                value=record,
            ))
            missing.append({
                "target_id": information_id,
                "affected_target_ids": [target_id],
                "text": reason,
                "classification": MISSING_EVIDENCE,
                "reason": reason,
                "missing": "Достаточное evidence для однозначного сравнения",
                "additional_evidence": "Уточнённая строка, режим или геометрическая привязка таблицы",
                "evidence_refs": _evidence_refs(record),
            })

    resolved_rows = {str(value) for value in resolved_row_ids if value}
    unproven_visible: dict[tuple[str, str, str], dict[str, Any]] = {}
    for record in (electrical_table_changes or {}).get("unproven") or ():
        if not isinstance(record, Mapping):
            continue
        if str(record.get("row_id") or "") in resolved_rows:
            continue
        target_id = unproven_target_id(record)
        reason = str(record.get("summary") or "Позиция не имеет доказанной пары.")
        visible_key = (
            str(record.get("side") or ""),
            str(record.get("subject") or ""),
            str(record.get("section_ref") or ""),
        )
        information_id = stable_id("hinfo_", *visible_key, length=24)
        visible = unproven_visible.setdefault(visible_key, {
            "target_id": information_id,
            "affected_target_ids": [],
            "text": reason,
            "classification": MISSING_EVIDENCE,
            "reason": reason,
            "missing": "Доказанная строка-пара на другой стороне",
            "additional_evidence": "Соответствующая таблица/страница другой редакции или проверенное тождество строки",
            "evidence_refs": [],
        })
        visible["affected_target_ids"].append(target_id)
        mapping.append(_atomic_mapping(
            target_id=target_id,
            target_kind="UNPROVEN_TABLE_ROW",
            origin="PRELIMINARY_REPORT",
            classification=MISSING_EVIDENCE,
            current_category=INFORMATIONAL_LIMITATION,
            human_action_required=False,
            group_id=information_id,
            reason=reason,
            value=record,
        ))
    merged_unproven: dict[str, dict[str, Any]] = {}
    unproven_group_remap: dict[str, str] = {}
    for visible in unproven_visible.values():
        display_key = str(visible.get("text") or "")
        merged_id = stable_id("hinfo_", "unproven", display_key, length=24)
        unproven_group_remap[str(visible["target_id"])] = merged_id
        if display_key not in merged_unproven:
            merged_unproven[display_key] = {
                **visible,
                "target_id": merged_id,
                "affected_target_ids": list(visible["affected_target_ids"]),
            }
        else:
            merged_unproven[display_key]["affected_target_ids"].extend(
                visible["affected_target_ids"]
            )
    for row in mapping:
        if row.get("group_id") in unproven_group_remap:
            row["group_id"] = unproven_group_remap[str(row["group_id"])]
    missing.extend(merged_unproven.values())

    mapping.sort(key=lambda row: (row["origin"], row["target_id"]))
    review_classification.sort(key=lambda row: row["item"])
    if len(stage7_ids) != len(synthesis.get("changes") or ()) + len(synthesis.get("review_items") or ()):
        raise AssertionError("duplicate Stage-7 target identity")
    mapped_stage7 = {row["target_id"] for row in mapping if row["origin"] == "STAGE7"}
    if mapped_stage7 != stage7_ids:
        raise AssertionError("human review plan silently dropped a Stage-7 target")
    target_ids = [row["target_id"] for row in mapping]
    if len(target_ids) != len(set(target_ids)):
        raise AssertionError("human review plan contains duplicate atomic mappings")
    expected_review_rows = len(review_change_ids) + len(synthesis.get("review_items") or ()) + len(mode_records)
    if len(review_classification) != expected_review_rows:
        raise AssertionError("Preliminary Report review accounting is incomplete")

    informational = [
        {
            "target_id": row["target_id"],
            "classification": row["new_category"],
            "reason": row["reason"],
            "group_id": row.get("group_id"),
        }
        for row in mapping
        if row["new_category"] in {
            INFORMATIONAL_LIMITATION,
            DOCUMENT_METADATA_CHANGE,
            TEXT_REQUIREMENT_CHANGE,
            MISSING_EVIDENCE,
            ROOT_CAUSE_GROUP_MEMBER,
            SYSTEM_DIAGNOSTIC,
        }
    ]
    mandatory_interactions = len(groups) + len(standalone)
    summary = {
        "atomic_stage7_targets": len(stage7_ids),
        "actionable_engineering_atoms": sum(
            row["new_category"] == ACTIONABLE_ENGINEERING_DECISION for row in mapping
        ),
        "review_groups": len(groups),
        "standalone_human_questions": len(standalone),
        "mandatory_human_interactions": mandatory_interactions,
        "informational_limitations": len(missing),
        "metadata_changes": len(metadata),
        "text_requirement_changes": len(requirements),
        "mode_atoms": len(mode_records),
        "mode_groups": len(groups),
        "unproven_items": len(merged_unproven),
        "unproven_atomic_targets": sum(
            row["target_kind"] == "UNPROVEN_TABLE_ROW" for row in mapping
        ),
        "preliminary_review_items_before": expected_review_rows,
        "preliminary_review_items_after": mandatory_interactions,
        "atomic_mappings": len(mapping),
        "proven_changes_awaiting_final_approval": sum(row["new_category"] == PROVEN_CHANGE for row in mapping),
    }
    payload = {
        "kind": KIND,
        "schema_version": SCHEMA_VERSION,
        "version": 1,
        "pair_id": pair_id,
        "generated_at": generated_at or utc_now(),
        "summary": summary,
        "groups": groups,
        "standalone_questions": standalone,
        "informational": informational,
        "metadata_changes": metadata,
        "text_requirement_changes": requirements,
        "missing_evidence": missing,
        "atomic_target_mapping": mapping,
        "review_item_classification": review_classification,
        "ai_routing": {
            row["target_id"]: {
                "routed_to_ai": row["new_category"] == ACTIONABLE_ENGINEERING_DECISION,
                "classification": row["new_category"],
                "reason": (
                    "AI_CAN_REDUCE_HUMAN_INTERACTION"
                    if row["new_category"] == ACTIONABLE_ENGINEERING_DECISION
                    else "CLASSIFIED_BEFORE_AI_ROUTING"
                ),
            }
            for row in mapping
        },
        "constraints": {
            "atomic_backend_preserved": True,
            "exact_stage7_accounting": True,
            "no_silent_drop": True,
            "missing_evidence_has_no_decision_button": True,
            "metadata_has_no_engineer_question": True,
            "human_override_has_priority": True,
            "document_inconsistency_not_duplicated": True,
            "uses_model": False,
        },
        "provenance": {
            "producer": PRODUCER,
            "sources": [
                "unified_synthesis", "engineer_decisions",
                "electrical_table_changes", "text_preparation",
            ],
        },
    }
    payload["input_signature"] = content_signature({
        "schema": SCHEMA_VERSION,
        "pair_id": pair_id,
        "mapping": mapping,
        "groups": groups,
        "standalone": standalone,
    })
    return payload


def materialize_group_decision(
    plan: Mapping[str, Any],
    *,
    group_id: str,
    answer: Mapping[str, Any],
    author: str,
    overrides: Iterable[Mapping[str, Any]] = (),
    generated_at: str | None = None,
) -> dict[str, Any]:
    """Map one human group answer to every atom, preserving per-atom audit."""
    if not author.strip():
        raise ValueError("human group decision author is required")
    group = next(
        (value for value in plan.get("groups") or () if value.get("group_id") == group_id),
        None,
    )
    if not isinstance(group, Mapping):
        raise ValueError("unknown human review group")
    answer_id = str(answer.get("answer_id") or "")
    allowed = {str(value.get("answer_id") or ""): value for value in group.get("allowed_answers") or ()}
    if answer_id not in allowed:
        raise ValueError("unsupported human review group answer")
    if allowed[answer_id].get("requires_mapping"):
        mapping = answer.get("mapping")
        if not isinstance(mapping, Mapping) or not mapping:
            raise ValueError("mode mapping answer requires at least one explicit mapping")
        left = set((group.get("mode_sets") or {}).get("LEFT") or ())
        right = set((group.get("mode_sets") or {}).get("RIGHT") or ())
        if not set(mapping).issubset(left) or not set(mapping.values()).issubset(right):
            raise ValueError("mode mapping contains a mode outside the allowed evidence set")

    affected = [str(value) for value in group.get("affected_target_ids") or ()]
    override_by_target: dict[str, Mapping[str, Any]] = {}
    for value in overrides:
        target_id = str(value.get("target_id") or "")
        if target_id not in affected:
            raise ValueError("atom override references a target outside the group")
        if target_id in override_by_target:
            raise ValueError("duplicate atom override")
        override_by_target[target_id] = value
    mapping_by_target = {
        str(value.get("target_id") or ""): value
        for value in plan.get("atomic_target_mapping") or ()
        if isinstance(value, Mapping)
    }
    now = generated_at or utc_now()
    group_decision_id = stable_id(
        "hgroupdec_", group_id, answer_id, answer.get("mapping"), author, length=28
    )
    atomic = []
    decision_updates = []
    for target_id in affected:
        source = mapping_by_target.get(target_id)
        if source is None:
            raise AssertionError("group member is missing from atomic target mapping")
        override = override_by_target.get(target_id)
        effective_answer = dict(override.get("answer") or {}) if override else dict(answer)
        resolution = {
            "resolution_id": stable_id("hatomdec_", group_decision_id, target_id, length=28),
            "target_id": target_id,
            "group_id": group_id,
            "group_decision_id": group_decision_id,
            "answer": effective_answer,
            "decision_source": "HUMAN_ATOM_OVERRIDE" if override else "HUMAN_GROUP_DECISION",
            "author": str(override.get("author") or author) if override else author,
            "comment": override.get("comment") if override else answer.get("comment"),
            "evidence_refs": list(source.get("evidence_refs") or ()),
            "atomic_target_snapshot": dict(source),
            "created_at": now,
        }
        atomic.append(resolution)
        if (group.get("materialization_policy") or {}).get("type") == "APPLY_ENGINEER_DECISION":
            decision_updates.append({
                "target_id": target_id,
                "decision": effective_answer.get("decision"),
                "author": resolution["author"],
                "comment": resolution["comment"],
                "reason_code": "HUMAN_REVIEW_GROUP",
            })
    if {row["target_id"] for row in atomic} != set(affected):
        raise AssertionError("group answer did not materialize every atomic member")
    return {
        "kind": "stage_comparison_human_review_group_decision",
        "schema_version": "human-review-group-decision.v1",
        "version": 1,
        "generated_at": now,
        "group_decision": {
            "group_decision_id": group_decision_id,
            "group_id": group_id,
            "answer": dict(answer),
            "author": author,
            "affected_target_ids": affected,
        },
        "atomic_resolutions": atomic,
        "engineer_decision_updates": decision_updates,
        "constraints": {
            "one_group_answer_many_atoms": True,
            "atomic_audit_preserved": True,
            "human_atom_override_priority": True,
            "invented_mapping": False,
        },
    }


def _answer_ids(question: Mapping[str, Any]) -> set[str]:
    return {
        str(value.get("answer_id") if isinstance(value, Mapping) else value)
        for value in question.get("allowed_answers") or ()
        if value
    }


def empty_human_review_decisions(
    plan: Mapping[str, Any], *, generated_at: str | None = None,
) -> dict[str, Any]:
    return {
        "kind": "stage_comparison_human_review_decisions",
        "schema_version": "human-review-decisions.v1",
        "version": 1,
        "generated_at": generated_at or utc_now(),
        "input_signature": plan.get("input_signature"),
        "revision": 0,
        "group_decisions": [],
        "standalone_answers": [],
        "constraints": {
            "clarification_is_not_final_approval": True,
            "human_atom_override_priority": True,
        },
    }


def update_human_review_decisions(
    plan: Mapping[str, Any],
    existing: Mapping[str, Any] | None,
    *,
    updates: Iterable[Mapping[str, Any]],
    author: str,
    generated_at: str | None = None,
) -> dict[str, Any]:
    """Validate and materialize clarification answers without approving findings."""
    if not author.strip():
        raise ValueError("human review author is required")
    current = (
        dict(existing)
        if isinstance(existing, Mapping)
        and existing.get("input_signature") == plan.get("input_signature")
        else empty_human_review_decisions(plan, generated_at=generated_at)
    )
    groups = {
        str(value.get("group_id") or ""): value
        for value in plan.get("groups") or () if isinstance(value, Mapping)
    }
    questions = {
        str(value.get("question_id") or ""): value
        for value in plan.get("standalone_questions") or ()
        if isinstance(value, Mapping)
    }
    group_records = {
        str(((value.get("group_decision") or {}).get("group_id")) or ""): dict(value)
        for value in current.get("group_decisions") or ()
        if isinstance(value, Mapping)
    }
    answer_records = {
        str(value.get("question_id") or ""): dict(value)
        for value in current.get("standalone_answers") or ()
        if isinstance(value, Mapping)
    }
    changed = False
    now = generated_at or utc_now()
    for update in updates:
        interaction_id = str(update.get("interaction_id") or "")
        answer = update.get("answer")
        if not interaction_id or not isinstance(answer, Mapping):
            raise ValueError("human review update requires interaction_id and answer")
        answer_id = str(answer.get("answer_id") or "")
        if interaction_id in groups:
            record = materialize_group_decision(
                plan,
                group_id=interaction_id,
                answer=answer,
                author=author,
                overrides=update.get("overrides") or (),
                generated_at=now,
            )
            group_records[interaction_id] = record
            changed = True
            continue
        question = questions.get(interaction_id)
        if question is None:
            raise ValueError("unknown human review interaction")
        if answer_id not in _answer_ids(question):
            raise ValueError("unsupported standalone human review answer")
        if answer_id == "SELECT_ROW_PAIR":
            allowed = next(
                value for value in question.get("allowed_answers") or ()
                if isinstance(value, Mapping)
                and value.get("answer_id") == "SELECT_ROW_PAIR"
            )
            left_row_id = str(answer.get("left_row_id") or "")
            right_row_id = str(answer.get("right_row_id") or "")
            if (
                left_row_id not in set(allowed.get("left_row_ids") or ())
                or right_row_id not in set(allowed.get("right_row_ids") or ())
            ):
                raise ValueError("selected table row is outside the evidence set")
        affected = [str(value) for value in question.get("affected_target_ids") or ()]
        answer_records[interaction_id] = {
            "answer_id": stable_id(
                "hanswer_", interaction_id, answer, author, length=28
            ),
            "question_id": interaction_id,
            "answer": dict(answer),
            "author": author,
            "comment": update.get("comment"),
            "affected_target_ids": affected,
            "atomic_resolutions": [
                {
                    "resolution_id": stable_id(
                        "hatomdec_", interaction_id, target_id, answer, length=28
                    ),
                    "target_id": target_id,
                    "answer": dict(answer),
                    "decision_source": "HUMAN_STANDALONE_DECISION",
                    "author": author,
                    "created_at": now,
                }
                for target_id in affected
            ],
            "does_not_approve_finding": True,
            "created_at": now,
        }
        changed = True
    if not changed:
        return current
    return {
        **current,
        "generated_at": now,
        "revision": int(current.get("revision") or 0) + 1,
        "group_decisions": [group_records[key] for key in sorted(group_records)],
        "standalone_answers": [answer_records[key] for key in sorted(answer_records)],
    }


def effective_human_resolution(
    target: Mapping[str, Any],
    *,
    group_resolution: Mapping[str, Any] | None = None,
    standalone_resolution: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Apply the documented priority without mutating the atomic finding."""
    if group_resolution:
        source = str(group_resolution.get("decision_source") or "")
        if source == "HUMAN_ATOM_OVERRIDE":
            return {**group_resolution, "priority": 400}
        return {**group_resolution, "priority": 300}
    if standalone_resolution:
        return {**standalone_resolution, "priority": 300}
    if str(target.get("current_category") or "") == PROVEN_CHANGE:
        return {
            "target_id": target.get("target_id"),
            "decision_source": "AI_VERIFIED_OR_DETERMINISTIC",
            "priority": 200,
        }
    return {
        "target_id": target.get("target_id"),
        "decision_source": "DETERMINISTIC_CANDIDATE",
        "priority": 100,
    }


def build_human_review_view(
    plan: Mapping[str, Any],
    *,
    synthesis: Mapping[str, Any],
    engineer_decisions: Mapping[str, Any] | None,
    human_decisions: Mapping[str, Any] | None = None,
    document_inconsistencies: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Stage-7 read model: questions outside, immutable atoms inside."""
    from .engineer_review import review_rows

    atomic_rows = {
        row["target_id"]: row for row in review_rows(synthesis, engineer_decisions)
    }
    mapping = {
        str(row.get("target_id") or ""): row
        for row in plan.get("atomic_target_mapping") or ()
    }
    group_decisions = {
        str(((value.get("group_decision") or {}).get("group_id")) or ""): value
        for value in (human_decisions or {}).get("group_decisions") or ()
        if isinstance(value, Mapping)
    }
    standalone_answers = {
        str(value.get("question_id") or ""): value
        for value in (human_decisions or {}).get("standalone_answers") or ()
        if isinstance(value, Mapping)
    }
    groups = []
    for group in plan.get("groups") or ():
        group_id = str(group.get("group_id") or "")
        decision = group_decisions.get(group_id) or {}
        resolutions = {
            str(value.get("target_id") or ""): value
            for value in decision.get("atomic_resolutions") or ()
            if isinstance(value, Mapping)
        }
        groups.append({
            **group,
            "human_decision": decision.get("group_decision"),
            "affected_atomic_changes": [
                {
                    **(atomic_rows.get(target_id) or mapping.get(target_id) or {}),
                    "effective_resolution": effective_human_resolution(
                        mapping.get(target_id) or {},
                        group_resolution=resolutions.get(target_id),
                    ),
                }
                for target_id in group.get("affected_target_ids") or ()
            ],
        })
    standalone = []
    for question in plan.get("standalone_questions") or ():
        question_id = str(question.get("question_id") or "")
        answer = standalone_answers.get(question_id)
        standalone.append({**question, "human_answer": answer})
    total = len(groups) + len(standalone)
    answered = sum(bool(group.get("human_decision")) for group in groups) + sum(
        bool(question.get("human_answer")) for question in standalone
    )
    return {
        "kind": "stage_comparison_human_review_view",
        "schema_version": "human-review-view.v1",
        "summary": {
            **dict(plan.get("summary") or {}),
            "interactions_total": total,
            "interactions_answered": answered,
            "interactions_pending": max(0, total - answered),
        },
        "review_groups": groups,
        "standalone_questions": standalone,
        "informational": list(plan.get("informational") or ()),
        "metadata_changes": list(plan.get("metadata_changes") or ()),
        "text_requirement_changes": list(plan.get("text_requirement_changes") or ()),
        "missing_evidence": list(plan.get("missing_evidence") or ()),
        "document_inconsistencies": list(
            (document_inconsistencies or {}).get("items") or ()
        ),
        "atomic_targets": [
            {**row, "review_classification": mapping.get(target_id)}
            for target_id, row in sorted(atomic_rows.items())
        ],
        "constraints": {
            "atomic_backend_preserved": True,
            "per_atom_override_allowed": True,
            "clarification_is_not_final_approval": True,
            "final_report_approved_only": True,
        },
        "input_signature": plan.get("input_signature"),
        "revision": int((human_decisions or {}).get("revision") or 0),
    }


__all__ = [
    "ACTIONABLE_ENGINEERING_DECISION",
    "DOCUMENT_METADATA_CHANGE",
    "FINAL_APPROVAL_ONLY",
    "INFORMATIONAL_LIMITATION",
    "KIND",
    "MISSING_EVIDENCE",
    "MODE_NOT_COMPARABLE",
    "PROVEN_CHANGE",
    "ROOT_CAUSE_GROUP_MEMBER",
    "SCHEMA_VERSION",
    "SYSTEM_DIAGNOSTIC",
    "TEXT_REQUIREMENT_CHANGE",
    "UNRESOLVED_CLASSES",
    "build_human_review_plan",
    "build_human_review_view",
    "blocked_target_id",
    "effective_human_resolution",
    "empty_human_review_decisions",
    "materialize_group_decision",
    "mode_target_id",
    "unproven_target_id",
    "update_human_review_decisions",
]
