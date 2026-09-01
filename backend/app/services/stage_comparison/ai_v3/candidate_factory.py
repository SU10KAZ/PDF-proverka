"""Deterministic finite candidate generation for AI Analyst v3.

The factory, not the model, owns every identifier, reference, value, unit and
materialization instruction.  Invalid options remain in the audit artifact but
are omitted from the selector payload.
"""
from __future__ import annotations

import copy
import re
import unicodedata
from difflib import SequenceMatcher
from typing import Any, Iterable, Mapping, Sequence

from backend.app.services.common.electrical_values import parse_cable

from ..production_artifacts import content_signature, stable_id
from ..ai_v2 import context as v2_context
from ..ai_v2 import inventory as v2_inventory
from . import schemas

_INSUFFICIENT = "INSUFFICIENT_EVIDENCE"
_NONE = "NONE"


def _normalize(value: Any) -> str:
    text = unicodedata.normalize("NFKC", str(value or "")).casefold()
    text = text.replace("ё", "е").replace("pe", "ре")
    return " ".join(re.sub(r"[^0-9a-zа-я]+", " ", text).split())


def _similarity(left: Any, right: Any) -> float:
    a, b = _normalize(left), _normalize(right)
    return round(SequenceMatcher(None, a, b).ratio(), 6) if a and b else 0.0


def _candidate_id(task_id: str, candidate_type: str, core: Mapping[str, Any]) -> str:
    return stable_id(
        "aiv3cand_", task_id, candidate_type, content_signature(core), length=24
    )


def _proof(code: str, status: str, detail: str = "") -> dict[str, str]:
    return {"code": code, "status": status, "detail": detail}


def _make_candidate(
    *,
    task_id: str,
    candidate_type: str,
    summary: str,
    left_refs: Iterable[Any] = (),
    right_refs: Iterable[Any] = (),
    values: Mapping[str, Any] | None = None,
    units: Mapping[str, Any] | None = None,
    entity_refs: Iterable[Any] = (),
    graph_refs: Iterable[Any] = (),
    table_refs: Iterable[Any] = (),
    text_refs: Iterable[Any] = (),
    deterministic_features: Mapping[str, Any] | None = None,
    proof_requirements: Sequence[Mapping[str, Any]] = (),
    eligibility: str = schemas.AUTO,
    prefilter_reasons: Iterable[Any] = (),
    resolution_effect: str = "HUMAN_REQUIRED",
    materialization: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    reasons = sorted({str(value) for value in prefilter_reasons if str(value)})
    if reasons:
        eligibility = schemas.INVALID
    core = {
        "candidate_type": candidate_type,
        "left_refs": sorted({str(value) for value in left_refs if str(value)}),
        "right_refs": sorted({str(value) for value in right_refs if str(value)}),
        "values": copy.deepcopy(dict(values or {})),
        "units": copy.deepcopy(dict(units or {})),
        "entity_refs": sorted({str(value) for value in entity_refs if str(value)}),
        "graph_refs": sorted({str(value) for value in graph_refs if str(value)}),
        "table_refs": sorted({str(value) for value in table_refs if str(value)}),
        "text_refs": sorted({str(value) for value in text_refs if str(value)}),
        "deterministic_features": copy.deepcopy(dict(deterministic_features or {})),
        "proof_requirements": [copy.deepcopy(dict(value)) for value in proof_requirements],
        "eligibility": eligibility,
        "prefilter_reasons": reasons,
        "resolution_effect": resolution_effect,
        "materialization": copy.deepcopy(dict(materialization or {})),
    }
    candidate_id = _candidate_id(task_id, candidate_type, core)
    result = {
        "candidate_id": candidate_id,
        "task_id": task_id,
        "summary": summary,
        **core,
    }
    result["candidate_signature"] = content_signature(result)
    return result


def _human_fallback(task_id: str) -> dict[str, Any]:
    return _make_candidate(
        task_id=task_id,
        candidate_type=_INSUFFICIENT,
        summary="Переданных доказательств недостаточно; решение остаётся инженеру.",
        proof_requirements=[_proof("FROZEN_TASK_EXISTS", "PROVEN")],
        resolution_effect="HUMAN_REQUIRED",
    )


def _row_values(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        str(value.get("facet") or ""): list(value.get("values") or ())
        for value in row.get("values") or ()
        if isinstance(value, Mapping) and value.get("facet")
    }


def _row_features(left: Mapping[str, Any], right: Mapping[str, Any]) -> dict[str, Any]:
    left_designations = {_normalize(value) for value in left.get("designations") or ()}
    right_designations = {_normalize(value) for value in right.get("designations") or ()}
    left_values, right_values = _row_values(left), _row_values(right)
    common_facets = sorted(set(left_values) & set(right_values))
    return {
        "section_equal": _normalize(left.get("section")) == _normalize(right.get("section")),
        "row_kind_equal": left.get("row_kind") == right.get("row_kind"),
        "designation_overlap": sorted((left_designations & right_designations) - {""}),
        "label_similarity": _similarity(left.get("label"), right.get("label")),
        "cable_similarity": _similarity(left.get("cables"), right.get("cables")),
        "common_facets": common_facets,
        "left_values": {key: left_values[key] for key in common_facets},
        "right_values": {key: right_values[key] for key in common_facets},
        "left_mode": left.get("mode") or "",
        "right_mode": right.get("mode") or "",
        "left_table_position": left.get("table_position"),
        "right_table_position": right.get("table_position"),
        "left_neighboring_rows": list(left.get("neighboring_row_refs") or ()),
        "right_neighboring_rows": list(right.get("neighboring_row_refs") or ()),
    }


def _row_pair_candidate(
    task_id: str,
    left_ref: str,
    right_ref: str,
    catalog: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    left, right = catalog.get(left_ref) or {}, catalog.get(right_ref) or {}
    features = _row_features(left, right)
    reasons: list[str] = []
    if left.get("side") != "LEFT" or right.get("side") != "RIGHT":
        reasons.append("WRONG_SIDE")
    if left.get("row_kind") and right.get("row_kind") and not features["row_kind_equal"]:
        reasons.append("INCOMPATIBLE_ENTITY_TYPE")
    if left.get("section") and right.get("section") and not features["section_equal"]:
        reasons.append("DIFFERENT_SECTION_WITHOUT_RELATION")
    units = {
        "LEFT": sorted({
            str(value.get("unit") or "") for value in left.get("values") or ()
            if isinstance(value, Mapping) and value.get("unit")
        }),
        "RIGHT": sorted({
            str(value.get("unit") or "") for value in right.get("values") or ()
            if isinstance(value, Mapping) and value.get("unit")
        }),
    }
    return _make_candidate(
        task_id=task_id,
        candidate_type="ROW_PAIR",
        summary=f"{left.get('label') or left_ref} ↔ {right.get('label') or right_ref}",
        left_refs=[left_ref],
        right_refs=[right_ref],
        values={"LEFT": _row_values(left), "RIGHT": _row_values(right)},
        units=units,
        entity_refs=[left_ref, right_ref],
        table_refs=[
            left_ref, right_ref,
            *list(left.get("neighboring_row_refs") or ()),
            *list(right.get("neighboring_row_refs") or ()),
        ],
        deterministic_features=features,
        proof_requirements=[
            _proof("OPPOSITE_SIDES", "PROVEN" if "WRONG_SIDE" not in reasons else "FAILED"),
            _proof("SECTION_COMPATIBLE", "PROVEN" if "DIFFERENT_SECTION_WITHOUT_RELATION" not in reasons else "FAILED"),
            _proof("ROW_KIND_COMPATIBLE", "PROVEN" if "INCOMPATIBLE_ENTITY_TYPE" not in reasons else "FAILED"),
            _proof("SEMANTIC_IDENTITY_RANKING", "REQUIRED"),
        ],
        prefilter_reasons=reasons,
        resolution_effect="VERIFIED_RELATION",
        materialization={
            "kind": "TABLE_ROW_IDENTITY",
            "left_row_id": left.get("row_id"),
            "right_row_id": right.get("row_id"),
        },
    )


def _table_candidates(
    task: Mapping[str, Any], catalog: Mapping[str, Mapping[str, Any]]
) -> list[dict[str, Any]]:
    task_id = str(task.get("task_id") or "")
    payload = task.get("routing_payload") or {}
    kind = str(task.get("source_kind") or "")
    pairs: list[tuple[str, str]] = []
    if kind == "TABLE_ROW_BLOCKED":
        pairs = [
            (f"LEFT:ROW:{left}", f"RIGHT:ROW:{right}")
            for left in payload.get("left_row_ids") or ()
            for right in payload.get("right_row_ids") or ()
        ]
    elif kind == "TABLE_ROW_UNPROVEN":
        anchor_id = str(payload.get("row_id") or "")
        side = str(payload.get("side") or task.get("side") or "")
        for other in payload.get("candidate_row_ids") or ():
            if side == "LEFT":
                pairs.append((f"LEFT:ROW:{anchor_id}", f"RIGHT:ROW:{other}"))
            elif side == "RIGHT":
                pairs.append((f"LEFT:ROW:{other}", f"RIGHT:ROW:{anchor_id}"))
    candidates = [
        _row_pair_candidate(task_id, left, right, catalog)
        for left, right in pairs[:8]
        if left in catalog and right in catalog
    ]
    candidates.append(_make_candidate(
        task_id=task_id,
        candidate_type=_NONE,
        summary="Ни одна переданная пара не доказана.",
        proof_requirements=[_proof("BOUNDED_CANDIDATES_REVIEWED", "REQUIRED")],
        eligibility=schemas.ADVISORY,
        resolution_effect="HUMAN_REQUIRED",
    ))
    candidates.append(_human_fallback(task_id))
    return candidates


def _cable_counts(
    refs: Iterable[str], catalog: Mapping[str, Mapping[str, Any]]
) -> dict[tuple[str, int], list[str]]:
    output: dict[tuple[str, int], list[str]] = {}
    for ref in refs:
        item = catalog.get(ref) or {}
        side = str(item.get("side") or "")
        raw_values = list(item.get("cables") or ())
        attrs = item.get("attrs") or {}
        if isinstance(attrs, Mapping):
            raw_values.extend(attrs.get("cables") or ())
        for raw in raw_values:
            parsed = parse_cable(raw)
            if not parsed or parsed.get("parallel_count_proven") is not True:
                continue
            output.setdefault((side, int(parsed["parallel_count"])), []).append(ref)
    return {key: sorted(set(value)) for key, value in output.items()}


def _change_candidates(
    task: Mapping[str, Any],
    focus: Mapping[str, Any],
    catalog: Mapping[str, Mapping[str, Any]],
) -> list[dict[str, Any]]:
    task_id = str(task.get("task_id") or "")
    refs = list(dict.fromkeys([
        *list(focus.get("candidate_refs") or ()),
        *list(focus.get("context_refs") or ()),
    ]))
    fast_ref = f"FAST:CHANGE:{task_id}"
    fast = catalog.get(fast_ref) or {}
    facet = str(fast.get("facet") or "")
    relation = fast.get("relation") or {}
    entity_refs = [
        ref for ref in focus.get("candidate_refs") or () if ":NODE:" in ref
    ]
    candidates: list[dict[str, Any]] = []
    if facet == "cable_parallel_count":
        counts = _cable_counts(refs, catalog)
        for left_count in sorted({count for (side, count) in counts if side == "LEFT"}):
            for right_count in sorted({count for (side, count) in counts if side == "RIGHT"}):
                left_refs = counts[("LEFT", left_count)]
                right_refs = counts[("RIGHT", right_count)]
                same = left_count == right_count
                kind = (
                    f"FORMATTING_ONLY_{left_count}_TO_{right_count}"
                    if same else f"REAL_CHANGE_{left_count}_TO_{right_count}"
                )
                candidates.append(_make_candidate(
                    task_id=task_id,
                    candidate_type=kind,
                    summary=(
                        f"Количество кабелей одинаково: {left_count} → {right_count}."
                        if same else
                        f"Реальное изменение числа кабелей: {left_count} → {right_count}."
                    ),
                    left_refs=left_refs,
                    right_refs=right_refs,
                    values={"before": left_count, "after": right_count},
                    units={"before": None, "after": None},
                    entity_refs=entity_refs,
                    table_refs=[
                        ref for ref in [*left_refs, *right_refs] if ":ROW:" in ref
                    ],
                    deterministic_features={
                        "fast_ref": fast_ref if fast_ref in catalog else None,
                        "fast_identity_method": relation.get("identity_match_method"),
                        "alternative_value_pairs": True,
                    },
                    proof_requirements=[
                        _proof("VALUES_PREBOUND", "PROVEN"),
                        _proof("SAME_ENTITY", "REQUIRED"),
                        _proof("FAST_SOURCE_BOUND", "PROVEN" if fast_ref in catalog else "FAILED"),
                    ],
                    prefilter_reasons=[] if fast_ref in catalog else ["FAST_REF_MISSING"],
                    resolution_effect="NO_CHANGE" if same else "MATERIALIZED_CHANGE",
                    materialization={
                        "kind": "CHANGE_INTERPRETATION",
                        "verdict": "FORMATTING_ONLY" if same else "SUPPORTED_CHANGE",
                        "source_kind": task.get("source_kind"),
                    },
                ))
    else:
        left_value = relation.get("left_value", fast.get("before_value"))
        right_value = relation.get("right_value", fast.get("after_value"))
        is_reserve = "reserve" in facet.casefold() or "резерв" in str(
            task.get("summary") or ""
        ).casefold()
        absence_proven = bool(
            relation.get("left_absence_proven") is True
            or fast.get("bounded_left_absence") is True
        )
        reasons = ["LEFT_ABSENCE_NOT_PROVEN"] if is_reserve and not absence_proven else []
        if left_value is not None or right_value is not None:
            candidates.append(_make_candidate(
                task_id=task_id,
                candidate_type=f"SUPPORTED_CHANGE_{left_value}_TO_{right_value}",
                summary=f"Изменение подтверждено: {left_value} → {right_value}.",
                left_refs=[ref for ref in refs if ref.startswith("LEFT:")],
                right_refs=[ref for ref in refs if ref.startswith("RIGHT:")],
                values={"before": left_value, "after": right_value},
                units={"before": relation.get("unit"), "after": relation.get("unit")},
                entity_refs=entity_refs,
                graph_refs=[ref for ref in refs if ":EDGE:" in ref],
                deterministic_features={"bounded_left_absence": absence_proven},
                proof_requirements=[
                    _proof("VALUES_PREBOUND", "PROVEN"),
                    _proof("BOUNDED_ABSENCE", "PROVEN" if not is_reserve or absence_proven else "FAILED"),
                ],
                prefilter_reasons=reasons,
                resolution_effect="MATERIALIZED_CHANGE",
                materialization={"kind": "CHANGE_INTERPRETATION", "verdict": "SUPPORTED_CHANGE"},
            ))
        if is_reserve:
            candidates.append(_make_candidate(
                task_id=task_id,
                candidate_type="EXISTING_LINES_NOT_MATCHED",
                summary="Правые резервные линии могли существовать слева, но не были сопоставлены.",
                right_refs=[ref for ref in refs if ref.startswith("RIGHT:")],
                entity_refs=entity_refs,
                graph_refs=[ref for ref in refs if ":EDGE:" in ref],
                proof_requirements=[_proof("ENTITY_CORRESPONDENCE", "REQUIRED")],
                eligibility=schemas.ADVISORY,
                resolution_effect="HUMAN_REQUIRED",
            ))
    candidates.append(_make_candidate(
        task_id=task_id,
        candidate_type="DIFFERENT_ENTITY",
        summary="Стороны могут относиться к разным объектам.",
        left_refs=[ref for ref in refs if ref.startswith("LEFT:")][:4],
        right_refs=[ref for ref in refs if ref.startswith("RIGHT:")][:4],
        entity_refs=entity_refs,
        proof_requirements=[_proof("DIFFERENT_ENTITY_PROOF", "REQUIRED")],
        eligibility=schemas.ADVISORY,
        resolution_effect="HUMAN_REQUIRED",
    ))
    candidates.append(_human_fallback(task_id))
    return candidates


def _graph_candidates(
    task: Mapping[str, Any], catalog: Mapping[str, Mapping[str, Any]]
) -> list[dict[str, Any]]:
    task_id = str(task.get("task_id") or "")
    payload = task.get("routing_payload") or {}
    left_ref = f"LEFT:NODE:{payload.get('left_node_id')}"
    candidates: list[dict[str, Any]] = []
    for right_id in payload.get("right_node_ids") or ():
        right_ref = f"RIGHT:NODE:{right_id}"
        left, right = catalog.get(left_ref) or {}, catalog.get(right_ref) or {}
        reasons = []
        if not left or not right:
            reasons.append("OBJECT_MISSING")
        if left.get("entity_type") != right.get("entity_type"):
            reasons.append("INCOMPATIBLE_ENTITY_TYPE")
        if left.get("section") and right.get("section") and _normalize(
            left.get("section")
        ) != _normalize(right.get("section")):
            reasons.append("DIFFERENT_SECTION_WITHOUT_RELATION")
        graph_refs = sorted({
            ref for ref, value in catalog.items()
            if ":EDGE:" in ref and isinstance(value, Mapping)
            and (
                str(value.get("from_entity") or "") in {
                    str(left.get("entity_id") or ""), str(right.get("entity_id") or "")
                }
                or str(value.get("to_entity") or "") in {
                    str(left.get("entity_id") or ""), str(right.get("entity_id") or "")
                }
            )
        })
        candidates.append(_make_candidate(
            task_id=task_id,
            candidate_type="ENTITY_PAIR",
            summary=f"{left.get('label') or left_ref} ↔ {right.get('label') or right_ref}",
            left_refs=[left_ref], right_refs=[right_ref],
            entity_refs=[left_ref, right_ref],
            graph_refs=graph_refs,
            deterministic_features={
                "type_equal": left.get("entity_type") == right.get("entity_type"),
                "section_equal": _normalize(left.get("section")) == _normalize(right.get("section")),
                "identity_equal": bool(left.get("canonical_identity")) and _normalize(
                    left.get("canonical_identity")
                ) == _normalize(right.get("canonical_identity")),
                "label_similarity": _similarity(left.get("label"), right.get("label")),
                "neighbor_relation_count": len(graph_refs),
            },
            proof_requirements=[
                _proof("ENTITY_TYPES_COMPATIBLE", "FAILED" if "INCOMPATIBLE_ENTITY_TYPE" in reasons else "PROVEN"),
                _proof("SEMANTIC_IDENTITY_RANKING", "REQUIRED"),
            ],
            prefilter_reasons=reasons,
            resolution_effect="VERIFIED_RELATION",
            materialization={"kind": "ENTITY_IDENTITY"},
        ))
    candidates.append(_make_candidate(
        task_id=task_id, candidate_type=_NONE,
        summary="Ни один переданный правый узел не соответствует левому.",
        left_refs=[left_ref] if left_ref in catalog else [],
        proof_requirements=[_proof("BOUNDED_CANDIDATES_REVIEWED", "REQUIRED")],
        eligibility=schemas.ADVISORY, resolution_effect="HUMAN_REQUIRED",
    ))
    candidates.append(_human_fallback(task_id))
    return candidates


def _fragment_catalog(
    artifacts: Mapping[str, Mapping[str, Any]],
) -> dict[str, dict[str, Any]]:
    output: dict[str, dict[str, Any]] = {}
    preparation = artifacts.get("text_preparation") or {}
    for side, key in (("LEFT", "left"), ("RIGHT", "right")):
        for fragment in (preparation.get("fragments") or {}).get(key) or ():
            if not isinstance(fragment, Mapping) or not fragment.get("id"):
                continue
            ref = f"{side}:TEXT:{fragment['id']}"
            output[ref] = {
                "ref": ref,
                "side": side,
                "fragment_id": str(fragment["id"]),
                "page": fragment.get("pdf_page"),
                "order": fragment.get("order"),
                "source": fragment.get("source"),
                "text": str(fragment.get("text") or ""),
            }
    return output


def _augment_table_context(
    catalog: dict[str, dict[str, Any]],
    artifacts: Mapping[str, Mapping[str, Any]],
) -> None:
    tables = (((artifacts.get("direct_page_mode2") or {}).get("diagnostics") or {}).get(
        "electrical_load_tables"
    ) or {})
    for side in ("LEFT", "RIGHT"):
        rows = [
            value for value in (tables.get(side) or {}).get("rows") or ()
            if isinstance(value, Mapping) and value.get("row_id")
        ]
        for index, row in enumerate(rows):
            ref = f"{side}:ROW:{row['row_id']}"
            if ref not in catalog:
                continue
            neighbors = []
            for neighbor_index in (index - 1, index + 1):
                if 0 <= neighbor_index < len(rows):
                    neighbors.append(f"{side}:ROW:{rows[neighbor_index]['row_id']}")
            catalog[ref]["table_position"] = index + 1
            catalog[ref]["neighboring_row_refs"] = neighbors


def _find_text_refs(
    catalog: Mapping[str, Mapping[str, Any]], side: str, needle: str
) -> list[str]:
    wanted = _normalize(needle)
    if not wanted:
        return []
    exact: list[str] = []
    containing: list[str] = []
    for ref, item in catalog.items():
        if not ref.startswith(side + ":TEXT:"):
            continue
        text = _normalize(item.get("text"))
        if text == wanted:
            exact.append(ref)
        elif wanted in text or text in wanted:
            containing.append(ref)
    return sorted(exact or containing, key=lambda ref: (
        len(str((catalog.get(ref) or {}).get("text") or "")), ref
    ))


def _text_candidates(
    *,
    task_id: str,
    question: Mapping[str, Any],
    catalog: Mapping[str, Mapping[str, Any]],
) -> list[dict[str, Any]]:
    evidence = question.get("candidate_evidence") or {}
    strong = str(evidence.get("strong_semantic_candidate") or "")
    left_refs: list[str] = []
    for part in [value.strip() for value in strong.split("|") if value.strip()]:
        left_refs.extend(_find_text_refs(catalog, "LEFT", part)[:2])
    target_text = str(question.get("target_text") or question.get("question") or "")
    right_refs = _find_text_refs(catalog, "RIGHT", target_text)
    if not right_refs:
        # The question wording is not the exact requirement; use its affected
        # target's known keyword-rich tail when supplied by the caller.
        right_refs = list(question.get("right_text_refs") or ())
    left_refs = list(dict.fromkeys(left_refs))
    right_refs = list(dict.fromkeys(right_refs))
    candidates: list[dict[str, Any]] = []
    if left_refs and right_refs:
        common = {
            "left_texts": [catalog[ref].get("text") for ref in left_refs],
            "right_texts": [catalog[ref].get("text") for ref in right_refs],
            "semantic_similarity": max(
                _similarity(catalog[left].get("text"), catalog[right].get("text"))
                for left in left_refs for right in right_refs
            ),
        }
        for candidate_type, effect in (
            ("SAME_REQUIREMENT", "RESOLVE_HUMAN_QUESTION"),
            ("REQUIREMENT_CHANGED", "RESOLVE_HUMAN_QUESTION"),
            ("DIFFERENT_REQUIREMENT", "RESOLVE_HUMAN_QUESTION"),
        ):
            candidates.append(_make_candidate(
                task_id=task_id,
                candidate_type=candidate_type,
                summary={
                    "SAME_REQUIREMENT": "Переданные фрагменты выражают одно техническое требование.",
                    "REQUIREMENT_CHANGED": "Требование существовало, но его содержание изменено.",
                    "DIFFERENT_REQUIREMENT": "Фрагменты относятся к разным техническим требованиям.",
                }[candidate_type],
                left_refs=left_refs, right_refs=right_refs,
                text_refs=[*left_refs, *right_refs],
                deterministic_features=common,
                proof_requirements=[
                    _proof("TEXT_SPANS_PREBOUND", "PROVEN"),
                    _proof("SEMANTIC_EQUIVALENCE_RANKING", "REQUIRED"),
                ],
                resolution_effect=effect,
                materialization={
                    "kind": "TEXT_EQUIVALENCE",
                    "answer": candidate_type,
                    "human_question_id": question.get("question_id"),
                    "affected_target_ids": list(question.get("affected_target_ids") or ()),
                },
            ))
    absence_proven = bool(
        evidence.get("full_searchable_text") is True
        and evidence.get("recognition_coverage") == "HIGH"
        and not left_refs
        and evidence.get("exact_match") is None
        and evidence.get("normalized_match") is None
        and not strong
    )
    candidates.append(_make_candidate(
        task_id=task_id,
        candidate_type="REQUIREMENT_ADDED",
        summary="Требование доказанно отсутствовало слева и добавлено справа.",
        right_refs=right_refs,
        text_refs=right_refs,
        deterministic_features={"bounded_absence": absence_proven},
        proof_requirements=[
            _proof("BOUNDED_LEFT_ABSENCE", "PROVEN" if absence_proven else "FAILED")
        ],
        prefilter_reasons=[] if absence_proven else ["LEFT_ABSENCE_NOT_PROVEN"],
        resolution_effect="RESOLVE_HUMAN_QUESTION",
        materialization={
            "kind": "TEXT_EQUIVALENCE", "answer": "REQUIREMENT_ADDED",
            "human_question_id": question.get("question_id"),
            "affected_target_ids": list(question.get("affected_target_ids") or ()),
        },
    ))
    candidates.append(_human_fallback(task_id))
    return candidates


def _label_candidates(
    task: Mapping[str, Any], focus: Mapping[str, Any]
) -> list[dict[str, Any]]:
    task_id = str(task.get("task_id") or "")
    refs = [
        ref for ref in [*list(focus.get("candidate_refs") or ()), *list(focus.get("context_refs") or ())]
        if ref
    ]
    return [
        _make_candidate(
            task_id=task_id, candidate_type="LABEL_IS_CORRECT",
            summary="Подпись согласована с переданным контекстом.",
            left_refs=[ref for ref in refs if ref.startswith("LEFT:")],
            right_refs=[ref for ref in refs if ref.startswith("RIGHT:")],
            graph_refs=[ref for ref in refs if ":EDGE:" in ref],
            text_refs=refs,
            proof_requirements=[_proof("LABEL_CONTEXT", "REQUIRED")],
            eligibility=schemas.ADVISORY, resolution_effect="HUMAN_REQUIRED",
        ),
        _make_candidate(
            task_id=task_id, candidate_type="DOCUMENT_CONFLICT_ONLY",
            summary="Переданные доказательства показывают внутренний конфликт подписи.",
            left_refs=[ref for ref in refs if ref.startswith("LEFT:")],
            right_refs=[ref for ref in refs if ref.startswith("RIGHT:")],
            graph_refs=[ref for ref in refs if ":EDGE:" in ref],
            text_refs=refs,
            proof_requirements=[_proof("CONFLICT_EVIDENCE", "REQUIRED")],
            resolution_effect="DOCUMENT_INCONSISTENCY",
            materialization={"kind": "LABEL_CONFLICT", "verdict": "DOCUMENT_ERROR"},
        ),
        _human_fallback(task_id),
    ]


def _mode_candidates(task: Mapping[str, Any]) -> list[dict[str, Any]]:
    task_id = str(task.get("task_id") or "")
    return [
        _make_candidate(
            task_id=task_id,
            candidate_type="ADVISORY_MODE_RANKING",
            summary=str(task.get("summary") or "Рекомендательное ранжирование режимов."),
            proof_requirements=[_proof("PROJECT_AUTHORITY", "HUMAN_ONLY")],
            eligibility=schemas.ADVISORY,
            resolution_effect="HUMAN_REQUIRED",
        ),
        _human_fallback(task_id),
    ]


def _task_type(task: Mapping[str, Any]) -> str:
    source_kind = str(task.get("source_kind") or "")
    old = str(task.get("task_type") or "")
    if old == "MODE_RELATION":
        return schemas.MODE_MAPPING
    if source_kind == "GRAPH_ENTITY_AMBIGUITY":
        return schemas.ENTITY_IDENTITY
    if "TABLE_ROW" in source_kind:
        return schemas.TABLE_ROW_IDENTITY
    if old == "LABEL_CONFLICT":
        return schemas.LABEL_CONFLICT
    return schemas.CHANGE_INTERPRETATION


def _selector_group(task_type: str) -> str:
    if task_type == schemas.TABLE_ROW_IDENTITY:
        return "table_feeder_identity"
    if task_type in {schemas.ENTITY_IDENTITY, schemas.LABEL_CONFLICT, schemas.MODE_MAPPING}:
        return "graph_entity_identity"
    return "text_change_interpretation"


def _decorate_task(
    task: Mapping[str, Any],
    task_type: str,
    candidates: Sequence[Mapping[str, Any]],
    *,
    question: str | None = None,
    human_question_id: str | None = None,
    affected_target_ids: Iterable[Any] = (),
) -> dict[str, Any]:
    ordered = sorted(
        (copy.deepcopy(dict(value)) for value in candidates),
        key=lambda value: (value["candidate_type"], value["candidate_id"]),
    )
    selectable = [
        value["candidate_id"] for value in ordered
        if value["eligibility"] != schemas.INVALID
    ]
    auto_effects = [
        value for value in ordered
        if value["eligibility"] == schemas.AUTO
        and value["resolution_effect"] != "HUMAN_REQUIRED"
    ]
    deterministic = None
    if len(auto_effects) == 1 and all(
        proof.get("status") == "PROVEN"
        for proof in auto_effects[0].get("proof_requirements") or ()
    ):
        deterministic = auto_effects[0]["candidate_id"]
    core = {
        "task_id": str(task.get("task_id") or ""),
        "task_type": task_type,
        "source_kind": str(task.get("source_kind") or ""),
        "question": question or str(task.get("summary") or ""),
        "subject": task.get("subject"),
        "selector_group": _selector_group(task_type),
        "human_question_id": human_question_id,
        "affected_target_ids": sorted({str(value) for value in affected_target_ids if str(value)}),
        "candidates": ordered,
        "selectable_candidate_ids": selectable,
        "deterministic_winner_candidate_id": deterministic,
    }
    core["task_signature"] = content_signature(core)
    return core


def _human_question_indexes(
    plan: Mapping[str, Any],
) -> tuple[dict[str, Mapping[str, Any]], Mapping[str, Any] | None, Mapping[str, Any] | None]:
    by_target: dict[str, Mapping[str, Any]] = {}
    table_question = None
    graph_question = None
    for question in plan.get("standalone_questions") or ():
        if not isinstance(question, Mapping):
            continue
        for target in question.get("affected_target_ids") or ():
            by_target[str(target)] = question
        if question.get("decision_type") == "TABLE_ROW_IDENTITY":
            table_question = question
        if question.get("decision_type") == "GRAPH_CORRESPONDENCE_CLARIFICATION":
            graph_question = question
    return by_target, table_question, graph_question


def build_candidate_factory(
    *,
    artifacts: Mapping[str, Mapping[str, Any]],
    pair_id: str,
    fast_input_signature: str | None = None,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, dict[str, Any]]]:
    """Build identical finite candidates for identical frozen artifacts."""
    human_plan = artifacts.get("human_review_plan") or {}
    inventory = v2_inventory.build_inventory(
        legacy_inventory=artifacts.get("ai_routing_inventory") or {},
        direct_page=artifacts.get("direct_page_mode2") or {},
        human_review_plan=None,
        pair_id=pair_id,
        generated_at="FROZEN",
    )
    bundle = v2_context.build_context_bundle(
        artifacts=artifacts, inventory=inventory, pair_id=pair_id
    )
    catalog = {key: copy.deepcopy(value) for key, value in bundle.evidence_catalog.items()}
    catalog.update(_fragment_catalog(artifacts))
    _augment_table_context(catalog, artifacts)
    by_target, table_hro, graph_hro = _human_question_indexes(human_plan)

    tasks: list[dict[str, Any]] = []
    for raw in inventory.get("items") or ():
        if not isinstance(raw, Mapping) or not raw.get("unresolved", True):
            continue
        decision = str(raw.get("decision") or "")
        task_type = _task_type(raw)
        if decision != v2_inventory.ELIGIBLE and task_type != schemas.MODE_MAPPING:
            continue
        task_id = str(raw.get("task_id") or "")
        focus = bundle.focused_by_task.get(task_id) or {}
        hro = by_target.get(task_id)
        if task_type == schemas.TABLE_ROW_IDENTITY:
            candidates = _table_candidates(raw, catalog)
            hro = table_hro if str(raw.get("source_kind")) == "TABLE_ROW_BLOCKED" else hro
        elif task_type == schemas.ENTITY_IDENTITY:
            candidates = _graph_candidates(raw, catalog)
            hro = graph_hro
        elif task_type == schemas.LABEL_CONFLICT:
            candidates = _label_candidates(raw, focus)
        elif task_type == schemas.MODE_MAPPING:
            candidates = _mode_candidates(raw)
        else:
            candidates = _change_candidates(raw, focus, catalog)
        tasks.append(_decorate_task(
            raw, task_type, candidates,
            human_question_id=str((hro or {}).get("question_id") or "") or None,
            affected_target_ids=(hro or {}).get("affected_target_ids") or (),
        ))

    existing_ids = {task["task_id"] for task in tasks}
    text_focus = bundle.focused_by_task
    for question in human_plan.get("standalone_questions") or ():
        if not isinstance(question, Mapping) or question.get("decision_type") != "TEXT_REQUIREMENT_EQUIVALENCE":
            continue
        target_ids = [str(value) for value in question.get("affected_target_ids") or ()]
        if not target_ids:
            continue
        task_id = target_ids[0]
        if task_id in existing_ids:
            tasks = [task for task in tasks if task["task_id"] != task_id]
        focus = text_focus.get(task_id) or {}
        text_record = catalog.get(f"FOCUS:TEXT:{task_id}") or {}
        after = (((text_record.get("text") or {}).get("after_value")) or "")
        right_refs = _find_text_refs(catalog, "RIGHT", after)
        enriched = {
            **dict(question),
            "target_text": after,
            "right_text_refs": right_refs,
        }
        candidates = _text_candidates(task_id=task_id, question=enriched, catalog=catalog)
        tasks.append(_decorate_task(
            {
                "task_id": task_id,
                "source_kind": "TEXT_REQUIREMENT_EQUIVALENCE",
                "summary": question.get("question"),
                "subject": None,
            },
            schemas.TEXT_EQUIVALENCE,
            candidates,
            question=str(question.get("question") or ""),
            human_question_id=str(question.get("question_id") or ""),
            affected_target_ids=target_ids,
        ))

    tasks.sort(key=lambda value: (value["selector_group"], value["task_type"], value["task_id"]))
    factory_core = {
        "kind": "stage_comparison_ai_v3_candidate_factory",
        "schema_version": schemas.CANDIDATE_SCHEMA_VERSION,
        "factory_version": schemas.FACTORY_VERSION,
        "pair_id": pair_id,
        "fast_input_signature": fast_input_signature or content_signature(artifacts),
        "tasks": tasks,
        "constraints": {
            "model_generated_ids": False,
            "model_generated_evidence": False,
            "mode_mapping_auto_materialization": False,
            "invalid_candidates_sent_to_model": False,
        },
    }
    factory_core["candidate_set_signature"] = content_signature(factory_core)
    bundles = {
        "kind": "stage_comparison_ai_v3_candidate_bundles",
        "schema_version": schemas.BUNDLE_SCHEMA_VERSION,
        "candidate_set_signature": factory_core["candidate_set_signature"],
        "bundles": [
            copy.deepcopy(candidate)
            for task in tasks for candidate in task["candidates"]
        ],
    }
    bundles["input_signature"] = content_signature(bundles)
    return factory_core, bundles, catalog


__all__ = ["build_candidate_factory"]
