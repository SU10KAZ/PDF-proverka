"""Read-only consumption of Presentation Grouping B by Human Contour.

Groups are never decision targets. Membership is rebuilt from the current
generation, then bound to DomainKeys; legacy IDs are local diagnostic aliases.
No evidence payloads, registry or grouping rules are duplicated here.
"""
from __future__ import annotations

from collections import Counter
import os
from typing import Any, Mapping

from . import human_contour, review_presentation_groups as grouping

FEATURE_FLAG = "PRESENTATION_HUMAN_V1_ENABLED"
STATUS_LABELS = {
    "ALL_PENDING": "Нужно решить",
    "PARTIALLY_RESOLVED": "Частично решено",
    "ALL_RESOLVED": "Решено",
    "HAS_STALE": "Исходные данные изменились",
    "HAS_REVALIDATION_REQUIRED": "Требует перепроверки",
    "HAS_UNAVAILABLE": "Историческое / недоступно",
}


def _generation(session_id, pair_id):
    from . import production_store
    return production_store.load_artifact(session_id, pair_id, "state") or {}


def _check_generation(session_id, pair_id, before):
    from . import production_store
    after = _generation(session_id, pair_id)
    if any(after.get(key) != before.get(key)
           for key in ("run_id", "input_signature", "revision", "status")):
        raise production_store.ProductionConflictError(
            "production generation changed while presentation was read"
        )


def _question_response(session_id, pair_id, response):
    from . import store
    return questions(response, identity=grouping.pair_identity(
        store.get_pair_for_production(session_id, pair_id)))


def get_review_questions(session_id, pair_id):
    from . import production_orchestrator as production
    if not enabled():
        return production.get_review_questions(session_id, pair_id)
    before = _generation(session_id, pair_id)
    response = _question_response(
        session_id, pair_id, production.get_review_questions(session_id, pair_id))
    _check_generation(session_id, pair_id, before)
    return response


def get_production_changes(session_id, pair_id):
    from . import production_orchestrator as production, production_store, store
    if not enabled():
        return production.get_production_changes(session_id, pair_id)
    before = _generation(session_id, pair_id)
    response = production.get_production_changes(session_id, pair_id)
    if not response.get("available"):
        return response
    synthesis = production_store.load_artifact(
        session_id, pair_id, "unified_synthesis", include_domain_keys=True) or {}
    response = reviews(
        response, identity=grouping.pair_identity(store.get_pair_for_production(session_id, pair_id)),
        synthesis=synthesis,
        plan=production_store.load_artifact(session_id, pair_id, "human_review_plan"),
        ownership=production_store.load_artifact(session_id, pair_id, "text_fact_ownership"),
    )
    _check_generation(session_id, pair_id, before)
    return response


def update_atomic_question_answers(session_id, pair_id, **kwargs):
    from . import production_orchestrator as production, store
    if not enabled():
        return production.update_atomic_question_answers(session_id, pair_id, **kwargs)
    identity = grouping.pair_identity(store.get_pair_for_production(session_id, pair_id))
    # The writer returns its generation-bound atomic snapshot under its lock.
    # Project that snapshot directly without re-reading children after the write.
    response = production.update_atomic_question_answers(session_id, pair_id, **kwargs)
    return questions(response, identity=identity)


def enabled() -> bool:
    return human_contour.enabled() and os.environ.get(FEATURE_FLAG, "").strip().lower() in {
        "1", "true", "yes", "on",
    }


def status_summary(children: list[Mapping[str, Any]]) -> dict[str, Any]:
    counts = Counter(str(child.get("question_state") or "UNAVAILABLE") for child in children)
    resolved = sum(counts[name] for name in ("ACTIVE", "RESOLVED", "LOCKED"))
    if counts["STALE"]:
        status = "HAS_STALE"
    elif counts["REQUIRES_REVALIDATION"]:
        status = "HAS_REVALIDATION_REQUIRED"
    elif counts["UNAVAILABLE"] or counts["SUPERSEDED"]:
        status = "HAS_UNAVAILABLE"
    elif resolved == len(children) and children:
        status = "ALL_RESOLVED"
    elif resolved:
        status = "PARTIALLY_RESOLVED"
    else:
        status = "ALL_PENDING"
    return {"status": status, "label": STATUS_LABELS[status], "counts": dict(counts),
            "resolved": resolved, "actionable": sum(bool(c.get("actionable")) for c in children)}


def _ref(row: Mapping[str, Any], legacy_field: str) -> dict[str, str]:
    return {"domain_key": str(row.get("domain_key") or ""),
            "legacy_id": str(row.get(legacy_field) or "")}


def _project(groups, rows, *, legacy_field, question, documents):
    by_legacy = {str(row[legacy_field]): row for row in rows}
    # Aliases are used only to bind this invocation of the existing builder.
    # A saved group is never rebound to another generation by a runtime ID.
    items, used = [], set()
    for group in groups:
        children = sorted(
            (by_legacy[key] for key in group["atomic_child_ids"]),
            key=lambda row: str(row.get("domain_key") or row[legacy_field]),
        )
        used.update(group["atomic_child_ids"])
        refs = [_ref(row, legacy_field) for row in children]
        states = children if question else [
            {"question_state": "UNAVAILABLE" if row.get("stale") else
             "RESOLVED" if row.get("decision") in {"APPROVED", "REJECTED"} else "ACTIONABLE",
             "actionable": False} for row in children
        ]
        title = ({
            "SHEET_MATCHING": "Не удалось однозначно сопоставить листы",
            "ENTITY_MATCHING": "Не удалось однозначно сопоставить объекты",
            "CHANGE_CONFIRMATION": "Нужно проверить изменения",
            "MISSING_DATA": "Недостаточно исходных данных",
            "CONFLICT": "Нужно разрешить противоречие",
        }.get(children[0].get("question_class"), "Требуется уточнение инженера")
            if question else group["display_summary"])
        reason = ("Автоматических данных недостаточно: проверьте каждый вопрос по его доказательствам."
                  if question else "Фрагменты объединены по одинаковому содержанию или основанию проверки.")
        items.append({
            "item_type": "QuestionGroup" if question else "ReviewGroup",
            "group_id": group["group_id"], "display_title": title, "display_reason": reason,
            "decision_policy": grouping.DISPLAY_ONLY_GROUP, "child_count": len(children),
            "atomic_question_domain_keys": [ref["domain_key"] for ref in refs if ref["domain_key"]] if question else [],
            "atomic_review_domain_keys": [ref["domain_key"] for ref in refs if ref["domain_key"]] if not question else [],
            "children": refs, "status_summary": status_summary(states),
            "affected_documents": documents, "affected_pages": group.get("affected_pages", {}),
            "affected_sheets": {side: [sheet for child in children
                for sheet in (child.get("context") or {}).get(side + "_sheets", [])]
                for side in ("left", "right")},
            # References address the already-returned atomic evidence, never bytes.
            "evidence_refs": refs,
            "diagnostics": {"kind": group["kind"], "reason": group["reason"]},
        })
    for row in rows:
        if str(row[legacy_field]) not in used:
            items.append({"item_type": "AtomicQuestion" if question else "AtomicReviewItem",
                          **_ref(row, legacy_field)})
    return items


def questions(response: dict[str, Any], *, identity: str) -> dict[str, Any]:
    rows = response.get("questions") or []
    built = grouping.build_presentation_groups(
        pair_id="", identity=identity, synthesis={}, review_questions={"questions": rows},
    )
    documents = (rows[0].get("evidence") or {}).get("document_versions", {}) if rows else {}
    items = _project(built["question_groups"], rows, legacy_field="question_id",
                     question=True, documents=documents)
    states = Counter(row.get("question_state") for row in rows)
    metrics = {"atomic_questions": len(rows), "top_level_shown": len(items),
               "groups": len(built["question_groups"]), "standalone": len(built["atomic_question_rows"]),
               "atomic_actionable": sum(bool(row.get("actionable")) for row in rows),
               "resolved_atomic": states["RESOLVED"] + states["LOCKED"],
               "unavailable_historical": states["UNAVAILABLE"],
               "revalidation_required": states["REQUIRES_REVALIDATION"]}
    return {**response, "presentation": {"enabled": True, "schema": "human-presentation.v1",
            "default_filter": "pending", "items": items, "metrics": metrics}}


def reviews(response, *, identity, synthesis, plan, ownership):
    built = grouping.build_presentation_groups(
        pair_id="", identity=identity, synthesis=synthesis,
        human_review_plan=plan, ownership=ownership,
    )
    by_id = {row["review_evidence_id"]: row for row in synthesis.get("review_items") or []}
    rows = [{**by_id[row["target_id"]],
             "decision": (row.get("engineer_decision") or {}).get("decision"),
             "stale": (row.get("engineer_decision") or {}).get("stale", False)}
            for row in response["rows"] if row["target_kind"] == "REVIEW_EVIDENCE"]
    items = _project(built["review_groups"], rows, legacy_field="review_evidence_id",
                     question=False, documents=(synthesis.get("stable_domain_keys") or {}).get("document_versions", {}))
    # DomainKeys are additive metadata and must not enter synthesis validation.
    domains = {**{row["change_id"]: row for row in synthesis.get("changes") or []}, **by_id}
    enriched = [{**row, "domain_key": domains.get(row["target_id"], {}).get("domain_key")}
                for row in response["rows"]]
    return {**response, "rows": enriched, "presentation": {
        "enabled": True, "schema": "human-presentation.v1", "items": items,
        "metrics": {"atomic_review": len(rows), "top_level_shown": len(items),
                    "groups": len(built["review_groups"]), "standalone": len(built["atomic_review_rows"])},
    }}
