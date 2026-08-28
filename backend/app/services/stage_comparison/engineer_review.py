"""Engineer decisions, feedback history and approved-only final reports."""
from __future__ import annotations

from typing import Any, Iterable, Mapping

from .production_artifacts import content_signature, stable_id, utc_now
from .review_presentation import review_finding_presentation
from .unified_change_synthesizer import canonical_synthesis_digest, validate_synthesis


DECISIONS_KIND = "stage_comparison_engineer_decisions"
DECISIONS_SCHEMA_VERSION = "engineer-decisions.v1"
FINAL_REPORT_KIND = "stage_comparison_approved_changes_report"
FINAL_REPORT_SCHEMA_VERSION = "approved-changes-report.v1"
DECISIONS = frozenset({"PENDING_REVIEW", "APPROVED", "REJECTED"})


def _synthesis_signature(synthesis: Mapping[str, Any]) -> str:
    return canonical_synthesis_digest(synthesis)


def _target_signature(
    synthesis: Mapping[str, Any],
    target_kind: str,
    target: Mapping[str, Any],
) -> str:
    """Fingerprint every review-visible semantic field, not only evidence ids."""
    return content_signature({
        "synthesis_version": synthesis.get("synthesis_version"),
        "policy_version": synthesis.get("policy_version"),
        "identity_version": synthesis.get("identity_version"),
        "target_kind": target_kind,
        "target": dict(target),
    })


def _targets(synthesis: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    targets: dict[str, dict[str, Any]] = {}
    for change in synthesis.get("changes") or []:
        target_id = str(change.get("change_id") or "")
        if target_id:
            targets[target_id] = {
                "target_id": target_id,
                "target_kind": "CHANGE",
                "input_signature": _target_signature(
                    synthesis, "CHANGE", change,
                ),
                "finding": dict(change),
            }
    for item in synthesis.get("review_items") or []:
        target_id = str(item.get("review_evidence_id") or "")
        if target_id:
            targets[target_id] = {
                "target_id": target_id,
                "target_kind": "REVIEW_EVIDENCE",
                "input_signature": _target_signature(
                    synthesis, "REVIEW_EVIDENCE", item,
                ),
                "finding": dict(item),
                "presentation": review_finding_presentation(item),
            }
    return targets


def _presentable(target: Mapping[str, Any]) -> bool:
    if target["target_kind"] == "CHANGE":
        return True
    presentation = target.get("presentation")
    return bool(isinstance(presentation, Mapping) and presentation.get("presentable"))


def _empty_decision(target: Mapping[str, Any], now: str) -> dict[str, Any]:
    return {
        "decision_id": stable_id("edec_", target["target_kind"], target["target_id"]),
        "target_id": target["target_id"],
        "target_kind": target["target_kind"],
        "presentable": _presentable(target),
        "presentation": dict(target.get("presentation") or {}) or None,
        "decision": "PENDING_REVIEW",
        "author": None,
        "comment": None,
        "reason_code": None,
        "input_signature": target["input_signature"],
        "created_at": now,
        "updated_at": now,
        "revision": 1,
        "stale": False,
        "finding_snapshot": target["finding"],
    }


def _normalize_update(update: Mapping[str, Any]) -> dict[str, Any]:
    target_id = update.get("target_id", update.get("change_id", update.get("review_evidence_id")))
    if not isinstance(target_id, str) or not target_id.strip():
        raise ValueError("engineer decision target_id is required")
    decision = update.get("decision")
    if decision not in DECISIONS:
        raise ValueError("unsupported engineer decision")
    author = update.get("author")
    if decision != "PENDING_REVIEW" and (
        not isinstance(author, str) or not author.strip()
    ):
        raise ValueError("engineer decision author is required")
    comment = update.get("comment")
    reason = update.get("reason_code")
    for value, where in ((comment, "comment"), (reason, "reason_code")):
        if value is not None and not isinstance(value, str):
            raise ValueError(f"engineer decision {where} must be a string or null")
    return {
        "target_id": target_id.strip(),
        "decision": decision,
        "author": author.strip() if isinstance(author, str) and author.strip() else None,
        "comment": comment.strip() if isinstance(comment, str) and comment.strip() else None,
        "reason_code": reason.strip() if isinstance(reason, str) and reason.strip() else None,
    }


def build_engineer_decisions(
    synthesis: Mapping[str, Any],
    *,
    existing: Mapping[str, Any] | None = None,
    updates: Iterable[Mapping[str, Any]] = (),
    generated_at: str | None = None,
) -> dict[str, Any]:
    """Upsert versioned decisions while retaining stale/changed history."""
    validated = validate_synthesis(dict(synthesis))
    targets = _targets(validated)
    now = generated_at or utc_now()
    existing_rows = {
        str(row.get("target_id")): dict(row)
        for row in (existing or {}).get("decisions") or []
        if isinstance(row, Mapping) and row.get("target_id")
    }
    history = [
        dict(row) for row in (existing or {}).get("history") or []
        if isinstance(row, Mapping)
    ]
    decisions: dict[str, dict[str, Any]] = {}
    for target_id, target in targets.items():
        previous = existing_rows.get(target_id)
        if previous and previous.get("input_signature") == target["input_signature"]:
            row = {
                **previous,
                "stale": False,
                "finding_snapshot": target["finding"],
                "presentable": _presentable(target),
                "presentation": dict(target.get("presentation") or {}) or None,
            }
        else:
            if previous:
                history.append({**previous, "stale": True, "stale_at": now})
            row = _empty_decision(target, now)
        decisions[target_id] = row
    # Findings removed by a new synthesis remain valuable feedback history.
    for target_id, previous in existing_rows.items():
        if target_id not in targets:
            history.append({**previous, "stale": True, "stale_at": now})

    normalized_updates = [_normalize_update(update) for update in updates]
    if len({update["target_id"] for update in normalized_updates}) != len(normalized_updates):
        raise ValueError("duplicate engineer decision update")
    for update in normalized_updates:
        target_id = update["target_id"]
        if target_id not in decisions:
            raise ValueError("engineer decision references unknown finding")
        previous = decisions[target_id]
        if (
            previous.get("target_kind") == "REVIEW_EVIDENCE"
            and update["decision"] == "APPROVED"
            and not previous.get("presentable")
        ):
            # A finding with a value and a page is reviewable as it stands, even
            # when its dimension stayed unknown: «EI 60 → EI 90, классификация
            # не определена» is a decision an engineer can make.  Only a row
            # with nothing to show still has to be resolved upstream first.
            raise ValueError(
                "review evidence must be resolved into an atomic change before approval"
            )
        changed = any(
            previous.get(key) != update[key]
            for key in ("decision", "author", "comment", "reason_code")
        )
        if changed:
            history.append({**previous, "superseded_at": now})
            decisions[target_id] = {
                **previous,
                **update,
                "updated_at": now,
                "revision": int(previous.get("revision") or 0) + 1,
                "stale": False,
            }

    ordered = sorted(decisions.values(), key=lambda row: row["target_id"])
    history.sort(key=lambda row: (
        str(row.get("target_id") or ""),
        int(row.get("revision") or 0),
        str(row.get("updated_at") or row.get("created_at") or ""),
    ))
    counts = {decision: 0 for decision in sorted(DECISIONS)}
    for row in ordered:
        counts[row["decision"]] += 1
    return {
        "kind": DECISIONS_KIND,
        "schema_version": DECISIONS_SCHEMA_VERSION,
        "version": 1,
        "revision": int((existing or {}).get("revision") or 0) + 1,
        "input_signature": _synthesis_signature(validated),
        "generated_at": now,
        "decisions": ordered,
        "history": history,
        "counts": counts,
        "provenance": {
            "finding_identity": "G2.4.6 change_id/review_evidence_id",
            "ui_row_number_used": False,
            "automatic_learning": False,
        },
    }


def review_rows(
    synthesis: Mapping[str, Any],
    decisions: Mapping[str, Any] | None,
) -> list[dict[str, Any]]:
    """Return one reviewable UI row per immutable atomic target."""
    validated = validate_synthesis(dict(synthesis))
    state = build_engineer_decisions(validated, existing=decisions)
    by_target = {row["target_id"]: row for row in state["decisions"]}
    rows = []
    presentation_by_change = {
        change_id: group["group_id"]
        for group in validated.get("presentation_groups") or []
        for change_id in group.get("change_ids") or []
    }
    for change in validated.get("changes") or []:
        change_id = change["change_id"]
        rows.append({
            "target_id": change_id,
            "target_kind": "CHANGE",
            "change": dict(change),
            "presentation_group_id": presentation_by_change.get(change_id),
            "engineer_decision": by_target[change_id],
        })
    for item in validated.get("review_items") or []:
        target_id = item["review_evidence_id"]
        rows.append({
            "target_id": target_id,
            "target_kind": "REVIEW_EVIDENCE",
            "change": dict(item),
            "presentation": review_finding_presentation(item),
            "presentation_group_id": None,
            "engineer_decision": by_target[target_id],
        })
    return sorted(rows, key=lambda row: row["target_id"])


def build_final_report(
    synthesis: Mapping[str, Any],
    decisions: Mapping[str, Any],
    *,
    object_ref: str | None = None,
    generated_at: str | None = None,
) -> dict[str, Any]:
    """Build a deterministic report containing APPROVED atomic changes only."""
    validated = validate_synthesis(dict(synthesis))
    target_map = _targets(validated)
    decision_rows = {
        str(row.get("target_id")): row
        for row in decisions.get("decisions") or []
        if isinstance(row, Mapping)
    }
    approved = []
    for change in validated.get("changes") or []:
        change_id = str(change["change_id"])
        decision = decision_rows.get(change_id)
        target = target_map[change_id]
        if not decision or decision.get("decision") != "APPROVED":
            continue
        if decision.get("stale") or decision.get("input_signature") != target["input_signature"]:
            continue
        approved.append({
            "change_id": change_id,
            "scope_ref": change.get("scope_ref"),
            "subject_ref": change.get("subject_ref"),
            "project_entity_ref": change.get("project_entity_ref"),
            "facet_ref": change.get("facet_ref"),
            "dimension": change.get("dimension"),
            "direction": change.get("direction"),
            "before_value": change.get("before_value"),
            "after_value": change.get("after_value"),
            "source_mode": change.get("source_mode"),
            "evidence_refs": change.get("evidence_refs"),
            "engineer_decision": {
                "decision_id": decision.get("decision_id"),
                "author": decision.get("author"),
                "comment": decision.get("comment"),
                "reason_code": decision.get("reason_code"),
                "updated_at": decision.get("updated_at"),
            },
        })
    approved.sort(key=lambda change: change["change_id"])
    # A finding the engineer approved in Stage 7 must reach the report, or the
    # approval means nothing.  Kept in its own list so the atomic-change
    # contract above is untouched: this report is still approved-only.
    approved_review_findings = []
    for item in validated.get("review_items") or []:
        target_id = str(item["review_evidence_id"])
        decision = decision_rows.get(target_id)
        target = target_map[target_id]
        if not decision or decision.get("decision") != "APPROVED":
            continue
        if decision.get("stale") or decision.get("input_signature") != target["input_signature"]:
            continue
        presentation = target.get("presentation") or {}
        approved_review_findings.append({
            "review_evidence_id": target_id,
            "scope_ref": item.get("scope_ref"),
            "subject_ref": item.get("subject_ref"),
            "project_entity_ref": item.get("project_entity_ref"),
            "dimension": item.get("dimension"),
            "direction": item.get("direction"),
            "before_value": item.get("before_value"),
            "after_value": item.get("after_value"),
            "left_pages": presentation.get("left_pages") or [],
            "right_pages": presentation.get("right_pages") or [],
            "reason_codes": sorted(item.get("reason_codes") or []),
            "evidence_refs": item.get("evidence_refs"),
            "engineer_decision": {
                "decision_id": decision.get("decision_id"),
                "author": decision.get("author"),
                "comment": decision.get("comment"),
                "reason_code": decision.get("reason_code"),
                "updated_at": decision.get("updated_at"),
            },
        })
    approved_review_findings.sort(key=lambda item: item["review_evidence_id"])
    report_input = {
        "synthesis": _synthesis_signature(validated),
        "decisions": [
            {
                "target_id": row.get("target_id"),
                "decision": row.get("decision"),
                "input_signature": row.get("input_signature"),
                "revision": row.get("revision"),
            }
            for row in sorted(decision_rows.values(), key=lambda row: str(row.get("target_id") or ""))
        ],
    }
    return {
        "kind": FINAL_REPORT_KIND,
        "schema_version": FINAL_REPORT_SCHEMA_VERSION,
        "version": 1,
        "object_ref": object_ref,
        "direction": "LEFT_TO_RIGHT",
        "input_signature": content_signature(report_input),
        "generated_at": generated_at or utc_now(),
        "approved_atomic_changes": approved,
        "approved_review_findings": approved_review_findings,
        "summary": {
            "approved": len(approved),
            "approved_review_findings": len(approved_review_findings),
        },
        "constraints": {
            "approved_only": True,
            "pending_included": False,
            "rejected_included": False,
            "uses_llm_summary": False,
        },
    }


__all__ = [
    "DECISIONS",
    "DECISIONS_KIND",
    "DECISIONS_SCHEMA_VERSION",
    "FINAL_REPORT_KIND",
    "FINAL_REPORT_SCHEMA_VERSION",
    "build_engineer_decisions",
    "build_final_report",
    "review_rows",
]
