"""
Tests for pipeline_v2_skip_readiness.py — mark-only слой,
определяющий что теоретически может быть пропущено в обогащении.

ВАЖНО: все тесты — offline (без Qwen/Opus/LLM); входные артефакты не меняются.
"""

from __future__ import annotations

import json
import pathlib
import tempfile

import pytest

from backend.app.services.stage_comparison.pipeline_v2_skip_readiness import (
    ARTIFACT_FILENAME,
    BLOCKED_MARK_ONLY_SAFETY,
    BLOCKED_MISSING_APPROVAL,
    BLOCKED_VALID_MAPPING,
    KEEP_OPERATOR,
    KEEP_PREVIEW_CLS,
    KEEP_REJECTED,
    REPORT_KIND,
    REPORT_VERSION,
    REVIEW_ABSENT_LV,
    REVIEW_LV_REQUIRED,
    REVIEW_MANUAL,
    REVIEW_ONLY_CLS,
    SKIP_SCOPE_MVP,
    STATUS_BLOCKED,
    STATUS_KEEP,
    STATUS_NEEDS_REVIEW,
    STATUS_READY,
    build_skip_readiness_report,
    build_skip_readiness_report_from_dir,
    write_skip_readiness_report,
)


# ─── Helpers ─────────────────────────────────────────────────────────────────


def _xp_item(item_id: str = "xp_bp::L__R",
             classification: str = "candidate_exclude",
             lb: str = "L", rb: str = "R",
             ll: str = "Obj-L", rl: str = "Obj-R",
             severity: str = "high",
             confidence: float = 0.95,
             lv: dict | None = None) -> dict:
    return {
        "item_id": item_id,
        "target_type": "block_pair",
        "left_block_id": lb,
        "right_block_id": rb,
        "left_entity_label": ll,
        "right_entity_label": rl,
        "classification": classification,
        "confidence": confidence,
        "severity": severity,
        "recommended_action": "exclude_from_enrichment",
        "link_validation": lv or {},
    }


def _xp_report(*items) -> dict:
    return {
        "version": "1",
        "kind": "exclusion_preview_v2_report",
        "status": "ok",
        "summary": {
            "items_total": len(items),
            "candidate_exclude": sum(1 for it in items
                                     if it["classification"] == "candidate_exclude"),
            "review_only": 0,
            "keep": sum(1 for it in items if it["classification"] == "keep"),
            "link_validation_required": sum(1 for it in items
                                            if it["classification"] == "link_validation_required"),
            "auto_enforce_enabled": False,
        },
        "items": list(items),
        "warnings": [],
    }


def _overrides(*decisions) -> dict:
    return {"decisions": list(decisions)}


def _decision(item_id: str, op: str, comment: str = "") -> dict:
    return {
        "exclusion_item_id": item_id,
        "operator_decision": op,
        "comment": comment,
        "updated_at": "2026-06-12T00:00:00Z",
    }


def _assert_hard_invariants(report: dict) -> None:
    """Проверяет HARD INVARIANTS на report-уровне и каждом item."""
    assert report.get("auto_enforce_enabled") is False
    assert report.get("enforce_allowed") is False
    assert report["summary"].get("auto_enforce_enabled") is False
    for it in report.get("items", []):
        assert it.get("auto_apply") is False, f"auto_apply=True on {it.get('item_id')}"
        assert it.get("enforce_allowed") is False, \
            f"enforce_allowed=True on {it.get('item_id')}"
        assert it.get("requires_explicit_operator_approval") is True, \
            f"requires_explicit_operator_approval not True on {it.get('item_id')}"


# ─── 1. missing_input ────────────────────────────────────────────────────────


def test_missing_exclusion_preview():
    """При отсутствии exclusion_preview_report → status=missing_input, 0 items."""
    report = build_skip_readiness_report(exclusion_preview_report=None)
    assert report["status"] == "missing_input"
    assert report["summary"]["items_total"] == 0
    assert report["summary"]["ready_to_skip"] == 0
    assert len(report["warnings"]) >= 1
    _assert_hard_invariants(report)


# ─── 2. ready_to_skip ────────────────────────────────────────────────────────


def test_ready_to_skip_candidate_with_approve():
    """candidate_exclude + approve_exclude → ready_to_skip."""
    item = _xp_item()
    ov = _overrides(_decision("xp_bp::L__R", "approve_exclude"))
    report = build_skip_readiness_report(
        exclusion_preview_report=_xp_report(item),
        overrides_report=ov,
    )
    assert report["summary"]["ready_to_skip"] == 1
    assert report["summary"]["blocked"] == 0
    r_item = report["items"][0]
    assert r_item["readiness_status"] == STATUS_READY
    assert r_item["skip_scope"] == SKIP_SCOPE_MVP
    assert r_item["skip_scope"]["exclude_from_enrichment"] is True
    assert r_item["skip_scope"]["exclude_from_findings"] is False
    _assert_hard_invariants(report)


# ─── 3. blocked — missing approval ───────────────────────────────────────────


def test_blocked_no_operator_decision():
    """candidate_exclude без operator decision → blocked (missing_operator_approval)."""
    item = _xp_item()
    report = build_skip_readiness_report(
        exclusion_preview_report=_xp_report(item),
        overrides_report=None,
    )
    assert report["summary"]["blocked"] == 1
    assert report["summary"]["ready_to_skip"] == 0
    r_item = report["items"][0]
    assert r_item["readiness_status"] == STATUS_BLOCKED
    assert r_item["blocked_reason"] == BLOCKED_MISSING_APPROVAL
    _assert_hard_invariants(report)


# ─── 4. blocked — valid_mapping ──────────────────────────────────────────────


def test_blocked_valid_mapping_embedded():
    """candidate_exclude + approve_exclude + lv.decision=valid_mapping → blocked."""
    item = _xp_item(lv={"decision": "valid_mapping", "confidence": 0.90})
    ov = _overrides(_decision("xp_bp::L__R", "approve_exclude"))
    report = build_skip_readiness_report(
        exclusion_preview_report=_xp_report(item),
        overrides_report=ov,
    )
    r_item = report["items"][0]
    assert r_item["readiness_status"] == STATUS_BLOCKED
    assert r_item["blocked_reason"] == BLOCKED_VALID_MAPPING
    _assert_hard_invariants(report)


def test_blocked_valid_mapping_from_lv_report():
    """candidate_exclude + approve_exclude + lv_report[item].decision=valid_mapping → blocked."""
    item = _xp_item(lb="AAA", rb="BBB")
    item["item_id"] = "xp_bp::AAA__BBB"
    ov = _overrides(_decision("xp_bp::AAA__BBB", "approve_exclude"))
    lv_report = {
        "items": [{"item_id": "lv_AAA__BBB", "left_block_id": "AAA",
                   "right_block_id": "BBB", "decision": "valid_mapping"}]
    }
    report = build_skip_readiness_report(
        exclusion_preview_report=_xp_report(item),
        overrides_report=ov,
        link_validation_report=lv_report,
    )
    r_item = report["items"][0]
    assert r_item["readiness_status"] == STATUS_BLOCKED
    assert r_item["blocked_reason"] == BLOCKED_VALID_MAPPING
    _assert_hard_invariants(report)


# ─── 5. keep — preview classification ───────────────────────────────────────


def test_keep_preview_classification():
    """preview.classification=keep → keep, reason=preview_classification_keep."""
    item = _xp_item(classification="keep")
    report = build_skip_readiness_report(
        exclusion_preview_report=_xp_report(item),
    )
    r_item = report["items"][0]
    assert r_item["readiness_status"] == STATUS_KEEP
    assert r_item["blocked_reason"] == KEEP_PREVIEW_CLS
    _assert_hard_invariants(report)


# ─── 6. keep — operator keep/reject_exclude ──────────────────────────────────


def test_keep_operator_keep_decision():
    """candidate_exclude + operator_decision=keep → keep."""
    item = _xp_item()
    ov = _overrides(_decision("xp_bp::L__R", "keep"))
    report = build_skip_readiness_report(
        exclusion_preview_report=_xp_report(item),
        overrides_report=ov,
    )
    r_item = report["items"][0]
    assert r_item["readiness_status"] == STATUS_KEEP
    assert r_item["blocked_reason"] == KEEP_OPERATOR
    _assert_hard_invariants(report)


def test_keep_operator_reject_exclude():
    """candidate_exclude + operator_decision=reject_exclude → keep."""
    item = _xp_item()
    ov = _overrides(_decision("xp_bp::L__R", "reject_exclude"))
    report = build_skip_readiness_report(
        exclusion_preview_report=_xp_report(item),
        overrides_report=ov,
    )
    r_item = report["items"][0]
    assert r_item["readiness_status"] == STATUS_KEEP
    assert r_item["blocked_reason"] == KEEP_REJECTED
    _assert_hard_invariants(report)


# ─── 7. needs_review — operator decisions ────────────────────────────────────


def test_needs_review_needs_review_decision():
    """candidate_exclude + operator_decision=needs_review → needs_review."""
    item = _xp_item()
    ov = _overrides(_decision("xp_bp::L__R", "needs_review"))
    report = build_skip_readiness_report(
        exclusion_preview_report=_xp_report(item),
        overrides_report=ov,
    )
    r_item = report["items"][0]
    assert r_item["readiness_status"] == STATUS_NEEDS_REVIEW
    assert r_item["blocked_reason"] == REVIEW_MANUAL
    _assert_hard_invariants(report)


def test_needs_review_run_link_validation_decision():
    """candidate_exclude + operator_decision=run_link_validation → needs_review."""
    item = _xp_item()
    ov = _overrides(_decision("xp_bp::L__R", "run_link_validation"))
    report = build_skip_readiness_report(
        exclusion_preview_report=_xp_report(item),
        overrides_report=ov,
    )
    r_item = report["items"][0]
    assert r_item["readiness_status"] == STATUS_NEEDS_REVIEW
    assert r_item["blocked_reason"] == REVIEW_LV_REQUIRED
    _assert_hard_invariants(report)


# ─── 8. needs_review — link_validation_required classification ───────────────


def test_needs_review_link_validation_required_no_decision():
    """link_validation_required + no operator → needs_review (absent_link_validation)."""
    item = _xp_item(classification="link_validation_required")
    report = build_skip_readiness_report(
        exclusion_preview_report=_xp_report(item),
        overrides_report=None,
    )
    r_item = report["items"][0]
    assert r_item["readiness_status"] == STATUS_NEEDS_REVIEW
    assert r_item["blocked_reason"] == REVIEW_ABSENT_LV
    _assert_hard_invariants(report)


def test_needs_review_review_only_classification():
    """review_only classification → needs_review (review_only_classification)."""
    item = _xp_item(classification="review_only")
    report = build_skip_readiness_report(
        exclusion_preview_report=_xp_report(item),
    )
    r_item = report["items"][0]
    assert r_item["readiness_status"] == STATUS_NEEDS_REVIEW
    assert r_item["blocked_reason"] == REVIEW_ONLY_CLS
    _assert_hard_invariants(report)


# ─── 9. link_validation_required + approve_exclude → ready_to_skip ──────────


def test_ready_to_skip_link_validation_required_with_approve():
    """link_validation_required + approve_exclude + lv≠valid_mapping → ready_to_skip."""
    item = _xp_item(classification="link_validation_required",
                    lv={"decision": "reject_mapping", "confidence": 0.90})
    ov = _overrides(_decision("xp_bp::L__R", "approve_exclude"))
    report = build_skip_readiness_report(
        exclusion_preview_report=_xp_report(item),
        overrides_report=ov,
    )
    r_item = report["items"][0]
    assert r_item["readiness_status"] == STATUS_READY
    _assert_hard_invariants(report)


# ─── 10. summary counts ──────────────────────────────────────────────────────


def test_summary_counts_mixed():
    """Проверка счётчиков summary на смешанном наборе items."""
    items = [
        _xp_item("xp_bp::A__B", "candidate_exclude", "A", "B"),   # → blocked (no approval)
        _xp_item("xp_bp::C__D", "candidate_exclude", "C", "D"),   # → ready_to_skip
        _xp_item("xp_bp::E__F", "keep", "E", "F"),               # → keep
        _xp_item("xp_bp::G__H", "link_validation_required",       # → needs_review
                 "G", "H"),
    ]
    ov = _overrides(_decision("xp_bp::C__D", "approve_exclude"))
    report = build_skip_readiness_report(
        exclusion_preview_report=_xp_report(*items),
        overrides_report=ov,
    )
    s = report["summary"]
    assert s["items_total"] == 4
    assert s["ready_to_skip"] == 1
    assert s["blocked"] == 1
    assert s["keep"] == 1
    assert s["needs_review"] == 1
    assert s["operator_approved"] == 1
    _assert_hard_invariants(report)


# ─── 11. report schema ───────────────────────────────────────────────────────


def test_report_schema():
    """Проверка обязательных полей схемы report."""
    report = build_skip_readiness_report(
        exclusion_preview_report=_xp_report(_xp_item()),
        session_id="ses123", pair_id="pair456",
    )
    for field in ("version", "kind", "status", "session_id", "pair_id",
                  "created_at", "summary", "items", "warnings",
                  "auto_enforce_enabled", "enforce_allowed"):
        assert field in report, f"missing field: {field}"
    assert report["version"] == REPORT_VERSION
    assert report["kind"] == REPORT_KIND
    assert report["session_id"] == "ses123"
    assert report["pair_id"] == "pair456"
    _assert_hard_invariants(report)


# ─── 12. MVP skip_scope ──────────────────────────────────────────────────────


def test_skip_scope_mvp_only_enrichment():
    """MVP skip_scope: только exclude_from_enrichment=True, остальное False."""
    item = _xp_item()
    ov = _overrides(_decision("xp_bp::L__R", "approve_exclude"))
    report = build_skip_readiness_report(
        exclusion_preview_report=_xp_report(item),
        overrides_report=ov,
    )
    scope = report["items"][0]["skip_scope"]
    assert scope["exclude_from_enrichment"] is True
    assert scope["exclude_from_grounded_evidence"] is False
    assert scope["exclude_from_delta_explanation"] is False
    assert scope["exclude_from_findings"] is False


# ─── 13. build_from_dir ──────────────────────────────────────────────────────


def test_build_from_dir_with_artifacts(tmp_path):
    """build_skip_readiness_report_from_dir читает артефакты из директории."""
    item = _xp_item()
    xp = _xp_report(item)
    ov = _overrides(_decision("xp_bp::L__R", "approve_exclude"))

    (tmp_path / "exclusion_preview_v2_report.json").write_text(
        json.dumps(xp), encoding="utf-8")
    (tmp_path / "exclusion_review_overrides.json").write_text(
        json.dumps(ov), encoding="utf-8")

    report = build_skip_readiness_report_from_dir(tmp_path)
    assert report["summary"]["ready_to_skip"] == 1
    _assert_hard_invariants(report)


def test_build_from_dir_missing_preview(tmp_path):
    """build_from_dir без exclusion_preview_v2_report → missing_input."""
    report = build_skip_readiness_report_from_dir(tmp_path)
    assert report["status"] == "missing_input"
    _assert_hard_invariants(report)


# ─── 14. write_skip_readiness_report ─────────────────────────────────────────


def test_write_skip_readiness_report(tmp_path):
    """write_skip_readiness_report атомарно пишет JSON-файл."""
    report = build_skip_readiness_report(
        exclusion_preview_report=_xp_report(_xp_item()),
    )
    out = tmp_path / "skip_readiness_report.json"
    result_path = write_skip_readiness_report(out, report)
    assert result_path == out
    loaded = json.loads(out.read_text(encoding="utf-8"))
    assert loaded["kind"] == REPORT_KIND
    assert loaded["summary"]["auto_enforce_enabled"] is False


# ─── 15. empty preview items ─────────────────────────────────────────────────


def test_empty_preview_items():
    """Пустой список items в exclusion_preview → нулевые счётчики, ok status."""
    xp = {
        "version": "1", "kind": "exclusion_preview_v2_report",
        "status": "ok",
        "summary": {"items_total": 0, "candidate_exclude": 0,
                    "review_only": 0, "keep": 0,
                    "link_validation_required": 0,
                    "auto_enforce_enabled": False},
        "items": [],
        "warnings": [],
    }
    report = build_skip_readiness_report(exclusion_preview_report=xp)
    assert report["status"] == "ok"
    assert report["summary"]["items_total"] == 0
    assert report["summary"]["ready_to_skip"] == 0
    _assert_hard_invariants(report)
