"""
Тесты warning-policy классификации projects_v2.

Чистые функции `classify_readiness` (для ALREADY_MIGRATED) и
`classify_warning_policy` (WARNINGS_AUTO_CANDIDATE / NEED_POLICY / BLOCKED)
проверяются на сконструированных сигналах. Реальные projects/ не трогаются.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts" / "projects_v2"))
import readiness  # noqa: E402


def _clean(**over) -> dict:
    """Полностью валидный сигнал (AUTO_SAFE), затем переопределяем поля."""
    base = dict(
        has_pdf=True, has_document_md=True, has_ocr_html=True, has_result_json=True,
        has_project_info=True, has_output=True, has_analysis=True,
        kind="plain", has_version_group=False,
        multiple_pdf=False, multiple_document_md=False, multiple_result_json=False,
        messy_legacy_artifacts=False, pdf_named_version_folder=False,
        document_code_conflict=False, object_resolved=True,
        v2_already_migrated=False, recorded_in_map=False,
        object_id="o1", discipline="EOM", document_code="X",
    )
    base.update(over)
    return base


# ---------------------------------------------------------------------------
# ALREADY_MIGRATED
# ---------------------------------------------------------------------------


def test_already_migrated_group():
    s = _clean(v2_already_migrated=True, recorded_in_map=True)
    assert readiness.classify_readiness(s)["group"] == readiness.ALREADY_MIGRATED


def test_v2_present_but_not_in_map_is_not_already_migrated():
    s = _clean(v2_already_migrated=True, recorded_in_map=False)
    v = readiness.classify_readiness(s)
    assert v["group"] == readiness.CAN_MIGRATE_WITH_WARNINGS
    assert "v2_present_not_in_map" in v["warnings"]
    # already-migrated тег больше не используется
    assert "already_migrated" not in v["warnings"]


# ---------------------------------------------------------------------------
# WARNINGS_AUTO_CANDIDATE
# ---------------------------------------------------------------------------


def test_messy_legacy_artifacts_is_auto_candidate():
    v = readiness.classify_warning_policy(_clean(messy_legacy_artifacts=True))
    assert v["policy_group"] == readiness.WARNINGS_AUTO_CANDIDATE
    assert v["recommendation"] == readiness.REC_CAN_BATCH


def test_pdf_named_with_version_group_is_auto_candidate():
    s = _clean(kind="container", has_version_group=True, pdf_named_version_folder=True)
    v = readiness.classify_warning_policy(s)
    assert v["policy_group"] == readiness.WARNINGS_AUTO_CANDIDATE
    assert v["recommendation"] == readiness.REC_CAN_BATCH


def test_no_analysis_full_quad_is_auto_candidate():
    v = readiness.classify_warning_policy(_clean(has_analysis=False))
    assert v["policy_group"] == readiness.WARNINGS_AUTO_CANDIDATE


def test_combined_auto_warnings_stay_auto():
    s = _clean(messy_legacy_artifacts=True, has_analysis=False)
    v = readiness.classify_warning_policy(s)
    assert v["policy_group"] == readiness.WARNINGS_AUTO_CANDIDATE


# ---------------------------------------------------------------------------
# WARNINGS_NEED_POLICY
# ---------------------------------------------------------------------------


def test_missing_ocr_html_needs_policy():
    v = readiness.classify_warning_policy(_clean(has_ocr_html=False))
    assert v["policy_group"] == readiness.WARNINGS_NEED_POLICY
    assert v["recommendation"] == readiness.REC_NEEDS_POLICY


def test_pdf_named_without_version_group_needs_policy():
    # plain-папка с .pdf в имени, без version_group -> неоднозначно
    s = _clean(kind="plain", has_version_group=False, pdf_named_version_folder=True)
    v = readiness.classify_warning_policy(s)
    assert v["policy_group"] == readiness.WARNINGS_NEED_POLICY


def test_object_not_in_registry_needs_policy():
    v = readiness.classify_warning_policy(_clean(object_resolved=False))
    assert v["policy_group"] == readiness.WARNINGS_NEED_POLICY


def test_need_policy_beats_auto_candidate():
    # есть и auto (messy), и need (missing_ocr) -> need_policy
    s = _clean(messy_legacy_artifacts=True, has_ocr_html=False)
    v = readiness.classify_warning_policy(s)
    assert v["policy_group"] == readiness.WARNINGS_NEED_POLICY


# ---------------------------------------------------------------------------
# WARNINGS_BLOCKED
# ---------------------------------------------------------------------------


def test_multiple_pdf_blocked():
    v = readiness.classify_warning_policy(_clean(multiple_pdf=True))
    assert v["policy_group"] == readiness.WARNINGS_BLOCKED
    assert v["recommendation"] == readiness.REC_MANUAL_ONLY
    assert "multiple_pdf" in v["blockers"]


def test_multiple_md_and_result_blocked():
    assert readiness.classify_warning_policy(_clean(multiple_document_md=True))[
        "policy_group"] == readiness.WARNINGS_BLOCKED
    assert readiness.classify_warning_policy(_clean(multiple_result_json=True))[
        "policy_group"] == readiness.WARNINGS_BLOCKED


def test_incomplete_quad_blocked():
    v = readiness.classify_warning_policy(_clean(has_result_json=False))
    assert v["policy_group"] == readiness.WARNINGS_BLOCKED
    assert "incomplete_input_quad" in v["blockers"]


def test_missing_project_info_blocked():
    v = readiness.classify_warning_policy(_clean(has_project_info=False))
    assert v["policy_group"] == readiness.WARNINGS_BLOCKED
    assert "missing_project_info" in v["blockers"]


def test_container_without_version_group_blocked():
    v = readiness.classify_warning_policy(_clean(kind="container", has_version_group=False))
    assert v["policy_group"] == readiness.WARNINGS_BLOCKED
    assert "container_without_version_group" in v["blockers"]


def test_document_code_conflict_blocked():
    v = readiness.classify_warning_policy(_clean(document_code_conflict=True))
    assert v["policy_group"] == readiness.WARNINGS_BLOCKED


# ---------------------------------------------------------------------------
# приоритет blocker над auto-candidate
# ---------------------------------------------------------------------------


def test_blocker_priority_over_auto_candidate():
    # auto-candidate warning (messy) + blocker (multiple_pdf) -> BLOCKED
    s = _clean(messy_legacy_artifacts=True, multiple_pdf=True)
    v = readiness.classify_warning_policy(s)
    assert v["policy_group"] == readiness.WARNINGS_BLOCKED
    assert v["recommendation"] == readiness.REC_MANUAL_ONLY
