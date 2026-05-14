"""
Smoke tests for the experimental Critic v2 UI Triage View frontend.

The frontend is a Vue 3 SPA without a build step (Vue via CDN, Vite for dev
proxy only). There is no JS test framework in this repo, so we verify:

  1) Frontend artifacts contain the wiring expected by the design.
  2) The UI export artifact produced by replay --ui-export round-trips
     through the parse rules that the frontend will apply.

These are smoke checks. Production pipeline is NOT touched.
"""
from __future__ import annotations

import json
import re
import subprocess
import sys
from pathlib import Path

import pytest

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

INDEX_HTML = _PROJECT_ROOT / "frontend" / "index.html"
APP_JS = _PROJECT_ROOT / "frontend" / "static" / "js" / "app.js"
STYLES_CSS = _PROJECT_ROOT / "frontend" / "static" / "css" / "styles.css"


# ─── Sidebar / route wiring ─────────────────────────────────────────────────


class TestSidebarAndRoute:
    def test_sidebar_has_critic_v2_ui_entry(self):
        html = INDEX_HTML.read_text(encoding="utf-8")
        assert "navigate('/critic-v2-ui')" in html
        # Sidebar entry now uses a short label + an "EXP" badge.
        assert "Critic v2" in html
        assert ">EXP<" in html

    def test_app_js_handles_critic_v2_ui_route(self):
        js = APP_JS.read_text(encoding="utf-8")
        assert "hash === '/critic-v2-ui'" in js
        assert "currentView.value = 'critic-v2-ui'" in js

    def test_view_block_present_in_html(self):
        html = INDEX_HTML.read_text(encoding="utf-8")
        assert 'currentView === \'critic-v2-ui\'' in html


# ─── Four tabs ──────────────────────────────────────────────────────────────


class TestFourTabsTemplate:
    def test_template_renders_tabs_loop(self):
        html = INDEX_HTML.read_text(encoding="utf-8")
        assert 'v-for="tab in cv2Export.tabs"' in html
        # Headers and bodies use the same loop key.
        assert "cv2-tab-headers" in html
        assert "cv2-tab-body" in html

    def test_template_uses_default_open_attribute(self):
        html = INDEX_HTML.read_text(encoding="utf-8")
        assert "tab.default_open" in html

    def test_template_shows_collapsed_note(self):
        """Свернутые вкладки показывают защитную подпись."""
        html = INDEX_HTML.read_text(encoding="utf-8")
        assert "Замечание не удалено" in html
        # Render-condition negates default_open
        assert 'v-if="!tab.default_open"' in html


# ─── Item fields ────────────────────────────────────────────────────────────


class TestItemFields:
    REQUIRED_FIELDS = [
        "title", "description", "recommendation",
        "reason", "explanation", "confidence", "score",
        "evidence_quality", "source_dependency", "taxonomy_reason",
        "queue", "can_restore",
    ]

    def test_all_required_item_fields_referenced(self):
        html = INDEX_HTML.read_text(encoding="utf-8")
        missing = []
        for f in self.REQUIRED_FIELDS:
            if f"item.{f}" not in html:
                missing.append(f)
        assert not missing, f"Item fields missing in template: {missing}"


# ─── Filters ────────────────────────────────────────────────────────────────


class TestFilters:
    def test_filter_state_has_all_keys(self):
        js = APP_JS.read_text(encoding="utf-8")
        # Filter object includes all 6 axes.
        for key in ("section", "queue", "reason",
                    "evidence", "scoreBucket", "human"):
            assert f"{key}:" in js, f"Filter missing axis: {key}"

    def test_filter_options_computed_present(self):
        js = APP_JS.read_text(encoding="utf-8")
        assert "cv2FilterOptions" in js
        assert "cv2ItemsByTab" in js
        assert "cv2VisibleCountByTab" in js


# ─── Warning banner ─────────────────────────────────────────────────────────


class TestExperimentalWarning:
    def test_html_renders_experimental_warning(self):
        html = INDEX_HTML.read_text(encoding="utf-8")
        assert "EXPERIMENTAL" in html
        assert "production critic" in html.lower() or "production pipeline" in html.lower() \
            or "legacy critic" in html.lower()

    def test_css_has_experimental_class(self):
        css = STYLES_CSS.read_text(encoding="utf-8")
        assert ".cv2-experimental-banner" in css


# ─── Loader ─────────────────────────────────────────────────────────────────


class TestLoader:
    def test_html_has_file_input(self):
        html = INDEX_HTML.read_text(encoding="utf-8")
        assert 'type="file"' in html
        assert "cv2OnFileSelected" in html

    def test_app_js_parses_export_via_helper(self):
        js = APP_JS.read_text(encoding="utf-8")
        assert "cv2ParseExport" in js
        assert "FileReader" in js


class TestEffectiveTabRouting:
    """Regression: badge "Критик рекомендует отклонить" was 1 while expert had
    moved several findings to suggested_reject. Fix: cv2ItemsByTab routes by
    effective tab = expert.preferred_tab || critic.tab."""

    def test_app_js_defines_effective_tab_helper(self):
        js = APP_JS.read_text(encoding="utf-8")
        assert "function cv2EffectiveTab(" in js

    def test_items_by_tab_uses_effective_tab(self):
        js = APP_JS.read_text(encoding="utf-8")
        # The computed must call cv2EffectiveTab, not just read it.tab.
        # Locate the cv2ItemsByTab block and assert it references the helper.
        idx = js.find("const cv2ItemsByTab")
        assert idx != -1, "cv2ItemsByTab not found"
        block = js[idx:idx + 500]
        assert "cv2EffectiveTab" in block, (
            "cv2ItemsByTab must route by effective tab to honor expert preferred_tab"
        )

    def test_app_js_exposes_debug_counts(self):
        js = APP_JS.read_text(encoding="utf-8")
        assert "cv2DebugCounts" in js


class TestFeedbackImport:
    def test_app_js_has_import_functions(self):
        js = APP_JS.read_text(encoding="utf-8")
        for name in (
            "cv2ImportFeedbackFromObject",
            "cv2OnFeedbackFileSelected",
            "cv2RefreshFeedbackFiles",
            "cv2ImportFeedbackFromServer",
        ):
            assert name in js, f"{name} missing from app.js"

    def test_html_has_import_ui(self):
        html = INDEX_HTML.read_text(encoding="utf-8")
        assert "cv2OnFeedbackFileSelected" in html
        assert "cv2RefreshFeedbackFiles" in html
        assert "cv2ImportFeedbackFromServer" in html

    def test_html_has_debug_counts_panel(self):
        html = INDEX_HTML.read_text(encoding="utf-8")
        assert "cv2DebugCounts" in html


# ─── Round-trip: parse rules vs replay output ───────────────────────────────


def _make_records():
    """Build a synthetic 3-finding manifest covering 3 different tabs."""
    return [
        {
            "finding_id": "F-001", "project_name": "P1",
            "critic_decision": "accept", "critic_score": 10,
            "evidence_quality": "valid",
            "human_decision": "rejected",
            "severity": "КРИТИЧЕСКОЕ", "section": "AR",
            "title": "Strong keep finding",
            "description": "desc1",
            "recommendation": "rec1",
        },
        {
            "finding_id": "F-002", "project_name": "P1",
            "critic_decision": "reject", "critic_score": 1,
            "evidence_quality": "none",
            "critic_reject_reason": "no_evidence",
            "human_decision": "rejected",
            "severity": "РЕКОМЕНДАТЕЛЬНОЕ", "section": "AR",
            "title": "Hidden finding", "description": "desc2",
        },
        {
            "finding_id": "F-003", "project_name": "P1",
            "critic_decision": "accept", "critic_score": 7,
            "evidence_quality": "partial",
            "human_decision": "accepted",
            "severity": "КРИТИЧЕСКОЕ", "section": "AR",
            "title": "Critical partial",
            "description": "desc3",
        },
    ]


@pytest.fixture
def ui_export_artifact(tmp_path):
    """Run replay --ui-export end-to-end and return the parsed JSON."""
    bench_dir = tmp_path / "bench"
    bench_dir.mkdir()
    (bench_dir / "human_benchmark_records.json").write_text(
        json.dumps(_make_records()), encoding="utf-8"
    )
    out_dir = tmp_path / "out"
    script = str(_PROJECT_ROOT / "backend" / "scripts"
                 / "replay_critic_v2_triage_policy.py")
    result = subprocess.run(
        [sys.executable, script,
         "--benchmark-output-dir", str(bench_dir),
         "--profile", "conservative",
         "--output-dir", str(out_dir),
         "--ui-export", "--quiet"],
        capture_output=True, text=True,
    )
    assert result.returncode == 0, f"replay failed: {result.stderr}"
    payload = json.loads(
        (out_dir / "critic_v2_triage_ui.json").read_text(encoding="utf-8")
    )
    return payload


class TestExportSmoke:
    """End-to-end: replay --ui-export → parse-rules from app.js."""

    def test_artifact_has_summary_tabs_items(self, ui_export_artifact):
        e = ui_export_artifact
        assert "summary" in e and isinstance(e["summary"], dict)
        assert "tabs" in e and isinstance(e["tabs"], list)
        assert "items" in e and isinstance(e["items"], list)

    def test_artifact_has_exactly_four_tabs(self, ui_export_artifact):
        assert len(ui_export_artifact["tabs"]) == 4

    def test_artifact_tab_keys_match_frontend_expectation(self, ui_export_artifact):
        # The frontend's parse rule (cv2ParseExport) checks all four keys.
        keys = [t["key"] for t in ui_export_artifact["tabs"]]
        assert set(keys) == {
            "primary", "needs_context",
            "suggested_reject", "hidden_by_critic",
        }

    def test_primary_tab_default_open(self, ui_export_artifact):
        primary = next(t for t in ui_export_artifact["tabs"]
                       if t["key"] == "primary")
        assert primary["default_open"] is True

    def test_collapsed_tabs_default_closed(self, ui_export_artifact):
        for key in ("needs_context", "suggested_reject", "hidden_by_critic"):
            tab = next(t for t in ui_export_artifact["tabs"]
                       if t["key"] == key)
            assert tab["default_open"] is False, f"{key} should be collapsed"

    def test_items_have_tab_field(self, ui_export_artifact):
        for it in ui_export_artifact["items"]:
            assert "tab" in it
            assert it["tab"] in {
                "primary", "needs_context",
                "suggested_reject", "hidden_by_critic",
            }

    def test_items_carry_required_fields(self, ui_export_artifact):
        required = {
            "finding_id", "title", "description", "recommendation",
            "queue", "reason", "explanation", "confidence",
            "evidence_quality", "score", "source_dependency",
            "taxonomy_reason", "can_restore",
        }
        for it in ui_export_artifact["items"]:
            missing = required - set(it.keys())
            assert not missing, (
                f"Item {it.get('finding_id')} missing fields: {missing}"
            )

    def test_items_can_restore_true(self, ui_export_artifact):
        for it in ui_export_artifact["items"]:
            assert it["can_restore"] is True

    def test_summary_carries_metric_keys_used_by_frontend(
        self, ui_export_artifact
    ):
        """Frontend reads these keys directly from summary."""
        s = ui_export_artifact["summary"]
        for key in (
            "total", "primary_count",
            "needs_context_count", "suggested_reject_count",
            "hidden_by_critic_count",
            "primary_queue_reduction_percent",
        ):
            assert key in s, f"Frontend expects summary.{key}"

    def test_human_decision_propagated_when_present(self, ui_export_artifact):
        """Frontend uses item.human_decision to enable the human filter."""
        labelled = [
            i for i in ui_export_artifact["items"] if i.get("human_decision")
        ]
        assert labelled, "human_decision should be populated for benchmark mode"
        for it in labelled:
            assert it["human_decision"] in ("accepted", "rejected")


# ─── Filter-logic mirror tests ──────────────────────────────────────────────


def _score_bucket(score):
    """Mirror cv2ScoreBucket from app.js."""
    if score is None:
        return "none"
    if score >= 10:
        return "10-11"
    if score >= 8:
        return "8-9"
    if score >= 6:
        return "6-7"
    if score >= 4:
        return "4-5"
    return "0-3"


def _matches_filter(item, f):
    """Mirror cv2ItemMatchesFilter from app.js."""
    if f.get("section") and item.get("section") != f["section"]:
        return False
    if f.get("queue") and item.get("queue") != f["queue"]:
        return False
    if f.get("reason") and item.get("reason") != f["reason"]:
        return False
    if f.get("evidence") and item.get("evidence_quality") != f["evidence"]:
        return False
    if f.get("scoreBucket") and _score_bucket(item.get("score")) != f["scoreBucket"]:
        return False
    if f.get("human"):
        if f["human"] == "__none__":
            if item.get("human_decision"):
                return False
        elif item.get("human_decision") != f["human"]:
            return False
    return True


class TestFilterMirror:
    """Re-implements the JS filter in Python to verify behaviour."""

    def test_queue_filter_keeps_only_selected_queue(
        self, ui_export_artifact
    ):
        items = ui_export_artifact["items"]
        q = "strong_keep"
        filtered = [i for i in items if _matches_filter(i, {"queue": q})]
        assert filtered, "expected at least one strong_keep item"
        assert all(i["queue"] == q for i in filtered)

    def test_reason_filter_keeps_only_selected_reason(
        self, ui_export_artifact
    ):
        items = ui_export_artifact["items"]
        # Pick a reason that exists.
        reasons = sorted({i.get("reason") for i in items if i.get("reason")})
        assert reasons, "no reasons in dataset"
        target = reasons[0]
        filtered = [
            i for i in items if _matches_filter(i, {"reason": target})
        ]
        assert filtered, f"expected items with reason={target}"
        assert all(i["reason"] == target for i in filtered)

    def test_score_bucket_filter_uses_buckets(self, ui_export_artifact):
        items = ui_export_artifact["items"]
        # F-003 has score=7 → 6-7
        bucketed = [
            i for i in items
            if _matches_filter(i, {"scoreBucket": "6-7"})
        ]
        # All filtered items must lie in 6-7 bucket
        for i in bucketed:
            assert _score_bucket(i["score"]) == "6-7"

    def test_human_decision_filter_accepted(self, ui_export_artifact):
        items = ui_export_artifact["items"]
        filtered = [
            i for i in items if _matches_filter(i, {"human": "accepted"})
        ]
        if filtered:
            assert all(i["human_decision"] == "accepted" for i in filtered)


# ─── Feedback export (frontend-only, no backend) ────────────────────────────
#
# Reviewer marks per-finding triage quality in the browser, then exports a
# feedback JSON for offline analysis. State lives in Vue; nothing hits the
# server. These tests verify:
#   - wiring (template + state methods + Vue.reactive used);
#   - Python mirror of cv2BuildFeedbackExport produces the contractual shape;
#   - production pipeline files are NOT touched by the feedback export.


class TestFeedbackWiring:
    """Static checks that the new feedback widgets exist in the SPA."""

    QUICK_BUTTON_LABELS = [
        "В основную проверку",
        "Требует смежников",
        "К отклонению",
        "Скрывать как мусор",
        "Не уверен",
    ]

    METHOD_NAMES = [
        "cv2EnsureFeedback",
        "cv2HasFeedback",
        "cv2SetTriageCorrect",
        "cv2SetPreferredTab",
        "cv2SetPriority",
        "cv2SetReviewerNote",
        "cv2QuickRoute",
        "cv2QuickUnsure",
        "cv2BuildFeedbackExport",
        "cv2ExportFeedback",
        "cv2FeedbackSummary",
    ]

    def test_html_has_all_quick_buttons(self):
        html = INDEX_HTML.read_text(encoding="utf-8")
        for label in self.QUICK_BUTTON_LABELS:
            assert label in html, f"Quick button missing: {label}"

    def test_html_has_export_button(self):
        html = INDEX_HTML.read_text(encoding="utf-8")
        assert "Экспортировать feedback JSON" in html
        assert "cv2ExportFeedback" in html

    def test_html_has_assessment_block_title(self):
        html = INDEX_HTML.read_text(encoding="utf-8")
        assert "Оценка раскладки" in html
        assert "cv2-feedback" in html

    def test_html_has_triage_correct_options(self):
        html = INDEX_HTML.read_text(encoding="utf-8")
        # The triage_correct selector must offer all three values.
        for v in ("yes", "no", "unsure"):
            assert f'value="{v}"' in html, f"triage_correct option missing: {v}"

    def test_html_has_preferred_tab_options(self):
        html = INDEX_HTML.read_text(encoding="utf-8")
        for v in ("primary", "needs_context",
                  "suggested_reject", "hidden_by_critic"):
            assert f'value="{v}"' in html, f"preferred_tab option missing: {v}"

    def test_html_has_priority_options(self):
        html = INDEX_HTML.read_text(encoding="utf-8")
        for v in ("normal", "important", "critical"):
            assert f'value="{v}"' in html, f"priority option missing: {v}"

    def test_html_has_reviewer_note_textarea(self):
        html = INDEX_HTML.read_text(encoding="utf-8")
        assert "cv2SetReviewerNote" in html
        assert "<textarea" in html  # at least one textarea (the feedback note)

    def test_app_js_defines_all_feedback_methods(self):
        js = APP_JS.read_text(encoding="utf-8")
        for name in self.METHOD_NAMES:
            assert name in js, f"app.js missing feedback method: {name}"

    def test_app_js_uses_reactive_for_feedback_state(self):
        js = APP_JS.read_text(encoding="utf-8")
        # Feedback dict must be Vue-reactive so the UI updates.
        assert "const cv2Feedback = reactive({})" in js

    def test_css_has_feedback_styles(self):
        css = STYLES_CSS.read_text(encoding="utf-8")
        for klass in (".cv2-feedback", ".cv2-feedback-bar",
                      ".cv2-feedback-quick", ".cv2-fb-quick"):
            assert klass in css, f"CSS missing class: {klass}"

    def test_summary_panel_shows_feedback_counters(self):
        html = INDEX_HTML.read_text(encoding="utf-8")
        # The four counters are rendered from cv2FeedbackSummary.
        assert "cv2FeedbackSummary.evaluated" in html
        assert "cv2FeedbackSummary.yes" in html
        assert "cv2FeedbackSummary.no" in html
        assert "cv2FeedbackSummary.unsure" in html


# ─── Python mirror of cv2BuildFeedbackExport ────────────────────────────────


CV2_TABS = ("primary", "needs_context", "suggested_reject", "hidden_by_critic")
CV2_PRIORITIES = ("normal", "important", "critical")
CV2_TRIAGE_VALUES = ("yes", "no", "unsure")


def _empty_feedback_entry():
    return {
        "triage_correct": "",
        "preferred_tab": "",
        "reviewer_note": "",
        "priority": "normal",
    }


def _mirror_has_feedback(fb: dict) -> bool:
    """Mirror of cv2HasFeedback from app.js."""
    if not fb:
        return False
    return bool(
        fb.get("triage_correct")
        or fb.get("preferred_tab")
        or (fb.get("reviewer_note") or "").strip()
        or (fb.get("priority") and fb.get("priority") != "normal")
    )


def _mirror_set_preferred_tab(feedback: dict, items_by_id: dict,
                              finding_id: str, tab: str):
    """Mirror of cv2SetPreferredTab — including its auto-mark side-effect."""
    if tab not in CV2_TABS:
        return
    fb = feedback.setdefault(finding_id, _empty_feedback_entry())
    fb["preferred_tab"] = tab
    item = items_by_id.get(finding_id)
    if item and item.get("tab") != tab and not fb.get("triage_correct"):
        fb["triage_correct"] = "no"


def _mirror_build_export(cv2_export: dict, feedback: dict) -> dict | None:
    """Mirror of cv2BuildFeedbackExport from app.js."""
    if not cv2_export:
        return None
    items_by_id = {it["finding_id"]: it for it in cv2_export.get("items", [])}
    src_summary = cv2_export.get("summary") or {}
    out = []
    for fid, fb in feedback.items():
        if not _mirror_has_feedback(fb):
            continue
        item = items_by_id.get(fid, {})
        out.append({
            "finding_id": fid,
            "project_name": item.get("project_name", ""),
            "section": item.get("section", ""),
            "original_tab": item.get("tab", ""),
            "original_queue": item.get("queue", ""),
            "triage_correct": fb.get("triage_correct", ""),
            "preferred_tab": fb.get("preferred_tab", ""),
            "priority": fb.get("priority", "normal"),
            "reviewer_note": (fb.get("reviewer_note") or "").strip(),
        })
    return {
        "export_type": "critic_v2_triage_feedback",
        "created_at": "2026-05-08T00:00:00Z",  # placeholder for shape check
        "source_file_summary": {
            "total": src_summary.get("total"),
            "profile": src_summary.get("profile"),
            "primary_queue_reduction_percent":
                src_summary.get("primary_queue_reduction_percent"),
        },
        "feedback": out,
    }


class TestFeedbackStateMirror:
    """Behaviour of the Python mirror of the JS feedback functions."""

    def test_state_starts_empty(self):
        assert _mirror_build_export(
            {"summary": {}, "items": []}, {}
        )["feedback"] == []

    def test_setting_triage_correct_persists(self):
        items = [{"finding_id": "F-1", "tab": "primary"}]
        cv2_export = {"summary": {"total": 1}, "items": items}
        feedback = {"F-1": _empty_feedback_entry()}
        feedback["F-1"]["triage_correct"] = "yes"
        out = _mirror_build_export(cv2_export, feedback)
        assert len(out["feedback"]) == 1
        assert out["feedback"][0]["triage_correct"] == "yes"

    def test_preferred_tab_persists(self):
        items = [{"finding_id": "F-1", "tab": "primary"}]
        cv2_export = {"summary": {"total": 1}, "items": items}
        feedback = {}
        items_by_id = {it["finding_id"]: it for it in items}
        _mirror_set_preferred_tab(feedback, items_by_id, "F-1", "needs_context")
        assert feedback["F-1"]["preferred_tab"] == "needs_context"
        # Cross-tab move auto-marks triage as wrong.
        assert feedback["F-1"]["triage_correct"] == "no"

    def test_preferred_tab_same_as_original_does_not_auto_mark_no(self):
        items = [{"finding_id": "F-1", "tab": "primary"}]
        feedback = {}
        items_by_id = {it["finding_id"]: it for it in items}
        _mirror_set_preferred_tab(feedback, items_by_id, "F-1", "primary")
        assert feedback["F-1"]["preferred_tab"] == "primary"
        assert feedback["F-1"]["triage_correct"] == ""

    def test_reviewer_note_persists_and_is_trimmed(self):
        items = [{"finding_id": "F-1", "tab": "primary"}]
        cv2_export = {"summary": {"total": 1}, "items": items}
        fb = _empty_feedback_entry()
        fb["reviewer_note"] = "  Нужно показывать в основной  "
        fb["triage_correct"] = "no"
        out = _mirror_build_export(cv2_export, {"F-1": fb})
        assert out["feedback"][0]["reviewer_note"] \
            == "Нужно показывать в основной"

    def test_priority_persists(self):
        items = [{"finding_id": "F-1", "tab": "primary"}]
        cv2_export = {"summary": {"total": 1}, "items": items}
        fb = _empty_feedback_entry()
        fb["priority"] = "critical"
        out = _mirror_build_export(cv2_export, {"F-1": fb})
        assert out["feedback"][0]["priority"] == "critical"

    def test_unmodified_entries_excluded(self):
        items = [
            {"finding_id": "F-1", "tab": "primary"},
            {"finding_id": "F-2", "tab": "primary"},
        ]
        cv2_export = {"summary": {"total": 2}, "items": items}
        feedback = {
            "F-1": _empty_feedback_entry(),  # untouched, must be excluded
            "F-2": {**_empty_feedback_entry(), "triage_correct": "yes"},
        }
        out = _mirror_build_export(cv2_export, feedback)
        ids = [e["finding_id"] for e in out["feedback"]]
        assert ids == ["F-2"]


class TestFeedbackExportShape:
    """Contractual JSON shape produced by the export."""

    def _build_realistic(self):
        items = [{
            "finding_id": "P1::F-001",
            "project_name": "P1",
            "section": "AR",
            "tab": "needs_context",
            "queue": "needs_context",
        }]
        cv2_export = {
            "summary": {
                "total": 874,
                "profile": "conservative",
                "primary_queue_reduction_percent": 17.7,
            },
            "items": items,
        }
        feedback = {"P1::F-001": {
            "triage_correct": "no",
            "preferred_tab": "primary",
            "priority": "important",
            "reviewer_note": "Это замечание нужно видеть в основной очереди.",
        }}
        return _mirror_build_export(cv2_export, feedback)

    def test_export_has_top_level_keys(self):
        out = self._build_realistic()
        for key in ("export_type", "created_at",
                    "source_file_summary", "feedback"):
            assert key in out, f"missing top-level key: {key}"

    def test_export_type_marker(self):
        out = self._build_realistic()
        assert out["export_type"] == "critic_v2_triage_feedback"

    def test_source_summary_passes_through_three_fields(self):
        out = self._build_realistic()
        s = out["source_file_summary"]
        assert s["total"] == 874
        assert s["profile"] == "conservative"
        assert s["primary_queue_reduction_percent"] == 17.7

    def test_feedback_item_has_all_contract_fields(self):
        out = self._build_realistic()
        entry = out["feedback"][0]
        for key in (
            "finding_id", "project_name", "section",
            "original_tab", "original_queue",
            "triage_correct", "preferred_tab",
            "priority", "reviewer_note",
        ):
            assert key in entry, f"feedback entry missing: {key}"

    def test_feedback_carries_original_position(self):
        """original_tab/original_queue come from the loaded UI export, not
        from reviewer input — that's how downstream tooling can see what the
        critic chose vs. what the human chose."""
        out = self._build_realistic()
        entry = out["feedback"][0]
        assert entry["original_tab"] == "needs_context"
        assert entry["original_queue"] == "needs_context"
        assert entry["preferred_tab"] == "primary"


# ─── No-backend / production-not-touched guarantees ─────────────────────────


class TestExportDoesNotCallBackend:
    """The export path must not reach the backend. Static check on app.js."""

    def test_export_function_uses_blob_download_not_fetch(self):
        js = APP_JS.read_text(encoding="utf-8")
        # Extract the cv2ExportFeedback function body.
        m = re.search(
            r"function cv2ExportFeedback\(\)\s*\{(.*?)\n\s{8}\}",
            js, re.DOTALL,
        )
        assert m, "cv2ExportFeedback function not found in app.js"
        body = m.group(1)
        assert "Blob(" in body, "expected Blob-based download"
        assert "URL.createObjectURL" in body
        # Negative checks: no network calls.
        for forbidden in ("fetch(", "axios", "XMLHttpRequest", "WebSocket"):
            assert forbidden not in body, (
                f"cv2ExportFeedback must not use {forbidden}"
            )

    def test_build_function_is_pure_no_network(self):
        js = APP_JS.read_text(encoding="utf-8")
        m = re.search(
            r"function cv2BuildFeedbackExport\(\)\s*\{(.*?)\n\s{8}\}",
            js, re.DOTALL,
        )
        assert m, "cv2BuildFeedbackExport function not found in app.js"
        body = m.group(1)
        for forbidden in ("fetch(", "axios", "XMLHttpRequest"):
            assert forbidden not in body, (
                f"cv2BuildFeedbackExport must not use {forbidden}"
            )


class TestProductionPipelineUntouched:
    """This experimental UI must not change anything in the production stack."""

    PRODUCTION_FILES = [
        "backend/app/pipeline/manager.py",
        "backend/app/pipeline/stages/findings_review/runner.py",
    ]

    @pytest.mark.parametrize("rel", PRODUCTION_FILES)
    def test_production_pipeline_files_have_no_feedback_hooks(self, rel):
        """No feedback symbol should leak into production pipeline files."""
        path = _PROJECT_ROOT / rel
        assert path.exists(), f"missing production file: {rel}"
        text = path.read_text(encoding="utf-8")
        for token in (
            "cv2BuildFeedbackExport",
            "cv2ExportFeedback",
            "critic_v2_triage_feedback",
        ):
            assert token not in text, (
                f"{rel} unexpectedly references feedback symbol {token!r}"
            )

    def test_findings_review_artifact_filename_not_referenced_by_feedback(self):
        """The feedback widget must not write to 03_findings_review.json."""
        js = APP_JS.read_text(encoding="utf-8")
        # Locate the feedback section by its anchor comment.
        anchor = "Critic v2 UI: Feedback"
        assert anchor in js
        section_start = js.index(anchor)
        # Take the next ~6000 chars that hold the feedback block.
        section = js[section_start:section_start + 6000]
        assert "03_findings_review.json" not in section
        assert "/api/" not in section
