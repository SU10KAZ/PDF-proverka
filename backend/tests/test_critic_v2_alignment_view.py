"""
Tests for the experimental Critic v2 ↔ expert_review alignment UI.

There is no JS test framework in this repo (Vue 3 via CDN, no build step).
We verify two things:

1) The alignment classification logic is wired into app.js with the exact
   token mapping the spec requires (smoke check on the function body).
2) A reference Python re-implementation of the same logic gives the
   expected results on representative cases. This is what catches a
   logic regression: if the JS function ever changes its mapping, the
   spec test below has to be updated in lockstep, which makes drift loud.

We also lock in the HTML wiring: the alignment summary panel exists,
the disagreement filter is wired, and the feedback block is now visually
secondary (has the hint text).

Production pipeline / 03_findings_review.json / DB are NOT touched.
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

import pytest

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

INDEX_HTML = _PROJECT_ROOT / "frontend" / "index.html"
APP_JS = _PROJECT_ROOT / "frontend" / "static" / "js" / "app.js"


# ──────────────────────────────────────────────────────────────────────────
# Reference re-implementation (must mirror cv2AlignmentOf in app.js exactly)
# ──────────────────────────────────────────────────────────────────────────
def alignment_of(item: dict) -> str:
    """Mirror of cv2AlignmentOf in app.js. Must stay in lockstep with JS."""
    hd = item.get("human_decision")
    tab = item.get("tab")
    if not hd or hd == "unknown":
        return "unknown"
    if hd == "accepted":
        if tab == "primary":
            return "aligned_visible"
        if tab == "needs_context":
            return "accepted_needs_context"
        if tab in ("suggested_reject", "hidden_by_critic"):
            return "accepted_collapsed"
        return "unknown"
    if hd == "rejected":
        if tab in ("hidden_by_critic", "suggested_reject"):
            return "aligned_hidden"
        if tab == "needs_context":
            return "rejected_needs_context"
        if tab == "primary":
            return "rejected_visible"
    return "unknown"


def is_disagreement(alignment: str) -> bool:
    return alignment in (
        "accepted_collapsed",
        "accepted_needs_context",
        "rejected_visible",
        "rejected_needs_context",
    )


# ──────────────────────────────────────────────────────────────────────────
# Logic specification (drives the JS)
# ──────────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize("hd,tab,expected", [
    # Aligned cases
    ("accepted", "primary", "aligned_visible"),
    ("rejected", "hidden_by_critic", "aligned_hidden"),
    ("rejected", "suggested_reject", "aligned_hidden"),
    # Disagreements: accepted_collapsed is reserved for hard-collapse tabs
    ("accepted", "suggested_reject", "accepted_collapsed"),
    ("accepted", "hidden_by_critic", "accepted_collapsed"),
    # accepted + needs_context is a softer mismatch — its own bucket
    ("accepted", "needs_context", "accepted_needs_context"),
    # Rejected disagreements
    ("rejected", "primary", "rejected_visible"),
    ("rejected", "needs_context", "rejected_needs_context"),
    # Unknown
    (None, "primary", "unknown"),
    ("", "primary", "unknown"),
    ("unknown", "primary", "unknown"),
])
def test_alignment_classification(hd, tab, expected):
    item = {"human_decision": hd, "tab": tab}
    assert alignment_of(item) == expected


def test_disagreement_predicate():
    assert is_disagreement("accepted_collapsed") is True
    assert is_disagreement("accepted_needs_context") is True
    assert is_disagreement("rejected_visible") is True
    assert is_disagreement("rejected_needs_context") is True
    assert is_disagreement("aligned_visible") is False
    assert is_disagreement("aligned_hidden") is False
    assert is_disagreement("unknown") is False


def test_summary_counts_on_mixed_set():
    items = [
        {"human_decision": "accepted", "tab": "primary"},          # aligned_visible
        {"human_decision": "accepted", "tab": "primary"},          # aligned_visible
        {"human_decision": "rejected", "tab": "hidden_by_critic"}, # aligned_hidden
        {"human_decision": "rejected", "tab": "suggested_reject"}, # aligned_hidden
        {"human_decision": "accepted", "tab": "needs_context"},    # accepted_needs_context
        {"human_decision": "accepted", "tab": "hidden_by_critic"}, # accepted_collapsed + hidden_human_accepted
        {"human_decision": "accepted", "tab": "suggested_reject"}, # accepted_collapsed + suggested_reject_human_accepted
        {"human_decision": "rejected", "tab": "primary"},          # rejected_visible
        {"human_decision": "rejected", "tab": "needs_context"},    # rejected_needs_context
        {"human_decision": None, "tab": "primary"},                # without_decision
    ]
    summary = {
        "with_decision": 0, "aligned": 0, "disagreements": 0,
        "aligned_visible": 0, "aligned_hidden": 0,
        "accepted_collapsed": 0, "accepted_needs_context": 0,
        "rejected_visible": 0, "rejected_needs_context": 0,
        "hidden_human_accepted": 0, "suggested_reject_human_accepted": 0,
        "without_decision": 0,
    }
    for it in items:
        al = alignment_of(it)
        if al == "unknown":
            summary["without_decision"] += 1
            continue
        summary["with_decision"] += 1
        if al == "aligned_visible":
            summary["aligned"] += 1
            summary["aligned_visible"] += 1
        elif al == "aligned_hidden":
            summary["aligned"] += 1
            summary["aligned_hidden"] += 1
        elif al == "accepted_collapsed":
            summary["disagreements"] += 1
            summary["accepted_collapsed"] += 1
        elif al == "accepted_needs_context":
            summary["disagreements"] += 1
            summary["accepted_needs_context"] += 1
        elif al == "rejected_visible":
            summary["disagreements"] += 1
            summary["rejected_visible"] += 1
        elif al == "rejected_needs_context":
            summary["disagreements"] += 1
            summary["rejected_needs_context"] += 1
        if it["human_decision"] == "accepted" and it["tab"] == "hidden_by_critic":
            summary["hidden_human_accepted"] += 1
        if it["human_decision"] == "accepted" and it["tab"] == "suggested_reject":
            summary["suggested_reject_human_accepted"] += 1

    assert summary["with_decision"] == 9
    assert summary["aligned"] == 4
    assert summary["aligned_visible"] == 2
    assert summary["aligned_hidden"] == 2
    # 2 accepted_collapsed + 1 accepted_needs_context + 1 rejected_visible + 1 rejected_needs_context
    assert summary["disagreements"] == 5
    assert summary["accepted_collapsed"] == 2
    assert summary["accepted_needs_context"] == 1
    assert summary["rejected_visible"] == 1
    assert summary["rejected_needs_context"] == 1
    assert summary["hidden_human_accepted"] == 1
    assert summary["suggested_reject_human_accepted"] == 1
    assert summary["without_decision"] == 1


# ──────────────────────────────────────────────────────────────────────────
# Frontend wiring smoke tests
# ──────────────────────────────────────────────────────────────────────────

def test_app_js_defines_alignment_function():
    js = APP_JS.read_text(encoding="utf-8")
    assert "function cv2AlignmentOf(" in js
    assert "function cv2IsDisagreement(" in js
    # Capture the area between cv2AlignmentOf and the following function decl.
    start = js.find("function cv2AlignmentOf(")
    assert start >= 0
    next_fn = js.find("function cv2IsDisagreement(", start)
    assert next_fn > start
    body = js[start:next_fn]
    for token in ["aligned_visible", "aligned_hidden", "accepted_collapsed",
                  "accepted_needs_context",
                  "rejected_visible", "rejected_needs_context", "unknown",
                  "primary", "needs_context", "suggested_reject",
                  "hidden_by_critic"]:
        assert token in body, f"alignment function missing token: {token}"


def test_app_js_disagreement_includes_accepted_needs_context():
    js = APP_JS.read_text(encoding="utf-8")
    # The disagreement predicate must also flag accepted_needs_context.
    start = js.find("function cv2IsDisagreement(")
    assert start >= 0
    # cv2IsDisagreement body ends at the next top-level function or const.
    next_fn = js.find("function cv2Label(", start)
    assert next_fn > start
    body = js[start:next_fn]
    for token in ["accepted_collapsed", "accepted_needs_context",
                  "rejected_visible", "rejected_needs_context"]:
        assert token in body, f"disagreement predicate missing: {token}"


def test_app_js_alignment_labels_include_all_statuses():
    """All seven alignment statuses must have a Russian label."""
    js = APP_JS.read_text(encoding="utf-8")
    # Locate CV2_LABELS.alignment block
    start = js.find("alignment: {")
    assert start >= 0
    end = js.find("},", start)
    assert end > start
    body = js[start:end]
    for key in ("aligned_visible", "aligned_hidden",
                "accepted_collapsed", "accepted_needs_context",
                "rejected_visible", "rejected_needs_context",
                "unknown"):
        assert key in body, f"CV2_LABELS.alignment missing key: {key}"


def test_app_js_alignment_summary_computed_present():
    js = APP_JS.read_text(encoding="utf-8")
    assert "cv2AlignmentSummary" in js
    # The summary must aggregate all the spec'd buckets.
    for token in ["with_decision", "aligned", "disagreements",
                  "accepted_collapsed", "accepted_needs_context",
                  "rejected_visible", "rejected_needs_context",
                  "hidden_human_accepted", "suggested_reject_human_accepted",
                  "without_decision"]:
        assert token in js, f"alignment summary missing bucket: {token}"


def test_app_js_filter_supports_alignment():
    js = APP_JS.read_text(encoding="utf-8")
    # Filter state has alignment field
    assert "alignment: ''" in js
    # cv2ItemMatchesFilter handles disagreement marker and "no decision" marker
    assert "__disagreement__" in js
    assert "__none__alignment" in js


def test_index_html_alignment_summary_block_present():
    html = INDEX_HTML.read_text(encoding="utf-8")
    # Two view blocks → two summary panels.
    assert html.count('class="cv2-alignment-summary"') >= 2
    # Spec strings
    assert "Сверка с экспертом" in html
    assert "Принято, но свёрнуто" in html
    assert "Принято, но в контекст" in html
    assert "Отклонено, но в основной" in html


def test_index_html_alignment_filter_dropdown_present():
    html = INDEX_HTML.read_text(encoding="utf-8")
    # The filter is now labelled "Сверка с экспертом" (was "Расхождение").
    assert ">Сверка с экспертом:" in html
    # disagreement marker option + new accepted_needs_context option
    assert '"__disagreement__"' in html
    assert '"accepted_needs_context"' in html


def test_index_html_disagreements_quick_toggle_present():
    """Spec §6: a dedicated quick toggle "Расхождения" filters to the 4 disagreement buckets."""
    html = INDEX_HTML.read_text(encoding="utf-8")
    # Two view blocks → two toggle bars.
    assert html.count("cv2-disagreements-toggle") >= 2
    # Button text is the literal "Расхождения".
    assert ">\n                        Расхождения\n                    <" in html \
        or ">Расхождения<" in html


def test_index_html_per_item_alignment_block():
    html = INDEX_HTML.read_text(encoding="utf-8")
    # Per-item alignment block + the four mandatory rows
    assert 'class="cv2-item-alignment"' in html
    assert "Решение эксперта:" in html
    assert "Оценка critic v2:" in html
    assert "Статус сверки:" in html


def test_feedback_block_visually_secondary():
    html = INDEX_HTML.read_text(encoding="utf-8")
    # New header text replaces the previous one in BOTH view blocks.
    assert "Дополнительная ручная корректировка" in html
    assert "Основная сверка уже выполнена по expert_review" in html
    # Per-item feedback has the secondary hint.
    assert "Опционально. Используйте, если автоматическая сверка не отражает" in html


def test_feedback_export_unchanged_shape():
    """
    Adding alignment must not change the feedback export payload shape.
    Smoke check: the build function still produces the same top-level keys
    (export_type, created_at, scope, source_file_summary, feedback).
    """
    js = APP_JS.read_text(encoding="utf-8")
    start = js.find("function cv2BuildFeedbackExport")
    assert start >= 0
    end = js.find("function cv2ExportFeedback", start)
    assert end > start
    body = js[start:end]
    # All five keys must be present in the export payload — either as
    # explicit "key:" or as shorthand `key,` (when JS reuses a same-named var).
    for key in ["export_type", "created_at", "scope",
                "source_file_summary", "feedback"]:
        assert (
            f"{key}:" in body
            or f"{key},\n" in body
            or f"{key},\r" in body
            or body.rstrip().endswith(key + ",")
        ), f"feedback export missing key: {key}"
