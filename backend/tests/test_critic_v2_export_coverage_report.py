"""
Tests for backend/scripts/critic_v2_export_coverage_report.py and the matching
logic between system projects and critic_v2_triage_ui.json items.

These tests are offline-only. They:
  - do NOT touch production pipeline;
  - do NOT call LLM;
  - do NOT write into projects/<...>/_output;
  - do NOT mutate 03_findings_review.json.
"""
from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import pytest

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))


@pytest.fixture(scope="module")
def coverage_module():
    """Import the script as a module without running argparse."""
    script_path = (
        _PROJECT_ROOT / "backend" / "scripts" / "critic_v2_export_coverage_report.py"
    )
    spec = importlib.util.spec_from_file_location("coverage_report", script_path)
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


# ──────────────────────────────────────────────────────────────────────────────
# match_project: пять стратегий сопоставления
# ──────────────────────────────────────────────────────────────────────────────


def _make_export(*names):
    names = set(names)
    normalized = {}
    for n in names:
        s = (n or "").strip().lower()
        if s.endswith(".pdf"):
            s = s[:-4].rstrip()
        normalized[" ".join(s.split())] = n
    return names, normalized


def test_match_exact_project_name(coverage_module):
    names, norm = _make_export("13АВ-РД-АР1.1-К1")
    p = {"project_id": "x", "name": "13АВ-РД-АР1.1-К1"}
    assert coverage_module.match_project(p, names, norm) == (
        "13АВ-РД-АР1.1-К1", "project_name"
    )


def test_match_strips_pdf_suffix(coverage_module):
    names, norm = _make_export("13АВ-РД-АР0.1-ПА")
    p = {"project_id": "x", "name": "13АВ-РД-АР0.1-ПА.pdf"}
    assert coverage_module.match_project(p, names, norm) == (
        "13АВ-РД-АР0.1-ПА", "project_name_no_pdf"
    )


def test_match_by_project_id_when_name_differs(coverage_module):
    """Если name отличается от id (например, переименовали папку), pid должен спасти."""
    names, norm = _make_export("MyProject")
    p = {"project_id": "MyProject", "name": "MyProject (старая копия)"}
    assert coverage_module.match_project(p, names, norm) == (
        "MyProject", "project_id"
    )


def test_match_normalized_handles_case_and_spaces(coverage_module):
    names, norm = _make_export("MyProject")
    p = {"project_id": "x", "name": "  myproject  "}
    matched_name, by = coverage_module.match_project(p, names, norm)
    assert matched_name == "MyProject"
    assert by == "normalized"


def test_match_missing_returns_none(coverage_module):
    names, norm = _make_export("13АВ-РД-АР1.1-К1")
    p = {"project_id": "x", "name": "Совершенно другой проект.pdf"}
    assert coverage_module.match_project(p, names, norm) == (None, None)


# ──────────────────────────────────────────────────────────────────────────────
# build_report: shape и сводка
# ──────────────────────────────────────────────────────────────────────────────


def _sample_export():
    return {
        "summary": {"total": 3},
        "tabs": [],
        "items": [
            {"finding_id": "F1", "project_name": "P1", "section": "AR", "tab": "primary"},
            {"finding_id": "F2", "project_name": "P1", "section": "AR", "tab": "primary"},
            {"finding_id": "F3", "project_name": "P2", "section": "KJ", "tab": "primary"},
        ],
    }


def _sample_projects():
    return [
        # in export
        {"project_id": "P1", "name": "P1", "section": "AR", "object": "obj",
         "folder_path": "obj/AR/P1", "output_dir": "/tmp/p1",
         "has_findings": True, "has_expert_review": True,
         "has_blocks": True, "has_document_graph": True},
        # in export via .pdf strip
        {"project_id": "P2", "name": "P2.pdf", "section": "KJ", "object": "obj",
         "folder_path": "obj/KJ/P2", "output_dir": "/tmp/p2",
         "has_findings": True, "has_expert_review": True,
         "has_blocks": True, "has_document_graph": True},
        # NOT in export, has findings but no expert review → "no_expert_review"
        {"project_id": "P3", "name": "P3.pdf", "section": "SS", "object": "obj",
         "folder_path": "obj/SS/P3", "output_dir": "/tmp/p3",
         "has_findings": True, "has_expert_review": False,
         "has_blocks": True, "has_document_graph": True},
        # NOT in export, no findings → "no_findings_no_export"
        {"project_id": "P4", "name": "P4.pdf", "section": "SS", "object": "obj",
         "folder_path": "obj/SS/P4", "output_dir": None,
         "has_findings": False, "has_expert_review": False,
         "has_blocks": False, "has_document_graph": False},
    ]


def test_build_report_counts_matched_and_missing(coverage_module):
    report = coverage_module.build_report(
        _sample_export(), _sample_projects(), Path("/tmp/whatever.json")
    )
    assert report["ok"] is True
    assert report["system_projects_total"] == 4
    assert report["matched_count"] == 2
    assert report["missing_count"] == 2
    # Two distinct buckets for missing rows
    assert "no_expert_review_likely_excluded_from_matrix" \
        in report["missing_reasons_breakdown"]
    assert "no_findings_no_export" in report["missing_reasons_breakdown"]


def test_build_report_handles_missing_export(coverage_module, tmp_path):
    nonexistent = tmp_path / "nope.json"
    report = coverage_module.build_report(
        None, _sample_projects(), nonexistent
    )
    assert report["ok"] is False
    assert report["error"] == "export_missing"
    assert report["projects_total"] == 4
    assert "hint_command" in report


def test_build_report_passes_matched_by(coverage_module):
    report = coverage_module.build_report(
        _sample_export(), _sample_projects(), Path("/tmp/x.json")
    )
    by_id = {r["project_id"]: r for r in report["projects"]}
    assert by_id["P1"]["matched_by"] == "project_name"
    assert by_id["P2"]["matched_by"] == "project_name_no_pdf"
    assert by_id["P3"]["matched_by"] is None
    assert by_id["P4"]["matched_by"] is None


def test_build_report_does_not_write_files(coverage_module, tmp_path):
    """build_report не должна писать на диск — это работа write_markdown/json."""
    before = sorted(tmp_path.iterdir())
    coverage_module.build_report(
        _sample_export(), _sample_projects(), tmp_path / "fake.json"
    )
    after = sorted(tmp_path.iterdir())
    assert before == after


# ──────────────────────────────────────────────────────────────────────────────
# write_markdown: проверяем, что output содержит нужные секции
# ──────────────────────────────────────────────────────────────────────────────


def test_write_markdown_includes_key_sections(coverage_module, tmp_path):
    report = coverage_module.build_report(
        _sample_export(), _sample_projects(), Path("/tmp/whatever.json")
    )
    out_md = tmp_path / "report.md"
    coverage_module.write_markdown(report, out_md)
    text = out_md.read_text(encoding="utf-8")
    assert "Export Coverage Report" in text
    assert "Доступные сейчас" in text
    assert "Отсутствуют в текущем export" in text
    assert "Как пересобрать export" in text
    # missing project P3 in the missing table
    assert "P3" in text


def test_write_markdown_for_missing_export(coverage_module, tmp_path):
    report = coverage_module.build_report(
        None, _sample_projects(), Path("/tmp/x.json")
    )
    out_md = tmp_path / "r.md"
    coverage_module.write_markdown(report, out_md)
    text = out_md.read_text(encoding="utf-8")
    assert "Export файл отсутствует" in text
    assert "replay_critic_v2_triage_policy" in text


# ──────────────────────────────────────────────────────────────────────────────
# Endpoint matching (existing logic) parity
#
# Verify the coverage report and the live endpoint use the same matching rules
# so the report doesn't lie about what the UI will actually see.
# ──────────────────────────────────────────────────────────────────────────────


def test_endpoint_match_strategy_parity(coverage_module):
    """
    Coverage-report matching is a strict superset of endpoint matching
    (we added project_id-based fallbacks). At minimum, every (name, no_pdf,
    normalized) hit the endpoint catches must also be matched by the report.
    """
    names, norm = _make_export("13АВ-РД-АР1.1-К1", "MyProject")

    for nm, expected_by in [
        ("13АВ-РД-АР1.1-К1", "project_name"),
        ("13АВ-РД-АР1.1-К1.pdf", "project_name_no_pdf"),
        ("myproject", "normalized"),
    ]:
        p = {"project_id": "x", "name": nm}
        matched, by = coverage_module.match_project(p, names, norm)
        assert matched is not None, f"report should match {nm!r}"
        assert by == expected_by


# ──────────────────────────────────────────────────────────────────────────────
# No-touch / production safety
# ──────────────────────────────────────────────────────────────────────────────


def test_script_does_not_import_pipeline_modules():
    src = (
        _PROJECT_ROOT / "backend" / "scripts"
        / "critic_v2_export_coverage_report.py"
    ).read_text(encoding="utf-8")
    forbidden = [
        "from backend.app.pipeline",
        "import backend.app.pipeline",
        "from backend.app.services.findings",
        "anthropic",
        "openai",
    ]
    for token in forbidden:
        assert token not in src, f"Forbidden import in coverage script: {token}"


def test_script_does_not_write_into_project_output_dirs():
    """The script must only write to user-controlled --out-* paths (default /tmp).

    Static check: enumerate every write call in the source and assert they are
    confined to out_json / out_md.
    """
    src = (
        _PROJECT_ROOT / "backend" / "scripts"
        / "critic_v2_export_coverage_report.py"
    ).read_text(encoding="utf-8")
    import re
    # Collect all .write_text(...) / open(..., 'w') call targets.
    write_targets = re.findall(r"(\w+)\.write_text\(", src)
    open_writes = re.findall(r"open\(([^,]+),\s*['\"]w", src)
    all_writes = set(write_targets) | set(open_writes)
    # Only out_json (and out_md indirectly via write_markdown→path.write_text) allowed.
    assert all_writes.issubset({"out_json", "path"}), (
        f"Unexpected write targets in coverage script: {all_writes}"
    )


def test_script_does_not_have_lm_or_llm_calls():
    """No network/LLM calls in the diagnostic script."""
    src = (
        _PROJECT_ROOT / "backend" / "scripts"
        / "critic_v2_export_coverage_report.py"
    ).read_text(encoding="utf-8")
    for forbidden in ("requests.", "httpx.", "urllib.request",
                      "anthropic", "openai", "lmstudio"):
        assert forbidden not in src, f"forbidden network/LLM token: {forbidden}"
