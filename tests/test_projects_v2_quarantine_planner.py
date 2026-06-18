"""Шаг 9/10 — тесты dry-run планировщика карантина legacy."""
from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pytest

_SCRIPT = (Path(__file__).resolve().parents[1]
           / "scripts" / "projects_v2" / "plan_legacy_quarantine.py")
_spec = importlib.util.spec_from_file_location("plan_legacy_quarantine", _SCRIPT)
plan = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(plan)


def _fake_projects(tmp_path) -> Path:
    root = tmp_path / "projects"
    (root / "213. Obj" / "KJ" / "DOC-1").mkdir(parents=True)
    (root / "213. Obj" / "KJ" / "DOC-1" / "a.pdf").write_bytes(b"x" * 100)
    (root / "213. Obj" / "KJ" / "DOC-1" / "_output").mkdir()
    (root / "213. Obj" / "KJ" / "DOC-1" / "_output" / "03_findings.json").write_text("{}")
    return root


def test_scan_counts_files_and_size(tmp_path):
    root = _fake_projects(tmp_path)
    info = plan.scan_projects(root)
    assert info["exists"] is True
    assert info["total_files"] == 2
    assert info["total_bytes"] >= 100
    assert info["top_level_entries"] == ["213. Obj"]


def test_build_plan_proposes_commands_no_execute(tmp_path):
    root = _fake_projects(tmp_path)
    p = plan.build_plan(root, date_token="20260618")
    pr = p["proposed"]
    assert "projects_legacy_archive_20260618" in pr["future_quarantine_command"]
    assert pr["future_quarantine_command"].startswith("mv ")
    assert "tar -czf" in pr["backup_command"]


def test_no_execute_flag_exists(tmp_path):
    """--execute НЕ должен существовать (argparse отвергает)."""
    root = _fake_projects(tmp_path)
    with pytest.raises(SystemExit):
        plan.main(["--projects-root", str(root), "--execute"])


def test_dry_run_writes_only_to_output_not_projects(tmp_path):
    root = _fake_projects(tmp_path)
    out = tmp_path / "plan.json"
    before = sorted(str(p) for p in root.rglob("*"))
    rc = plan.main(["--projects-root", str(root), "--output", str(out), "--date-token", "20260618"])
    assert rc == 0
    assert out.is_file()
    data = json.loads(out.read_text(encoding="utf-8"))
    assert data["total_files"] == 2
    # projects/ не изменён
    after = sorted(str(p) for p in root.rglob("*"))
    assert before == after


def test_output_refuses_inside_projects(tmp_path):
    root = _fake_projects(tmp_path)
    bad = root / "plan.json"
    rc = plan.main(["--projects-root", str(root), "--output", str(bad)])
    assert rc == 2
    assert not bad.exists()


def test_missing_projects_root_returns_2(tmp_path):
    rc = plan.main(["--projects-root", str(tmp_path / "nope")])
    assert rc == 2
