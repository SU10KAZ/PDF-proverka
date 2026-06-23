"""Тесты version-aware MD resolution (Часть B).

Покрывает реальные кейсы инцидента:
* latest_version_id=v2 + queue version_id=None → MD ищется в папке версии;
* md_file=None + единственный *_document.md → найден;
* pdf_file указывает на соседний проект → не ломает поиск MD (ищем по папке);
* несколько кандидатов → ambiguous_md_candidates (не угадываем);
* отсутствует MD → md_not_found + cross-version подсказка.
+ план безопасного ремонта project_info (только md_file).

Run: python -m pytest tests/test_md_resolver.py -v
"""
from __future__ import annotations

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from backend.app.services.common.md_resolver import (  # noqa: E402
    resolve_project_md,
    plan_project_info_repair,
    STATUS_OK,
    STATUS_AMBIGUOUS,
    STATUS_NOT_FOUND,
)


def _mkdir(p: Path) -> Path:
    p.mkdir(parents=True, exist_ok=True)
    return p


def _touch(p: Path, content="x"):
    p.write_text(content, encoding="utf-8")
    return p


# ─── resolve_project_md ──────────────────────────────────────────────────────

def test_single_document_md_ok(tmp_path):
    vd = _mkdir(tmp_path / "13АВ-РД-АР3-К3")
    _touch(vd / "13АВ-РД-АР3-К3 (Изм.1)_document.md")
    res = resolve_project_md(vd, "13АВ-РД-АР3-К3")
    assert res.status == STATUS_OK
    assert res.md_name == "13АВ-РД-АР3-К3 (Изм.1)_document.md"
    assert res.diagnostics["selected_by"] == "single_candidate"


def test_md_file_none_falls_back_to_glob(tmp_path):
    vd = _mkdir(tmp_path / "proj")
    _touch(vd / "proj_document.md")
    res = resolve_project_md(vd, "proj", project_info={"md_file": None})
    assert res.status == STATUS_OK
    assert res.md_name == "proj_document.md"


def test_explicit_md_file_preferred_when_exists(tmp_path):
    vd = _mkdir(tmp_path / "proj")
    _touch(vd / "a_document.md")
    _touch(vd / "b_document.md")
    res = resolve_project_md(vd, "proj", project_info={"md_file": "b_document.md"})
    assert res.status == STATUS_OK
    assert res.md_name == "b_document.md"
    assert res.diagnostics["selected_by"] == "project_info.md_file"


def test_stale_md_file_ignored_then_single(tmp_path):
    vd = _mkdir(tmp_path / "proj")
    _touch(vd / "real_document.md")
    res = resolve_project_md(vd, "proj", project_info={"md_file": "missing_document.md"})
    assert res.status == STATUS_OK
    assert res.md_name == "real_document.md"
    assert res.diagnostics.get("project_info_md_file_stale") is True


def test_wrong_pdf_file_does_not_break_md_search(tmp_path):
    # АР1.2-К6: pdf_file указывает на АР1.1-К6, но MD в папке корректный.
    vd = _mkdir(tmp_path / "13АВ-РД-АР1.2-К6")
    _touch(vd / "13АВ-РД-АР1.2-К6 (1)_document.md")
    res = resolve_project_md(
        vd, "13АВ-РД-АР1.2-К6",
        project_info={"md_file": None, "pdf_file": "13АВ-РД-АР1.1-К6 V1.pdf"},
    )
    assert res.status == STATUS_OK
    assert res.md_name == "13АВ-РД-АР1.2-К6 (1)_document.md"
    assert res.pdf_file_mismatch is True   # отмечено, но поиск не сломан


def test_ambiguous_multiple_candidates(tmp_path):
    vd = _mkdir(tmp_path / "proj")
    _touch(vd / "alpha_document.md")
    _touch(vd / "beta_document.md")
    res = resolve_project_md(vd, "totally_other_id")
    assert res.status == STATUS_AMBIGUOUS
    assert set(res.candidates) == {"alpha_document.md", "beta_document.md"}


def test_multiple_candidates_basename_match(tmp_path):
    vd = _mkdir(tmp_path / "ЭМ1")
    _touch(vd / "ЭМ1_document.md")
    _touch(vd / "пояснительная_document.md")
    res = resolve_project_md(vd, "ЭМ1")
    assert res.status == STATUS_OK
    assert res.md_name == "ЭМ1_document.md"
    assert res.diagnostics["selected_by"] == "basename_match"


def test_not_found_with_root_hint(tmp_path):
    # latest=v2 dir пуст, но MD есть в V1/root → понятная подсказка.
    root = _mkdir(tmp_path / "proj")
    _touch(root / "proj_document.md")
    vd = _mkdir(tmp_path / "proj V2")
    res = resolve_project_md(vd, "proj", root_dir=root, latest_version_id="v2")
    assert res.status == STATUS_NOT_FOUND
    assert res.root_candidates == ["proj_document.md"]
    msg = res.error_message("proj")
    assert "md_not_found" in msg and "V1" in msg


def test_excludes_service_md(tmp_path):
    vd = _mkdir(tmp_path / "proj")
    _touch(vd / "audit_report_document.md")
    _touch(vd / "_combined_document.md")
    res = resolve_project_md(vd, "proj")
    assert res.status == STATUS_NOT_FOUND   # служебные не считаются исходником


# ─── plan_project_info_repair ────────────────────────────────────────────────

def test_repair_plan_sets_md_file_when_stale(tmp_path):
    vd = _mkdir(tmp_path / "13АВ-РД-АР1.2-К6")
    _touch(vd / "13АВ-РД-АР1.2-К6 (1)_document.md")
    info = {"md_file": None, "pdf_file": "13АВ-РД-АР1.1-К6 V1.pdf"}
    plan = plan_project_info_repair(vd, "13АВ-РД-АР1.2-К6", info)
    assert plan.needs_repair is True
    assert plan.set_md_file == "13АВ-РД-АР1.2-К6 (1)_document.md"
    assert plan.pdf_file_mismatch is True
    assert any("md_file" in a for a in plan.actions)
    assert any("pdf_file" in a for a in plan.actions)


def test_repair_plan_noop_when_md_file_correct(tmp_path):
    vd = _mkdir(tmp_path / "proj")
    _touch(vd / "proj_document.md")
    info = {"md_file": "proj_document.md", "pdf_file": "proj.pdf"}
    plan = plan_project_info_repair(vd, "proj", info)
    assert plan.needs_repair is False
    assert plan.set_md_file is None
    assert plan.actions == []


def test_repair_plan_reports_not_found(tmp_path):
    vd = _mkdir(tmp_path / "proj V2")
    root = _mkdir(tmp_path / "proj")
    _touch(root / "proj_document.md")
    plan = plan_project_info_repair(
        vd, "proj", {"md_file": None}, root_dir=root, latest_version_id="v2"
    )
    assert plan.md_status == STATUS_NOT_FOUND
    assert plan.set_md_file is None
    assert plan.needs_repair is True
    assert any("MD_NOT_FOUND" in a for a in plan.actions)
