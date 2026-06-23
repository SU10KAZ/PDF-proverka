"""Тесты v2-aware смены раздела: set_project_section физически переносит
документ между папками дисциплин под projects_v2_primary + пишет section в
project_info перенесённого документа, НЕ создавая пустой дубль в старой
дисциплине.

Регрессия cutover: read_canary группирует проекты по физической папке
``disciplines/<code>/``, а старый set_project_section писал только поле
``section`` → смена раздела в UI «не срабатывала».

Регрессия 2026-06-23 (ОВ1.1-ПА): после переноса вызывался legacy
save_project_info, чей v2-target резолвится из legacy-папки (осталась в старой
дисциплине) → заскаффолдил пустой документ-дубль. Фикс: под v2-primary при
успешном переносе legacy save_project_info НЕ вызывается.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest


def _make_doc(v2_root: Path, code: str, disc: str) -> Path:
    doc_dir = v2_root / "objects" / "OBJ" / "disciplines" / disc / "documents" / code
    vdir = doc_dir / "versions" / "v001"
    (vdir / "01_input").mkdir(parents=True, exist_ok=True)
    (doc_dir / "document.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "document_code": code,
                "object_folder": "OBJ",
                "object_id": "obj-x",
                "discipline": disc,
                "current_version": "v001",
                "version_ids": ["v001"],
                "versions": [{"version_id": "v001", "version_no": 1, "label": "V1"}],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    (doc_dir / "current_version.txt").write_text("v001", encoding="utf-8")
    (vdir / "version.json").write_text(
        json.dumps(
            {"version_id": "v001", "project_info": {"section": disc, "name": code}},
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    (vdir / "01_input" / "project_info.json").write_text(
        json.dumps({"section": disc, "name": code}, ensure_ascii=False),
        encoding="utf-8",
    )
    return doc_dir


def _section_in(doc_dir: Path) -> set[str]:
    out = set()
    for vj in doc_dir.glob("versions/*/version.json"):
        pi = json.loads(vj.read_text(encoding="utf-8")).get("project_info") or {}
        out.add(pi.get("section"))
    for pij in doc_dir.glob("versions/*/01_input/project_info.json"):
        out.add(json.loads(pij.read_text(encoding="utf-8")).get("section"))
    return out


def test_move_folder_updates_document_json_section_and_returns_true(monkeypatch, tmp_path):
    v2_root = tmp_path / "projects_v2"
    doc_dir = _make_doc(v2_root, "DOC-MOVE", "OV")
    (doc_dir / "versions" / "v001" / "01_input" / "marker.txt").write_text("m", encoding="utf-8")
    monkeypatch.setenv("AUDIT_PROJECTS_V2_DIR", str(v2_root))

    from backend.app.services.common import project_service

    assert project_service._move_v2_document_discipline("DOC-MOVE", "SS") is True

    old = v2_root / "objects" / "OBJ" / "disciplines" / "OV" / "documents" / "DOC-MOVE"
    new = v2_root / "objects" / "OBJ" / "disciplines" / "SS" / "documents" / "DOC-MOVE"
    assert not old.exists()
    assert new.is_dir()
    assert (new / "versions" / "v001" / "01_input" / "marker.txt").is_file()
    assert json.loads((new / "document.json").read_text(encoding="utf-8"))["discipline"] == "SS"
    # section записан в project_info перенесённого документа
    assert _section_in(new) == {"SS"}


def test_noop_same_discipline_still_writes_section_and_returns_true(monkeypatch, tmp_path):
    v2_root = tmp_path / "projects_v2"
    doc_dir = _make_doc(v2_root, "DOC-SAME", "SS")
    monkeypatch.setenv("AUDIT_PROJECTS_V2_DIR", str(v2_root))

    from backend.app.services.common import project_service

    assert project_service._move_v2_document_discipline("DOC-SAME", "SS") is True
    assert doc_dir.is_dir()  # без перемещения
    assert _section_in(doc_dir) == {"SS"}


def test_conflict_in_target_raises_and_keeps_source(monkeypatch, tmp_path):
    v2_root = tmp_path / "projects_v2"
    _make_doc(v2_root, "DOC-CONF", "OV")
    _make_doc(v2_root, "DOC-CONF", "SS")  # одноимённый уже есть в целевой дисциплине
    monkeypatch.setenv("AUDIT_PROJECTS_V2_DIR", str(v2_root))

    from backend.app.services.common import project_service

    with pytest.raises(ValueError):
        project_service._move_v2_document_discipline("DOC-CONF", "SS")
    assert (
        v2_root / "objects" / "OBJ" / "disciplines" / "OV" / "documents" / "DOC-CONF"
    ).is_dir()


def test_returns_false_when_document_not_in_v2(monkeypatch, tmp_path):
    v2_root = tmp_path / "projects_v2"
    (v2_root / "objects").mkdir(parents=True)
    monkeypatch.setenv("AUDIT_PROJECTS_V2_DIR", str(v2_root))

    from backend.app.services.common import project_service

    assert project_service._move_v2_document_discipline("NOPE", "SS") is False


def test_empty_section_returns_false(monkeypatch, tmp_path):
    v2_root = tmp_path / "projects_v2"
    doc_dir = _make_doc(v2_root, "DOC-EMPTY", "OV")
    monkeypatch.setenv("AUDIT_PROJECTS_V2_DIR", str(v2_root))

    from backend.app.services.common import project_service

    assert project_service._move_v2_document_discipline("DOC-EMPTY", "") is False
    assert doc_dir.is_dir()


def test_move_does_not_create_duplicate_in_old_discipline(monkeypatch, tmp_path):
    """Регрессия ОВ1.1-ПА: после переноса в старой дисциплине НЕ должно остаться
    ни оригинала, ни пустого дубля."""
    v2_root = tmp_path / "projects_v2"
    _make_doc(v2_root, "DOC-DUP", "OV")
    monkeypatch.setenv("AUDIT_PROJECTS_V2_DIR", str(v2_root))

    from backend.app.services.common import project_service

    project_service._move_v2_document_discipline("DOC-DUP", "VK")

    ov_docs = list((v2_root / "objects" / "OBJ" / "disciplines" / "OV" / "documents").glob("*")) \
        if (v2_root / "objects" / "OBJ" / "disciplines" / "OV" / "documents").exists() else []
    vk_docs = list((v2_root / "objects" / "OBJ" / "disciplines" / "VK" / "documents").glob("*"))
    assert ov_docs == []          # старая дисциплина пуста — нет дубля
    assert [d.name for d in vk_docs] == ["DOC-DUP"]



def _read_doc_json(doc_dir: Path) -> dict:
    return json.loads((doc_dir / "document.json").read_text(encoding="utf-8"))


def _write_doc_json(doc_dir: Path, data: dict) -> None:
    (doc_dir / "document.json").write_text(
        json.dumps(data, ensure_ascii=False),
        encoding="utf-8",
    )


def _set_legacy_project_path(doc_dir: Path, legacy_path: Path) -> None:
    data = _read_doc_json(doc_dir)
    data["legacy_project_path"] = str(legacy_path)
    _write_doc_json(doc_dir, data)


def _enable_v2_primary(monkeypatch, v2_root: Path) -> None:
    monkeypatch.setenv("AUDIT_PROJECTS_V2_DIR", str(v2_root))
    monkeypatch.setenv("AUDIT_STORAGE_BACKEND", "projects_v2")
    monkeypatch.setenv("AUDIT_PROJECTS_V2_WRITE_MODE", "projects_v2_primary")
    monkeypatch.setenv("AUDIT_PROJECTS_V2_READ_DEFAULT_ENABLED", "true")


def _legacy_project(root: Path, object_name: str, disc: str, code: str) -> Path:
    path = root / object_name / disc / code
    path.mkdir(parents=True, exist_ok=True)
    (path / "project_info.json").write_text(
        json.dumps({"project_id": code, "section": disc}, ensure_ascii=False),
        encoding="utf-8",
    )
    return path


def test_v2_primary_section_move_moves_plain_legacy_folder(monkeypatch, tmp_path):
    v2_root = tmp_path / "projects_v2"
    projects_root = tmp_path / "projects"
    doc_dir = _make_doc(v2_root, "DOC-LEGACY", "OV")
    legacy = _legacy_project(projects_root, "OBJ", "OV", "DOC-LEGACY")
    (legacy / "source.pdf").write_text("pdf", encoding="utf-8")
    _set_legacy_project_path(doc_dir, legacy)
    _enable_v2_primary(monkeypatch, v2_root)

    from backend.app.services.common import project_service

    monkeypatch.setattr(project_service, "_get_projects_dir", lambda: projects_root / "OBJ")

    assert project_service._move_v2_document_discipline("DOC-LEGACY", "SS") is True

    moved_doc = v2_root / "objects" / "OBJ" / "disciplines" / "SS" / "documents" / "DOC-LEGACY"
    moved_legacy = projects_root / "OBJ" / "SS" / "DOC-LEGACY"
    assert moved_doc.is_dir()
    assert moved_legacy.is_dir()
    assert not legacy.exists()
    assert (moved_legacy / "source.pdf").read_text(encoding="utf-8") == "pdf"
    assert _read_doc_json(moved_doc)["legacy_project_path"] == str(moved_legacy)


def test_v2_primary_section_move_moves_container_legacy_unit(monkeypatch, tmp_path):
    v2_root = tmp_path / "projects_v2"
    projects_root = tmp_path / "projects"
    doc_dir = _make_doc(v2_root, "DOC-CONT", "OV")
    container = projects_root / "OBJ" / "OV" / "DOC-CONT(main)"
    inner = container / "DOC-CONT"
    inner.mkdir(parents=True)
    (inner / "project_info.json").write_text(
        json.dumps({"project_id": "DOC-CONT", "section": "OV"}, ensure_ascii=False),
        encoding="utf-8",
    )
    (container / "version_group.json").write_text("{}", encoding="utf-8")
    _set_legacy_project_path(doc_dir, inner)
    _enable_v2_primary(monkeypatch, v2_root)

    from backend.app.services.common import project_service

    monkeypatch.setattr(project_service, "_get_projects_dir", lambda: projects_root / "OBJ")

    assert project_service._move_v2_document_discipline("DOC-CONT", "SS") is True

    moved_doc = v2_root / "objects" / "OBJ" / "disciplines" / "SS" / "documents" / "DOC-CONT"
    moved_container = projects_root / "OBJ" / "SS" / "DOC-CONT(main)"
    assert moved_container.is_dir()
    assert (moved_container / "DOC-CONT" / "project_info.json").is_file()
    assert (moved_container / "version_group.json").is_file()
    assert not container.exists()
    assert _read_doc_json(moved_doc)["legacy_project_path"] == str(moved_container / "DOC-CONT")


def test_v2_primary_section_move_legacy_missing_is_noop(monkeypatch, tmp_path):
    v2_root = tmp_path / "projects_v2"
    projects_root = tmp_path / "projects"
    _make_doc(v2_root, "DOC-V2-ONLY", "OV")
    _enable_v2_primary(monkeypatch, v2_root)

    from backend.app.services.common import project_service

    monkeypatch.setattr(project_service, "_get_projects_dir", lambda: projects_root / "OBJ")

    assert project_service._move_v2_document_discipline("DOC-V2-ONLY", "SS") is True
    assert (
        v2_root / "objects" / "OBJ" / "disciplines" / "SS" / "documents" / "DOC-V2-ONLY"
    ).is_dir()


def test_v2_primary_section_move_legacy_conflict_raises_before_v2_move(monkeypatch, tmp_path):
    v2_root = tmp_path / "projects_v2"
    projects_root = tmp_path / "projects"
    doc_dir = _make_doc(v2_root, "DOC-CONF-LEGACY", "OV")
    legacy = _legacy_project(projects_root, "OBJ", "OV", "DOC-CONF-LEGACY")
    target_conflict = _legacy_project(projects_root, "OBJ", "SS", "DOC-CONF-LEGACY")
    _set_legacy_project_path(doc_dir, legacy)
    _enable_v2_primary(monkeypatch, v2_root)

    from backend.app.services.common import project_service

    monkeypatch.setattr(project_service, "_get_projects_dir", lambda: projects_root / "OBJ")

    with pytest.raises(ValueError):
        project_service._move_v2_document_discipline("DOC-CONF-LEGACY", "SS")

    assert legacy.is_dir()
    assert target_conflict.is_dir()
    assert doc_dir.is_dir()
    assert not (
        v2_root / "objects" / "OBJ" / "disciplines" / "SS" / "documents" / "DOC-CONF-LEGACY"
    ).exists()


def test_v2_primary_section_move_legacy_already_in_target_is_noop(monkeypatch, tmp_path):
    v2_root = tmp_path / "projects_v2"
    projects_root = tmp_path / "projects"
    doc_dir = _make_doc(v2_root, "DOC-SAME-LEGACY", "SS")
    legacy = _legacy_project(projects_root, "OBJ", "SS", "DOC-SAME-LEGACY")
    _set_legacy_project_path(doc_dir, legacy)
    _enable_v2_primary(monkeypatch, v2_root)

    from backend.app.services.common import project_service

    monkeypatch.setattr(project_service, "_get_projects_dir", lambda: projects_root / "OBJ")

    assert project_service._move_v2_document_discipline("DOC-SAME-LEGACY", "SS") is True
    assert legacy.is_dir()
    assert _read_doc_json(doc_dir)["legacy_project_path"] == str(legacy)


def test_set_project_section_legacy_off_does_not_move_legacy_or_v2(monkeypatch, tmp_path):
    v2_root = tmp_path / "projects_v2"
    projects_root = tmp_path / "projects"
    doc_dir = _make_doc(v2_root, "DOC-OFF", "OV")
    legacy = _legacy_project(projects_root, "OBJ", "OV", "DOC-OFF")
    _set_legacy_project_path(doc_dir, legacy)
    monkeypatch.setenv("AUDIT_PROJECTS_V2_DIR", str(v2_root))
    monkeypatch.setenv("AUDIT_STORAGE_BACKEND", "legacy")
    monkeypatch.setenv("AUDIT_PROJECTS_V2_WRITE_MODE", "dual_write_shadow")
    monkeypatch.setenv("AUDIT_PROJECTS_V2_READ_DEFAULT_ENABLED", "false")

    from backend.app.services.common import project_service

    monkeypatch.setattr(project_service, "_get_projects_dir", lambda: projects_root / "OBJ")

    project_service.set_project_section("DOC-OFF", "SS")

    assert legacy.is_dir()
    assert not (projects_root / "OBJ" / "SS" / "DOC-OFF").exists()
    assert doc_dir.is_dir()
    info = json.loads((legacy / "project_info.json").read_text(encoding="utf-8"))
    assert info["section"] == "SS"


def test_scan_v2_legacy_discipline_drift_reports_mismatch(tmp_path):
    import importlib.util

    script_path = Path("scripts/projects_v2/scan_v2_legacy_discipline_drift.py")
    spec = importlib.util.spec_from_file_location("scan_v2_legacy_discipline_drift", script_path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)

    v2_root = tmp_path / "projects_v2"
    projects_root = tmp_path / "projects"
    doc_dir = _make_doc(v2_root, "DOC-DRIFT", "SS")
    legacy = _legacy_project(projects_root, "OBJ", "OV", "DOC-DRIFT")
    _set_legacy_project_path(doc_dir, legacy)

    report = module.scan(v2_root, projects_root)

    assert report["documents_scanned"] == 1
    assert report["drift_count"] == 1
    row = report["drift_documents"][0]
    assert row["document_code"] == "DOC-DRIFT"
    assert row["v2_discipline"] == "SS"
    assert row["legacy_discipline"] == "OV"
    assert set(row["reasons"]) == {"v2_folder_vs_legacy", "document_json_vs_legacy"}
