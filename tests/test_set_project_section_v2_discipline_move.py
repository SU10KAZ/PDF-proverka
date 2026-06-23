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
