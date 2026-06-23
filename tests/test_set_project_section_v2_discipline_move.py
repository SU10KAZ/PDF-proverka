"""Тесты v2-aware смены раздела: set_project_section физически переносит
документ между папками дисциплин под projects_v2_primary.

Регрессия cutover: read_canary группирует проекты по физической папке
``disciplines/<code>/``, а старый set_project_section писал только поле
``section`` в project_info → смена раздела в UI «не срабатывала».
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest


def _make_doc(v2_root: Path, code: str, disc: str) -> Path:
    doc_dir = v2_root / "objects" / "OBJ" / "disciplines" / disc / "documents" / code
    (doc_dir / "versions" / "v001" / "01_input").mkdir(parents=True, exist_ok=True)
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
    return doc_dir


def test_move_folder_and_update_document_json(monkeypatch, tmp_path):
    v2_root = tmp_path / "projects_v2"
    doc_dir = _make_doc(v2_root, "DOC-MOVE", "OV")
    marker = doc_dir / "versions" / "v001" / "01_input" / "marker.txt"
    marker.write_text("m", encoding="utf-8")
    monkeypatch.setenv("AUDIT_PROJECTS_V2_DIR", str(v2_root))

    from backend.app.services.common import project_service

    project_service._move_v2_document_discipline("DOC-MOVE", "SS")

    old = v2_root / "objects" / "OBJ" / "disciplines" / "OV" / "documents" / "DOC-MOVE"
    new = v2_root / "objects" / "OBJ" / "disciplines" / "SS" / "documents" / "DOC-MOVE"
    assert not old.exists()
    assert new.is_dir()
    # весь документ переехал целиком (не только document.json)
    assert (new / "versions" / "v001" / "01_input" / "marker.txt").is_file()
    dj = json.loads((new / "document.json").read_text(encoding="utf-8"))
    assert dj["discipline"] == "SS"


def test_noop_when_already_in_target_discipline(monkeypatch, tmp_path):
    v2_root = tmp_path / "projects_v2"
    doc_dir = _make_doc(v2_root, "DOC-SAME", "SS")
    monkeypatch.setenv("AUDIT_PROJECTS_V2_DIR", str(v2_root))

    from backend.app.services.common import project_service

    project_service._move_v2_document_discipline("DOC-SAME", "SS")
    assert doc_dir.is_dir()  # без изменений


def test_conflict_in_target_raises_and_keeps_source(monkeypatch, tmp_path):
    v2_root = tmp_path / "projects_v2"
    _make_doc(v2_root, "DOC-CONF", "OV")
    _make_doc(v2_root, "DOC-CONF", "SS")  # одноимённый уже есть в целевой дисциплине
    monkeypatch.setenv("AUDIT_PROJECTS_V2_DIR", str(v2_root))

    from backend.app.services.common import project_service

    with pytest.raises(ValueError):
        project_service._move_v2_document_discipline("DOC-CONF", "SS")
    # источник не тронут
    assert (
        v2_root / "objects" / "OBJ" / "disciplines" / "OV" / "documents" / "DOC-CONF"
    ).is_dir()


def test_noop_when_document_not_in_v2(monkeypatch, tmp_path):
    v2_root = tmp_path / "projects_v2"
    (v2_root / "objects").mkdir(parents=True)
    monkeypatch.setenv("AUDIT_PROJECTS_V2_DIR", str(v2_root))

    from backend.app.services.common import project_service

    # документа нет в v2 → no-op, без исключений
    project_service._move_v2_document_discipline("NOPE", "SS")


def test_empty_section_is_noop(monkeypatch, tmp_path):
    v2_root = tmp_path / "projects_v2"
    doc_dir = _make_doc(v2_root, "DOC-EMPTY", "OV")
    monkeypatch.setenv("AUDIT_PROJECTS_V2_DIR", str(v2_root))

    from backend.app.services.common import project_service

    project_service._move_v2_document_discipline("DOC-EMPTY", "")
    assert doc_dir.is_dir()  # пустой раздел — ничего не двигаем
