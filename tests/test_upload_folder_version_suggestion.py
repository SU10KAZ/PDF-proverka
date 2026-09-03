"""Авто-связывание загружаемой папки с существующим проектом как ВЕРСИИ.

Покрывает два дефекта, из-за которых «..._V2» приезжал отдельным проектом:
  - имена-основания искались только в legacy-папке объекта, а в
    projects_v2-primary она пустая (или её нет вовсе);
  - нормализация снимала только « V2»/«_в2», но не «_V2»/«-V2».

Run:
    python -m pytest tests/test_upload_folder_version_suggestion.py -v
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from backend.app.services.common import object_service, project_service  # noqa: E402

_PDF = b"%PDF-1.4\n%alpha\n%%EOF\n"


def _v2_doc(v2_root: Path, code: str, discipline: str = "AR",
            object_id: str = "obj-1", versions: int = 1,
            object_folder: str = "OBJ") -> None:
    doc = (v2_root / "objects" / object_folder / "disciplines" / discipline
           / "documents" / code)
    doc.mkdir(parents=True, exist_ok=True)
    (doc / "document.json").write_text(json.dumps({
        "document_code": code, "object_id": object_id,
        "versions": [{"version_id": f"v{i + 1:03d}"} for i in range(versions)],
        "current_version_id": f"v{versions:03d}",
    }), encoding="utf-8")


@pytest.fixture
def env(tmp_path, monkeypatch):
    """Объект БЕЗ legacy-папки проектов (как в projects_v2-primary) + v2-корень."""
    projects_dir = tmp_path / "projects" / "OBJ"
    projects_dir.mkdir(parents=True)
    obj = {"id": "obj-1", "name": "Объект 1", "projects_dir": str(projects_dir)}
    monkeypatch.setattr(object_service, "get_object_by_id",
                        lambda oid: obj if oid == "obj-1" else None)
    monkeypatch.setattr(object_service, "get_projects_dir_for",
                        lambda oid: projects_dir if oid == "obj-1" else None)
    v2_root = tmp_path / "projects_v2"
    (v2_root / "objects" / "OBJ").mkdir(parents=True)
    (v2_root / "objects" / "OBJ" / "object.json").write_text(
        json.dumps({"object_id": "obj-1", "display_name": "Объект 1"}), encoding="utf-8")
    monkeypatch.setenv("AUDIT_PROJECTS_V2_DIR", str(v2_root))
    return v2_root


def _pre(name, discipline="AR", files=None):
    return project_service.precheck_uploaded_project_folder(
        object_id="obj-1", discipline=discipline, project_name=name,
        files=files if files is not None else [(f"{name}.pdf", _PDF)],
        folder_name=name)


# ─── нормализация имени ──────────────────────────────────────────────────────


@pytest.mark.parametrize("name,expected", [
    ("СТ26_01-14-АР3-3-РД_V2", "ст26_01-14-ар3-3-рд"),
    ("СТ26_01-14-АР3-3-РД-V2", "ст26_01-14-ар3-3-рд"),
    ("СТ26_01-14-АР3-3-РД V2", "ст26_01-14-ар3-3-рд"),
    ("СТ26_01-14-АР3-3-РД", "ст26_01-14-ар3-3-рд"),
    ("проект_в2", "проект"),
    ("проект Изм.2", "проект"),
])
def test_normalize_strips_version_suffix(name, expected):
    assert project_service._normalize_name_for_similarity(name) == expected


def test_normalize_keeps_section_letter():
    """«...-В2» — литера секции, а не версия: склеивать разные проекты нельзя."""
    a = project_service._normalize_name_for_similarity("СТ26_01-14-АР1-В1")
    b = project_service._normalize_name_for_similarity("СТ26_01-14-АР1-В2")
    assert a != b


@pytest.mark.parametrize("name,expected", [
    ("X_V2", 2), ("X-V3", 3), ("X V10", 10), ("X", 0), ("X-В2", 0),
])
def test_version_suffix_number(name, expected):
    assert project_service._version_suffix_number(name) == expected


# ─── предложение основания ───────────────────────────────────────────────────


def test_v2_only_project_is_offered_as_version_base(env):
    """Основание живёт только в projects_v2 — раньше не находилось вовсе."""
    _v2_doc(env, "СТ26_01-14-АР3-3-РД_V1")
    v = _pre("СТ26_01-14-АР3-3-РД_V2")
    assert v["suggested_target_project"] == "AR/СТ26_01-14-АР3-3-РД_V1"
    assert v["suggested_reason"] == "version_suffix"
    assert v["suggested_version_label"] == "V2"
    assert v["status"] == "warning"
    assert any(w["code"] == "similar_name" for w in v["warnings"])


def test_version_label_counts_existing_versions(env):
    _v2_doc(env, "AAA_V1", versions=2)
    v = _pre("AAA_V2")
    assert v["suggested_version_label"] == "V3"


def test_base_is_nearest_lower_version(env):
    _v2_doc(env, "BBB_V1")
    _v2_doc(env, "BBB_V2")
    v = _pre("BBB_V3")
    assert v["suggested_target_project"] == "AR/BBB_V2"


def test_exact_name_duplicate_suggests_base_without_auto_switch(env):
    _v2_doc(env, "CCC_V1")
    v = _pre("CCC_V1")
    assert v["status"] == "duplicate"
    assert v["suggested_target_project"] == "AR/CCC_V1"
    assert v["suggested_reason"] == "same_name"
    # точный дубль имени не выдаётся за «похожий проект»
    assert not any(w["code"] == "similar_name" for w in v["warnings"])


def test_other_discipline_is_not_a_base(env):
    _v2_doc(env, "DDD_V1", discipline="EOM")
    v = _pre("DDD_V2", discipline="AR")
    assert v["suggested_target_project"] is None
    assert v["status"] == "ready"


def test_other_object_is_not_a_base(env):
    (env / "objects" / "OBJ2").mkdir(parents=True)
    (env / "objects" / "OBJ2" / "object.json").write_text(
        json.dumps({"object_id": "obj-2", "display_name": "Объект 2"}), encoding="utf-8")
    _v2_doc(env, "EEE_V1", object_id="obj-2", object_folder="OBJ2")
    v = _pre("EEE_V2")
    assert v["suggested_target_project"] is None


def test_unrelated_name_has_no_base(env):
    _v2_doc(env, "FFF_V1")
    v = _pre("GGG_V2")
    assert v["suggested_target_project"] is None
    assert v["status"] == "ready"
