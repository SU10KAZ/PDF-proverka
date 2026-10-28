"""Раздел IRD (исходно-разрешительная документация): не аудируется, читается анализом."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from backend.app.services.common import discipline_identity, discipline_service, ird_service
from backend.app.pipeline.stages.prepare import task_builder


# ─── реестр ──────────────────────────────────────────────────────────────────

def test_ird_registered_last_and_not_auditable():
    discipline_identity.invalidate_cache()
    discs = discipline_service.get_supported_disciplines()
    assert discs[-1]["code"] == "IRD"
    assert discs[-1]["auditable"] is False
    assert all(d["auditable"] for d in discs[:-1])


@pytest.mark.parametrize("raw, expected", [
    ("IRD", False), ("ИРД", False), ("ird", False),
    ("EOM", True), ("ЭОМ", True), ("", True), (None, True), ("НЕИЗВЕСТНО", True),
])
def test_is_auditable(raw, expected):
    discipline_identity.invalidate_cache()
    assert discipline_identity.is_auditable(raw) is expected


def test_non_auditable_codes():
    discipline_identity.invalidate_cache()
    assert discipline_identity.non_auditable_codes() == {"IRD"}


# ─── тип документа ───────────────────────────────────────────────────────────

@pytest.mark.parametrize("name, expected", [
    ("Приложение 1.7 к ТЗ. Перечень рекомендуемых производителей оборудования", "vendor_list"),
    ("вендор лист", "vendor_list"),
    ("Vendor list rev2", "vendor_list"),
    ("ТЗ на проектирование", "tz"),
    ("Техническое задание на проектирование МКД", "tz"),
    ("ТУ на подключение к сетям", "tu"),
    ("Технические условия МОЭСК", "tu"),
    ("ГПЗУ", "other"),
    ("СТЗ-1 разрез", "other"),   # «ТЗ» внутри слова — не задание
])
def test_detect_ird_type(name, expected):
    assert ird_service.detect_ird_type(name) == expected


# ─── поиск документов ИРД объекта ────────────────────────────────────────────

def _make_doc(root: Path, folder: str, disc: str, code: str, *, name: str,
              md: str = "| 1 | x |", ird_type: str | None = None) -> Path:
    doc_dir = root / "objects" / folder / "disciplines" / disc / "documents" / code
    vdir = doc_dir / "versions" / "v001"
    (vdir / "01_input").mkdir(parents=True)
    info = {"name": name, "section": disc}
    if ird_type:
        info["ird_type"] = ird_type
    (vdir / "01_input" / "project_info.json").write_text(json.dumps(info, ensure_ascii=False), encoding="utf-8")
    (vdir / "01_input" / f"{code}_results.md").write_text(md, encoding="utf-8")
    (doc_dir / "document.json").write_text(json.dumps({
        "document_code": code, "object_id": "obj1",
        "versions": [{"version_id": "v001"}], "current_version": "v001",
    }), encoding="utf-8")
    return vdir


@pytest.fixture()
def v2_root(tmp_path, monkeypatch):
    root = tmp_path / "projects_v2"
    (root / "objects" / "001_Obj").mkdir(parents=True)
    (root / "objects" / "001_Obj" / "object.json").write_text(
        json.dumps({"object_id": "obj1"}), encoding="utf-8")
    monkeypatch.setenv("AUDIT_PROJECTS_V2_DIR", str(root))
    return root


def test_list_ird_documents(v2_root):
    _make_doc(v2_root, "001_Obj", "IRD", "VL", name="Вендор-лист")
    _make_doc(v2_root, "001_Obj", "IRD", "TZ1", name="Документ 1", ird_type="tz")
    _make_doc(v2_root, "001_Obj", "EOM", "ЭОМ-1", name="ЭОМ")
    docs = {d["document_code"]: d for d in ird_service.list_ird_documents("obj1")}
    assert set(docs) == {"VL", "TZ1"}
    assert docs["VL"]["ird_type"] == "vendor_list"
    assert docs["TZ1"]["ird_type"] == "tz"          # явный тип важнее названия
    assert docs["VL"]["md_path"].endswith("VL_results.md")


def test_list_ird_documents_unknown_object(v2_root):
    assert ird_service.list_ird_documents("нет-такого") == []


def test_catalog_lists_paths_not_text(v2_root):
    _make_doc(v2_root, "001_Obj", "IRD", "TZ1", name="ТЗ на проектирование",
              md="СЕКРЕТНЫЙ_ТЕКСТ_ТЗ")
    catalog = ird_service.render_ird_catalog("obj1")
    assert "Техническое задание" in catalog
    assert "TZ1_results.md" in catalog
    assert "СЕКРЕТНЫЙ_ТЕКСТ_ТЗ" not in catalog
    assert ird_service.render_ird_catalog("нет-такого") == ""


def test_vendor_list_taken_from_ird_first(v2_root, monkeypatch):
    vdir = _make_doc(v2_root, "001_Obj", "IRD", "VL", name="Вендор-лист",
                     md="| 8. ФАСАДЫ | | |\n| 8.22 | Утеплитель | ROCKWOOL |")
    monkeypatch.setattr(ird_service, "_object_folder", lambda adapter, oid: "001_Obj")
    path = task_builder._resolve_vendor_list_path()
    assert path == vdir / "01_input" / "VL_results.md"
    text = task_builder._load_vendor_list_for_discipline("AR")
    assert "ROCKWOOL" in text


def test_append_ird_catalog(monkeypatch):
    monkeypatch.setattr(ird_service, "render_ird_catalog", lambda object_id=None: "## ИРД")
    assert task_builder._append_ird_catalog("задание\n") == "задание\n\n## ИРД"
    monkeypatch.setattr(ird_service, "render_ird_catalog", lambda object_id=None: "")
    assert task_builder._append_ird_catalog("задание") == "задание"

    def boom(object_id=None):
        raise RuntimeError
    monkeypatch.setattr(ird_service, "render_ird_catalog", boom)
    assert task_builder._append_ird_catalog("задание") == "задание"


# ─── запрет аудита ───────────────────────────────────────────────────────────

def test_project_auditable_by_section(monkeypatch):
    from backend.app.services.common import project_service
    discipline_identity.invalidate_cache()
    monkeypatch.setattr(project_service, "get_project_info", lambda pid, **kw: {"section": "IRD"})
    assert project_service.is_project_auditable("x") is False
    monkeypatch.setattr(project_service, "get_project_info", lambda pid, **kw: {"section": "EOM"})
    assert project_service.is_project_auditable("x") is True

    def boom(pid, **kw):
        raise OSError
    monkeypatch.setattr(project_service, "get_project_info", boom)
    assert project_service.is_project_auditable("x") is True


def test_check_project_rejects_ird(monkeypatch):
    from fastapi import HTTPException
    from backend.app.api.routers import audit
    monkeypatch.setattr(audit.project_service, "get_project_status", lambda pid, **kw: object())
    monkeypatch.setattr(audit.project_service, "is_project_auditable", lambda pid: False)
    with pytest.raises(HTTPException) as exc:
        audit._check_project("VL")
    assert exc.value.status_code == 409
    assert "ИРД" in exc.value.detail


@pytest.mark.asyncio
async def test_enqueue_single_rejects_ird(monkeypatch):
    from backend.app.pipeline.manager import pipeline_manager
    from backend.app.services.common import project_service
    monkeypatch.setattr(project_service, "is_project_auditable", lambda pid: False)
    with pytest.raises(RuntimeError, match="ИРД"):
        await pipeline_manager._enqueue_single("VL", "full")
