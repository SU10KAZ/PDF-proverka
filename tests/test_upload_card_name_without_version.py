"""Имя карточки без суффикса версии (признак объекта `card_name_without_version`).

Андрей Иванович, 28.09.2026: в Алии имя карточки хранится без «_V<N>», при
загрузке «X_V1» карточка называется «X». Другие объекты — как прежде.

Run:
    python -m pytest tests/test_upload_card_name_without_version.py -v
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
import backend.app.services.storage.storage_write_facade as swf  # noqa: E402

_PDF = b"%PDF-1.4\n%fake\n%%EOF\n"
_MD = b"## STR 1\n\n**List:** 1\n"


@pytest.mark.parametrize("raw,expected", [
    ("13АВ-РД-АР1.2-К4_V5", "13АВ-РД-АР1.2-К4"),
    ("13АВ-РД-ВК2-К1 V1", "13АВ-РД-ВК2-К1"),
    ("13АВ-РД-АК-К4 (Книга 1)_V3", "13АВ-РД-АК-К4 (Книга 1)"),
    ("М077-02508-08-НФС-СМК-14.4._V1", "М077-02508-08-НФС-СМК-14.4"),
    ("13АВ-РД-ГП2 v2", "13АВ-РД-ГП2"),
    ("13АВ-РД-КМ-К1", "13АВ-РД-КМ-К1"),
    ("133-23-ГК-ОВ3", "133-23-ГК-ОВ3"),   # кириллическая «В» — не версия
    ("Mockup 2", "Mockup 2"),
])
def test_strip_card_version_suffix(raw, expected):
    assert project_service.strip_card_version_suffix(raw) == expected


def _env(tmp_path, monkeypatch, flag):
    projects_dir = tmp_path / "projects" / "OBJ"
    projects_dir.mkdir(parents=True)
    obj = {"id": "obj-1", "name": "Объект 1", "projects_dir": str(projects_dir)}
    if flag:
        obj["card_name_without_version"] = True
    monkeypatch.setattr(object_service, "get_object_by_id",
                        lambda oid: obj if oid == "obj-1" else None)
    monkeypatch.setattr(object_service, "get_projects_dir_for",
                        lambda oid: projects_dir if oid == "obj-1" else None)
    monkeypatch.setattr(project_service, "_v2_document_exists", lambda *a, **k: False)
    monkeypatch.setattr(swf, "shadow_mirror_project_path_safe", lambda p: None)
    return projects_dir


def _files():
    return [("13АВ-РД-АР1.2-К4_V1.pdf", _PDF), ("13АВ-РД-АР1.2-К4_V1_results.md", _MD)]


def test_object_with_flag_strips_suffix(tmp_path, monkeypatch):
    projects_dir = _env(tmp_path, monkeypatch, flag=True)
    res = project_service.save_uploaded_project_folder(
        object_id="obj-1", discipline="AR", project_name="13АВ-РД-АР1.2-К4_V1", files=_files())
    assert res["project_id"] == "AR/13АВ-РД-АР1.2-К4"
    assert (projects_dir / "AR" / "13АВ-РД-АР1.2-К4").is_dir()


def test_object_without_flag_keeps_name(tmp_path, monkeypatch):
    projects_dir = _env(tmp_path, monkeypatch, flag=False)
    res = project_service.save_uploaded_project_folder(
        object_id="obj-1", discipline="AR", project_name="13АВ-РД-АР1.2-К4_V1", files=_files())
    assert res["project_id"] == "AR/13АВ-РД-АР1.2-К4_V1"


def test_header_display_name_logic_in_frontend():
    js = (_ROOT / "frontend" / "static" / "js" / "app.js").read_text(encoding="utf-8")
    html = (_ROOT / "frontend" / "index.html").read_text(encoding="utf-8")
    assert "const activeVersionDisplayName = computed(" in js
    assert "activeVersionDisplayName," in js
    assert "{{ activeVersionDisplayName }}" in html
