"""A queue bound to object A must still resolve documents of object B (20.10.2026)."""
import json

import pytest

from backend.app.pipeline.manager import PipelineManager
from backend.app.services.common.project_service import bind_object, unbind_object


def _doc(root, folder, object_id, code):
    obj = root / "objects" / folder
    obj.mkdir(parents=True, exist_ok=True)
    (obj / "object.json").write_text(json.dumps({"object_id": object_id}), encoding="utf-8")
    doc = obj / "disciplines" / "OV" / "documents" / code
    (doc / "versions" / "v001").mkdir(parents=True)
    (doc / "document.json").write_text(json.dumps({
        "document_code": code, "object_id": object_id,
        "versions": [{"version_id": "v001"}], "current_version": "v001",
    }), encoding="utf-8")


@pytest.fixture
def v2_root(monkeypatch, tmp_path):
    root = tmp_path / "projects_v2"
    _doc(root, "214_Alia", "obj-214", "13АВ-РД-ОВ1.1-К3")
    _doc(root, "314_Sobytie", "obj-314", "ПД-00260560-ОВ2-1")
    monkeypatch.setenv("AUDIT_PROJECTS_V2_WRITE_MODE", "projects_v2_primary")
    monkeypatch.setenv("AUDIT_PROJECTS_V2_DIR", str(root))
    return root


def _resolve_bound_to(object_id, project_id):
    token = bind_object(object_id)
    try:
        return PipelineManager._resolve_object_id_for_project(None, project_id, "v001")
    finally:
        unbind_object(token)


def test_document_of_other_object_resolves_inside_bound_queue(v2_root):
    assert _resolve_bound_to("obj-214", "ПД-00260560-ОВ2-1") == "obj-314"


def test_document_of_bound_object_still_resolves(v2_root):
    assert _resolve_bound_to("obj-214", "13АВ-РД-ОВ1.1-К3") == "obj-214"


def test_binding_is_restored_after_cross_object_lookup(v2_root):
    from backend.app.services.common import project_service

    token = bind_object("obj-214")
    try:
        PipelineManager._resolve_object_id_for_project(None, "ПД-00260560-ОВ2-1", "v001")
        assert project_service._get_bound_object_id() == "obj-214"
    finally:
        unbind_object(token)


def test_code_in_several_objects_keeps_previous_fallback(v2_root, monkeypatch):
    import backend.app.pipeline.manager as manager

    _doc(v2_root, "256_Spartak", "obj-256", "ПД-00260560-ОВ2-1")
    monkeypatch.setattr(manager, "_current_object_id_or_none", lambda: "current")
    assert _resolve_bound_to("obj-214", "ПД-00260560-ОВ2-1") == "current"
