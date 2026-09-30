"""Группы «один документ слева → несколько справа» и их сборки."""
from __future__ import annotations

import json
import shutil
from pathlib import Path

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.app.api.routers import stage_comparison as router_mod
from backend.app.services.project_assemblies import service as assemblies
from backend.app.services.stage_comparison import document_groups, store
from tests.test_project_assemblies import _source


def _stage_document(object_dir: Path, stage: str, code: str, version: str = "v001", pages: int = 1) -> None:
    """Документ стадии сравнения в формате новой выгрузки OCR."""
    scratch = object_dir.parent / f"_scratch_{stage}_{code}_{version}"
    _source(scratch, code, version, pages=pages)
    source_doc = scratch / "disciplines" / "OV" / "documents" / code
    target_doc = object_dir / "comparison" / stage / "documents" / code
    target_doc.parent.mkdir(parents=True, exist_ok=True)
    if target_doc.exists():
        shutil.copytree(source_doc / "versions" / version, target_doc / "versions" / version)
        meta = json.loads((target_doc / "document.json").read_text(encoding="utf-8"))
        meta["current_version"] = version
        (target_doc / "document.json").write_text(json.dumps(meta), encoding="utf-8")
    else:
        shutil.copytree(source_doc, target_doc)
    shutil.rmtree(scratch)


@pytest.fixture
def groups_env(tmp_path, monkeypatch):
    object_dir = tmp_path / "projects_v2" / "objects" / "obj-folder"
    comparison = object_dir / "comparison"
    comparison.mkdir(parents=True)
    (object_dir / "object.json").write_text(json.dumps({
        "object_id": "obj-test", "display_name": "Test", "folder_name": "obj-folder",
    }), encoding="utf-8")
    monkeypatch.setenv("PROJECT_ASSEMBLIES_ENABLED", "1")
    monkeypatch.setattr(assemblies, "_comparison_dir", lambda object_id, create=False: comparison)
    monkeypatch.setattr(assemblies, "MIN_FREE_RESERVE", 0)
    # Сборку выполняем в том же процессе: фоновый воркер не видит подмен теста.
    monkeypatch.setattr(assemblies, "launch_version", assemblies.build_version)
    monkeypatch.setattr(document_groups, "_comparison_dir", lambda object_id: comparison)
    _stage_document(object_dir, "stage_1", "P-1")
    _stage_document(object_dir, "stage_1", "P-2")
    for code in ("RD-K1", "RD-K2", "RD-K3"):
        _stage_document(object_dir, "stage_2", code)
    return object_dir


def _attached(object_id: str = "obj-test") -> list[dict]:
    return assemblies.attachments(object_id)["stage_2"]


def test_single_right_document_is_a_plain_pair_without_group(groups_env):
    result = document_groups.add_members("obj-test", "P-1", ["RD-K1"])

    assert result["group"] is None
    assert result["released"] == [{"left_document_code": "P-1", "document_code": "RD-K1"}]
    assert document_groups.list_groups("obj-test")["groups"] == []
    assert _attached() == []


def test_two_right_documents_build_and_attach_one_assembly(groups_env):
    result = document_groups.add_members("obj-test", "P-1", ["RD-K1", "RD-K2"])
    group = result["group"]
    assert [m["document_code"] for m in group["members"]] == ["RD-K1", "RD-K2"]
    assert group["status"] == "READY"

    listed = document_groups.list_groups("obj-test")["groups"][0]
    assert listed["attached_version_id"] == group["assembly_version_id"]
    assert listed["page_count"] == 2
    assert listed["stale"] is False
    assert [(row["assembly_id"], row["version_id"]) for row in _attached()] == [
        (group["assembly_id"], group["assembly_version_id"]),
    ]


def test_adding_member_rebuilds_and_swaps_attached_version(groups_env):
    first = document_groups.add_members("obj-test", "P-1", ["RD-K1", "RD-K2"])["group"]
    document_groups.list_groups("obj-test")

    second = document_groups.add_members("obj-test", "P-1", ["RD-K3"])["group"]
    assert second["assembly_id"] == first["assembly_id"]
    assert second["assembly_version_id"] != first["assembly_version_id"]

    listed = document_groups.list_groups("obj-test")["groups"][0]
    assert listed["page_count"] == 3
    assert [row["version_id"] for row in _attached()] == [second["assembly_version_id"]]


def test_removing_member_down_to_one_dissolves_group(groups_env):
    group = document_groups.add_members("obj-test", "P-1", ["RD-K1", "RD-K2"])["group"]
    document_groups.list_groups("obj-test")

    result = document_groups.remove_member("obj-test", group["group_id"], "RD-K2")

    assert result["group"] is None
    assert result["released"] == [{"left_document_code": "P-1", "document_code": "RD-K1"}]
    assert document_groups.list_groups("obj-test")["groups"] == []
    assert _attached() == []


def test_right_document_belongs_to_one_group_only(groups_env):
    document_groups.add_members("obj-test", "P-1", ["RD-K1", "RD-K2"])

    result = document_groups.add_members("obj-test", "P-2", ["RD-K2", "RD-K3"])

    groups = {g["left_document_code"]: g for g in document_groups.list_groups("obj-test")["groups"]}
    assert set(groups) == {"P-2"}
    assert [m["document_code"] for m in groups["P-2"]["members"]] == ["RD-K2", "RD-K3"]
    assert result["released"] == [{"left_document_code": "P-1", "document_code": "RD-K1"}]


def test_new_member_version_marks_group_stale(groups_env):
    document_groups.add_members("obj-test", "P-1", ["RD-K1", "RD-K2"])
    _stage_document(groups_env, "stage_2", "RD-K2", version="v002", pages=2)

    listed = document_groups.list_groups("obj-test")["groups"][0]
    assert listed["stale"] is True

    rebuilt = document_groups.rebuild("obj-test", listed["group_id"])["group"]
    assert rebuilt["stale"] is False
    assert document_groups.list_groups("obj-test")["groups"][0]["page_count"] == 3


def test_hidden_right_documents_cover_members_and_group_assembly(groups_env):
    group = document_groups.add_members("obj-test", "P-1", ["RD-K1", "RD-K2"])["group"]
    document_groups.list_groups("obj-test")
    stage_2 = groups_env / "comparison" / "stage_2"
    documents = [
        {"pdf_path": "/k1.pdf", "document_code": "RD-K1"},
        {"pdf_path": "/k2.pdf", "document_code": "RD-K2"},
        {"pdf_path": "/k3.pdf", "document_code": "RD-K3"},
        {"pdf_path": "/asm.pdf", "document_code": "x", "assembly_ref": {"assembly_id": group["assembly_id"]}},
        {"pdf_path": "/manual.pdf", "document_code": "y", "assembly_ref": {"assembly_id": "asm_manual"}},
    ]

    assert document_groups.hidden_right_documents(stage_2, documents) == {"/k1.pdf", "/k2.pdf", "/asm.pdf"}
    assert document_groups.grouped_left_codes(groups_env / "comparison" / "stage_1") == {"P-1"}


def test_groups_are_unavailable_when_assemblies_disabled(groups_env, monkeypatch):
    monkeypatch.setenv("PROJECT_ASSEMBLIES_ENABLED", "0")
    with pytest.raises(document_groups.DocumentGroupError) as error:
        document_groups.add_members("obj-test", "P-1", ["RD-K1", "RD-K2"])
    assert error.value.code == "ASSEMBLIES_DISABLED"
    assert document_groups.hidden_right_documents(groups_env / "comparison" / "stage_2", [
        {"pdf_path": "/k1.pdf", "document_code": "RD-K1"},
    ]) == set()


# ── Сессия: одна сторона, скрытые документы, перенос раскладки ─────────────

def _app() -> FastAPI:
    app = FastAPI()
    app.include_router(router_mod.router)
    return app


def _pdf(path: Path, label: str) -> Path:
    import fitz
    document = fitz.open()
    document.new_page(width=200, height=100).insert_text((20, 40), label)
    document.save(path)
    document.close()
    return path


@pytest.fixture
def session_env(tmp_path, monkeypatch):
    stage_1 = tmp_path / "object" / "comparison" / "stage_1"
    stage_2 = tmp_path / "object" / "comparison" / "stage_2"
    stage_1.mkdir(parents=True)
    stage_2.mkdir(parents=True)
    monkeypatch.setenv("COMPARISON_ROOT", str(tmp_path / "runtime"))
    monkeypatch.setenv("AUDIT_STAGE_COMPARISON_ROOTS", str(tmp_path))
    return stage_1, stage_2


def test_session_opens_with_only_left_side_uploaded(session_env):
    stage_1, stage_2 = session_env
    left = _pdf(stage_1 / "P.pdf", "left")
    client = TestClient(_app())

    session = client.post("/api/stage-comparison/sessions", json={
        "stage_a_path": str(stage_1), "stage_b_path": str(stage_2),
    }).json()
    saved = client.put(f"/api/stage-comparison/sessions/{session['id']}/document-pairing", json={
        "left_order": [str(left)], "right_order": [None], "confirmed_pairs": [],
    })

    assert [d["pdf_path"] for d in session["documents"]["stage_1"]] == [str(left)]
    assert session["documents"]["stage_2"] == []
    assert saved.status_code == 200


def test_new_session_inherits_previous_document_pairing(session_env):
    stage_1, stage_2 = session_env
    left_a = _pdf(stage_1 / "A.pdf", "a")
    left_b = _pdf(stage_1 / "B.pdf", "b")
    client = TestClient(_app())
    first = client.post("/api/stage-comparison/sessions", json={
        "stage_a_path": str(stage_1), "stage_b_path": str(stage_2),
    }).json()
    client.put(f"/api/stage-comparison/sessions/{first['id']}/document-pairing", json={
        "left_order": [str(left_b), str(left_a)], "right_order": [None, None], "confirmed_pairs": [],
    })

    right = _pdf(stage_2 / "R.pdf", "r")
    second = client.post("/api/stage-comparison/sessions", json={
        "stage_a_path": str(stage_1), "stage_b_path": str(stage_2),
    }).json()

    assert second["id"] != first["id"]
    assert second["document_pairing"]["left_order"] == [str(left_b), str(left_a)]
    assert second["document_pairing"]["inherited_from"] == first["id"]
    assert [d["pdf_path"] for d in second["documents"]["stage_2"]] == [str(right)]


def test_pairing_accepts_order_without_grouped_right_documents(session_env, monkeypatch):
    stage_1, stage_2 = session_env
    left = _pdf(stage_1 / "P.pdf", "left")
    member_a = _pdf(stage_2 / "K1.pdf", "k1")
    member_b = _pdf(stage_2 / "K2.pdf", "k2")
    free = _pdf(stage_2 / "K3.pdf", "k3")
    monkeypatch.setattr(document_groups, "hidden_right_documents",
                        lambda _path, _docs: {str(member_a), str(member_b)})
    client = TestClient(_app())
    session = client.post("/api/stage-comparison/sessions", json={
        "stage_a_path": str(stage_1), "stage_b_path": str(stage_2),
    }).json()

    saved = client.put(f"/api/stage-comparison/sessions/{session['id']}/document-pairing", json={
        "left_order": [str(left), None],
        # участник группы, присланный старым клиентом, просто выпадает из строки
        "right_order": [str(member_a), str(free)],
        "confirmed_pairs": [{"left_pdf": str(left), "right_pdf": str(member_a)}],
    })

    assert saved.status_code == 200, saved.text
    assert saved.json()["right_order"] == [None, str(free)]
    assert saved.json()["confirmed_pairs"] == []
    missing_free = client.put(f"/api/stage-comparison/sessions/{session['id']}/document-pairing", json={
        "left_order": [str(left)], "right_order": [None], "confirmed_pairs": [],
    })
    assert missing_free.status_code == 400


def test_document_groups_api_reports_disabled_assemblies(monkeypatch):
    monkeypatch.setenv("PROJECT_ASSEMBLIES_ENABLED", "0")
    response = TestClient(_app()).get("/api/stage-comparison/objects/obj/document-groups")
    assert response.status_code == 409
    assert response.json()["detail"]["code"] == "ASSEMBLIES_DISABLED"
