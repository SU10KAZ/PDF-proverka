"""Группы документов на доске пар (1 → N, N → 1, N ↔ M) и их сборки."""
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
    assert result["released"] == [{"left_document_code": "P-1", "right_document_code": "RD-K1", "anchor_side": "left"}]
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
    assert result["released"] == [{"left_document_code": "P-1", "right_document_code": "RD-K1", "anchor_side": "left"}]
    assert document_groups.list_groups("obj-test")["groups"] == []
    assert _attached() == []


def test_right_document_belongs_to_one_group_only(groups_env):
    document_groups.add_members("obj-test", "P-1", ["RD-K1", "RD-K2"])

    result = document_groups.add_members("obj-test", "P-2", ["RD-K2", "RD-K3"])

    groups = {g["left_document_code"]: g for g in document_groups.list_groups("obj-test")["groups"]}
    assert set(groups) == {"P-2"}
    assert [m["document_code"] for m in groups["P-2"]["members"]] == ["RD-K2", "RD-K3"]
    assert result["released"] == [{"left_document_code": "P-1", "right_document_code": "RD-K1", "anchor_side": "left"}]


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

    assert document_groups.hidden_documents(stage_2, "right", documents) == {"/k1.pdf", "/k2.pdf", "/asm.pdf"}
    assert document_groups.grouped_codes(groups_env / "comparison" / "stage_1", "left") == {"P-1"}
    # Якорь строки остаётся в раскладке.
    assert document_groups.hidden_documents(groups_env / "comparison" / "stage_1", "left", [
        {"pdf_path": "/p1.pdf", "document_code": "P-1"},
    ]) == set()


def test_groups_are_unavailable_when_assemblies_disabled(groups_env, monkeypatch):
    monkeypatch.setenv("PROJECT_ASSEMBLIES_ENABLED", "0")
    with pytest.raises(document_groups.DocumentGroupError) as error:
        document_groups.add_members("obj-test", "P-1", ["RD-K1", "RD-K2"])
    assert error.value.code == "ASSEMBLIES_DISABLED"
    assert document_groups.hidden_documents(groups_env / "comparison" / "stage_2", "right", [
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
    monkeypatch.setattr(document_groups, "hidden_documents",
                        lambda _path, side, _docs: {str(member_a), str(member_b)} if side == "right" else set())
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


def test_natural_key_orders_sections_not_separators():
    assert document_groups._ordered([
        "СТ26_01-14-ОВ3-4-РД_V1", "СТ26-01-14-ОВ3-1-РД_", "СТ26-01-14-ОВ3-10-РД", "СТ26-01-14-ОВ3-2-РД",
    ]) == ["СТ26-01-14-ОВ3-1-РД_", "СТ26-01-14-ОВ3-2-РД", "СТ26_01-14-ОВ3-4-РД_V1", "СТ26-01-14-ОВ3-10-РД"]
    assert document_groups._ordered(["ВК.НС", "ВК-АС-1", "ВК-7-1", "ВК-1-1"]) == [
        "ВК-1-1", "ВК-7-1", "ВК-АС-1", "ВК.НС",
    ]


def test_members_are_glued_in_natural_order_regardless_of_upload_order(groups_env):
    document_groups.add_members("obj-test", "P-1", ["RD-K3", "RD-K2"])
    group = document_groups.add_members("obj-test", "P-1", ["RD-K1"])["group"]

    assert [m["document_code"] for m in group["members"]] == ["RD-K1", "RD-K2", "RD-K3"]
    state = assemblies.get_assembly("obj-test", group["assembly_id"], group["assembly_version_id"])
    assert [s["document_code"] for s in state["sources"]] == ["RD-K1", "RD-K2", "RD-K3"]
    assert group["stale"] is False


def test_group_built_out_of_order_needs_rebuild(groups_env):
    group = document_groups.add_members("obj-test", "P-1", ["RD-K1", "RD-K2"])["group"]
    # Сборка, склеенная до сортировки: в реестре и в сборке обратный порядок.
    registry = document_groups._read("obj-test")
    registry["groups"][0]["right"]["members"] = ["RD-K2", "RD-K1"]
    refs = {row["document_code"]: row["source_ref"] for row in assemblies.list_sources("obj-test")["items"]
            if row["stage"] == "stage_2"}
    legacy = assemblies.create_assembly(object_id="obj-test", name="old", section="OV",
                                        source_refs=[refs["RD-K2"], refs["RD-K1"]],
                                        assembly_id=group["assembly_id"])
    registry["groups"][0]["right"]["assembly_version_id"] = legacy["version_id"]
    document_groups._write("obj-test", registry)

    listed = document_groups.list_groups("obj-test")["groups"][0]
    assert listed["stale"] is True
    assert [m["document_code"] for m in listed["members"]] == ["RD-K1", "RD-K2"]

    rebuilt = document_groups.rebuild("obj-test", listed["group_id"])["group"]
    assert rebuilt["stale"] is False


def _pair_with_work(stage_1: Path, stage_2: Path) -> tuple[dict, dict, Path, Path]:
    """Сессия с парой A → R, по которой построена карта листов и есть прогон V3."""
    from backend.app.services.stage_comparison import paths

    left = _pdf(stage_1 / "A.pdf", "a")
    right = _pdf(stage_2 / "R.pdf", "r")
    session, _ = store.create_session(str(stage_1), str(stage_2))
    store.save_document_pairing(session["id"], [str(left)], [str(right)], [])
    pair = store.create_pair(session["id"], str(left), str(right))["pair"]
    store._atomic_write_json(paths.sheet_match_suggestions_path(session["id"], pair["id"]),
                             {"version": 2, "pair_id": pair["id"], "suggestions": [{"left_page": 1}]})
    store._atomic_write_json(paths.sheet_links_path(session["id"], pair["id"]),
                             {"version": 1, "pair_id": pair["id"], "links": [{"id": "link_1"}],
                              "unlinked_left_pages": []})
    store._atomic_write_json(paths.production_state_path(session["id"], pair["id"]),
                             {"session_id": session["id"], "pair_id": pair["id"]})
    return session, pair, left, right


def test_new_session_pair_inherits_sheet_map_of_same_documents(session_env):
    from backend.app.services.stage_comparison import paths

    stage_1, stage_2 = session_env
    first, old_pair, left, right = _pair_with_work(stage_1, stage_2)
    _pdf(stage_1 / "B.pdf", "b")  # новая загрузка в объект меняет подпись источников

    second, _ = store.create_session(str(stage_1), str(stage_2))
    view = store.create_pair(second["id"], str(left), str(right))
    pair = view["pair"]

    assert second["id"] != first["id"]
    assert view["sheet_matching"]["links"]["links"] == [{"id": "link_1"}]
    links = json.loads(paths.sheet_links_path(second["id"], pair["id"]).read_text(encoding="utf-8"))
    assert links["pair_id"] == pair["id"]
    # Прогоны V3 привязаны к своей сессии и не копируются.
    assert not paths.production_dir(second["id"], pair["id"]).exists()
    raw = json.loads(paths.pair_json_path(second["id"], pair["id"]).read_text(encoding="utf-8"))
    assert raw["inherited_work"]["session_id"] == first["id"]
    assert raw["inherited_work"]["pair_id"] == old_pair["id"]


def test_reopened_session_heals_pair_created_before_inheritance(session_env, monkeypatch):
    from backend.app.services.stage_comparison import paths

    stage_1, stage_2 = session_env
    _pair_with_work(stage_1, stage_2)
    _pdf(stage_1 / "B.pdf", "b")
    second, _ = store.create_session(str(stage_1), str(stage_2))
    with monkeypatch.context() as patch:
        patch.setattr(store, "_inherit_pair_work", lambda meta, pair_id: False)
        pair = store.create_pair(second["id"], str(stage_1 / "A.pdf"), str(stage_2 / "R.pdf"))["pair"]
    assert not paths.sheet_links_path(second["id"], pair["id"]).exists()

    reopened, _ = store.create_session(str(stage_1), str(stage_2))

    assert reopened["id"] == second["id"]
    assert [p["sheet_matching_ready"] for p in reopened["pairs"]] == [True]


def test_pair_of_other_documents_and_own_work_are_not_overwritten(session_env):
    from backend.app.services.stage_comparison import paths

    stage_1, stage_2 = session_env
    _pair_with_work(stage_1, stage_2)
    other = _pdf(stage_1 / "B.pdf", "b")
    second, _ = store.create_session(str(stage_1), str(stage_2))

    foreign = store.create_pair(second["id"], str(other), str(stage_2 / "R.pdf"))["pair"]
    assert not paths.sheet_links_path(second["id"], foreign["id"]).exists()

    with_own = store.create_pair(second["id"], str(stage_1 / "A.pdf"), str(stage_2 / "R.pdf"))["pair"]
    own = {"version": 1, "pair_id": with_own["id"], "links": [{"id": "mine"}], "unlinked_left_pages": []}
    store._atomic_write_json(paths.sheet_links_path(second["id"], with_own["id"]), own)
    assert store._inherit_pair_work(store._load_session_meta(second["id"]), with_own["id"]) is False
    saved = json.loads(paths.sheet_links_path(second["id"], with_own["id"]).read_text(encoding="utf-8"))
    assert saved["links"] == [{"id": "mine"}]


def test_independent_group_reuses_left_without_changing_original(groups_env):
    first = document_groups.add_members("obj-test", "P-1", ["RD-K1", "RD-K2"])["group"]
    second = document_groups.add_members("obj-test", "P-1", ["RD-K3"], new_group=True)["group"]
    groups = document_groups.list_groups("obj-test")["groups"]
    assert len(groups) == 2
    assert groups[0]["assembly_id"] == first["assembly_id"]
    assert [m["document_code"] for m in groups[0]["members"]] == ["RD-K1", "RD-K2"]
    assert second["independent"] is True
    assert second["left_document_code"] == first["left_document_code"]
    assert second["assembly_id"] != first["assembly_id"]
    assert all(g["status"] == "READY" and not g["stale"] for g in groups)
    assert len(_attached()) == 2
    _stage_document(groups_env, "stage_2", "RD-K4")
    updated = document_groups.add_members("obj-test", "P-1", ["RD-K4"], group_id=second["group_id"])["group"]
    assert [m["document_code"] for m in updated["members"]] == ["RD-K3", "RD-K4"]
    assert document_groups.list_groups("obj-test")["groups"][0]["assembly_id"] == first["assembly_id"]


def test_independent_group_cannot_take_existing_group_members(groups_env):
    first = document_groups.add_members("obj-test", "P-1", ["RD-K1", "RD-K2"])["group"]
    with pytest.raises(document_groups.DocumentGroupError) as error:
        document_groups.add_members("obj-test", "P-1", ["RD-K2", "RD-K3"], new_group=True)
    assert error.value.code == "MEMBER_ALREADY_GROUPED"
    assert document_groups.list_groups("obj-test")["groups"][0]["assembly_id"] == first["assembly_id"]


def test_independent_group_api_accepts_new_group_and_explicit_target(groups_env):
    client = TestClient(_app())
    url = "/api/stage-comparison/objects/obj-test/document-groups"
    first = client.post(url, json={"left_document_code": "P-1", "member_codes": ["RD-K1", "RD-K2"]})
    assert first.status_code == 200
    second = client.post(url, json={"left_document_code": "P-1", "member_codes": ["RD-K3"], "new_group": True})
    assert second.status_code == 200
    assert second.json()["group"]["independent"] is True
    assert second.json()["group"]["group_id"] != first.json()["group"]["group_id"]
    bad = client.post(url, json={"left_document_code": "P-2", "member_codes": ["RD-K3"], "group_id": second.json()["group"]["group_id"]})
    assert bad.status_code == 404


def test_manual_order_is_saved_then_used_by_rebuild(groups_env):
    group = document_groups.add_members("obj-test", "P-1", ["RD-K1", "RD-K2", "RD-K3"])["group"]
    document_groups.list_groups("obj-test")
    codes = ["RD-K3", "RD-K1", "RD-K2"]
    client = TestClient(_app())
    response = client.put(f"/api/stage-comparison/objects/obj-test/document-groups/{group['group_id']}/order",
                          json={"member_codes": codes})
    assert response.status_code == 200
    changed = response.json()["group"]
    assert changed["stale"] is True
    assert changed["assembly_version_id"] == group["assembly_version_id"]
    assert [m["document_code"] for m in document_groups.list_groups("obj-test")["groups"][0]["members"]] == codes
    rebuilt = document_groups.rebuild("obj-test", group["group_id"])["group"]
    state = assemblies.get_assembly("obj-test", rebuilt["assembly_id"], rebuilt["assembly_version_id"])
    assert [s["document_code"] for s in state["sources"]] == codes
    assert rebuilt["stale"] is False


@pytest.mark.parametrize("codes", [["RD-K1"], ["RD-K1", "RD-K1"], ["RD-K1", "RD-K3"]])
def test_reorder_rejects_missing_duplicate_and_foreign_members(groups_env, codes):
    group = document_groups.add_members("obj-test", "P-1", ["RD-K1", "RD-K2"])["group"]
    with pytest.raises(document_groups.DocumentGroupError, match="ровно один раз"):
        document_groups.reorder_members("obj-test", group["group_id"], codes)
    assert document_groups._read("obj-test")["groups"][0]["right"]["members"] == ["RD-K1", "RD-K2"]


def test_adding_and_removing_members_preserve_manual_order(groups_env):
    group = document_groups.add_members("obj-test", "P-1", ["RD-K1", "RD-K2"])["group"]
    document_groups.reorder_members("obj-test", group["group_id"], ["RD-K2", "RD-K1"])
    added = document_groups.add_members("obj-test", "P-1", ["RD-K3"])["group"]
    assert [m["document_code"] for m in added["members"]] == ["RD-K2", "RD-K1", "RD-K3"]
    removed = document_groups.remove_member("obj-test", group["group_id"], "RD-K1")["group"]
    assert [m["document_code"] for m in removed["members"]] == ["RD-K2", "RD-K3"]
    assert removed["stale"] is False


def test_reorder_rejects_building_group(groups_env, monkeypatch):
    group = document_groups.add_members("obj-test", "P-1", ["RD-K1", "RD-K2"])["group"]
    monkeypatch.setattr(document_groups, "_assembly_state", lambda *args: {"status": "RUNNING"})
    with pytest.raises(document_groups.DocumentGroupError) as error:
        document_groups.reorder_members("obj-test", group["group_id"], ["RD-K2", "RD-K1"])
    assert error.value.code == "GROUP_BUSY"


# ── Обратная группа (несколько слева → один справа) и сборка против сборки ──

def _attached_left(object_id: str = "obj-test") -> list[dict]:
    return assemblies.attachments(object_id)["stage_1"]


def _codes(side_view: dict) -> list[str]:
    return [m["document_code"] for m in side_view["members"]]


def test_two_left_documents_build_assembly_attached_to_stage_1(groups_env):
    group = document_groups.add_members("obj-test", "RD-K1", ["P-2", "P-1"], side="left")["group"]

    assert _codes(group["left"]) == ["P-1", "P-2"]
    assert _codes(group["right"]) == ["RD-K1"]
    assert group["anchor"] == {"side": "right", "document_code": "RD-K1"}
    assert group["left"]["status"] == "READY" and group["right"]["status"] == "SINGLE"
    listed = document_groups.list_groups("obj-test")["groups"][0]
    assert listed["left"]["attached_version_id"] == group["left"]["assembly_version_id"]
    assert listed["left"]["page_count"] == 2
    assert [row["assembly_id"] for row in _attached_left()] == [group["left"]["assembly_id"]]
    assert _attached() == []
    manifest = assemblies.get_assembly("obj-test", group["left"]["assembly_id"], group["left"]["assembly_version_id"])
    assert manifest["name"] == "RD-K1 · сборка из 2"
    assert {s["stage"] for s in manifest["sources"]} == {"stage_1"}


def test_left_group_hides_left_members_and_keeps_right_anchor(groups_env):
    group = document_groups.add_members("obj-test", "RD-K1", ["P-1", "P-2"], side="left")["group"]
    stage_1 = groups_env / "comparison" / "stage_1"
    stage_2 = groups_env / "comparison" / "stage_2"
    left_docs = [
        {"pdf_path": "/p1.pdf", "document_code": "P-1"},
        {"pdf_path": "/p2.pdf", "document_code": "P-2"},
        {"pdf_path": "/asm.pdf", "document_code": "x", "assembly_ref": {"assembly_id": group["left"]["assembly_id"]}},
    ]

    assert document_groups.hidden_documents(stage_1, "left", left_docs) == {"/p1.pdf", "/p2.pdf", "/asm.pdf"}
    assert document_groups.hidden_documents(stage_2, "right", [
        {"pdf_path": "/k1.pdf", "document_code": "RD-K1"},
    ]) == set()
    assert document_groups.grouped_codes(stage_2, "right") == {"RD-K1"}


def test_left_group_shrinking_to_one_becomes_plain_pair(groups_env):
    group = document_groups.add_members("obj-test", "RD-K1", ["P-1", "P-2"], side="left")["group"]
    document_groups.list_groups("obj-test")

    result = document_groups.remove_member("obj-test", group["group_id"], "P-2", side="left")

    assert result["group"] is None
    assert result["released"] == [{"left_document_code": "P-1", "right_document_code": "RD-K1", "anchor_side": "right"}]
    assert _attached_left() == []


def test_single_document_of_side_cannot_be_removed(groups_env):
    group = document_groups.add_members("obj-test", "RD-K1", ["P-1", "P-2"], side="left")["group"]
    with pytest.raises(document_groups.DocumentGroupError) as error:
        document_groups.remove_member("obj-test", group["group_id"], "RD-K1", side="right")
    assert error.value.code == "MEMBER_LAST"


def test_right_group_extended_on_the_left_compares_assembly_with_assembly(groups_env):
    right_group = document_groups.add_members("obj-test", "P-1", ["RD-K1", "RD-K2"])["group"]
    # Строку ищем по любому документу правой стороны: RD-K2 — участник сборки.
    both = document_groups.add_members("obj-test", "RD-K2", ["P-1", "P-2"], side="left")["group"]

    assert both["group_id"] == right_group["group_id"]
    assert both["anchor"] == {"side": "left", "document_code": "P-1"}
    assert _codes(both["left"]) == ["P-1", "P-2"] and _codes(both["right"]) == ["RD-K1", "RD-K2"]
    assert both["left"]["assembled"] and both["right"]["assembled"]
    assert both["right"]["assembly_version_id"] == right_group["assembly_version_id"]
    document_groups.list_groups("obj-test")
    assert len(_attached_left()) == 1 and len(_attached()) == 1
    # Якорь P-1 стоит в раскладке, второй левый и левая сборка — нет.
    stage_1 = groups_env / "comparison" / "stage_1"
    assert document_groups.hidden_documents(stage_1, "left", [
        {"pdf_path": "/p1.pdf", "document_code": "P-1"},
        {"pdf_path": "/p2.pdf", "document_code": "P-2"},
    ]) == {"/p2.pdf"}


def test_left_group_extended_on_the_right_keeps_right_anchor(groups_env):
    left_group = document_groups.add_members("obj-test", "RD-K1", ["P-1", "P-2"], side="left")["group"]
    both = document_groups.add_members(
        "obj-test", "P-1", ["RD-K1", "RD-K3"], side="right", group_id=left_group["group_id"],
    )["group"]

    assert both["group_id"] == left_group["group_id"]
    assert both["anchor"] == {"side": "right", "document_code": "RD-K1"}
    assert _codes(both["right"]) == ["RD-K1", "RD-K3"]
    # Правая сборка подписана якорем-соседом, левая — якорем своей строки.
    manifest = assemblies.get_assembly("obj-test", both["right"]["assembly_id"], both["right"]["assembly_version_id"])
    assert manifest["name"] == "P-1 · сборка из 2"


def test_side_shrinking_to_one_drops_only_its_assembly(groups_env):
    document_groups.add_members("obj-test", "P-1", ["RD-K1", "RD-K2"])
    group = document_groups.add_members("obj-test", "RD-K1", ["P-1", "P-2"], side="left")["group"]
    document_groups.list_groups("obj-test")

    shrunk = document_groups.remove_member("obj-test", group["group_id"], "RD-K2", side="right")["group"]

    assert shrunk is not None
    assert shrunk["right"]["assembled"] is False and shrunk["right"]["status"] == "SINGLE"
    assert shrunk["right"]["assembly_version_id"] is None
    assert shrunk["left"]["assembled"] is True
    assert _attached() == [] and len(_attached_left()) == 1


def test_removed_anchor_is_replaced_by_next_member_of_its_side(groups_env):
    _stage_document(groups_env, "stage_1", "P-3")
    document_groups.add_members("obj-test", "P-1", ["RD-K1", "RD-K2"])
    group = document_groups.add_members("obj-test", "RD-K1", ["P-1", "P-2", "P-3"], side="left")["group"]

    result = document_groups.remove_member("obj-test", group["group_id"], "P-1", side="left")["group"]

    assert result["anchor"] == {"side": "left", "document_code": "P-2"}


def test_anchor_of_another_group_cannot_be_moved(groups_env):
    document_groups.add_members("obj-test", "P-1", ["RD-K1", "RD-K2"])
    with pytest.raises(document_groups.DocumentGroupError) as error:
        document_groups.add_members("obj-test", "RD-K3", ["P-2", "P-1"], side="left")
    assert error.value.code == "MEMBER_IS_GROUP_ANCHOR"


def test_independent_comparison_takes_documents_only_on_the_right(groups_env):
    with pytest.raises(document_groups.DocumentGroupError) as error:
        document_groups.add_members("obj-test", "RD-K1", ["P-1"], side="left", new_group=True)
    assert error.value.code == "BAD_GROUP_TARGET"


def test_same_code_on_both_stages_is_removed_by_side(groups_env):
    _stage_document(groups_env, "stage_1", "X-1")
    _stage_document(groups_env, "stage_2", "X-1")
    group = document_groups.add_members("obj-test", "P-1", ["X-1", "RD-K1"])["group"]
    group = document_groups.add_members("obj-test", "RD-K1", ["P-1", "X-1", "P-2"], side="left",
                                        group_id=group["group_id"])["group"]

    result = document_groups.remove_member("obj-test", group["group_id"], "X-1", side="left")["group"]

    assert _codes(result["left"]) == ["P-1", "P-2"]
    assert _codes(result["right"]) == ["RD-K1", "X-1"]


def test_v1_registry_is_read_and_v1_fields_are_kept_for_rollback(groups_env):
    right = document_groups.add_members("obj-test", "P-1", ["RD-K1", "RD-K2"])["group"]
    path = groups_env / "comparison" / "document_groups.json"
    raw = json.loads(path.read_text(encoding="utf-8"))
    assert raw["schema"] == "stage_document_groups/2"
    record = raw["groups"][0]
    assert record["left_document_code"] == "P-1"
    assert record["members"] == ["RD-K1", "RD-K2"]
    assert record["assembly_id"] == right["assembly_id"]

    # Реестр прежнего релиза (схема 1) читается как «один слева → N справа».
    v1 = {key: record[key] for key in ("group_id", "left_document_code", "members", "assembly_id",
                                       "assembly_version_id", "attached_version_id", "created_at", "updated_at")}
    path.write_text(json.dumps({"schema": "stage_document_groups/1", "groups": [v1]}), encoding="utf-8")
    listed = document_groups.list_groups("obj-test")["groups"][0]
    assert listed["anchor"] == {"side": "left", "document_code": "P-1"}
    assert _codes(listed["right"]) == ["RD-K1", "RD-K2"] and listed["stale"] is False

    # Группу с несколькими документами слева схема 1 не выражает: прежний релиз видит её пустой.
    document_groups.add_members("obj-test", "RD-K1", ["P-1", "P-2"], side="left")
    record = json.loads(path.read_text(encoding="utf-8"))["groups"][0]
    assert record["left"]["members"] == ["P-1", "P-2"]
    assert record["left_document_code"] == "P-1" and record["members"] == []


def test_document_groups_api_accepts_left_side(groups_env):
    client = TestClient(_app())
    url = "/api/stage-comparison/objects/obj-test/document-groups"

    missing = client.post(url, json={"side": "left", "left_document_code": "P-1", "member_codes": ["P-2"]})
    assert missing.status_code == 422
    created = client.post(url, json={"side": "left", "right_document_code": "RD-K1", "member_codes": ["P-1", "P-2"]})
    assert created.status_code == 200, created.text
    group = created.json()["group"]
    assert _codes(group["left"]) == ["P-1", "P-2"]

    order = client.put(f"{url}/{group['group_id']}/order", json={"side": "left", "member_codes": ["P-2", "P-1"]})
    assert order.status_code == 200 and order.json()["group"]["left"]["stale"] is True
    rebuilt = client.post(f"{url}/{group['group_id']}/rebuild?side=left")
    assert rebuilt.status_code == 200 and rebuilt.json()["group"]["left"]["stale"] is False
    last = client.delete(f"{url}/{group['group_id']}/members/RD-K1?side=right")
    assert last.status_code == 409 and last.json()["detail"]["code"] == "MEMBER_LAST"
    removed = client.delete(f"{url}/{group['group_id']}/members/P-2?side=left")
    assert removed.status_code == 200
    assert removed.json()["released"] == [{"left_document_code": "P-1", "right_document_code": "RD-K1", "anchor_side": "right"}]


def test_pairing_accepts_order_without_grouped_left_documents(session_env, monkeypatch):
    stage_1, stage_2 = session_env
    member_a = _pdf(stage_1 / "P1.pdf", "p1")
    member_b = _pdf(stage_1 / "P2.pdf", "p2")
    right = _pdf(stage_2 / "R.pdf", "r")
    monkeypatch.setattr(document_groups, "hidden_documents",
                        lambda _path, side, _docs: {str(member_a), str(member_b)} if side == "left" else set())
    client = TestClient(_app())
    session = client.post("/api/stage-comparison/sessions", json={
        "stage_a_path": str(stage_1), "stage_b_path": str(stage_2),
    }).json()

    saved = client.put(f"/api/stage-comparison/sessions/{session['id']}/document-pairing", json={
        "left_order": [str(member_a)], "right_order": [str(right)],
        "confirmed_pairs": [{"left_pdf": str(member_a), "right_pdf": str(right)}],
    })

    assert saved.status_code == 200, saved.text
    assert saved.json()["left_order"] == [None]
    assert saved.json()["right_order"] == [str(right)]
    assert saved.json()["confirmed_pairs"] == []


def test_pair_inherits_completeness_from_left_assembly(session_env, tmp_path, monkeypatch):
    stage_1, stage_2 = session_env
    right = _pdf(stage_2 / "R.pdf", "r")
    assembly_dir = tmp_path / "asm"
    assembly_dir.mkdir()
    assembly_pdf = _pdf(assembly_dir / "document.pdf", "asm")
    attached = {
        "pdf_path": str(assembly_pdf), "filename": "R · сборка из 2.pdf", "document_code": "R · сборка из 2",
        "assembly_ref": {"assembly_id": "asm_x", "version_id": "v001", "composition_completeness": "INCOMPLETE"},
        "assembly_members": [],
    }
    monkeypatch.setattr(assemblies, "attached_documents_for_stage_path",
                        lambda path: [attached] if Path(path).name == "stage_1" else [])
    session, _ = store.create_session(str(stage_1), str(stage_2))

    pair = store.create_pair(session["id"], str(assembly_pdf), str(right))["pair"]

    assert pair["composition_completeness"] == "INCOMPLETE"
