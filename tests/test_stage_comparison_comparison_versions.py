"""Версии сравнения стадий: несколько независимых сравнений одного объекта (18.11.2026).

Версия 1 — прежняя папка ``comparison/``; Версия N — ``comparison/versions/cvNNN``
со своими сторонами, группами и сборками. Сессии, раскладка, прогоны и отчёты
различаются по путям папок стадий.
"""
from __future__ import annotations

import hashlib
import io
import json
import os
import threading
from pathlib import Path

import fitz
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.app.services.common import object_service
from backend.app.services.project_assemblies import service as assemblies
from backend.app.services.stage_comparison import comparison_versions as cv
from backend.app.services.stage_comparison import document_groups, stage_storage, stage_upload, store

# Настоящий запуск сборки (отдельный процесс): фикстуры подменяют его сборкой в процессе.
_REAL_LAUNCH = assemblies.launch_version

OID = "a1b2c3d4"
OTHER_OID = "e5f6a7b8"
OBJECTS = {
    OID: {"id": OID, "name": "199. ЖК Тест", "projects_dir": ""},
    OTHER_OID: {"id": OTHER_OID, "name": "Другой объект", "projects_dir": ""},
}


def _pdf_bytes(label: str, pages: int = 1) -> bytes:
    document = fitz.open()
    for number in range(pages):
        page = document.new_page(width=200, height=120)
        page.insert_text((20, 40), f"{label} {number + 1}")
    payload = document.tobytes()
    document.close()
    return payload


def _folder(code: str, label: str | None = None) -> list[tuple[io.BytesIO, str]]:
    label = label or code
    return [
        (io.BytesIO(_pdf_bytes(label)), f"{code}/{code}.pdf"),
        (io.BytesIO(f"# {label}\n\n## Page 1\n\nТекст {label}\n".encode()), f"{code}/{code}_results.md"),
        (io.BytesIO(json.dumps({"schema_version": 1, "blocks": []}).encode()), f"{code}/{code}_blocks.json"),
    ]


def _upload(scope: str, stage: str, code: str, label: str | None = None) -> dict:
    return stage_upload.replace_stage_from_folder(scope, stage, _folder(code, label), code, False)


def _tree_digest(root: Path) -> dict[str, str]:
    return {str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in sorted(root.rglob("*")) if p.is_file()}


@pytest.fixture
def world(tmp_path, monkeypatch):
    """Объект реестра с настоящим разрешением папок projects_v2."""
    v2 = tmp_path / "projects_v2"
    for oid, folder in ((OID, "Test"), (OTHER_OID, "Other")):
        object_dir = v2 / "objects" / folder
        object_dir.mkdir(parents=True)
        (object_dir / "object.json").write_text(json.dumps({
            "object_id": oid, "display_name": folder, "folder_name": folder,
        }), encoding="utf-8")
    monkeypatch.setenv("AUDIT_PROJECTS_V2_DIR", str(v2))
    monkeypatch.setenv("COMPARISON_ROOT", str(tmp_path / "runtime"))
    monkeypatch.delenv("AUDIT_STAGE_COMPARISON_ROOTS", raising=False)
    monkeypatch.setattr(object_service, "get_object_by_id",
                        lambda object_id: dict(OBJECTS[object_id]) if object_id in OBJECTS else None)
    monkeypatch.setattr(object_service, "list_objects", lambda: [dict(item) for item in OBJECTS.values()])
    return v2 / "objects" / "Test"


@pytest.fixture
def version_1(world):
    """Версия 1: П1 слева (два документа), РД1 справа."""
    _upload(OID, "stage_1", "P-AR", "П АР")
    _upload(OID, "stage_1", "P-OV", "П ОВ")
    _upload(OID, "stage_2", "RD-AR", "РД АР")
    return world / "comparison"


def _existing(version: str, stage: str) -> dict:
    return {"source": "existing", "comparison_version": version, "stage": stage}


NEW = {"source": "new"}


# ─── области и пути ────────────────────────────────────────────────────────

def test_scope_round_trip_and_rejects_ambiguous_forms():
    assert cv.scope_id(OID) == OID
    assert cv.scope_id(OID, "cv001") == OID
    assert cv.scope_id(OID, "cv002") == f"{OID}@cv002"
    assert cv.split_scope(f"{OID}@cv002") == (OID, "cv002")
    assert cv.split_scope(OID) == (OID, "cv001")
    for bad in (f"{OID}@cv001", f"{OID}@v002", f"{OID}@cv000", "@cv002", f"x@{OID}@cv002"):
        with pytest.raises(cv.ComparisonVersionError):
            cv.split_scope(bad)
    with pytest.raises(cv.ComparisonVersionError):
        cv.normalize("v002")


def test_stage_path_parsing_covers_both_layouts(tmp_path):
    legacy = tmp_path / "objects" / "Obj" / "comparison"
    info = cv.parse_stage_path(legacy / "stage_1")
    assert (info.comparison_version, info.stage, info.object_dir) == ("cv001", "stage_1", legacy.parent.resolve())
    info = cv.parse_stage_path(legacy / "versions" / "cv002" / "stage_2")
    assert (info.comparison_version, info.stage, info.legacy_dir) == ("cv002", "stage_2", legacy.resolve())
    assert info.object_dir == legacy.parent.resolve()
    assert cv.parse_stage_path(legacy / "versions" / "cv002" / "stage_3") is None
    assert cv.parse_stage_path(legacy / "versions" / "v002" / "stage_1") is None
    assert cv.parse_stage_path(tmp_path / "elsewhere" / "stage_1") is None
    assert cv.legacy_dir_for_workspace(legacy / "versions" / "cv002") == legacy
    assert cv.legacy_dir_for_workspace(legacy) == legacy


# ─── реестр ────────────────────────────────────────────────────────────────

def test_without_registry_version_1_is_the_existing_comparison(version_1):
    listed = cv.list_versions(OID)

    assert [v["comparison_version"] for v in listed["versions"]] == ["cv001"]
    first = listed["versions"][0]
    assert first["label"] == "Версия 1" and first["legacy"] is True
    assert first["stages"]["stage_1"]["path"] == str(version_1 / "stage_1")
    assert first["stages"]["stage_1"]["documents"] == 2
    assert first["stages"]["stage_2"]["documents"] == 1
    assert listed["latest"] == "cv001"
    # Чтение ничего не пишет: реестр появляется только при изменении.
    assert not (version_1 / cv.REGISTRY_NAME).exists()


def test_create_version_reuses_left_set_by_hard_links_and_leaves_right_for_upload(version_1):
    before = _tree_digest(version_1 / "stage_1")

    created = cv.create_version(OID, stage_1=_existing("cv001", "stage_1"), stage_2=NEW)

    assert created["comparison_version"] == "cv002" and created["label"] == "Версия 2"
    assert created["stages"]["stage_1"]["documents"] == 2
    assert created["stages"]["stage_2"]["documents"] == 0
    assert created["stages"]["stage_1"]["source"]["comparison_version"] == "cv001"
    assert created["stages"]["stage_2"]["source"] == {"kind": "new"}
    # Тот же комплект — тот же ключ: в списке выбора он склеивается.
    first = cv.get_version(OID, "cv001")
    assert created["stages"]["stage_1"]["set_key"] == first["stages"]["stage_1"]["set_key"]

    workspace = version_1 / "versions" / "cv002"
    for source in (version_1 / "stage_1").rglob("*"):
        if not source.is_file():
            continue
        copy = workspace / "stage_1" / source.relative_to(version_1 / "stage_1")
        assert copy.is_file(), copy
        if source.suffix == ".pdf":
            assert os.stat(source).st_ino == os.stat(copy).st_ino  # жёсткая ссылка
        else:
            assert os.stat(source).st_ino != os.stat(copy).st_ino  # JSON/MD пишутся на месте — копия
    assert stage_storage.is_versioned_stage(workspace / "stage_2")
    assert json.loads((workspace / "stage_1" / "stage.json").read_text(encoding="utf-8"))["stage"] == "stage_1"
    assert _tree_digest(version_1 / "stage_1") == before
    registry = json.loads((version_1 / cv.REGISTRY_NAME).read_text(encoding="utf-8"))
    assert [v["comparison_version"] for v in registry["versions"]] == ["cv001", "cv002"]


def test_set_of_the_other_side_can_be_reused_and_gets_its_new_side_name(version_1):
    created = cv.create_version(
        OID, stage_1=_existing("cv001", "stage_2"), stage_2=_existing("cv001", "stage_1"), label="РД против П",
    )

    workspace = version_1 / "versions" / created["comparison_version"]
    assert created["label"] == "РД против П"
    assert json.loads((workspace / "stage_1" / "stage.json").read_text(encoding="utf-8"))["stage"] == "stage_1"
    assert (workspace / "stage_1" / "documents" / "RD-AR").is_dir()
    assert (workspace / "stage_2" / "documents" / "P-AR").is_dir()


@pytest.mark.parametrize("stage_1, stage_2, code", [
    (_existing("cv001", "stage_1"), _existing("cv001", "stage_1"), "SAME_SET"),
    (_existing("cv009", "stage_1"), NEW, "UNKNOWN_COMPARISON_VERSION"),
    ({"source": "existing", "stage": "stage_1"}, NEW, "BAD_SOURCE"),
    ({"source": "existing", "comparison_version": "v001", "stage": "stage_1"}, NEW, "BAD_COMPARISON_VERSION"),
    ({"source": "copy"}, NEW, "BAD_SOURCE"),
])
def test_create_version_rejects_bad_sources(version_1, stage_1, stage_2, code):
    with pytest.raises(cv.ComparisonVersionError) as error:
        cv.create_version(OID, stage_1=stage_1, stage_2=stage_2)
    assert error.value.code == code
    assert not (version_1 / "versions").exists() or not any((version_1 / "versions").iterdir())


def test_copy_of_a_set_counts_as_the_same_set(version_1):
    cv.create_version(OID, stage_1=_existing("cv001", "stage_1"), stage_2=NEW)

    with pytest.raises(cv.ComparisonVersionError) as error:
        cv.create_version(OID, stage_1=_existing("cv001", "stage_1"), stage_2=_existing("cv002", "stage_1"))
    assert error.value.code == "SAME_SET"


def test_empty_set_cannot_be_reused(world):
    _upload(OID, "stage_1", "P-AR")
    with pytest.raises(cv.ComparisonVersionError) as error:
        cv.create_version(OID, stage_1=_existing("cv001", "stage_1"), stage_2=_existing("cv001", "stage_2"))
    assert error.value.code == "EMPTY_SET"


def test_rename_any_version_and_label_rules(version_1):
    cv.create_version(OID, stage_1=_existing("cv001", "stage_1"), stage_2=NEW)

    renamed = cv.rename_version(OID, "cv001", "  П против РД, выпуск 1  ", author="engineer-1")
    assert renamed["label"] == "П против РД, выпуск 1" and renamed["renamed_by"] == "engineer-1"
    assert cv.get_version(OID, "cv002")["label"] == "Версия 2"
    for label, code in (("", "LABEL_REQUIRED"), ("x" * 101, "LABEL_TOO_LONG"),
                        ("a\nb", "LABEL_INVALID"), ("версия 2", "LABEL_TAKEN")):
        with pytest.raises(cv.ComparisonVersionError) as error:
            cv.rename_version(OID, "cv001", label)
        assert error.value.code == code
    with pytest.raises(cv.ComparisonVersionError) as error:
        cv.rename_version(OID, "cv007", "Нет такой")
    assert error.value.code == "UNKNOWN_COMPARISON_VERSION"


def test_parallel_creation_allocates_distinct_numbers(version_1):
    results, errors = [], []

    def create():
        try:
            results.append(cv.create_version(OID, stage_1=_existing("cv001", "stage_1"), stage_2=NEW))
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=create) for _ in range(3)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert errors == []
    assert sorted(item["comparison_version"] for item in results) == ["cv002", "cv003", "cv004"]
    assert [v["comparison_version"] for v in cv.list_versions(OID)["versions"]] == ["cv001", "cv002", "cv003", "cv004"]


# ─── загрузка и сессии ─────────────────────────────────────────────────────

def test_upload_into_version_2_does_not_touch_version_1(version_1):
    cv.create_version(OID, stage_1=_existing("cv001", "stage_1"), stage_2=NEW)
    before = _tree_digest(version_1 / "stage_1")
    scope = cv.scope_id(OID, "cv002")

    _upload(scope, "stage_1", "P-AR", "П АР изм. 1")  # тот же код — новая версия документа
    result = _upload(scope, "stage_2", "RD-AR", "РД АР изм. 1")

    assert result["stage_paths"]["stage_2"] == str(version_1 / "versions" / "cv002" / "stage_2")
    copy = version_1 / "versions" / "cv002" / "stage_1" / "documents" / "P-AR"
    assert (copy / "current_version.txt").read_text(encoding="utf-8").strip() == "v002"
    assert (version_1 / "stage_1" / "documents" / "P-AR" / "current_version.txt").read_text(encoding="utf-8").strip() == "v001"
    assert _tree_digest(version_1 / "stage_1") == before
    assert not (version_1 / "stage_2" / "documents" / "RD-AR" / "versions" / "v002").exists()


def test_upload_into_unknown_version_is_refused(version_1):
    with pytest.raises(stage_upload.StageUploadError):
        _upload(cv.scope_id(OID, "cv005"), "stage_2", "RD-AR")
    assert not (version_1 / "versions").exists()


def test_sessions_of_versions_are_separate_and_owned_by_the_object(version_1, monkeypatch):
    from backend.app.core import object_scope
    from backend.app.services.project_change_v3 import scope

    cv.create_version(OID, stage_1=_existing("cv001", "stage_1"), stage_2=NEW)
    _upload(cv.scope_id(OID, "cv002"), "stage_2", "RD-AR", "РД АР изм. 1")
    paths = cv.stage_paths_for_legacy_dir(version_1)
    first, _ = store.create_session(*map(str, paths["cv001"]))
    second, _ = store.create_session(*map(str, paths["cv002"]))

    assert first["id"] != second["id"]
    assert (first["comparison_version"], second["comparison_version"]) == ("cv001", "cv002")
    assert scope.object_id_for_session(second["id"]) == OID
    assert scope.sessions_for_object(OID, "cv002") == [second["id"]]
    assert scope.sessions_for_object(OID, "cv001") == [first["id"]]
    assert set(scope.sessions_for_object(OID)) == {first["id"], second["id"]}
    assert scope.sessions_for_object(OTHER_OID) == []
    assert object_scope._session_belongs(second["id"], OID) is True
    assert object_scope._session_belongs(second["id"], OTHER_OID) is False
    # Прогоны Версии 1 в новую версию не подтягиваются: другие корни доски.
    assert store.get_session(second["id"])["comparison_runs"] == []


def test_allowlist_accepts_stages_of_registered_versions_only(version_1, tmp_path, monkeypatch):
    monkeypatch.setenv("AUDIT_STAGE_COMPARISON_ROOTS", str(tmp_path / "somewhere-else"))
    cv.create_version(OID, stage_1=_existing("cv001", "stage_1"), stage_2=NEW)

    store.assert_path_in_allowlist(str(version_1 / "stage_1"))
    store.assert_path_in_allowlist(str(version_1 / "versions" / "cv002" / "stage_2"))
    for rejected in (version_1 / "versions" / "cv003" / "stage_1",
                     version_1 / "versions" / "cv002" / "stage_3",
                     version_1 / "versions" / "cv002"):
        with pytest.raises(PermissionError):
            store.assert_path_in_allowlist(str(rejected))


def test_objects_listing_returns_stages_of_the_selected_version(version_1):
    from backend.app.services.stage_comparison import objects

    cv.create_version(OID, stage_1=_existing("cv001", "stage_1"), stage_2=NEW)
    item = objects.list_objects(OID, "cv002")["items"][0]

    assert item["comparison_version"] == "cv002"
    assert item["default_stage_a"]["path"] == str(version_1 / "versions" / "cv002" / "stage_1")
    assert [stage["pdf_count"] for stage in item["stages"]] == [2, 0]
    legacy = objects.list_objects(OID)["items"][0]
    assert legacy["default_stage_a"]["path"] == str(version_1 / "stage_1")
    with pytest.raises(cv.ComparisonVersionError):
        objects.list_objects(OID, "cv004")


# ─── группы и сборки ───────────────────────────────────────────────────────

@pytest.fixture
def groups_world(world, monkeypatch):
    from tests.test_stage_comparison_document_groups import _stage_document

    comparison = world / "comparison"
    stage_storage.ensure_comparison_object_scaffold(comparison, object_id=OID, object_name="Тест")
    _stage_document(world, "stage_1", "P-1")
    for code in ("RD-K1", "RD-K2"):
        _stage_document(world, "stage_2", code)
    monkeypatch.setenv("PROJECT_ASSEMBLIES_ENABLED", "1")
    monkeypatch.setattr(assemblies, "MIN_FREE_RESERVE", 0)
    # Сборка в том же процессе: фоновый воркер не видит подмен теста.
    monkeypatch.setattr(assemblies, "launch_version", assemblies.build_version)
    return comparison


def test_groups_and_assemblies_live_in_their_version(groups_world):
    cv.create_version(OID, stage_1=_existing("cv001", "stage_1"), stage_2=_existing("cv001", "stage_2"))
    scope = cv.scope_id(OID, "cv002")

    group = document_groups.add_members(scope, "P-1", ["RD-K1", "RD-K2"])["group"]
    listed = document_groups.list_groups(scope)

    assert group["status"] == "READY" and listed["object_id"] == OID
    assert len(listed["groups"]) == 1
    assert document_groups.list_groups(OID)["groups"] == []
    workspace = groups_world / "versions" / "cv002"
    assert (workspace / "document_groups.json").is_file()
    assert not (groups_world / "document_groups.json").exists()
    assert json.loads((workspace / "document_groups.json").read_text(encoding="utf-8"))["object_id"] == OID
    assert (workspace / "assemblies" / group["assembly_id"]).is_dir()
    assert assemblies.attachments(scope)["stage_2"] and assemblies.attachments(OID)["stage_2"] == []

    # Сессия версии видит свою сборку, сессия Версии 1 — нет.
    paths = cv.stage_paths_for_legacy_dir(groups_world)
    second, _ = store.create_session(*map(str, paths["cv002"]))
    first, _ = store.create_session(*map(str, paths["cv001"]))
    assert any(d.get("assembly_ref") for d in second["documents"]["stage_2"])
    assert not any(d.get("assembly_ref") for d in first["documents"]["stage_2"])
    hidden = document_groups.hidden_documents(paths["cv002"][1], "right", second["documents"]["stage_2"])
    assert hidden and document_groups.hidden_documents(paths["cv001"][1], "right", first["documents"]["stage_2"]) == set()

    # Отчёт ZIP находит файлы участников сборки по области из карты происхождения.
    from backend.app.services.project_change_consolidator import report_bundle

    assembly_doc = next(d for d in second["documents"]["stage_2"] if d.get("assembly_ref"))
    documents = report_bundle.side_documents(assembly_doc)
    assert [d["code"] for d in documents] == ["RD-K1", "RD-K2"]
    assert all(str(d["version_dir"]).startswith(str(workspace)) for d in documents)


def test_background_assembly_worker_receives_the_version_scope(groups_world, monkeypatch):
    cv.create_version(OID, stage_1=_existing("cv001", "stage_1"), stage_2=_existing("cv001", "stage_2"))
    scope = cv.scope_id(OID, "cv002")
    # Настоящий launch_version, но вместо процесса — запись команды.
    monkeypatch.setattr(assemblies, "launch_version", _REAL_LAUNCH)
    commands = []

    class _Process:
        pid = 4242

    monkeypatch.setattr(assemblies.subprocess, "Popen", lambda command, **kwargs: commands.append(command) or _Process())

    sources = [row["source_ref"] for row in assemblies.list_sources(scope)["items"]
               if row["stage"] == "stage_2" and row["current"]]
    assemblies.create_assembly(object_id=scope, name="РД · сборка из 2", section="OV",
                               source_refs=sources, build=False)

    assert commands and commands[0][2:4] == ["backend.app.services.project_assemblies.worker", scope]
    assert any((groups_world / "versions" / "cv002" / "assemblies").glob("asm_*/pending/*.json"))
    assert not (groups_world / "assemblies").exists()


def test_assembly_build_lock_is_shared_by_all_versions(tmp_path):
    legacy_root = tmp_path / "objects" / "Obj" / "comparison" / "assemblies"
    version_root = tmp_path / "objects" / "Obj" / "comparison" / "versions" / "cv002" / "assemblies"
    for root in (legacy_root, version_root):
        root.mkdir(parents=True)
        with assemblies._build_lock(root):
            pass
    assert (tmp_path / "objects" / "_system" / "project_assemblies.lock").is_file()
    assert not (tmp_path / "objects" / "Obj" / "_system").exists()


# ─── каталог и отчёт ───────────────────────────────────────────────────────

def test_catalog_groups_pairs_per_version_and_keeps_version_1_keys():
    from backend.app.services.project_change_catalog import catalog

    old, new = "a" * 64, "b" * 64
    assert catalog._pair_group_key(OID, old, new, "cv001") == catalog._pair_group_key(OID, old, new)
    assert catalog._pair_group_key(OID, old, new, "cv002") != catalog._pair_group_key(OID, old, new)


def test_report_header_names_the_comparison_version(version_1):
    from backend.app.api.routers import stage_comparison as router_mod

    cv.create_version(OID, stage_1=_existing("cv001", "stage_1"), stage_2=NEW)
    _upload(cv.scope_id(OID, "cv002"), "stage_2", "RD-AR", "РД АР изм. 1")
    cv.rename_version(OID, "cv002", "Выпуск РД 2")
    paths = cv.stage_paths_for_legacy_dir(version_1)
    for version, label in (("cv001", "Test"), ("cv002", "Test · Выпуск РД 2")):
        session, _ = store.create_session(*map(str, paths[version]))
        left, right = session["documents"]["stage_1"][0], session["documents"]["stage_2"][0]
        pair = store.create_pair(session["id"], left["pdf_path"], right["pdf_path"])["pair"]
        assert router_mod._report_labels(session["id"], pair["id"]) == ("P-AR", label)


# ─── API ───────────────────────────────────────────────────────────────────

def _client() -> TestClient:
    from backend.app.api.routers import stage_comparison as router_mod

    app = FastAPI()
    app.include_router(router_mod.router)
    return TestClient(app)


def test_api_lists_creates_renames_and_scopes_object_routes(version_1):
    client = _client()
    base = f"/api/stage-comparison/objects/{OID}"

    listed = client.get(f"{base}/comparison-versions")
    assert listed.status_code == 200
    assert [v["label"] for v in listed.json()["versions"]] == ["Версия 1"]
    assert not (version_1 / cv.REGISTRY_NAME).exists()

    created = client.post(f"{base}/comparison-versions", json={
        "stage_1": _existing("cv001", "stage_1"), "stage_2": NEW,
    })
    assert created.status_code == 200, created.text
    assert created.json()["comparison_version"] == "cv002"

    upload = client.post(
        f"{base}/stages/stage_2/upload-folder?comparison_version=cv002",
        files=[("files", (rel, stream, "application/octet-stream")) for stream, rel in _folder("RD-AR", "РД 2")],
        data={"relative_paths": json.dumps([rel for _stream, rel in _folder("RD-AR")]),
              "folder_name": "RD-AR", "retain_backup": "false"},
    )
    assert upload.status_code == 200, upload.text
    assert upload.json()["stage_path"] == str(version_1 / "versions" / "cv002" / "stage_2")

    objects = client.get(f"/api/stage-comparison/objects?object_id={OID}&comparison_version=cv002").json()
    assert objects["items"][0]["default_stage_b"]["path"] == str(version_1 / "versions" / "cv002" / "stage_2")

    renamed = client.patch(f"{base}/comparison-versions/cv002", json={"label": "РД изм. 1"})
    assert renamed.status_code == 200 and renamed.json()["label"] == "РД изм. 1"
    taken = client.patch(f"{base}/comparison-versions/cv001", json={"label": "рд изм. 1"})
    assert taken.status_code == 409 and taken.json()["detail"]["code"] == "LABEL_TAKEN"

    unknown = client.get(f"{base}/document-groups?comparison_version=cv009")
    assert unknown.status_code == 404 and unknown.json()["detail"]["code"] == "unknown_comparison_version"
    assert client.get(f"{base}/document-groups?comparison_version=v2").status_code == 422
    missing = client.post(f"{base}/stages/stage_2/upload-folder?comparison_version=cv009",
                          files=[("files", ("a.pdf", io.BytesIO(_pdf_bytes("a")), "application/pdf"))],
                          data={"relative_paths": json.dumps(["a.pdf"])})
    assert missing.status_code == 404
    same = client.post(f"{base}/comparison-versions", json={
        "stage_1": _existing("cv001", "stage_1"), "stage_2": _existing("cv002", "stage_1"),
    })
    assert same.status_code == 409 and same.json()["detail"]["code"] == "SAME_SET"


def test_viewer_listing_hides_server_paths(version_1, monkeypatch):
    from backend.app.core import access

    monkeypatch.setattr(access, "read_only_request", lambda: True)
    listed = _client().get(f"/api/stage-comparison/objects/{OID}/comparison-versions").json()
    assert all("path" not in stage for v in listed["versions"] for stage in v["stages"].values())


# ─── сквозной сценарий из задачи ──────────────────────────────────────────

def test_two_versions_keep_their_own_sets_and_results(world, tmp_path, monkeypatch):
    """П1 ↔ РД1 — «Версия 1»; затем «Версия 2»: тот же П1 из загруженных + новый РД2.

    У каждой версии своя пара комплектов и свой результат; результат Версии 1
    после прогона Версии 2 не меняется. Модель — тестовый провайдер (0 вызовов).
    """
    from backend.app.api.routers import project_change_preview
    from backend.app.api.routers import stage_comparison as router_mod
    from backend.app.services.project_change_catalog import catalog
    from backend.app.services.project_change_v3.provider import FakeProvider, reset_test_provider, set_test_provider
    from backend.tests.project_change_v3 import generic_fixture as gf

    monkeypatch.setenv("PROJECT_COMPARISON_ENGINE", "v3")
    monkeypatch.setenv("PROJECT_COMPARISON_V3_ALLOW_INFERENCE", "0")
    monkeypatch.setenv("PROJECT_COMPARISON_V3_FORCE_UNAVAILABLE", "1")
    comparison = world / "comparison"
    gf.build_stage(comparison, "OLD", "П1")
    gf.build_stage(comparison, "NEW", "РД1")
    app = FastAPI()
    for router in (router_mod.router, project_change_preview.availability_router):
        app.include_router(router)
    client = TestClient(app)

    def run(stage_a: Path, stage_b: Path) -> tuple[dict, dict, dict]:
        session = client.post("/api/stage-comparison/sessions", json={
            "stage_a_path": str(stage_a), "stage_b_path": str(stage_b)}).json()
        left, right = session["documents"]["stage_1"][0], session["documents"]["stage_2"][0]
        pair = client.post(f"/api/stage-comparison/sessions/{session['id']}/pairs", json={
            "left_pdf": left["pdf_path"], "right_pdf": right["pdf_path"]}).json()["pair"]
        set_test_provider(FakeProvider(handlers=gf.fake_handlers(pair["id"])))
        try:
            response = client.post(
                f"/api/stage-comparison/sessions/{session['id']}/pairs/{pair['id']}/production/run",
                json={"input_mode": "DOCUMENT"})
        finally:
            reset_test_provider()
        assert response.status_code == 200, response.text
        return session, pair, response.json()

    first_session, first_pair, first_state = run(comparison / "stage_1", comparison / "stage_2")
    assert first_state["engine"] == "projectchange_v3" and first_state["model_calls"] == 0
    first_changes_url = f"/api/stage-comparison/sessions/{first_session['id']}/pairs/{first_pair['id']}/production/changes"
    first_changes = client.get(first_changes_url).json()
    runs_root = tmp_path / "runtime" / "sessions" / first_session["id"] / "pairs" / first_pair["id"] / "production"
    first_digest = _tree_digest(runs_root / "runs" / first_state["run_id"])

    # «Версия 2»: тот же комплект П из уже загруженных + загрузка обновлённого РД.
    created = client.post(f"/api/stage-comparison/objects/{OID}/comparison-versions", json={
        "label": "Версия 2", "stage_1": _existing("cv001", "stage_1"), "stage_2": NEW}).json()
    assert created["comparison_version"] == "cv002"
    workspace = comparison / "versions" / "cv002"
    scratch = tmp_path / "rd2"
    scratch.mkdir()
    gf._pdf(scratch / "РД2.pdf", "NEW")
    document = fitz.open(scratch / "РД2.pdf")
    document[0].insert_text((300, 380), "РД изм. 1", fontsize=8)
    document.save(scratch / "РД2-changed.pdf")
    document.close()
    gf._markdown(scratch / "РД2_results.md", "NEW")
    gf._blocks(scratch / "РД2_blocks.json", "NEW")
    payload = [((scratch / "РД2-changed.pdf").open("rb"), "РД2/РД2.pdf"),
               ((scratch / "РД2_results.md").open("rb"), "РД2/РД2_results.md"),
               ((scratch / "РД2_blocks.json").open("rb"), "РД2/РД2_blocks.json")]
    try:
        stage_upload.replace_stage_from_folder(cv.scope_id(OID, "cv002"), "stage_2", payload, "РД2", False)
    finally:
        for stream, _rel in payload:
            stream.close()

    second_session, second_pair, second_state = run(workspace / "stage_1", workspace / "stage_2")

    assert second_session["id"] != first_session["id"]
    assert second_session["comparison_version"] == "cv002"
    assert second_session["documents"]["stage_1"][0]["document_code"] == "П1"
    assert second_session["documents"]["stage_2"][0]["document_code"] == "РД2"
    assert first_session["documents"]["stage_2"][0]["document_code"] == "РД1"
    # Тот же П — те же байты (жёсткая ссылка), новый РД — другие.
    left_1, left_2 = first_session["documents"]["stage_1"][0], second_session["documents"]["stage_1"][0]
    assert os.stat(left_1["pdf_path"]).st_ino == os.stat(left_2["pdf_path"]).st_ino
    manifest = json.loads((tmp_path / "runtime" / "sessions" / second_session["id"] / "pairs" / second_pair["id"]
                           / "production" / "runs" / second_state["run_id"]
                           / "project_change_v3_source_manifest.json").read_text(encoding="utf-8"))
    assert manifest["new_pdf_sha256"] == hashlib.sha256((scratch / "РД2-changed.pdf").read_bytes()).hexdigest()

    # Результат Версии 1 на месте и не изменился.
    assert _tree_digest(runs_root / "runs" / first_state["run_id"]) == first_digest
    assert client.get(first_changes_url).json() == first_changes

    # Каталог и общий вид объекта разделены по версиям.
    catalog.clear_cache()
    for version, session_id in (("cv001", first_session["id"]), ("cv002", second_session["id"])):
        entries = catalog.cached_catalog(object_id=OID, comparison_version=version)["entries"]
        assert [(e["session_id"], e["comparison_version"]) for e in entries] == [(session_id, version)]
        envelope = client.get(f"/api/stage-comparison/objects/{OID}/project-changes?comparison_version={version}").json()
        assert {run["session_id"] for run in envelope["runs"]} == {session_id}
    keys = {e["pair_group_key"] for e in catalog.cached_catalog(object_id=OID, comparison_version=None)["entries"]}
    assert len(keys) == 2
