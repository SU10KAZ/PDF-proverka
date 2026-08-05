"""
test_project_rename.py
----------------------
Тесты безопасного переименования папки проекта.

Сервис: backend/app/services/common/project_rename_service.py
Эндпоинт: PATCH /api/projects/{project_id}/rename

Run:
    python -m pytest tests/test_project_rename.py -v
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import backend.app.services.common.project_rename_service as prs  # noqa: E402
import backend.app.services.common.project_service as ps  # noqa: E402

_PDF = b"%PDF-1.4\n%doc\n%%EOF\n"


def _mk_flat_project(root: Path, name: str, section: str = "EOM") -> Path:
    d = root / name
    (d / "_output").mkdir(parents=True)
    (d / "project_info.json").write_text(
        json.dumps({"project_id": name, "name": name, "section": section,
                    "pdf_file": "document.pdf"}, ensure_ascii=False),
        encoding="utf-8",
    )
    (d / "document.pdf").write_bytes(_PDF)
    (d / "_output" / "03_findings.json").write_text(
        json.dumps({"findings": [{"id": f"F-{name}"}]}), encoding="utf-8",
    )
    return d


def _mk_container_project(root: Path, base: str, section: str = "AR") -> Path:
    """Контейнер `<base>(main)/` с V1 (folder=base) и V2 (folder='base V2')."""
    container = root / f"{base}(main)"
    v1 = container / base
    v2 = container / f"{base} V2"
    for vdir, vid in ((v1, "v1"), (v2, "v2")):
        (vdir / "_output").mkdir(parents=True)
        (vdir / "project_info.json").write_text(
            json.dumps({"project_id": base, "name": base, "section": section,
                        "version_id": vid, "pdf_file": "document.pdf"},
                       ensure_ascii=False),
            encoding="utf-8",
        )
        (vdir / "document.pdf").write_bytes(_PDF)
    (v1 / "_output" / "03_findings.json").write_text(
        json.dumps({"findings": [{"id": "F-V1"}]}), encoding="utf-8")
    (v2 / "_output" / "03_findings.json").write_text(
        json.dumps({"findings": [{"id": "F-V2"}]}), encoding="utf-8")
    (container / "version_group.json").write_text(
        json.dumps({
            "schema_version": 1, "logical_project_id": base,
            "container": f"{base}(main)", "primary_version_id": "v1",
            "latest_version_id": "v2",
            "versions": [
                {"version_id": "v1", "version_no": 1, "label": "V1", "folder": base},
                {"version_id": "v2", "version_no": 2, "label": "V2", "folder": f"{base} V2"},
            ],
        }, ensure_ascii=False),
        encoding="utf-8",
    )
    return v1


@pytest.fixture
def stores(tmp_path):
    """Tmp-копии всех keyed-сторов."""
    dec = tmp_path / "decisions_log.json"
    usage = tmp_path / "usage_data.json"
    groups = tmp_path / "project_groups.json"
    vault = tmp_path / "missing_norms_vault.json"
    rev = tmp_path / "rename.reverse.json"
    dec.write_text(json.dumps({"entries": []}), encoding="utf-8")
    usage.write_text(json.dumps({"records": []}), encoding="utf-8")
    groups.write_text(json.dumps({}), encoding="utf-8")
    vault.write_text(json.dumps({"version": 1, "norms": {}}), encoding="utf-8")
    return {"decisions_log_file": dec, "usage_data_file": usage,
            "project_groups_file": groups, "missing_norms_vault_file": vault,
            "reverse_log_file": rev}


@pytest.fixture
def projects_dir(tmp_path, monkeypatch):
    p = tmp_path / "projects"
    p.mkdir()
    monkeypatch.setattr(ps, "_get_projects_dir", lambda: p)
    monkeypatch.setattr(ps, "_PROJECT_DIRS_CACHE", [])
    monkeypatch.setattr(ps, "_PROJECT_DIRS_CACHE_TIME", 0.0)
    # Изолируем projects_v2 в tmp, чтобы rename-тесты не сканировали/не трогали
    # реальный prod projects_v2 (_default_v2_root читает этот env).
    monkeypatch.setenv("AUDIT_PROJECTS_V2_DIR", str(tmp_path / "projects_v2"))
    return p


def _mk_v2_doc(v2_root: Path, *, object_folder: str, object_id: str,
               discipline: str, doc_code: str, legacy_projects_root: Path,
               versions=True) -> Path:
    """Синтетический v2-документ projects_v2/.../documents/<doc_code>/document.json."""
    doc_dir = v2_root / "objects" / object_folder / "disciplines" / discipline / "documents" / doc_code
    doc_dir.mkdir(parents=True)
    (v2_root / "objects" / object_folder / "object.json").write_text(
        json.dumps({"object_id": object_id, "display_name": object_folder}), encoding="utf-8")
    dj = {
        "schema_version": 1,
        "document_code": doc_code,
        "object_id": object_id,
        "discipline": discipline,
        "kind": "plain",
        "legacy_project_name": doc_code,
        "legacy_project_path": str(legacy_projects_root / discipline / doc_code),
        "current_version": "v001",
    }
    if versions:
        dj["versions"] = [{"version_id": "v001", "version_no": 1, "label": "V1",
                           "legacy_folder_name": doc_code}]
    (doc_dir / "document.json").write_text(json.dumps(dj, ensure_ascii=False), encoding="utf-8")
    return doc_dir


def _rename(project_id, new_name, stores, **kw):
    return prs.rename_project(project_id, new_name, object_id="OBJ",
                              check_running=False, **stores, **kw)


# ─── Test 3: invalid rename rejected ────────────────────────────────────────
@pytest.mark.parametrize("bad", ["", "   ", "../bad", "bad/name", "bad\\name",
                                 ".", "..", ".hidden", "x" * 300, "name\x01ctl"])
def test_invalid_names_rejected(projects_dir, stores, bad):
    _mk_flat_project(projects_dir, "PROJ")
    with pytest.raises(prs.InvalidProjectNameError):
        _rename("PROJ", bad, stores)
    # filesystem не изменился: единственная папка — исходная PROJ
    assert [d.name for d in projects_dir.iterdir()] == ["PROJ"]


# ─── Test 4: rename conflict ────────────────────────────────────────────────
def test_rename_conflict(projects_dir, stores):
    _mk_flat_project(projects_dir, "PROJ_A")
    _mk_flat_project(projects_dir, "PROJ_B")
    with pytest.raises(prs.RenameConflictError):
        _rename("PROJ_A", "PROJ_B", stores)
    assert (projects_dir / "PROJ_A").exists()
    assert (projects_dir / "PROJ_B").exists()
    # A не переименован, B не тронут
    assert (projects_dir / "PROJ_A" / "_output" / "03_findings.json").exists()


# ─── Test 5: legacy flat folder rename ──────────────────────────────────────
def test_legacy_folder_rename(projects_dir, stores):
    _mk_flat_project(projects_dir, "OLD")
    # стор-данные на OLD
    Path(stores["decisions_log_file"]).write_text(json.dumps({"entries": [
        {"object_id": "OBJ", "source_project": "OLD", "item_id": "F-001"},
        {"object_id": "OTHER", "source_project": "OLD", "item_id": "F-002"},
    ]}), encoding="utf-8")
    Path(stores["usage_data_file"]).write_text(json.dumps({"records": [
        {"project_id": "OLD", "cost_usd": 1.0}]}), encoding="utf-8")
    Path(stores["project_groups_file"]).write_text(json.dumps({
        "OBJ": {"EOM": [{"project_ids": ["OLD", "KEEP"]}]}}), encoding="utf-8")
    Path(stores["missing_norms_vault_file"]).write_text(json.dumps({
        "version": 1, "norms": {"СП 1": {"occurrences": [
            {"project_id": "OLD", "findings": ["F-001"]}]}}}), encoding="utf-8")

    res = _rename("OLD", "NEW", stores)
    assert res["status"] == "renamed"
    assert res["project_id"] == "NEW"
    assert res["old_name"] == "OLD"
    assert res["new_name"] == "NEW"
    assert res["storage_layer"] == "legacy"

    # папки
    assert not (projects_dir / "OLD").exists()
    assert (projects_dir / "NEW").exists()
    # artifacts сохранены
    f = json.loads((projects_dir / "NEW" / "_output" / "03_findings.json").read_text())
    assert f["findings"][0]["id"] == "F-OLD"
    # project_info обновлён
    info = json.loads((projects_dir / "NEW" / "project_info.json").read_text())
    assert info["project_id"] == "NEW" and info["name"] == "NEW"

    # стор-ремап (scope по object_id=OBJ: запись OTHER не трогаем)
    dec = json.loads(Path(stores["decisions_log_file"]).read_text())["entries"]
    assert dec[0]["source_project"] == "NEW"
    assert dec[1]["source_project"] == "OLD"   # другой object_id — не тронут
    usage = json.loads(Path(stores["usage_data_file"]).read_text())["records"]
    assert usage[0]["project_id"] == "NEW"
    groups = json.loads(Path(stores["project_groups_file"]).read_text())
    assert groups["OBJ"]["EOM"][0]["project_ids"] == ["NEW", "KEEP"]
    vault = json.loads(Path(stores["missing_norms_vault_file"]).read_text())
    assert vault["norms"]["СП 1"]["occurrences"][0]["project_id"] == "NEW"
    # reverse-log записан
    assert Path(stores["reverse_log_file"]).exists()


# ─── Test 6: container (versioned) rename ───────────────────────────────────
def test_container_rename(projects_dir, stores):
    _mk_container_project(projects_dir, "BASE")
    Path(stores["decisions_log_file"]).write_text(json.dumps({"entries": [
        {"object_id": "OBJ", "source_project": "BASE", "item_id": "F-1"},
        {"object_id": "OBJ", "source_project": "BASE V2", "item_id": "F-2"},
    ]}), encoding="utf-8")

    res = _rename("BASE", "RENAMED", stores)
    assert res["storage_layer"] == "container"
    assert res["project_id"] == "RENAMED"

    # контейнер и папки версий переименованы
    assert not (projects_dir / "BASE(main)").exists()
    cont = projects_dir / "RENAMED(main)"
    assert cont.exists()
    assert (cont / "RENAMED").exists()
    assert (cont / "RENAMED V2").exists()
    # данные V1 и V2 сохранены
    assert json.loads((cont / "RENAMED" / "_output" / "03_findings.json").read_text())["findings"][0]["id"] == "F-V1"
    assert json.loads((cont / "RENAMED V2" / "_output" / "03_findings.json").read_text())["findings"][0]["id"] == "F-V2"
    # манифест обновлён
    man = json.loads((cont / "version_group.json").read_text())
    assert man["logical_project_id"] == "RENAMED"
    assert man["container"] == "RENAMED(main)"
    folders = {v["version_id"]: v["folder"] for v in man["versions"]}
    assert folders == {"v1": "RENAMED", "v2": "RENAMED V2"}
    # project_info каждой версии
    assert json.loads((cont / "RENAMED" / "project_info.json").read_text())["project_id"] == "RENAMED"
    assert json.loads((cont / "RENAMED V2" / "project_info.json").read_text())["name"] == "RENAMED"
    # стор-ремап обоих source_project (BASE и 'BASE V2')
    dec = json.loads(Path(stores["decisions_log_file"]).read_text())["entries"]
    assert {e["source_project"] for e in dec} == {"RENAMED", "RENAMED V2"}


# ─── Test 7: sibling project untouched ──────────────────────────────────────
def test_sibling_untouched(projects_dir, stores):
    _mk_flat_project(projects_dir, "TARGET")
    _mk_flat_project(projects_dir, "SIBLING")
    _rename("TARGET", "TARGET2", stores)
    assert (projects_dir / "SIBLING" / "_output" / "03_findings.json").exists()
    info = json.loads((projects_dir / "SIBLING" / "project_info.json").read_text())
    assert info["project_id"] == "SIBLING"


# ─── Test 8: path traversal guard ───────────────────────────────────────────
def test_path_traversal_guard(projects_dir, stores):
    _mk_flat_project(projects_dir, "P")
    sentinel = projects_dir.parent / "escape_target"
    with pytest.raises(prs.InvalidProjectNameError):
        _rename("P", "../escape_target", stores)
    assert (projects_dir / "P").exists()
    assert not sentinel.exists()


# ═══ Endpoint tests (HTTP codes + response contract) ═══════════════════════
@pytest.fixture
def client(projects_dir, stores, monkeypatch):
    # store-пути → tmp (не трогаем прод data)
    for attr, key in (("DECISIONS_LOG_FILE", "decisions_log_file"),
                      ("USAGE_DATA_FILE", "usage_data_file"),
                      ("PROJECT_GROUPS_FILE", "project_groups_file"),
                      ("MISSING_NORMS_VAULT_FILE", "missing_norms_vault_file")):
        monkeypatch.setattr(prs.config, attr, Path(stores[key]), raising=False)
    monkeypatch.setattr(prs.config, "APP_DATA_DIR", projects_dir.parent, raising=False)
    from backend.app.main import app
    return TestClient(app), projects_dir


def test_endpoint_404(client):
    c, _ = client
    r = c.patch("/api/projects/NOPE/rename", json={"name": "X"})
    assert r.status_code == 404


def test_endpoint_400_invalid(client):
    c, projects_dir = client
    _mk_flat_project(projects_dir, "P1")
    r = c.patch("/api/projects/P1/rename", json={"name": "bad/name"})
    assert r.status_code == 400
    assert (projects_dir / "P1").exists()


def test_endpoint_409_conflict(client):
    c, projects_dir = client
    _mk_flat_project(projects_dir, "A1")
    _mk_flat_project(projects_dir, "B1")
    r = c.patch("/api/projects/A1/rename", json={"name": "B1"})
    assert r.status_code == 409
    assert (projects_dir / "A1").exists() and (projects_dir / "B1").exists()


def test_endpoint_success_contract(client):
    c, projects_dir = client
    _mk_flat_project(projects_dir, "SRC")
    r = c.patch("/api/projects/SRC/rename", json={"name": "DST"})
    assert r.status_code == 200, r.text
    body = r.json()
    # контракт ответа для обновления состояния фронтенда
    for key in ("status", "project_id", "old_name", "new_name",
                "old_path", "new_path", "warnings"):
        assert key in body
    assert body["project_id"] == "DST"
    assert body["old_name"] == "SRC"
    assert body["new_name"] == "DST"
    assert not (projects_dir / "SRC").exists()
    assert (projects_dir / "DST").exists()


# ═══ projects_v2 shadow sync ════════════════════════════════════════════════
def _v2_root_for(projects_dir):
    return projects_dir.parent / "projects_v2"


def test_v2_rename_updates_document_json(projects_dir, stores):
    """Test 1: rename обновляет document_code/legacy_* в v2 document.json."""
    _mk_flat_project(projects_dir, "DOC1 V1", section="EOM")
    v2 = _v2_root_for(projects_dir)
    _mk_v2_doc(v2, object_folder="OBJ_F", object_id="OBJ", discipline="EOM",
               doc_code="DOC1 V1", legacy_projects_root=projects_dir)
    res = _rename("DOC1 V1", "DOC1", stores)
    assert res["v2_shadow"]["updated"], res["v2_shadow"]
    # папка документа переименована в новый код → document.json лежит там
    djp = v2 / "objects/OBJ_F/disciplines/EOM/documents/DOC1/document.json"
    assert djp.exists()
    dj = json.loads(djp.read_text())
    assert dj["document_code"] == "DOC1"
    assert dj["legacy_project_name"] == "DOC1"
    assert dj["legacy_project_path"].endswith("/EOM/DOC1")


def test_v2_rename_updates_version_refs(projects_dir, stores):
    """Test 2: versions[].legacy_folder_name обновляется."""
    _mk_flat_project(projects_dir, "DOC2 V1", section="AR")
    v2 = _v2_root_for(projects_dir)
    _mk_v2_doc(v2, object_folder="OBJ_F", object_id="OBJ", discipline="AR",
               doc_code="DOC2 V1", legacy_projects_root=projects_dir)
    _rename("DOC2 V1", "DOC2", stores)
    dj = json.loads((v2 / "objects/OBJ_F/disciplines/AR/documents/DOC2/document.json").read_text())
    assert dj["versions"][0]["legacy_folder_name"] == "DOC2"


def test_v2_missing_is_failsoft(projects_dir, stores):
    """Test 3: v2-shadow отсутствует → rename успешен с warning, без падения."""
    _mk_flat_project(projects_dir, "NOV2 V1")
    # v2 root не создаём
    res = _rename("NOV2 V1", "NOV2", stores)
    assert res["status"] == "renamed"
    assert (projects_dir / "NOV2").exists()
    assert any("projects_v2" in w for w in res["warnings"])


def test_v2_sibling_documents_untouched(projects_dir, stores):
    """Test 4: соседний v2-документ не тронут."""
    _mk_flat_project(projects_dir, "T1 V1", section="EOM")
    v2 = _v2_root_for(projects_dir)
    _mk_v2_doc(v2, object_folder="OBJ_F", object_id="OBJ", discipline="EOM",
               doc_code="T1 V1", legacy_projects_root=projects_dir)
    _mk_v2_doc(v2, object_folder="OBJ_F", object_id="OBJ", discipline="EOM",
               doc_code="SIB V1", legacy_projects_root=projects_dir)
    _rename("T1 V1", "T1", stores)
    sib = v2 / "objects/OBJ_F/disciplines/EOM/documents/SIB V1/document.json"
    assert sib.exists()
    assert json.loads(sib.read_text())["document_code"] == "SIB V1"


def test_v2_sync_rejects_bad_name(projects_dir):
    """Test 5: невалидное имя отвергается ДО любых fs-операций (path guard)."""
    v2 = _v2_root_for(projects_dir)
    _mk_v2_doc(v2, object_folder="OBJ_F", object_id="OBJ", discipline="EOM",
               doc_code="X V1", legacy_projects_root=projects_dir)
    with pytest.raises(prs.InvalidProjectNameError):
        prs.sync_v2_shadow_rename("X V1", "../escape", object_id="OBJ", v2_root=v2)
    # ничего не создано за пределами objects/
    assert not (v2.parent / "escape").exists()
    assert (v2 / "objects/OBJ_F/disciplines/EOM/documents/X V1/document.json").exists()


def test_v2_one_time_repair(projects_dir):
    """Test 6: repair обновляет устаревший v2-shadow + делает backup."""
    v2 = _v2_root_for(projects_dir)
    _mk_v2_doc(v2, object_folder="OBJ_F", object_id="OBJ", discipline="AR",
               doc_code="R1 V1", legacy_projects_root=projects_dir)
    # dry-run: ничего не пишет
    dr = prs.sync_v2_shadow_rename("R1 V1", "R1", object_id="OBJ", v2_root=v2, dry_run=True)
    assert dr["updated"] and dr["dry_run"] is True
    assert (v2 / "objects/OBJ_F/disciplines/AR/documents/R1 V1/document.json").exists()  # не тронут
    # apply
    res = prs.sync_v2_shadow_rename("R1 V1", "R1", object_id="OBJ", v2_root=v2)
    new_dj = v2 / "objects/OBJ_F/disciplines/AR/documents/R1/document.json"
    assert new_dj.exists()
    assert json.loads(new_dj.read_text())["document_code"] == "R1"
    assert (new_dj.with_name("document.json.rename_bak")).exists()  # backup создан


def test_v2_resolve_prefers_new_after_rename(projects_dir, stores):
    """Test 7: после rename adapter.find_document видит новое имя, не старое."""
    from backend.app.services.storage.projects_v2_adapter import ProjectsV2Adapter
    _mk_flat_project(projects_dir, "RES V1", section="EOM")
    v2 = _v2_root_for(projects_dir)
    _mk_v2_doc(v2, object_folder="OBJ_F", object_id="OBJ", discipline="EOM",
               doc_code="RES V1", legacy_projects_root=projects_dir)
    _rename("RES V1", "RES", stores)
    adapter = ProjectsV2Adapter(v2_root=v2)
    assert adapter.find_document("RES", object_id="OBJ") is not None
    assert adapter.find_document("RES V1", object_id="OBJ") is None


# ─── projects_v2-primary: rename без legacy-папки ──────────────────────────
def _mk_v2_version(doc_dir: Path, version_id: str = "v001", *, project_id: str) -> Path:
    """Наполнить версию v2-документа метаданными, которые ремапит rename."""
    vdir = doc_dir / "versions" / version_id
    (vdir / "01_input").mkdir(parents=True)
    (vdir / "04_review").mkdir(parents=True)
    (vdir / "01_input" / "project_info.json").write_text(
        json.dumps({"project_id": project_id, "name": project_id, "section": "AR"},
                   ensure_ascii=False), encoding="utf-8")
    (vdir / "04_review" / "expert_review.json").write_text(
        json.dumps({"project_id": project_id, "decisions": {"F-001": "accepted"}},
                   ensure_ascii=False), encoding="utf-8")
    return vdir


@pytest.fixture
def v2_primary(monkeypatch):
    monkeypatch.setenv("AUDIT_PROJECTS_V2_WRITE_MODE", "projects_v2_primary")


def test_v2_primary_rename_moves_doc_dir_without_legacy(projects_dir, stores, v2_primary):
    """В v2-primary rename работает по projects_v2, даже если legacy-папки нет."""
    v2 = _v2_root_for(projects_dir)
    doc_dir = _mk_v2_doc(v2, object_folder="OBJ_F", object_id="OBJ", discipline="AR",
                         doc_code="0000-OLD", legacy_projects_root=projects_dir)
    _mk_v2_version(doc_dir, project_id="0000-OLD")

    res = _rename("0000-OLD", "NEW-CODE", stores, v2_root=v2)

    assert res["storage_layer"] == "projects_v2"
    assert res["project_id"] == "NEW-CODE"
    new_dir = v2 / "objects/OBJ_F/disciplines/AR/documents/NEW-CODE"
    assert new_dir.is_dir() and not doc_dir.exists()
    assert json.loads((new_dir / "document.json").read_text())["document_code"] == "NEW-CODE"
    # метаданные версии переехали на новый project_id
    vdir = new_dir / "versions" / "v001"
    assert json.loads((vdir / "01_input" / "project_info.json").read_text())["project_id"] == "NEW-CODE"
    review = json.loads((vdir / "04_review" / "expert_review.json").read_text())
    assert review["project_id"] == "NEW-CODE"
    assert review["decisions"] == {"F-001": "accepted"}  # вердикты не тронуты


def test_v2_primary_rename_writes_backup_and_confirmation(projects_dir, stores, v2_primary):
    """Safety-контракт: снимок метаданных + append-only confirmation log."""
    v2 = _v2_root_for(projects_dir)
    _mk_v2_doc(v2, object_folder="OBJ_F", object_id="OBJ", discipline="AR",
               doc_code="0000-OLD", legacy_projects_root=projects_dir)

    res = _rename("0000-OLD", "NEW-CODE", stores, v2_root=v2)

    backup_dir = v2 / "_system" / "destructive_backups" / res["backup_id"]
    assert backup_dir.is_dir()
    saved = list(backup_dir.rglob("document.json"))
    assert saved, list(backup_dir.rglob("*"))
    assert json.loads(saved[0].read_text())["document_code"] == "0000-OLD"  # до-состояние
    conf = (v2 / "_system" / "destructive_confirmations.jsonl").read_text(encoding="utf-8")
    rows = [json.loads(l) for l in conf.splitlines() if l.strip()]
    assert any(r["op"] == "rename_project" and r["backup_id"] == res["backup_id"] for r in rows)


def test_v2_primary_rename_remaps_stores(projects_dir, stores, v2_primary):
    """decisions_log / usage_data / project_groups / vault переезжают на новый id."""
    v2 = _v2_root_for(projects_dir)
    _mk_v2_doc(v2, object_folder="OBJ_F", object_id="OBJ", discipline="AR",
               doc_code="0000-OLD", legacy_projects_root=projects_dir)
    Path(stores["decisions_log_file"]).write_text(json.dumps({"entries": [
        {"object_id": "OBJ", "source_project": "0000-OLD", "item_id": "F-001"},
        {"object_id": "OTHER", "source_project": "0000-OLD", "item_id": "F-002"},
    ]}), encoding="utf-8")
    Path(stores["usage_data_file"]).write_text(json.dumps({"records": [
        {"project_id": "0000-OLD", "cost_usd": 1.0}]}), encoding="utf-8")
    Path(stores["project_groups_file"]).write_text(json.dumps({
        "OBJ": {"AR": [{"project_ids": ["0000-OLD", "KEEP"]}]}}), encoding="utf-8")

    res = _rename("0000-OLD", "NEW-CODE", stores, v2_root=v2)

    assert res["stores"]["decisions_log"] == 1  # только свой object_id
    dec = json.loads(Path(stores["decisions_log_file"]).read_text())["entries"]
    assert dec[0]["source_project"] == "NEW-CODE"
    assert dec[1]["source_project"] == "0000-OLD"  # чужой объект не тронут
    usage = json.loads(Path(stores["usage_data_file"]).read_text())["records"]
    assert usage[0]["project_id"] == "NEW-CODE"
    groups = json.loads(Path(stores["project_groups_file"]).read_text())
    assert groups["OBJ"]["AR"][0]["project_ids"] == ["NEW-CODE", "KEEP"]


def test_v2_primary_rename_conflict_leaves_everything_intact(projects_dir, stores, v2_primary):
    """Занятое имя → 409 и НИ одной мутации (папка, backup, confirmation)."""
    v2 = _v2_root_for(projects_dir)
    _mk_v2_doc(v2, object_folder="OBJ_F", object_id="OBJ", discipline="AR",
               doc_code="0000-OLD", legacy_projects_root=projects_dir)
    _mk_v2_doc(v2, object_folder="OBJ_F", object_id="OBJ", discipline="AR",
               doc_code="TAKEN", legacy_projects_root=projects_dir)

    with pytest.raises(prs.RenameConflictError):
        _rename("0000-OLD", "TAKEN", stores, v2_root=v2)

    docs = v2 / "objects/OBJ_F/disciplines/AR/documents"
    assert (docs / "0000-OLD" / "document.json").exists()
    assert json.loads((docs / "TAKEN" / "document.json").read_text())["document_code"] == "TAKEN"
    assert not (v2 / "_system" / "destructive_backups").exists()
    assert not (v2 / "_system" / "destructive_confirmations.jsonl").exists()


def test_v2_primary_rename_unknown_project_is_404(projects_dir, stores, v2_primary):
    v2 = _v2_root_for(projects_dir)
    _mk_v2_doc(v2, object_folder="OBJ_F", object_id="OBJ", discipline="AR",
               doc_code="0000-OLD", legacy_projects_root=projects_dir)
    with pytest.raises(prs.ProjectNotFoundError):
        _rename("NOPE", "NEW-CODE", stores, v2_root=v2)


def test_v2_primary_rename_updates_old_to_new_map(projects_dir, stores, v2_primary):
    """old_to_new_map: document_code + v2-пути переезжают вместе с папкой."""
    v2 = _v2_root_for(projects_dir)
    doc_dir = _mk_v2_doc(v2, object_folder="OBJ_F", object_id="OBJ", discipline="AR",
                         doc_code="0000-OLD", legacy_projects_root=projects_dir)
    map_path = v2 / "_system" / "old_to_new_map.json"
    map_path.parent.mkdir(parents=True, exist_ok=True)
    map_path.write_text(json.dumps({"migrations": [
        {"object_id": "OBJ", "discipline": "AR", "document_code": "0000-OLD",
         "v2_document_dir": str(doc_dir),
         "files": [{"old_path": "/tmp/x.pdf", "new_path": str(doc_dir / "versions/v001/01_input/x.pdf")}]},
        {"object_id": "OBJ", "discipline": "AR", "document_code": "OTHER",
         "v2_document_dir": str(doc_dir.parent / "OTHER"), "files": []},
    ]}, ensure_ascii=False), encoding="utf-8")

    _rename("0000-OLD", "NEW-CODE", stores, v2_root=v2)

    migs = json.loads(map_path.read_text())["migrations"]
    assert migs[0]["document_code"] == "NEW-CODE"
    assert migs[0]["v2_document_dir"].endswith("/documents/NEW-CODE")
    assert "/documents/NEW-CODE/versions/v001/01_input/x.pdf" in migs[0]["files"][0]["new_path"]
    assert migs[0]["files"][0]["old_path"] == "/tmp/x.pdf"  # legacy-история не переписана
    assert migs[1]["document_code"] == "OTHER"  # чужая запись не тронута


def test_v2_primary_rename_same_name_is_noop(projects_dir, stores, v2_primary):
    v2 = _v2_root_for(projects_dir)
    _mk_v2_doc(v2, object_folder="OBJ_F", object_id="OBJ", discipline="AR",
               doc_code="SAME", legacy_projects_root=projects_dir)
    res = _rename("SAME", "SAME", stores, v2_root=v2)
    assert res["warnings"] == ["Имя не изменилось"]
    assert not (v2 / "_system" / "destructive_backups").exists()
