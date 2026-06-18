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
    return p


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
