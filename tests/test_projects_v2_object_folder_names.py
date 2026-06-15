"""
Тесты человекочитаемых имён папок объектов projects_v2.

Гермётичны: синтетическое legacy-дерево + projects_v2 в tmp_path. Реальные
projects/ не трогаются.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

_SCRIPTS = Path(__file__).resolve().parents[1] / "scripts" / "projects_v2"
sys.path.insert(0, str(_SCRIPTS))
import v2lib                       # noqa: E402
import rename_object_folders as rof  # noqa: E402


# ---------------------------------------------------------------------------
# make_object_folder_name
# ---------------------------------------------------------------------------


def test_name_alia():
    assert v2lib.make_object_folder_name("214. Alia (ASTERUS)", "73a0e59a") == "214_Alia_ASTERUS"


def test_name_mosfilmovskaya_kingsons():
    assert v2lib.make_object_folder_name(
        '213. Мосфильмовская 31А "King&Sons"', "0b540226") == "213_Mosfilmovskaya_31A_KingSons"


def test_name_collapses_and_strips():
    assert v2lib.make_object_folder_name("  ...Foo   Bar...  ", "x") == "Foo_Bar"


def test_name_empty_falls_back_to_obj():
    assert v2lib.make_object_folder_name("", "abcd1234") == "obj_abcd1234"


def test_name_is_fs_safe():
    n = v2lib.make_object_folder_name('A/B\\C:"D"<E>', "z")
    assert "/" not in n and "\\" not in n and '"' not in n


# ---------------------------------------------------------------------------
# миграция создаёт читаемую папку (без obj_), object_id в object.json
# ---------------------------------------------------------------------------


def _make_legacy_plain(disc_dir: Path, base: str) -> Path:
    proj = disc_dir / base
    proj.mkdir(parents=True)
    (proj / f"{base}.pdf").write_text("%PDF", encoding="utf-8")
    (proj / f"{base}_document.md").write_text("# md", encoding="utf-8")
    (proj / f"{base}_ocr.html").write_text("<html>", encoding="utf-8")
    (proj / f"{base}_result.json").write_text("{}", encoding="utf-8")
    (proj / "project_info.json").write_text("{}", encoding="utf-8")
    (proj / "_output").mkdir()
    (proj / "_output" / "03_findings.json").write_text("{}", encoding="utf-8")
    return proj


def test_migration_creates_readable_folder(tmp_path):
    legacy = tmp_path / "projects"
    disc = legacy / "214. Alia (ASTERUS)" / "EOM"
    proj = _make_legacy_plain(disc, "PROJ")
    v2 = tmp_path / "projects_v2"
    objects_map = {"by_name": {"214. Alia (ASTERUS)": "73a0e59a"}, "by_path": {}, "by_id": {}}
    result = v2lib.migrate_project(proj, v2, objects_map=objects_map, run_id="run_test")

    # папка объекта читаемая, без obj_
    obj_dirs = [d.name for d in (v2 / "objects").iterdir() if d.is_dir()]
    assert obj_dirs == ["214_Alia_ASTERUS"]
    assert not any(n.startswith("obj_") for n in obj_dirs)

    # object_id сохранён в object.json + display_name + folder_name
    meta = json.loads((v2 / "objects" / "214_Alia_ASTERUS" / "object.json").read_text(encoding="utf-8"))
    assert meta["object_id"] == "73a0e59a"
    assert meta["display_name"] == "214. Alia (ASTERUS)"
    assert meta["folder_name"] == "214_Alia_ASTERUS"

    # document_dir_in_v2 находит этот же путь (через resolve по display_name)
    dd = v2lib.document_dir_in_v2(v2, "73a0e59a", "EOM", "PROJ", display_name="214. Alia (ASTERUS)")
    assert dd == v2 / "objects" / "214_Alia_ASTERUS" / "disciplines" / "EOM" / "documents" / "PROJ"
    assert (dd / "document.json").exists()


# ---------------------------------------------------------------------------
# rename script
# ---------------------------------------------------------------------------


def _make_obj_folder(v2: Path, folder: str, object_id: str, display_name: str) -> Path:
    d = v2 / "objects" / folder
    (d / "disciplines").mkdir(parents=True)
    (d / "object.json").write_text(json.dumps({
        "schema_version": 1, "object_id": object_id,
        "legacy_name": display_name, "legacy_path": f"/legacy/{display_name}",
    }, ensure_ascii=False), encoding="utf-8")
    return d


def test_rename_dry_run_changes_nothing(tmp_path):
    v2 = tmp_path / "projects_v2"
    v2lib.ensure_v2_skeleton(v2)
    _make_obj_folder(v2, "obj_73a0e59a", "73a0e59a", "214. Alia (ASTERUS)")
    plan, conflicts = rof.plan_renames(v2)
    assert conflicts == []
    assert {p["old"]: p["new"] for p in plan} == {"obj_73a0e59a": "214_Alia_ASTERUS"}
    # dry-run через main() ничего не переименовывает
    rc = rof.main(["--dry-run", "--v2-root", str(v2)])
    assert rc == 0
    assert (v2 / "objects" / "obj_73a0e59a").exists()
    assert not (v2 / "objects" / "214_Alia_ASTERUS").exists()


def test_rename_execute_and_update_map(tmp_path):
    v2 = tmp_path / "projects_v2"
    v2lib.ensure_v2_skeleton(v2)
    _make_obj_folder(v2, "obj_73a0e59a", "73a0e59a", "214. Alia (ASTERUS)")
    # old_to_new_map с путём на старую папку
    map_path = v2 / "_system" / "old_to_new_map.json"
    map_path.write_text(json.dumps({"schema_version": 1, "migrations": [{
        "object_id": "73a0e59a", "document_code": "X",
        "v2_document_dir": str(v2 / "objects" / "obj_73a0e59a" / "disciplines" / "EOM" / "documents" / "X"),
        "files": [{"old_path": "/legacy/x", "new_path": str(v2 / "objects" / "obj_73a0e59a" / "v.json"), "sha256": None}],
    }]}, ensure_ascii=False), encoding="utf-8")

    rc = rof.main(["--execute", "--v2-root", str(v2)])
    assert rc == 0
    assert not (v2 / "objects" / "obj_73a0e59a").exists()
    assert (v2 / "objects" / "214_Alia_ASTERUS").exists()
    # object.json обновлён folder_name
    meta = json.loads((v2 / "objects" / "214_Alia_ASTERUS" / "object.json").read_text(encoding="utf-8"))
    assert meta["folder_name"] == "214_Alia_ASTERUS"
    # old_to_new_map больше не указывает на obj_*
    map_text = map_path.read_text(encoding="utf-8")
    assert "obj_73a0e59a" not in map_text
    assert "214_Alia_ASTERUS" in map_text


def test_rename_conflict_stops_execute(tmp_path):
    v2 = tmp_path / "projects_v2"
    v2lib.ensure_v2_skeleton(v2)
    # два obj_* с одинаковым display_name -> одинаковое new_name -> конфликт
    _make_obj_folder(v2, "obj_aaa", "aaa", "100. Dup Name")
    _make_obj_folder(v2, "obj_bbb", "bbb", "100. Dup Name")
    plan, conflicts = rof.plan_renames(v2)
    assert conflicts  # обнаружен конфликт
    rc = rof.main(["--execute", "--v2-root", str(v2)])
    assert rc == 1
    # ничего не переименовано
    assert (v2 / "objects" / "obj_aaa").exists()
    assert (v2 / "objects" / "obj_bbb").exists()


def test_rename_does_not_touch_legacy(tmp_path):
    legacy = tmp_path / "projects"
    disc = legacy / "214. Alia (ASTERUS)" / "EOM"
    _make_legacy_plain(disc, "PROJ")
    before = {str(p.relative_to(legacy)): p.stat().st_mtime
              for p in legacy.rglob("*") if p.is_file()}
    v2 = tmp_path / "projects_v2"
    v2lib.ensure_v2_skeleton(v2)
    _make_obj_folder(v2, "obj_73a0e59a", "73a0e59a", "214. Alia (ASTERUS)")
    rof.main(["--execute", "--v2-root", str(v2)])
    after = {str(p.relative_to(legacy)): p.stat().st_mtime
             for p in legacy.rglob("*") if p.is_file()}
    assert before == after  # legacy не тронут
