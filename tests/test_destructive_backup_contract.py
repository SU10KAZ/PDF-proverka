from __future__ import annotations

import hashlib
import json
import shutil
from pathlib import Path

from backend.app.services.storage.storage_write_facade import V2Target
from backend.app.services.storage.v2_primary_wiring import (
    backup_version_before_destructive,
    restore_from_backup_id,
)


def _tree_digest(root: Path) -> str:
    h = hashlib.sha256()
    for p in sorted(root.rglob("*")):
        rel = p.relative_to(root).as_posix()
        h.update(rel.encode("utf-8"))
        if p.is_dir():
            h.update(b"/\0")
        else:
            h.update(b"\0")
            h.update(p.read_bytes())
    return h.hexdigest()


def _make_version(v2_root: Path, target: V2Target) -> Path:
    vdir = target.version_dir(v2_root)
    (vdir / "01_input").mkdir(parents=True, exist_ok=True)
    (vdir / "02_work").mkdir(parents=True, exist_ok=True)
    (vdir / "03_analysis" / "latest").mkdir(parents=True, exist_ok=True)
    (vdir / "03_analysis" / "runs" / "run-1").mkdir(parents=True, exist_ok=True)
    (vdir / "01_input" / "DOC-BACK.pdf").write_bytes(b"%PDF-1.4 backup")
    (vdir / "02_work" / "document.md").write_text("# document\n", encoding="utf-8")
    (vdir / "03_analysis" / "latest" / "03_findings.json").write_text(
        json.dumps({"findings": [{"id": "F-1"}]}), encoding="utf-8",
    )
    (vdir / "03_analysis" / "runs" / "run-1" / "pipeline_log.json").write_text(
        json.dumps({"status": "done"}), encoding="utf-8",
    )
    (vdir / "version.json").write_text(
        json.dumps({"version_id": "v001", "project_info": {"name": "DOC-BACK"}}),
        encoding="utf-8",
    )
    return vdir


def test_backup_version_before_destructive_copies_whole_version(tmp_path):
    v2 = tmp_path / "projects_v2"
    target = V2Target("OBJ", "EOM", "DOC-BACK", "v001")
    vdir = _make_version(v2, target)

    backup_id = backup_version_before_destructive(target, v2, "clean_project_data")
    backup_dir = v2 / "_system" / "destructive_backups" / backup_id

    assert backup_dir.is_dir()
    assert "DOC-BACK" in backup_id
    assert _tree_digest(backup_dir) == _tree_digest(vdir)


def test_restore_from_backup_id_restores_version_byte_for_byte(tmp_path):
    v2 = tmp_path / "projects_v2"
    target = V2Target("OBJ", "EOM", "DOC-BACK", "v001")
    vdir = _make_version(v2, target)
    backup_id = backup_version_before_destructive(target, v2, "clean_project_data")
    backup_dir = v2 / "_system" / "destructive_backups" / backup_id
    expected = _tree_digest(backup_dir)

    shutil.rmtree(vdir / "03_analysis")
    (vdir / "mutated.txt").write_text("new data", encoding="utf-8")

    result = restore_from_backup_id(target, v2, backup_id)

    assert result["backup_id"] == backup_id
    assert result["pre_restore_backup_id"]
    assert (v2 / "_system" / "destructive_backups" / result["pre_restore_backup_id"]).is_dir()
    assert _tree_digest(vdir) == expected
