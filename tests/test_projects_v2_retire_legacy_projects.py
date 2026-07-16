from __future__ import annotations

import json
import sys
import tarfile
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts" / "projects_v2"))
import retire_legacy_projects as retire  # noqa: E402


def _source(tmp_path: Path) -> Path:
    root = tmp_path / "projects"
    (root / "obj" / "AR" / "doc" / "_output").mkdir(parents=True)
    (root / "obj" / "AR" / "doc" / "a.pdf").write_bytes(b"pdf")
    (root / "obj" / "AR" / "doc" / "_output" / "03_findings.json").write_text(
        '{"findings": []}', encoding="utf-8")
    (root / "obj" / "AR" / "doc" / "latest.json").symlink_to(
        root / "obj" / "AR" / "doc" / "_output" / "03_findings.json")
    return root


def test_manifest_detects_source_change(tmp_path):
    source = _source(tmp_path)
    manifest = retire.build_manifest(source)
    retire.verify_source_unchanged(manifest, source)
    (source / "obj" / "AR" / "doc" / "a.pdf").write_bytes(b"changed")
    with pytest.raises(RuntimeError, match="changed after"):
        retire.verify_source_unchanged(manifest, source)


def test_directory_backup_is_verified_byte_for_byte(tmp_path):
    source = _source(tmp_path)
    manifest = retire.build_manifest(source)
    backup = tmp_path / "external" / "projects"
    import shutil
    shutil.copytree(source, backup, symlinks=True)
    detail = retire.verify_directory(manifest, backup)
    assert detail["backup_kind"] == "directory"
    (backup / "obj" / "AR" / "doc" / "a.pdf").write_bytes(b"bad")
    with pytest.raises(RuntimeError, match="mismatch"):
        retire.verify_directory(manifest, backup)


def test_tar_backup_is_verified_without_extracting(tmp_path):
    source = _source(tmp_path)
    manifest = retire.build_manifest(source)
    archive = tmp_path / "projects.tgz"
    with tarfile.open(archive, "w:gz") as tf:
        tf.add(source, arcname="projects")
    detail = retire.verify_tar(manifest, archive)
    assert detail["backup_kind"] == "tar"
    assert len(detail["backup_content_sha256"]) == 64


def test_tampered_manifest_is_rejected(tmp_path):
    source = _source(tmp_path)
    obj = retire.build_manifest(source)
    obj["files"][0]["bytes"] += 1
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(obj), encoding="utf-8")
    with pytest.raises(RuntimeError, match="content_id"):
        retire.load_manifest(path)


def test_wrong_confirmation_is_refused_before_preflight(tmp_path):
    with pytest.raises(RuntimeError, match="--confirm"):
        retire.execute_retirement(tmp_path / "m", tmp_path / "r", "NO")
