"""Очистка старых релизов после успешной выкатки центра."""
from __future__ import annotations

import os
import stat
from pathlib import Path

import pytest

from scripts import deploy_center_release as dcr


def _release(root: Path, name: str, mtime: float, readonly: bool = False) -> Path:
    path = root / name
    (path / "venv" / "bin").mkdir(parents=True)
    (path / "venv" / "bin" / "python").write_text("x")
    if readonly:
        for item in (path / "venv" / "bin", path / "venv"):
            item.chmod(stat.S_IRUSR | stat.S_IXUSR)
    os.utime(path, (mtime, mtime))
    return path


@pytest.fixture(autouse=True)
def _no_processes(monkeypatch):
    monkeypatch.setattr(dcr, "_release_dirs_in_use", lambda _root: set())


def test_keeps_current_and_previous_removes_rest(tmp_path):
    old1 = _release(tmp_path, "r1", 100, readonly=True)
    old2 = _release(tmp_path, "r2", 200)
    prev = _release(tmp_path, "r3", 300)
    cur = _release(tmp_path, "r4", 400)
    removed = dcr.prune_old_releases(tmp_path, current=cur, previous=prev, keep=2)
    assert sorted(removed) == ["r1", "r2"]
    assert not old1.exists() and not old2.exists()
    assert cur.exists() and prev.exists()


def test_previous_is_kept_even_if_not_newest_by_mtime(tmp_path):
    prev = _release(tmp_path, "rollback", 100)
    other = _release(tmp_path, "undeployed", 300)
    cur = _release(tmp_path, "cur", 400)
    removed = dcr.prune_old_releases(tmp_path, current=cur, previous=prev, keep=2)
    assert removed == ["undeployed"]
    assert prev.exists()


def test_releases_newer_than_current_are_kept(tmp_path):
    prev = _release(tmp_path, "prev", 100)
    cur = _release(tmp_path, "cur", 200)
    pending = _release(tmp_path, "pending", 300)
    assert dcr.prune_old_releases(tmp_path, current=cur, previous=prev, keep=2) == []
    assert pending.exists()


def test_release_in_use_is_kept(tmp_path, monkeypatch):
    busy = _release(tmp_path, "busy", 50)
    prev = _release(tmp_path, "prev", 100)
    cur = _release(tmp_path, "cur", 200)
    monkeypatch.setattr(dcr, "_release_dirs_in_use", lambda _root: {busy.resolve()})
    assert dcr.prune_old_releases(tmp_path, current=cur, previous=prev, keep=2) == []


def test_symlinks_and_zero_keep_are_left_alone(tmp_path):
    old = _release(tmp_path, "old", 50)
    cur = _release(tmp_path, "cur", 200)
    (tmp_path / "link").symlink_to(old)
    assert dcr.prune_old_releases(tmp_path, current=cur, previous=None, keep=0) == []
    assert dcr.prune_old_releases(tmp_path, current=cur, previous=None, keep=2) == []
    assert old.exists()
    assert dcr.prune_old_releases(tmp_path, current=cur, previous=None, keep=1) == ["old"]
