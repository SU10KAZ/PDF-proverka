"""reserc.md #77 — сбой записи манифеста версии больше не глотается.

_write_*manifest логируют исключение и возвращают False; create_next_version/
delete_version/promote_to_container поднимают VersionFileError при False вместо
молчаливого продолжения с неконсистентным манифестом.
"""
from __future__ import annotations

import pytest

from backend.app.services.common import version_service as vs


def test_write_group_manifest_failure_returns_false(tmp_path):
    # parent — файл → mkdir рейзит OSError → False (и пишет в лог).
    afile = tmp_path / "afile"
    afile.write_text("x", encoding="utf-8")
    bad_container = afile / "sub"
    assert vs._write_group_manifest(bad_container, {"a": 1}) is False


def test_write_manifest_failure_returns_false(tmp_path):
    afile = tmp_path / "f2"
    afile.write_text("x", encoding="utf-8")
    assert vs._write_manifest(afile / "deep", {"a": 1}) is False


def test_promote_raises_on_manifest_write_failure(tmp_path, monkeypatch):
    proj = tmp_path / "proj"
    proj.mkdir()
    (proj / "project_info.json").write_text("{}", encoding="utf-8")
    # Запись манифеста "падает" → промоут должен поднять VersionFileError,
    # а не вернуть полу-созданный контейнер молча.
    monkeypatch.setattr(vs, "_write_group_manifest", lambda *a, **k: False)
    with pytest.raises(vs.VersionFileError):
        vs.promote_to_container(proj, "proj")
