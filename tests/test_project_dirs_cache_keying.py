"""reserc.md #78 — кеш iter_project_dirs ключуется по projects_dir + инвалидация.

Раньше кеш был единым глобальным списком: при смене PROJECTS_DIR (тесты,
smoke-sandbox, override) отдавался чужой список ~30 сек. Теперь кеш валиден
только под тот projects_dir, под который построен; register_project/
set_project_section сбрасывают кеш.
"""
from __future__ import annotations

import json

from backend.app.services.common import project_service as ps


def _mk_project(d, name):
    p = d / name
    p.mkdir(parents=True)
    (p / "project_info.json").write_text(
        json.dumps({"project_id": name}), encoding="utf-8"
    )
    return p


def test_cache_keyed_by_projects_dir(tmp_path, monkeypatch):
    dir_a = tmp_path / "A"
    dir_b = tmp_path / "B"
    _mk_project(dir_a, "pa")
    _mk_project(dir_b, "pb")
    ps.invalidate_project_cache()

    monkeypatch.setattr(ps, "_get_projects_dir", lambda: dir_a)
    a = dict(ps.iter_project_dirs())
    assert "pa" in a and "pb" not in a

    # Сменили projects_dir — кеш НЕ должен отдать список A.
    monkeypatch.setattr(ps, "_get_projects_dir", lambda: dir_b)
    b = dict(ps.iter_project_dirs())
    assert "pb" in b and "pa" not in b
    ps.invalidate_project_cache()


def test_invalidate_resets_cache(tmp_path, monkeypatch):
    dir_a = tmp_path / "A"
    _mk_project(dir_a, "pa")
    monkeypatch.setattr(ps, "_get_projects_dir", lambda: dir_a)
    ps.invalidate_project_cache()
    assert "pa" in dict(ps.iter_project_dirs())
    # добавили проект — без invalidate кеш бы его не показал
    _mk_project(dir_a, "pb")
    ps.invalidate_project_cache()
    assert "pb" in dict(ps.iter_project_dirs())
    ps.invalidate_project_cache()


def test_register_project_invalidates_cache(tmp_path, monkeypatch):
    folder = tmp_path / "f"
    folder.mkdir()
    (folder / "doc.pdf").write_text("x", encoding="utf-8")
    monkeypatch.setattr(ps, "resolve_project_dir", lambda fid: folder)
    calls = []
    monkeypatch.setattr(ps, "invalidate_project_cache", lambda: calls.append(1))
    ps.register_project("f", "doc.pdf")
    assert calls, "register_project не сбросил кеш проектов"
