"""
test_resolve_project_dir_pdf_orphan_fix.py
------------------------------------------
Регрессия на системный баг: `resolve_project_dir()` при неуспешном резолве
возвращал несуществующий `direct = projects_dir / project_id`, а writer-ы
(`save_expert_review`, audit) потом молча создавали там `_output` на корне
объекта → orphan-папки (по `.pdf`-id и по полностью невалидным id вроде ВК3/75СГ).

Фикс:
  * `resolve_project_dir(..., must_exist=True)` бросает `ProjectNotResolvedError`,
    если путь не найден (вместо возврата несуществующего direct);
  * fallback со снятием суффикса `.pdf` (id `<база>.pdf` → реальный `<база>`);
  * `_output_dir(project_id, must_exist=True)` в knowledge_base_service не создаёт
    orphan на несуществующем пути.

Run:
    python -m pytest tests/test_resolve_project_dir_pdf_orphan_fix.py -v
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import backend.app.services.common.project_service as ps
from backend.app.services.common.project_service import (
    resolve_project_dir,
    ProjectNotResolvedError,
)


@pytest.fixture
def projects_dir(tmp_path, monkeypatch):
    """projects/ с дисциплиной AR и контейнером версий:
        AR/<base>(main)/<base>   ← реальный проект (V1)
    Реальной папки `<base>.pdf` НЕТ — это и есть orphan-паттерн.
    """
    p = tmp_path / "projects"
    p.mkdir()
    ar = p / "AR"
    ar.mkdir()
    base = "13АВ-РД-АР1.1-К3-К4"
    container = ar / f"{base}(main)"
    container.mkdir()
    v1 = container / base
    v1.mkdir()
    (v1 / "project_info.json").write_text(
        json.dumps({"project_id": base, "name": f"{base}.pdf", "pdf_file": "v1.pdf"},
                   ensure_ascii=False),
        encoding="utf-8",
    )
    (v1 / "v1.pdf").write_bytes(b"%PDF-1.4 v1")
    (container / "version_group.json").write_text(
        json.dumps({
            "schema_version": 1, "logical_project_id": base,
            "container": f"{base}(main)", "primary_version_id": "v1",
            "latest_version_id": "v1",
            "versions": [{"version_id": "v1", "version_no": 1, "label": "V1",
                          "folder": base}],
        }, ensure_ascii=False),
        encoding="utf-8",
    )

    monkeypatch.setattr(ps, "_get_projects_dir", lambda: p)
    monkeypatch.setattr(ps, "_PROJECT_DIRS_CACHE", [])
    monkeypatch.setattr(ps, "_PROJECT_DIRS_CACHE_TIME", 0.0)
    return p, base, v1


def test_resolve_plain_id_finds_container(projects_dir):
    p, base, v1 = projects_dir
    got = resolve_project_dir(base)
    assert got == v1
    assert got.exists()


def test_resolve_pdf_suffixed_id_finds_real_container(projects_dir):
    """`<base>.pdf` (id из version-имени V2) → реальный контейнер без `.pdf`,
    а НЕ orphan `projects/<base>.pdf`."""
    p, base, v1 = projects_dir
    got = resolve_project_dir(f"{base}.pdf")
    assert got == v1, f"ожидали контейнер {v1}, получили {got}"
    assert got.exists()
    # orphan-папка не должна была быть создана резолвом (read-only):
    assert not (p / f"{base}.pdf").exists()


def test_resolve_missing_id_must_exist_raises(projects_dir):
    p, base, v1 = projects_dir
    with pytest.raises(ProjectNotResolvedError):
        resolve_project_dir("totally-missing-xyz", must_exist=True)
    # никакой папки не создано
    assert not (p / "totally-missing-xyz").exists()


def test_resolve_missing_pdf_id_must_exist_raises(projects_dir):
    """`<missing>.pdf` без реального соответствия → ошибка (strip не нашёл)."""
    p, base, v1 = projects_dir
    with pytest.raises(ProjectNotResolvedError):
        resolve_project_dir("133_23-ГК-ВК3.pdf", must_exist=True)
    assert not (p / "133_23-ГК-ВК3.pdf").exists()


def test_resolve_missing_id_default_returns_direct_no_regression(projects_dir):
    """Без must_exist поведение прежнее: возвращается direct (несуществующий),
    но папка НЕ создаётся (это просто Path)."""
    p, base, v1 = projects_dir
    got = resolve_project_dir("totally-missing-xyz")
    assert got == p / "totally-missing-xyz"
    assert not got.exists()


def test_output_dir_writer_guard_raises_for_invalid_id(projects_dir, monkeypatch):
    """`_output_dir(project_id, must_exist=True)` (путь writer-а expert_review)
    бросает ошибку для невалидного id и НЕ создаёт orphan _output."""
    p, base, v1 = projects_dir
    import backend.app.services.knowledge_base.knowledge_base_service as kb
    with pytest.raises(ProjectNotResolvedError):
        kb._output_dir("133_23-ГК-ВК3", must_exist=True)
    assert not (p / "133_23-ГК-ВК3").exists()
    assert not (p / "133_23-ГК-ВК3" / "_output").exists()


def test_output_dir_valid_id_points_inside_project(projects_dir):
    """Для валидного id _output_dir указывает ВНУТРЬ проекта, не на корень."""
    p, base, v1 = projects_dir
    import backend.app.services.knowledge_base.knowledge_base_service as kb
    out = kb._output_dir(base, must_exist=True)
    assert out == v1 / "_output"
    # сам путь _output ещё может не существовать (создаётся при записи) —
    # важно, что он ВНУТРИ реального проекта, а не на корне объекта.
    assert out.parent == v1
