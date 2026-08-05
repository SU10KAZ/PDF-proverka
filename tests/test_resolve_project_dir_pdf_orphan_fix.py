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


# ─────────────────────────────────────────────────────────────────────────────
# Обратный `.pdf`-fallback: legacy-папка названа `<id>.pdf`, а project_id пришёл
# БЕЗ `.pdf` (из projects_v2 read-cutover). resolve должен найти `.pdf`-папку.
# ─────────────────────────────────────────────────────────────────────────────


@pytest.fixture
def projects_dir_pdf_folder(tmp_path, monkeypatch):
    """projects/ с дисциплиной KJ и flat-проектом, чья ПАПКА названа `<id>.pdf`:
        KJ/13АВ-РД-КЖ5.35.1-К1 V1.pdf/   ← реальная папка (с .pdf в имени)
    Эмулирует кейс продакшна: projects_v2 отдаёт id без `.pdf`.
    """
    p = tmp_path / "projects"
    p.mkdir()
    kj = p / "KJ"
    kj.mkdir()
    base_no_pdf = "13АВ-РД-КЖ5.35.1-К1 V1"
    folder = kj / f"{base_no_pdf}.pdf"
    folder.mkdir()
    (folder / "project_info.json").write_text(
        json.dumps({"project_id": f"{base_no_pdf}.pdf", "name": f"{base_no_pdf}.pdf"},
                   ensure_ascii=False),
        encoding="utf-8",
    )
    out = folder / "_output"
    out.mkdir()
    (out / "03_findings.json").write_text(
        json.dumps({"findings": [{"id": "F-001"}]}, ensure_ascii=False),
        encoding="utf-8",
    )

    monkeypatch.setattr(ps, "_get_projects_dir", lambda: p)
    monkeypatch.setattr(ps, "_PROJECT_DIRS_CACHE", [])
    monkeypatch.setattr(ps, "_PROJECT_DIRS_CACHE_TIME", 0.0)
    return p, base_no_pdf, folder


def test_reverse_fallback_no_pdf_id_finds_pdf_folder(projects_dir_pdf_folder):
    """id БЕЗ `.pdf` → находим реальную папку `<id>.pdf`."""
    p, base_no_pdf, folder = projects_dir_pdf_folder
    got = resolve_project_dir(base_no_pdf)
    assert got == folder, f"ожидали {folder}, получили {got}"
    assert (got / "_output" / "03_findings.json").exists()


def test_reverse_fallback_must_exist_resolves(projects_dir_pdf_folder):
    """must_exist=True тоже резолвит `.pdf`-папку (а не бросает ошибку)."""
    p, base_no_pdf, folder = projects_dir_pdf_folder
    got = resolve_project_dir(base_no_pdf, must_exist=True)
    assert got == folder


def test_reverse_fallback_pdf_id_still_works(projects_dir_pdf_folder):
    """project_id уже С `.pdf` продолжает резолвиться напрямую (без регресса)."""
    p, base_no_pdf, folder = projects_dir_pdf_folder
    got = resolve_project_dir(f"{base_no_pdf}.pdf")
    assert got == folder


def test_reverse_fallback_prefers_plain_folder_over_pdf(tmp_path, monkeypatch):
    """Если существует папка БЕЗ `.pdf` — берём её, а не `.pdf`-fallback."""
    p = tmp_path / "projects"
    p.mkdir()
    kj = p / "KJ"
    kj.mkdir()
    base = "13АВ-РД-КЖ-ДВЕ"
    plain = kj / base
    plain.mkdir()
    (plain / "project_info.json").write_text("{}", encoding="utf-8")
    pdf_folder = kj / f"{base}.pdf"   # «двойник» с .pdf — НЕ должен побеждать
    pdf_folder.mkdir()
    (pdf_folder / "project_info.json").write_text("{}", encoding="utf-8")

    monkeypatch.setattr(ps, "_get_projects_dir", lambda: p)
    monkeypatch.setattr(ps, "_PROJECT_DIRS_CACHE", [])
    monkeypatch.setattr(ps, "_PROJECT_DIRS_CACHE_TIME", 0.0)

    got = resolve_project_dir(base)
    assert got == plain, f"прямой путь должен иметь приоритет, получили {got}"


def test_reverse_fallback_ambiguous_pdf_folders_no_guess(tmp_path, monkeypatch):
    """`<id>.pdf` существует в ДВУХ дисциплинах → не угадываем (None-fallback)."""
    p = tmp_path / "projects"
    p.mkdir()
    base = "13АВ-РД-ДУБЛЬ"
    for disc in ("KJ", "OV"):
        d = p / disc
        d.mkdir()
        folder = d / f"{base}.pdf"
        folder.mkdir()
        (folder / "project_info.json").write_text("{}", encoding="utf-8")

    monkeypatch.setattr(ps, "_get_projects_dir", lambda: p)
    monkeypatch.setattr(ps, "_PROJECT_DIRS_CACHE", [])
    monkeypatch.setattr(ps, "_PROJECT_DIRS_CACHE_TIME", 0.0)

    # must_exist → прежняя ошибка (не угадали), без must_exist → direct (несущ.)
    with pytest.raises(ProjectNotResolvedError):
        resolve_project_dir(base, must_exist=True)
    got = resolve_project_dir(base)
    assert got == p / base and not got.exists()


def test_reverse_fallback_no_traversal(projects_dir_pdf_folder):
    """Path traversal через id невозможен: `../<id>` + суффикс не уводит наружу
    projects_dir (кандидат вне projects_dir отбрасывается)."""
    p, base_no_pdf, folder = projects_dir_pdf_folder
    # снаружи projects_dir кладём «приманку» с .pdf
    outside = p.parent / "evil"
    outside.mkdir()
    (outside / "x.pdf").mkdir()
    # id, который при добавлении .pdf указывал бы наружу
    got = resolve_project_dir("../evil/x")
    # fallback не должен вернуть путь вне projects_dir
    assert outside not in [got, *got.parents] or not got.exists(), (
        f"traversal-кандидат не должен приниматься, получили {got}"
    )


def test_reverse_fallback_missing_no_pdf_folder_no_regression(projects_dir_pdf_folder):
    """id без соответствия (нет ни папки, ни `<id>.pdf`) → прежнее поведение."""
    p, base_no_pdf, folder = projects_dir_pdf_folder
    with pytest.raises(ProjectNotResolvedError):
        resolve_project_dir("совсем-нет-такого", must_exist=True)
    got = resolve_project_dir("совсем-нет-такого")
    assert got == p / "совсем-нет-такого" and not got.exists()


def test_missing_projects_dir_must_exist_raises(tmp_path, monkeypatch):
    """projects_dir НЕ существует (cutover на projects_v2 — legacy `projects/`
    пуста) → `must_exist=True` обязан бросить, а не вернуть фантомный путь.

    Регрессия 500 на `POST /api/knowledge-base/upload-excel`: ранний
    `return direct` при несуществующем projects_dir делал `_pid_resolves()`
    в импорте Excel истинным для ЛЮБОГО стейл-project_id → импорт брал
    протухший id из скрытой ячейки вместо фолбэка на project_id из UI и падал
    в `save_expert_review` с «Папка проекта … не найдена».
    """
    gone = tmp_path / "projects"  # не создаём
    monkeypatch.setattr(ps, "_get_projects_dir", lambda: gone)
    monkeypatch.setattr(ps, "_PROJECT_DIRS_CACHE", [])
    monkeypatch.setattr(ps, "_PROJECT_DIRS_CACHE_TIME", 0.0)

    with pytest.raises(ProjectNotResolvedError):
        resolve_project_dir("AR/СТ26-01-14-АР1-4-2-РД_V1", must_exist=True)

    # без must_exist поведение прежнее: несуществующий direct, ничего не создано
    got = resolve_project_dir("AR/СТ26-01-14-АР1-4-2-РД_V1")
    assert got == gone / "AR/СТ26-01-14-АР1-4-2-РД_V1"
    assert not got.exists()
    assert not gone.exists()
