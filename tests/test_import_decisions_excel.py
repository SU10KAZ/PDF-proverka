"""Регрессия: импорт решений из Excel со стейл-метками версии в project_id.

Инцидент 2026-06-19: инженеры получали 500 «Ошибка импорта: Project directory
not resolved for project_id='KM/1232-ЧМ-КМ-1 V1'». В скрытую ячейку project_id
старого/внешнего экспорта был запечён композит «<дисциплина>/<имя> V1», который
не резолвится (реальная папка — «KM/1232-ЧМ-КМ-1», без метки версии). Импорт
слепо доверял ячейке и не падал назад ни на default_project_id из UI, ни на
вариант без метки версии.
"""
import openpyxl
import pytest

import backend.app.services.common.project_service as project_service
import backend.app.services.knowledge_base.knowledge_base_service as kb


@pytest.fixture
def fake_projects(tmp_path, monkeypatch):
    """Tmp-объект (его корень = projects_dir) с проектом KM/1232-ЧМ-КМ-1.

    Зеркалит прод: `_get_projects_dir()` отдаёт КОРЕНЬ объекта (из objects.json
    current_id), а дисциплины (KM) — его прямые подпапки.
    """
    projects_dir = tmp_path / "214. Obj"  # корень объекта = projects_dir
    proj = projects_dir / "KM" / "1232-ЧМ-КМ-1"
    (proj / "_output").mkdir(parents=True)
    (proj / "project_info.json").write_text(
        '{"project_id": "1232-ЧМ-КМ-1", "name": "1232-ЧМ-КМ-1", "section": "KM"}',
        encoding="utf-8",
    )
    (proj / "_output" / "03_findings.json").write_text(
        '{"findings": [{"id": "F-001", "severity": "КРИТИЧЕСКОЕ", "problem": "x"}]}',
        encoding="utf-8",
    )

    monkeypatch.setattr(project_service, "_get_projects_dir", lambda: projects_dir)
    monkeypatch.setattr(project_service, "_PROJECT_DIRS_CACHE", [], raising=False)
    monkeypatch.setattr(project_service, "_PROJECT_DIRS_CACHE_TIME", 0, raising=False)
    monkeypatch.setattr(kb, "DECISIONS_LOG_FILE", tmp_path / "decisions_log.json")
    return proj


def _make_xlsx(path, hidden_pid):
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "KM-1232-ЧМ-КМ-1 V1"
    ws.append(["№", "ID", "Решение эксперта", "Причина отклонения", "project_id"])
    ws.append(["", "", "", "", hidden_pid])  # строка 2 — скрытая ячейка project_id
    ws.append([1, "F-001", "Принято", "", ""])
    wb.save(path)


def test_stale_version_label_falls_back_to_ui_default(fake_projects, tmp_path):
    """Стейл «KM/<id> V1» в ячейке + валидный default из UI → default выигрывает."""
    xlsx = tmp_path / "decisions.xlsx"
    _make_xlsx(xlsx, hidden_pid="KM/1232-ЧМ-КМ-1 V1")

    results = kb.import_decisions_from_excel(str(xlsx), default_project_id="1232-ЧМ-КМ-1")

    assert "1232-ЧМ-КМ-1" in results
    assert results["1232-ЧМ-КМ-1"]["saved"] == 1
    assert results["1232-ЧМ-КМ-1"]["accepted"] == 1
    assert (fake_projects / "_output" / "expert_review.json").exists()


def test_stale_version_label_stripped_when_no_default(fake_projects, tmp_path):
    """Только стейл «KM/<id> V1» (default нет) → метка версии снимается, резолвится."""
    xlsx = tmp_path / "decisions.xlsx"
    _make_xlsx(xlsx, hidden_pid="KM/1232-ЧМ-КМ-1 V1")

    results = kb.import_decisions_from_excel(str(xlsx), default_project_id=None)

    assert "KM/1232-ЧМ-КМ-1" in results
    assert results["KM/1232-ЧМ-КМ-1"]["saved"] == 1
    assert (fake_projects / "_output" / "expert_review.json").exists()


def test_valid_hidden_cell_still_wins(fake_projects, tmp_path):
    """Валидная скрытая ячейка важнее default (per-sheet идентичность отчётов)."""
    xlsx = tmp_path / "decisions.xlsx"
    _make_xlsx(xlsx, hidden_pid="1232-ЧМ-КМ-1")

    results = kb.import_decisions_from_excel(str(xlsx), default_project_id="bogus-does-not-exist")

    assert "1232-ЧМ-КМ-1" in results
    assert results["1232-ЧМ-КМ-1"]["saved"] == 1


def test_nothing_resolves_is_graceful_not_500(fake_projects, tmp_path):
    """Ничего не резолвится → пустой результат, без исключения (не 500)."""
    xlsx = tmp_path / "decisions.xlsx"
    _make_xlsx(xlsx, hidden_pid="KM/totally-unknown V9")

    results = kb.import_decisions_from_excel(str(xlsx), default_project_id="also-unknown")

    assert results == {}  # ни одна папка не найдена → ничего не сохранили, но не упали
