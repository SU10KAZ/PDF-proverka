"""reserc.md #41 — generate_excel_report использует version-aware обход проектов.

Прежняя ручная рекурсия для контейнера `<база>(main)/` выдавала ОБЕ версии как
отдельные проекты (двойной счёт + не та папка). Теперь делегирует каноническому
project_service.iter_project_dirs (одна primary-версия на контейнер).
"""
from __future__ import annotations

from pathlib import Path

import backend.app.pipeline.stages.report.generate_excel_report as ger
from backend.app.services.common import project_service


def test_iter_delegates_to_canonical(monkeypatch):
    monkeypatch.setattr(
        project_service, "iter_project_dirs",
        lambda *a, **k: [("EOM/p1", Path("/x/p1")), ("AR/p2", Path("/x/p2"))],
    )
    out = ger._iter_project_dirs("ignored-root")
    # делегирует канону, пути приведены к str
    assert out == [("EOM/p1", "/x/p1"), ("AR/p2", "/x/p2")]


def test_iter_returns_one_entry_per_container(monkeypatch):
    # Канон уже схлопывает контейнер до primary — отчёт не должен видеть дубли версий.
    monkeypatch.setattr(
        project_service, "iter_project_dirs",
        lambda *a, **k: [("EOM/13АВ-ЭМ1", Path("/p/13АВ-ЭМ1(main)/13АВ-ЭМ1"))],
    )
    out = ger._iter_project_dirs()
    assert len(out) == 1
    assert out[0][0] == "EOM/13АВ-ЭМ1"
    assert "(main)" in out[0][1]  # путь ведёт в primary-версию внутри контейнера
