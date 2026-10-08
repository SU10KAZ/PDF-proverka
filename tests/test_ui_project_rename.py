"""Static grep-smoke для UI переименования папки проекта.

Task — кнопка «N версий» убрана, рядом с версией показано имя проекта +
карандаш, по клику открывается rename-редактор, после успеха фронт обновляет
состояние (project_id) и навигирует на новый проект.

Source of truth — `frontend/` (webapp/static покрыт parity-тестом отдельно).
"""
from __future__ import annotations

from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
HTML = (_ROOT / "frontend" / "index.html").read_text(encoding="utf-8")
JS = (_ROOT / "frontend" / "static" / "js" / "app.js").read_text(encoding="utf-8")


# ─── Test 1: кнопка «N версий» убрана ───────────────────────────────────────
def test_no_versions_count_button_near_selector():
    # старая кнопка-тоггл рядом с селектором версии удалена
    assert "versionsPanelOpen = !versionsPanelOpen" not in HTML
    assert "}} версии" not in HTML
    assert "project-version-switch__btn" not in HTML


# ─── Test 2: имя проекта + карандаш рядом с версией ─────────────────────────
def test_project_name_and_pencil_shown():
    assert "project-version-switch__name" in HTML
    assert "currentProject.name" in HTML
    assert "project-version-switch__pencil" in HTML
    assert "@click=\"startRename()\"" in HTML


def test_rename_editor_present():
    assert "v-model=\"renameValue\"" in HTML
    assert "@click=\"submitRename()\"" in HTML
    assert "@click=\"cancelRename()\"" in HTML
    assert "Сохранить" in HTML and "Отмена" in HTML


# ─── Test 9: фронт обновляет состояние после успешного rename ───────────────
def test_js_rename_functions_and_state_update():
    assert "async function apiPatch(" in JS
    assert "function startRename(" in JS
    assert "async function submitRename(" in JS
    # вызывает PATCH .../rename
    assert "/rename`" in JS
    # после смены project_id — controlled-навигация на новый проект
    assert "navigate('/project/' + newId)" in JS
    # refs/функции проброшены в setup return
    for token in ("renameEditing", "renameValue", "renameError",
                  "startRename", "submitRename", "cancelRename"):
        assert token in JS, token


# ─── создание версии остаётся доступным ──────────────────────────────────────
def test_versions_panel_still_exists():
    # Панель «Версии проекта» и модалку «Создать новую версию» удалили
    # намеренно (91f48104, 02.07.2026). Версия теперь создаётся привязкой
    # проекта к существующему в окне «Изменить выбранные» — этот путь и
    # обязан остаться.
    assert "versions-panel" not in HTML
    assert "Привязать как версию" in HTML
