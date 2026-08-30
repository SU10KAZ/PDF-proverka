"""Ручная пара страниц — ось ИСХОДНЫХ данных, а не оформление.

Инженер открыл пару листов, руками перетащил правый лист к левому, и раздел
продолжил показывать прогон двухдневной давности как текущий результат:
26 вопросов и 27 изменений от другой пары, главная кнопка — «Продолжить
проверку». Причина в том, что ручное сопоставление сохранялось в
``sheet_links.json``, которого конвейер не читал вовсе, а признак устаревания
считался сравнением состояния С САМИМ СОБОЙ и потому замечал только правку
PDF.

Здесь закреплено обратное: правка ручной пары делает опубликованный прогон
не текущим, записи инженера закрываются, а повторный запуск создаёт новую
generation. И столь же важное обратное: пара, которую после прогона не
трогали, устаревшей НЕ становится — иначе выкатка обнулила бы все прошлые
прогоны разом.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from backend.app.services.stage_comparison import (
    paths,
    production_orchestrator as orchestrator,
    production_store,
)
from backend.app.services.stage_comparison.ai import settings as ai_settings

from tests.test_stage_comparison_production_orchestrator import (  # noqa: E402
    _atom,
    _graphic_ledger,
    _install_run_fakes,
    _run,
)

SESSION = "session-1"
PAIR = "pair-1"


def _fakes(monkeypatch, tmp_path: Path) -> dict:
    return _install_run_fakes(
        monkeypatch,
        tmp_path,
        text_atoms=[_atom("text-1", "TEXT")],
        graphic_atoms=[_atom("graphic-1", "GRAPHIC")],
        graphic_ledger=_graphic_ledger("chg-1", 1, 1),
    )


def _write_sheet_links(
    *,
    left_pages: list[int],
    right_pages: list[int],
    updated_at: str,
    source: str = "manual",
) -> None:
    """Записать связку ровно так, как это делает store.save_sheet_links."""
    path = paths.sheet_links_path(SESSION, PAIR)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "version": 1,
                "pair_id": PAIR,
                "links": [
                    {
                        "id": "link_manual",
                        "left_pages": left_pages,
                        "right_pages": right_pages,
                        "source": source,
                        "confidence": "manual",
                        "reason": ["user_reordered"],
                    }
                ],
                "unlinked_left_pages": [],
                "updated_at": updated_at,
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )


def _drop_source_scope() -> None:
    """Превратить состояние в «старое»: до появления оси ручной пары."""
    state = production_store.load_artifact(SESSION, PAIR, "state")
    state.pop("source_scope", None)
    production_store.save_artifact(SESSION, PAIR, "state", state)


# ── A. Изменённая ручная пара обесценивает прежний прогон ────────────────

def test_manual_pairing_saved_after_the_run_makes_it_not_current(
    tmp_path, monkeypatch
):
    """A. Пара пересобрана руками — прежний прогон больше не текущий."""
    _fakes(monkeypatch, tmp_path)
    _run()
    assert orchestrator.get_production_state(SESSION, PAIR)["stale"] is False, (
        "предпосылка: сразу после прогона результат актуален"
    )

    _write_sheet_links(
        left_pages=[1], right_pages=[1], updated_at="2100-01-01T00:00:00+00:00"
    )

    state = orchestrator.get_production_state(SESSION, PAIR)
    assert state["stale"] is True
    assert state["stale_reason"] == orchestrator.STALE_MANUAL_PAGE_PAIRING_CHANGED


def test_changed_manual_pairing_is_detected_by_content_not_by_clock(
    tmp_path, monkeypatch
):
    """Ось сравнивается по содержанию: отпечаток прогона записан в состояние."""
    _fakes(monkeypatch, tmp_path)
    _write_sheet_links(
        left_pages=[1], right_pages=[1], updated_at="2000-01-01T00:00:00+00:00"
    )
    _run()
    recorded = orchestrator.get_production_state(SESSION, PAIR)
    assert recorded["source_scope"]["manual_page_pairing"], (
        "прогон обязан запомнить, при какой ручной паре он посчитан"
    )
    assert recorded["stale"] is False

    # Время записи то же самое — изменилось только содержание пары.
    _write_sheet_links(
        left_pages=[1], right_pages=[2], updated_at="2000-01-01T00:00:00+00:00"
    )
    changed = orchestrator.get_production_state(SESSION, PAIR)
    assert changed["stale"] is True
    assert changed["stale_reason"] == orchestrator.STALE_MANUAL_PAGE_PAIRING_CHANGED


def test_untouched_manual_pairing_keeps_the_run_current(tmp_path, monkeypatch):
    """Ту же пару никто не трогал — прогон остаётся текущим."""
    _fakes(monkeypatch, tmp_path)
    _write_sheet_links(
        left_pages=[1], right_pages=[1], updated_at="2000-01-01T00:00:00+00:00"
    )
    _run()

    state = orchestrator.get_production_state(SESSION, PAIR)
    assert state["stale"] is False
    assert state["stale_reason"] is None


def test_automatic_links_are_not_the_engineers_choice(tmp_path, monkeypatch):
    """Пересчёт автоматических подсказок прогон устаревшим не делает."""
    _fakes(monkeypatch, tmp_path)
    _run()

    _write_sheet_links(
        left_pages=[1],
        right_pages=[1],
        updated_at="2100-01-01T00:00:00+00:00",
        source="auto",
    )

    assert orchestrator.get_production_state(SESSION, PAIR)["stale"] is False


# ── Старые состояния: выкатка не обнуляет прошлые прогоны ────────────────

def test_legacy_state_with_pairing_made_before_the_run_stays_current(
    tmp_path, monkeypatch
):
    """Прогон старше самой оси, а пару после него не трогали — он текущий."""
    _fakes(monkeypatch, tmp_path)
    _write_sheet_links(
        left_pages=[1], right_pages=[1], updated_at="2000-01-01T00:00:00+00:00"
    )
    _run()
    _drop_source_scope()

    state = orchestrator.get_production_state(SESSION, PAIR)
    assert state["stale"] is False, (
        "выкатка новой оси не имеет права объявить устаревшими прогоны, "
        "у которых ничего не менялось"
    )


def test_legacy_state_with_pairing_touched_after_the_run_is_not_current(
    tmp_path, monkeypatch
):
    """Записанного отпечатка нет, но человек тронул пару ПОСЛЕ прогона."""
    _fakes(monkeypatch, tmp_path)
    _run()
    _drop_source_scope()
    _write_sheet_links(
        left_pages=[1], right_pages=[1], updated_at="2100-01-01T00:00:00+00:00"
    )

    state = orchestrator.get_production_state(SESSION, PAIR)
    assert state["stale"] is True
    assert state["stale_reason"] == orchestrator.STALE_MANUAL_PAGE_PAIRING_CHANGED


# ── F/G. Прошлые вопросы и изменения не выдаются за текущие ──────────────

def test_previous_questions_and_changes_are_marked_not_current(
    tmp_path, monkeypatch
):
    """F+G. Прошлые вопросы этапа 5 и изменения этапа 7 помечены устаревшими."""
    _fakes(monkeypatch, tmp_path)
    _run()
    _write_sheet_links(
        left_pages=[1], right_pages=[1], updated_at="2100-01-01T00:00:00+00:00"
    )

    assert orchestrator.get_review_questions(SESSION, PAIR)["stale"] is True
    assert orchestrator.get_production_changes(SESSION, PAIR)["stale"] is True


def test_engineer_writes_are_refused_while_the_pair_is_not_current(
    tmp_path, monkeypatch
):
    """Решение по чужой паре записать нельзя — конвейер отвечает конфликтом."""
    _fakes(monkeypatch, tmp_path)
    _run()
    rows = orchestrator.get_production_changes(SESSION, PAIR)["rows"]
    assert rows, "предпосылка: прогон опубликовал хотя бы одно изменение"

    _write_sheet_links(
        left_pages=[1], right_pages=[1], updated_at="2100-01-01T00:00:00+00:00"
    )

    with pytest.raises(orchestrator.ProductionStateConflictError):
        orchestrator.update_engineer_decisions(
            SESSION,
            PAIR,
            updates=[
                {
                    "target_id": rows[0]["target_id"],
                    "target_kind": "CHANGE",
                    "decision": "APPROVED",
                }
            ],
            author="инженер",
        )


# ── D/E/J. Повторный запуск создаёт новую generation ─────────────────────

def test_rerun_creates_a_new_generation_and_clears_the_stale_pair(
    tmp_path, monkeypatch
):
    """D+J. Новый прогон — новый run_id, новое время, снова текущий."""
    _fakes(monkeypatch, tmp_path)
    _run()
    first = orchestrator.get_production_state(SESSION, PAIR)
    _write_sheet_links(
        left_pages=[1], right_pages=[1], updated_at="2100-01-01T00:00:00+00:00"
    )
    assert orchestrator.get_production_state(SESSION, PAIR)["stale"] is True

    _run()
    second = orchestrator.get_production_state(SESSION, PAIR)

    assert second["run_id"] != first["run_id"]
    assert second["started_at"] >= first["started_at"]
    assert second["stale"] is False
    assert second["stale_reason"] is None
    assert second["source_scope"]["manual_page_pairing"] == (
        orchestrator.manual_page_pairing(SESSION, PAIR)["digest"]
    )


def test_rerun_receives_the_current_page_selection(tmp_path, monkeypatch):
    """E. Новый прогон считает выбранные страницы, а не страницы прошлого."""
    _fakes(monkeypatch, tmp_path)
    _run()
    assert orchestrator.get_production_state(SESSION, PAIR)["selection"][
        "right_pages"
    ] == [1]

    orchestrator.run_production_comparison(
        SESSION,
        PAIR,
        input_mode="PAGE",
        left_pages=[1],
        right_pages=[2],
        left_block_ids=[],
        right_block_ids=[],
    )

    state = orchestrator.get_production_state(SESSION, PAIR)
    assert state["selection"]["right_pages"] == [2]
    assert state["generation_scope"]["page_groups"][0]["right_pages"] == [2]


# ── H. «Быстро» не зовёт модели ──────────────────────────────────────────

def test_fast_rerun_calls_no_model(tmp_path, monkeypatch):
    """H. Режим «Быстро» — детерминированный конвейер без единого вызова."""
    _fakes(monkeypatch, tmp_path)

    def forbidden(*_args, **_kwargs):
        raise AssertionError("режим «Быстро» не имеет права звать модель")

    monkeypatch.setattr(orchestrator.ai_gateway, "run_gateway", forbidden, raising=False)

    state = orchestrator.run_production_comparison(
        SESSION,
        PAIR,
        input_mode="PAGE",
        left_pages=[1],
        right_pages=[1],
        left_block_ids=[],
        right_block_ids=[],
        ai_mode=ai_settings.MODE_FAST,
    )

    assert state["status"] in {"COMPLETED", "PARTIAL"}
    ai_stage = (state.get("stages") or {}).get("ai") or {}
    assert int(ai_stage.get("model_calls") or 0) == 0


# ── I. Ручная пара не превращается в вопрос сопоставителя листов ─────────

def test_manual_page_pair_asks_no_blocking_sheet_question(tmp_path, monkeypatch):
    """I. Пару выбрал человек — переспрашивать «тот ли это лист» нельзя."""
    _fakes(monkeypatch, tmp_path)
    _write_sheet_links(
        left_pages=[1], right_pages=[1], updated_at="2000-01-01T00:00:00+00:00"
    )
    state = _run()

    assert state["constraints"]["sheet_matcher_is_page_gate"] is False
    questions = orchestrator.get_review_questions(SESSION, PAIR)
    assert int((questions.get("counts") or {}).get("SHEET") or 0) == 0


# ── §11. Оси не смешиваются ──────────────────────────────────────────────

def test_analysis_mode_does_not_touch_the_manual_pairing_axis(
    tmp_path, monkeypatch
):
    """FAST → DEEP при той же паре: источник тот же, меняется конфигурация."""
    _fakes(monkeypatch, tmp_path)
    _write_sheet_links(
        left_pages=[1], right_pages=[1], updated_at="2000-01-01T00:00:00+00:00"
    )
    pairing = orchestrator.manual_page_pairing(SESSION, PAIR)

    _write_sheet_links(
        left_pages=[1], right_pages=[1], updated_at="2050-06-06T00:00:00+00:00"
    )

    assert orchestrator.manual_page_pairing(SESSION, PAIR)["digest"] == (
        pairing["digest"]
    ), "время записи связок не входит в отпечаток — только содержание пары"


def test_document_mode_ignores_the_manual_pairing_axis(tmp_path, monkeypatch):
    """Ось объявлена для «Страница ↔ страница» и режим документов не трогает."""
    _fakes(monkeypatch, tmp_path)
    _run(input_mode="DOCUMENT")
    _write_sheet_links(
        left_pages=[1], right_pages=[1], updated_at="2100-01-01T00:00:00+00:00"
    )

    assert orchestrator.get_production_state(SESSION, PAIR)["stale"] is False
