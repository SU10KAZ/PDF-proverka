"""Две оси прогона: исходные данные и конфигурация анализа.

«Изменился ли вход» и «в каком режиме это посчитано» — разные вопросы, и
смешивать их нельзя. Когда глубина анализа попала в подпись ИСХОДНЫХ ДАННЫХ,
каждый прогон, сделанный до появления режимов, стал устаревшим на ровном месте:
PDF те же, версии те же, выбор листов тот же — а раздел проверки ушёл в режим
«только чтение» и начал отвечать 409 на любую запись инженера.
"""
from __future__ import annotations

import copy
from pathlib import Path

import pytest

from backend.app.services.stage_comparison import (
    production_orchestrator as orchestrator,
    production_store,
)
from backend.app.services.stage_comparison.ai import (
    cache as ai_cache,
    resolution as ai_resolution,
    settings as ai_settings,
)

from tests.test_stage_comparison_production_orchestrator import (  # noqa: E402
    _atom,
    _graphic_ledger,
    _install_run_fakes,
    _pair,
    _run,
    _unlocated_atom,
)


#: Запрос ровно в той форме, в какой он существовал до появления режимов
#: глубины. Именно такие словари лежат в сохранённых боевых состояниях.
LEGACY_REQUEST = {
    "input_mode": "PAGE",
    "left_pages": [1],
    "right_pages": [1],
    "left_block_ids": [],
    "right_block_ids": [],
}


def _write_pdfs(tmp_path: Path) -> dict:
    pair = _pair(tmp_path)
    for side in ("left", "right"):
        Path(pair[side]["pdf_path"]).write_bytes(b"%PDF-1.4 " + side.encode())
        Path(pair[side]["md_path"]).write_text(side, encoding="utf-8")
    return pair


# ── A. Появление режима не меняет идентичность исходных данных ────────────

def test_default_mode_does_not_change_the_source_identity_of_a_legacy_request(
    tmp_path,
):
    """A. Старый запрос и сегодняшний нормализованный — один и тот же вход."""
    pair = _write_pdfs(tmp_path)
    modern = orchestrator.normalize_run_request(**LEGACY_REQUEST)

    assert modern["ai_mode"] == ai_settings.MODE_FAST, (
        "предпосылка теста: без пожелания клиента прогон получает «Быстро»"
    )
    assert orchestrator._input_signature(
        pair, LEGACY_REQUEST
    ) == orchestrator._input_signature(pair, modern), (
        "подпись исходных данных обязана совпасть с той, что была посчитана "
        "до появления ai_mode: документы не менялись"
    )


def test_source_signature_ignores_the_analysis_mode_entirely(tmp_path):
    """Ни один режим не сдвигает подпись входа — не только режим по умолчанию."""
    pair = _write_pdfs(tmp_path)
    signatures = {
        orchestrator._input_signature(
            pair, orchestrator.normalize_run_request(**LEGACY_REQUEST, ai_mode=mode)
        )
        for mode in ("FAST", "STANDARD", "DEEP")
    }

    assert len(signatures) == 1


# ── B/C. Настоящее изменение входа подпись обязана заметить ───────────────

def test_changed_document_content_changes_the_source_signature(tmp_path):
    """B. Меняется содержимое PDF — меняется подпись исходных данных."""
    pair = _write_pdfs(tmp_path)
    request = orchestrator.normalize_run_request(**LEGACY_REQUEST)
    before = orchestrator._input_signature(pair, request)

    Path(pair["right"]["pdf_path"]).write_bytes(b"%PDF-1.4 right revised")

    assert orchestrator._input_signature(pair, request) != before


def test_changed_document_version_changes_the_source_signature(tmp_path):
    """B. Версия документа — часть входа, а не оформление."""
    pair = _write_pdfs(tmp_path)
    request = orchestrator.normalize_run_request(**LEGACY_REQUEST)
    before = orchestrator._input_signature(pair, request)

    moved = copy.deepcopy(pair)
    moved["right"]["version_id"] = "right-v2"

    assert orchestrator._input_signature(moved, request) != before


def test_changed_page_selection_changes_the_source_signature(tmp_path):
    """C. Другая страница — другой вход."""
    pair = _write_pdfs(tmp_path)
    first = orchestrator.normalize_run_request(**LEGACY_REQUEST)
    second = orchestrator.normalize_run_request(
        **{**LEGACY_REQUEST, "right_pages": [2]}
    )

    assert orchestrator._input_signature(
        pair, first
    ) != orchestrator._input_signature(pair, second)


def test_changed_page_scope_changes_the_source_signature(tmp_path):
    """C. Область сравнения — тоже вход, а не конфигурация анализа."""
    pair = _write_pdfs(tmp_path)
    request = orchestrator.normalize_run_request(**LEGACY_REQUEST)
    groups = [{"id": "g1", "left_pages": [1], "right_pages": [1]}]
    widened = [{"id": "g1", "left_pages": [1], "right_pages": [1, 2]}]

    assert orchestrator._input_signature(
        pair, request, page_groups=groups
    ) != orchestrator._input_signature(pair, request, page_groups=widened)


# ── D. Режим — отдельная ось ──────────────────────────────────────────────

def test_fast_to_deep_moves_the_analysis_axis_and_leaves_the_source_axis(
    tmp_path,
):
    """D. FAST → DEEP: вход тот же, конфигурация анализа другая."""
    pair = _write_pdfs(tmp_path)
    fast = orchestrator.normalize_run_request(**LEGACY_REQUEST, ai_mode="FAST")
    deep = orchestrator.normalize_run_request(**LEGACY_REQUEST, ai_mode="DEEP")

    assert orchestrator._input_signature(
        pair, fast
    ) == orchestrator._input_signature(pair, deep), "источник не менялся"
    assert orchestrator.analysis_config_signature(
        fast
    ) != orchestrator.analysis_config_signature(deep), (
        "режим обязан различаться на своей оси, иначе «глубокая проверка» "
        "неотличима от «быстрой»"
    )
    assert orchestrator.analysis_config(deep) == {
        "ai_mode": ai_settings.MODE_DEEP, "recorded": True,
    }


def test_a_run_without_a_recorded_mode_is_not_relabelled_as_fast():
    """Прогон до появления режимов читается как «режим не записан»."""
    assert orchestrator.analysis_config(LEGACY_REQUEST) == {
        "ai_mode": None, "recorded": False,
    }
    assert orchestrator.analysis_config_signature(
        LEGACY_REQUEST
    ) != orchestrator.analysis_config_signature(
        orchestrator.normalize_run_request(**LEGACY_REQUEST)
    ), "«не записан» и «Быстро» — разные вещи, и их нельзя склеивать"


def test_source_request_keeps_exactly_the_pre_mode_keys():
    """Состав оси источников закреплён: молчаливое расширение ломает прошлое."""
    assert set(orchestrator.SOURCE_REQUEST_KEYS) == set(LEGACY_REQUEST)
    assert orchestrator.ANALYSIS_CONFIG_KEYS == ("ai_mode",)
    assert orchestrator.source_request(
        orchestrator.normalize_run_request(**LEGACY_REQUEST, ai_mode="DEEP")
    ) == LEGACY_REQUEST


# ── Обратная совместимость сохранённых прогонов ───────────────────────────

def test_a_generation_stored_before_modes_existed_does_not_become_stale(
    tmp_path, monkeypatch
):
    """Главный боевой инвариант: старые пары не устаревают из-за ai_mode."""
    _install_run_fakes(
        monkeypatch, tmp_path,
        text_atoms=[_atom("text-voltage", "TEXT")],
        graphic_atoms=[],
    )
    _run()
    stored = production_store.load_artifact("session-1", "pair-1", "state")
    legacy = copy.deepcopy(dict(stored))
    # Ровно то, что лежит на диске у прогонов, сделанных до появления режимов:
    # выбор из пяти ключей и подпись, посчитанная тогда же.
    legacy["selection"].pop("ai_mode")
    legacy.pop("analysis_config", None)
    legacy.pop("analysis_config_signature", None)
    production_store.save_artifact("session-1", "pair-1", "state", legacy)

    public = orchestrator.get_production_state("session-1", "pair-1")

    assert public["stale"] is False, (
        "документы не менялись — появление нового поля не повод требовать "
        "повторного прогона"
    )
    assert public["analysis_config"] == {"ai_mode": None, "recorded": False}, (
        "интерфейс имеет право честно сказать «выполнено до появления "
        "режимов глубины», но не имеет права выдумать режим"
    )


def test_narrowing_the_installation_policy_does_not_make_a_finished_run_stale(
    tmp_path, monkeypatch
):
    """Политика сервера ограничивает ЗАПУСК анализа, а не чтение готового."""
    _install_run_fakes(
        monkeypatch, tmp_path,
        text_atoms=[_atom("text-voltage", "TEXT")],
        graphic_atoms=[],
    )
    orchestrator.run_production_comparison(
        "session-1", "pair-1",
        input_mode="PAGE", left_pages=[1], right_pages=[1],
        left_block_ids=[], right_block_ids=[], ai_mode="DEEP",
    )
    assert orchestrator.get_production_state(
        "session-1", "pair-1"
    )["stale"] is False

    monkeypatch.setenv("STAGE_COMPARISON_AI_ALLOWED_MODES", "FAST")

    public = orchestrator.get_production_state("session-1", "pair-1")

    assert public["stale"] is False, (
        "запрет режима задним числом не меняет документы прошлого прогона"
    )
    assert public["analysis_config"]["ai_mode"] == ai_settings.MODE_DEEP


def test_restore_selection_does_not_invent_a_mode_for_a_legacy_selection():
    restored = orchestrator.restore_selection(LEGACY_REQUEST)

    assert "ai_mode" not in restored
    assert orchestrator.source_request(restored) == LEGACY_REQUEST


def test_a_finished_run_records_both_axes(tmp_path, monkeypatch):
    """Режим остаётся в провенансе результата, а не только в подписи."""
    _install_run_fakes(
        monkeypatch, tmp_path,
        text_atoms=[_atom("text-voltage", "TEXT")],
        graphic_atoms=[],
    )
    state = orchestrator.run_production_comparison(
        "session-1", "pair-1",
        input_mode="PAGE", left_pages=[1], right_pages=[1],
        left_block_ids=[], right_block_ids=[], ai_mode="STANDARD",
    )

    assert state["selection"]["ai_mode"] == ai_settings.MODE_STANDARD
    assert state["analysis_config"] == {
        "ai_mode": ai_settings.MODE_STANDARD, "recorded": True,
    }
    assert state["analysis_config_signature"] == (
        orchestrator.analysis_config_signature(state["selection"])
    )
    assert state["stages"]["ai_resolution"]["run_mode"] == (
        ai_settings.MODE_STANDARD
    )


# ── E. Кэш ИИ не смешивает режимы ─────────────────────────────────────────

def test_fast_never_puts_anything_into_the_ai_cache(tmp_path, monkeypatch):
    """E. «Быстро» не зовёт моделей, поэтому и в кэш ему нечего положить."""
    from backend.app.services.stage_comparison.ai import gateway as ai_gateway

    _install_run_fakes(
        monkeypatch, tmp_path,
        text_atoms=[_atom("text-voltage", "TEXT")],
        graphic_atoms=[],
    )
    monkeypatch.setattr(ai_gateway, "reap_orphaned_processes", lambda **_: 0)
    monkeypatch.setattr(
        ai_gateway, "validate_runtime",
        lambda **_: pytest.fail("«Быстро» не имеет права проверять среду ИИ"),
    )
    orchestrator.run_production_comparison(
        "session-1", "pair-1",
        input_mode="PAGE", left_pages=[1], right_pages=[1],
        left_block_ids=[], right_block_ids=[], ai_mode="FAST",
    )

    cache_dir = production_store.artifact_path(
        "session-1", "pair-1", "ai_resolutions"
    ).parent / "ai_response_cache"

    assert not cache_dir.exists() or not list(cache_dir.glob("*"))


def test_a_deep_only_role_can_never_be_served_from_another_roles_entry():
    """E. Проверка критика не читается из ответа массового аналитика."""
    common = {
        "evidence_digest": "digest-1",
        "model": "gpt-5.6-sol",
        "reasoning_level": "low",
        "prompt_version": "v1",
        "schema_version": "s1",
    }
    keys = {
        role: ai_cache.cache_key(**common, role=role)
        for role in ("analyst", "critic", "vision")
    }

    assert len(set(keys.values())) == 3


def test_the_deep_result_artifact_is_not_the_fast_result_artifact():
    """E/§3. Результат «глубокой проверки» отличим от результата «Быстро»."""
    fast = ai_resolution.empty_artifact(generated_at="fixed")
    deep = ai_resolution.empty_artifact(
        generated_at="fixed", mode=ai_settings.MODE_DEEP
    )

    assert fast["run_mode"] == ai_settings.MODE_FAST
    assert deep["run_mode"] == ai_settings.MODE_DEEP
    assert fast["input_signature"] != deep["input_signature"], (
        "иначе результат быстрого прогона молча сойдёт за глубокий"
    )


def test_an_empty_deep_run_still_reports_itself_as_deep(tmp_path, monkeypatch):
    """Разбирать было нечего — это не повод записать прогон как «Быстро»."""
    _install_run_fakes(
        monkeypatch, tmp_path, text_atoms=[], graphic_atoms=[],
    )
    state = orchestrator.run_production_comparison(
        "session-1", "pair-1",
        input_mode="PAGE", left_pages=[1], right_pages=[1],
        left_block_ids=[], right_block_ids=[], ai_mode="DEEP",
    )

    assert state["stages"]["ai_resolution"]["mode"] == ai_settings.MODE_DEEP
