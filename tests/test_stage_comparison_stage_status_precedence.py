"""Один источник статуса этапа и честный исход ИИ-слоя.

Обобщённый прогресс публиковал «готово» поверх исхода, который объявил сам
этап, — и «ИИ-анализ не запущен: среда не готова» показывалось инженеру как
«Готово». Здесь закреплён порядок: терминальный исход, объявленный этапом,
сильнее обобщённого статуса прогресса.
"""
from __future__ import annotations

import pytest

from backend.app.services.stage_comparison import (
    production_orchestrator as orchestrator,
    production_store,
)
from backend.app.services.stage_comparison.ai import (
    resolution as ai_resolution,
    settings as ai_settings,
)

from tests.test_stage_comparison_production_orchestrator import (  # noqa: E402
    _atom,
    _graphic_ledger,
    _install_run_fakes,
    _unlocated_atom,
)


# ── Один источник статуса этапа ───────────────────────────────────────────

def _publish(tmp_path, monkeypatch, *, stage_status, stage_update):
    monkeypatch.setenv("COMPARISON_ROOT", str(tmp_path / "comparison"))
    production_store.save_artifact("session-1", "pair-1", "state", {
        "run_id": "run-1", "status": "RUNNING", "revision": 0, "stages": {},
    })
    return orchestrator._publish_progress_event(
        "session-1", "pair-1", "run-1",
        current_stage="unified_synthesis",
        current_substage="ai_resolution",
        message="…",
        stage_key="ai_resolution",
        stage_status=stage_status,
        stage_update=stage_update,
    )


@pytest.mark.parametrize(
    "declared",
    ["PARTIAL", "FAILED", "CHECK_BLOCKED", "CANCELLED", "NEEDS_REVIEW"],
)
def test_progress_may_not_promote_a_terminal_stage_outcome_to_completed(
    tmp_path, monkeypatch, declared
):
    """Исход объявляет этап. Прогресс — это индикация, а не результат."""
    state = _publish(
        tmp_path, monkeypatch,
        stage_status="COMPLETED",
        stage_update={"status": declared, "total": 3},
    )

    stage = state["stages"]["ai_resolution"]
    assert stage["status"] == declared
    assert stage["progress"]["status"] == declared


def test_progress_status_still_fills_the_gap_when_the_stage_declares_nothing(
    tmp_path, monkeypatch
):
    """Обобщённый статус не отменяется — он лишь перестал перекрывать исход."""
    state = _publish(
        tmp_path, monkeypatch,
        stage_status="RUNNING",
        stage_update={"total": 3, "processed": 1},
    )

    assert state["stages"]["ai_resolution"]["status"] == "RUNNING"


def test_a_stage_may_still_announce_its_own_completion(tmp_path, monkeypatch):
    state = _publish(
        tmp_path, monkeypatch,
        stage_status="COMPLETED",
        stage_update={"status": "COMPLETED", "total": 3},
    )

    assert state["stages"]["ai_resolution"]["status"] == "COMPLETED"


# ── Исходы ИИ-слоя ────────────────────────────────────────────────────────

def _ai_artifact(**diagnostics) -> dict:
    mode = diagnostics.pop("mode", ai_settings.MODE_STANDARD)
    artifact = ai_resolution.empty_artifact(generated_at="fixed", mode=mode)
    artifact["diagnostics"].update(diagnostics)
    return artifact


def test_a_layer_that_could_not_start_is_partial_not_completed():
    """A. Среда не готова: слой обещал разбор и не начал его."""
    artifact = ai_resolution.unavailable_artifact(
        [{"review_evidence_id": "re-1", "atom_id": "a-1"}],
        runtime={"ok": False, "problems": ["CLI не найден"]},
        mode=ai_settings.MODE_STANDARD,
    )

    stage = orchestrator._ai_resolution_stage(artifact)

    assert stage["status"] == "PARTIAL"
    assert stage["runtime_ready"] is False


def test_deep_without_the_promised_critic_is_partial():
    """B. «Глубокая проверка» без состоявшегося критика — не та проверка."""
    stage = orchestrator._ai_resolution_stage(_ai_artifact(
        mode=ai_settings.MODE_DEEP,
        critic_required=4,
        critic_unavailable=4,
        mode_completeness="PARTIAL",
    ))

    assert stage["status"] == "PARTIAL"
    assert stage["critic_unavailable"] == 4


def test_unrecovered_model_timeouts_are_partial():
    """C. Таймаут, переживший повторы, — работа, которая не сделана."""
    stage = orchestrator._ai_resolution_stage(_ai_artifact(model_timeouts=2))

    assert stage["status"] == "PARTIAL"


def test_a_layer_that_crashed_is_partial():
    """Упавший слой не имеет права выглядеть завершённым."""
    stage = orchestrator._ai_resolution_stage(
        _ai_artifact(layer_error="RuntimeError")
    )

    assert stage["status"] == "PARTIAL"
    assert stage["layer_error"] == "RuntimeError"


def test_a_cancelled_layer_is_cancelled_not_completed():
    """D. Инженер нажал «остановить»: это не отказ и не «готово»."""
    stage = orchestrator._ai_resolution_stage(_ai_artifact(cancelled=True))

    assert stage["status"] == "CANCELLED"


def test_a_fast_run_reports_the_layer_as_not_applicable():
    """Режим «Быстро» ничего не обещал — и ничего не должен опускать."""
    stage = orchestrator._ai_resolution_stage(
        ai_resolution.empty_artifact(generated_at="fixed")
    )

    assert stage["status"] == "NOT_APPLICABLE"


def test_only_a_layer_that_did_everything_it_promised_is_completed():
    """E. «Готово» остаётся достижимым — но только по-настоящему."""
    stage = orchestrator._ai_resolution_stage(_ai_artifact(
        input_items=5, ai_resolved=5, human_required=0,
        model_failures=0, model_timeouts=0, mode_completeness="COMPLETE",
    ))

    assert stage["status"] == "COMPLETED"


# ── Верхняя сводка наследует честный исход ────────────────────────────────

def _complete_run_fixture(tmp_path, monkeypatch, *, review_item: bool):
    """Прогон, у которого текст и графика заведомо завершены полностью.

    Иначе «частично» на прогоне ничего не доказывает: его дала бы графика.
    """
    from backend.app.services.stage_comparison.ai import gateway as ai_gateway

    _install_run_fakes(
        monkeypatch, tmp_path,
        text_atoms=[
            _unlocated_atom("text-unresolved", "TEXT") if review_item
            else _atom("text-voltage", "TEXT")
        ],
        graphic_atoms=[_atom("graphic-voltage", "GRAPHIC")],
        graphic_ledger=_graphic_ledger("graphic-change", 1, 1),
    )
    monkeypatch.setattr(ai_gateway, "reap_orphaned_processes", lambda **_: 0)
    return ai_gateway


def test_the_run_status_inherits_a_degraded_ai_layer(tmp_path, monkeypatch):
    """§9. «Готово» на прогоне рядом с «ИИ-анализ не выполнен» — неправда."""
    ai_gateway = _complete_run_fixture(tmp_path, monkeypatch, review_item=True)
    monkeypatch.setattr(
        ai_gateway, "validate_runtime",
        lambda **_: {"ok": False, "problems": ["CLI не найден"], "checks": {}},
    )

    state = orchestrator.run_production_comparison(
        "session-1", "pair-1",
        input_mode="PAGE", left_pages=[1], right_pages=[1],
        left_block_ids=[], right_block_ids=[], ai_mode="STANDARD",
    )

    assert state["stages"]["text"]["status"] == "COMPLETED"
    assert state["stages"]["graphic"]["status"] == "COMPLETED"
    assert state["stages"]["ai_resolution"]["status"] == "PARTIAL"
    assert state["status"] == "PARTIAL", (
        "верхняя сводка обязана наследовать исход этапа, а не игнорировать его"
    )


def test_a_not_applicable_ai_layer_does_not_degrade_the_run(
    tmp_path, monkeypatch
):
    """Обратная сторона: «Быстро» ничего не обещал и ничего не портит."""
    _complete_run_fixture(tmp_path, monkeypatch, review_item=False)

    state = orchestrator.run_production_comparison(
        "session-1", "pair-1",
        input_mode="PAGE", left_pages=[1], right_pages=[1],
        left_block_ids=[], right_block_ids=[], ai_mode="FAST",
    )

    assert state["stages"]["text"]["status"] == "COMPLETED"
    assert state["stages"]["graphic"]["status"] == "COMPLETED"
    assert state["stages"]["ai_resolution"]["status"] == "NOT_APPLICABLE"
    assert state["status"] == "COMPLETED"
