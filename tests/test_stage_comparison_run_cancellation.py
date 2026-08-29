"""Отмена прогона сравнения и проверка среды перед вызовом модели.

До этого остановить анализ было нечем. Токен отмены существовал внутри шлюза,
опрашивался в шести местах — и не создавался никем снаружи: слой заводил его
себе сам, а `.cancel()` не вызывался нигде во всей кодовой базе. Единственным
ограничителем оставался предел времени сеанса, 30 минут.

Проверка среды была написана, экспортирована и мертва: отсутствующий CLI
обнаруживался не на старте этапа, а на первом реальном вызове — и превращался
в четыреста отказов модели вместо одного честного отказа этапа.
"""
from __future__ import annotations

import subprocess
import threading

import pytest

from backend.app.services.stage_comparison import production_orchestrator as production
from backend.app.services.stage_comparison.ai import (
    gateway,
    resolution as resolution_module,
)


@pytest.fixture(autouse=True)
def _clean_registry():
    yield
    with production._RUN_CONTROLS_LOCK:
        production._RUN_CONTROLS.clear()


# ── Ручка прогона ─────────────────────────────────────────────────────────

def test_cancelling_a_run_nobody_started_says_so_instead_of_pretending():
    result = production.cancel_production_comparison("s1", "p1")

    assert result["cancelled"] is False
    assert result["reason_code"] == "no_active_run"


def test_cancelling_a_live_run_sets_the_token_the_gateway_polls():
    control = production._register_run("s1", "p1", "prun_1")

    result = production.cancel_production_comparison("s1", "p1", requested_by="ing")

    assert result["cancelled"] is True
    assert result["run_id"] == "prun_1"
    assert result["requested_by"] == "ing"
    assert control.cancel_token.cancelled is True
    assert control.cancelled is True


def test_a_cancelled_control_stops_the_pipeline_at_the_next_stage_boundary():
    control = production._register_run("s1", "p1", "prun_1")
    production._raise_if_cancelled(control)  # пока не отменён — молчит

    control.cancel_token.cancel()

    with pytest.raises(production.ProductionRunCancelled):
        production._raise_if_cancelled(control)


def test_releasing_a_run_leaves_nothing_to_cancel():
    control = production._register_run("s1", "p1", "prun_1")

    production._release_run(control)

    assert production.active_run_control("s1", "p1") is None
    assert production.cancel_production_comparison("s1", "p1")["cancelled"] is False


def test_a_newer_run_of_the_same_pair_replaces_the_handle():
    first = production._register_run("s1", "p1", "prun_1")
    second = production._register_run("s1", "p1", "prun_2")

    assert production.active_run_control("s1", "p1") is second
    # Освобождение старой ручки не должно снимать защиту с новой.
    production._release_run(first)
    assert production.active_run_control("s1", "p1") is second


# ── Отмена не убивает соседей ─────────────────────────────────────────────

class _FakeProcess:
    def __init__(self) -> None:
        self.killed = False
        self.pid = 1

    def wait(self, timeout: float | None = None) -> int:  # pragma: no cover
        return 0


def test_cancelling_one_pair_never_kills_the_calls_of_another(monkeypatch):
    mine, theirs = _FakeProcess(), _FakeProcess()
    gateway._REGISTRY.add(mine, "prun_mine")
    gateway._REGISTRY.add(theirs, "prun_theirs")
    monkeypatch.setattr(
        gateway, "_kill_process_group",
        lambda process: setattr(process, "killed", True),
    )
    try:
        killed = gateway.kill_live_processes("prun_mine")

        assert killed == 1
        assert mine.killed is True
        assert theirs.killed is False
        assert gateway.live_process_count("prun_theirs") == 1
    finally:
        gateway._REGISTRY.discard(mine)
        gateway._REGISTRY.discard(theirs)


def test_without_a_run_id_everything_the_gateway_started_is_killed(monkeypatch):
    first, second = _FakeProcess(), _FakeProcess()
    gateway._REGISTRY.add(first, "a")
    gateway._REGISTRY.add(second, "b")
    monkeypatch.setattr(
        gateway, "_kill_process_group",
        lambda process: setattr(process, "killed", True),
    )
    try:
        assert gateway.kill_live_processes() == 2
    finally:
        gateway._REGISTRY.discard(first)
        gateway._REGISTRY.discard(second)


def test_a_cancel_token_reaches_the_running_call(monkeypatch):
    started = threading.Event()
    token = gateway.CancelToken()

    def fake_popen(*args, **kwargs):
        started.set()
        raise RuntimeError("процесс не должен стартовать после отмены")

    monkeypatch.setattr(subprocess, "Popen", fake_popen)
    monkeypatch.setattr(gateway, "_resolve_codex_binary", lambda: "/bin/codex")
    token.cancel()

    result = gateway.call_codex("промпт", model="m", cancel=token)

    assert result.ok is False
    assert result.error_kind == "CANCELLED"
    assert started.is_set() is False


# ── Проверка среды ────────────────────────────────────────────────────────

def test_an_unready_runtime_hands_every_item_to_a_human_with_a_reason():
    items = [
        {"review_evidence_id": "ureview_2", "atom_id": "tatom_2"},
        {"review_evidence_id": "ureview_1", "atom_id": "tatom_1"},
    ]

    artifact = resolution_module.unavailable_artifact(
        items,
        runtime={"ok": False, "problems": ["codex CLI не найден"]},
        generated_at="fixed",
    )

    assert artifact["diagnostics"]["ai_resolved"] == 0
    assert artifact["diagnostics"]["human_required"] == 2
    assert artifact["diagnostics"]["runtime_ready"] is False
    assert artifact["diagnostics"]["mode_completeness"] == "PARTIAL"
    assert [value["review_evidence_id"] for value in artifact["resolutions"]] == [
        "ureview_1", "ureview_2",
    ]
    for value in artifact["resolutions"]:
        assert value["status"] == resolution_module.HUMAN_REQUIRED
        assert value["reason_code"] == resolution_module.REASON_RUNTIME_UNAVAILABLE
        assert "codex CLI не найден" in value["reason_detail"]
        assert value["typed_resolution"] is None


def test_an_unready_runtime_is_not_the_same_as_the_off_mode():
    # В OFF система ничего не обещала. Здесь обещала и не смогла — и каждый
    # элемент обязан получить причину, а не молча остаться без разбора.
    off = resolution_module.empty_artifact(generated_at="fixed")
    unready = resolution_module.unavailable_artifact(
        [{"review_evidence_id": "ureview_1", "atom_id": "tatom_1"}],
        runtime={"ok": False, "problems": ["нет claude"]},
        generated_at="fixed",
    )

    assert off["resolutions"] == []
    assert off["diagnostics"]["mode_completeness"] == "COMPLETE"
    assert len(unready["resolutions"]) == 1
    assert unready["diagnostics"]["mode_completeness"] == "PARTIAL"


# ── Режим анализа как параметр прогона ────────────────────────────────────

def test_the_run_mode_is_chosen_per_run_not_by_a_machine_wide_variable():
    from backend.app.services.stage_comparison.ai import settings as ai_settings

    assert ai_settings.resolve_run_mode("FAST") == ai_settings.MODE_OFF
    assert ai_settings.resolve_run_mode("STANDARD") == ai_settings.MODE_STANDARD
    assert ai_settings.resolve_run_mode("DEEP") == ai_settings.MODE_DEEP


def test_without_a_choice_the_installation_setting_still_decides(monkeypatch):
    from backend.app.services.stage_comparison.ai import settings as ai_settings

    monkeypatch.setenv("STAGE_COMPARISON_AI_MODE", "STANDARD")

    assert ai_settings.resolve_run_mode(None) == ai_settings.MODE_STANDARD


def test_the_server_and_not_the_client_decides_which_modes_are_allowed(monkeypatch):
    from backend.app.services.stage_comparison.ai import settings as ai_settings

    monkeypatch.setenv("STAGE_COMPARISON_AI_ALLOWED_MODES", "FAST,STANDARD")

    assert ai_settings.allowed_run_modes() == ("FAST", "STANDARD")
    with pytest.raises(ValueError):
        ai_settings.resolve_run_mode("DEEP")


def test_fast_can_never_be_forbidden(monkeypatch):
    from backend.app.services.stage_comparison.ai import settings as ai_settings

    monkeypatch.setenv("STAGE_COMPARISON_AI_ALLOWED_MODES", "DEEP")

    # Работа без моделей вообще — не режим, который можно запретить.
    assert "FAST" in ai_settings.allowed_run_modes()


def test_off_is_never_shown_as_a_user_mode():
    from backend.app.services.stage_comparison.ai import settings as ai_settings

    assert "OFF" not in ai_settings.RUN_MODES
    assert ai_settings.run_mode_label(ai_settings.MODE_OFF) == "FAST"


def test_the_layer_keeps_its_mode_for_the_whole_run(monkeypatch):
    from backend.app.services.stage_comparison.ai import settings as ai_settings

    monkeypatch.setenv("STAGE_COMPARISON_AI_MODE", "STANDARD")
    layer = resolution_module.AiResolutionLayer(mode="DEEP")

    # Переменная окружения, изменённая посреди прогона, не имеет права
    # поменять глубину у соседних партий одного и того же анализа.
    monkeypatch.setenv("STAGE_COMPARISON_AI_MODE", "OFF")

    assert layer.mode == ai_settings.MODE_DEEP
    assert layer.deep is True


def test_a_normalized_request_carries_the_run_mode():
    request = production.normalize_run_request(
        input_mode="DOCUMENT", ai_mode="DEEP",
    )

    assert request["ai_mode"] == "DEEP"


def test_a_forbidden_run_mode_is_refused_at_the_request_boundary(monkeypatch):
    monkeypatch.setenv("STAGE_COMPARISON_AI_ALLOWED_MODES", "FAST")

    with pytest.raises(ValueError):
        production.normalize_run_request(input_mode="DOCUMENT", ai_mode="DEEP")
