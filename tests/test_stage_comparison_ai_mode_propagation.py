"""Глубина анализа доезжает от HTTP до ИИ-слоя и ничем по дороге не подменяется.

Проверяется не то, что параметр объявлен в сигнатурах, а то, что ИМЕННО ЭТОТ
режим получил слой, который решает, звать модель или нет. Разрыв, который эти
тесты закрывают, выглядел как исправная цепочка: публичная функция принимала
режим, следующая за ней объявляла его в сигнатуре — и не передавала дальше.
Внешне ничего не ломалось, потому что «не указан» — легальное значение,
означающее «решает установка». Инженер выбирал «глубокую проверку», установка
работала в своём режиме и молчала об этом.

Отдельно проверяется главное обещание режима «Быстро»: ноль вызовов моделей,
какой бы ни была настройка установки.
"""
from __future__ import annotations

from pathlib import Path
from unittest import mock

import pytest

from backend.app.services.stage_comparison import (
    production_orchestrator as orchestrator,
)
from backend.app.services.stage_comparison.ai import settings as ai_settings

from tests.test_stage_comparison_production_orchestrator import (  # noqa: E402
    _install_run_fakes,
    _unlocated_atom,
)


@pytest.fixture
def run_pair(tmp_path, monkeypatch):
    """Готовый прогон с подменёнными этапами и записью того, что получил слой."""
    from backend.app.services.stage_comparison.ai import (
        gateway as ai_gateway,
        resolution as ai_resolution,
    )

    monkeypatch.setenv("STAGE_COMPARISON_AI_CACHE_ENABLED", "false")
    _install_run_fakes(
        monkeypatch, tmp_path,
        text_atoms=[_unlocated_atom("text-unresolved", "TEXT")],
        graphic_atoms=[],
    )
    monkeypatch.setattr(ai_gateway, "reap_orphaned_processes", lambda **_: 0)

    observed: dict[str, object] = {"modes": [], "runtime_modes": [], "calls": 0}

    original_init = ai_resolution.AiResolutionLayer.__init__

    def spy_init(self, **kwargs):
        observed["modes"].append(kwargs.get("mode"))
        original_init(self, **kwargs)

    def fake_resolve(self, **kwargs):
        artifact = ai_resolution.empty_artifact(generated_at="fixed")
        artifact["mode"] = self.mode
        return artifact

    def spy_validate(**kwargs):
        observed["runtime_modes"].append(kwargs.get("mode"))
        return {"ok": True, "problems": [], "binaries": {}, "checks": {},
                "mode": kwargs.get("mode")}

    def forbidden_call(*args, **kwargs):
        observed["calls"] = int(observed["calls"]) + 1
        raise AssertionError("режим «Быстро» не имеет права звать модель")

    monkeypatch.setattr(ai_resolution.AiResolutionLayer, "__init__", spy_init)
    monkeypatch.setattr(ai_resolution.AiResolutionLayer, "resolve", fake_resolve)
    monkeypatch.setattr(ai_gateway, "validate_runtime", spy_validate)
    monkeypatch.setattr(ai_gateway, "call_codex", forbidden_call)
    monkeypatch.setattr(ai_gateway, "call_claude", forbidden_call)

    def run(ai_mode: str | None):
        kwargs: dict[str, object] = {
            "input_mode": "PAGE",
            "left_pages": [1],
            "right_pages": [1],
            "left_block_ids": [],
            "right_block_ids": [],
        }
        if ai_mode is not None:
            kwargs["ai_mode"] = ai_mode
        state = orchestrator.run_production_comparison(
            "session-1", "pair-1", **kwargs
        )
        return state, observed

    return run


# ── Режим прогона доезжает до слоя ────────────────────────────────────────

@pytest.mark.parametrize(
    "requested, expected",
    [("DEEP", ai_settings.MODE_DEEP)],
)
def test_the_requested_mode_reaches_the_layer_that_calls_the_model(
    run_pair, monkeypatch, requested, expected
):
    monkeypatch.setenv("STAGE_COMPARISON_AI_MODE", "STANDARD")

    state, observed = run_pair(requested)

    assert observed["modes"] == [expected], (
        "слой обязан получить режим ЭТОГО прогона, а не настройку установки"
    )
    assert state["stages"]["ai_resolution"]["mode"] == expected
    assert state["selection"]["ai_mode"] == ai_settings.run_mode_label(expected)


def test_standard_runs_question_closure_without_general_analyst(
    run_pair, monkeypatch,
):
    monkeypatch.setenv("STAGE_COMPARISON_AI_QUESTION_CLOSURE", "true")
    monkeypatch.setenv("STAGE_COMPARISON_AI_ANALYST_V2", "false")
    closure_calls = []

    def close_questions(
        _session_id, _pair_id, *, human_review_plan, **_kwargs
    ):
        closure_calls.append(True)
        return {
            "stage": {
                "status": "COMPLETED",
                "model_calls": 2,
                "hro_before": 1,
                "hro_after": 1,
                "closed": 0,
            },
            "plan": human_review_plan,
        }

    monkeypatch.setattr(
        orchestrator, "_run_ai_question_closure_candidate", close_questions
    )

    state, observed = run_pair("STANDARD")

    assert observed["modes"] == []
    assert observed["calls"] == 0
    assert closure_calls == [True]
    assert state["selection"]["ai_mode"] == ai_settings.MODE_STANDARD
    assert state["stages"]["question_closure"]["model_calls"] == 2


def test_fast_never_reaches_the_layer_at_all(run_pair, monkeypatch):
    """«Быстро» — это работа без моделей, а не быстрая работа с ними."""
    monkeypatch.setenv("STAGE_COMPARISON_AI_MODE", "STANDARD")

    state, observed = run_pair("FAST")

    assert observed["modes"] == []
    assert observed["calls"] == 0
    assert state["selection"]["ai_mode"] == ai_settings.MODE_FAST


def test_fast_skips_question_closure_even_when_feature_is_enabled(
    run_pair, monkeypatch,
):
    monkeypatch.setenv("STAGE_COMPARISON_AI_QUESTION_CLOSURE", "true")
    monkeypatch.setattr(
        orchestrator,
        "_run_ai_question_closure_candidate",
        lambda *_args, **_kwargs: pytest.fail(
            "FAST must not call AI Question Closure"
        ),
    )

    state, observed = run_pair("FAST")

    assert observed["calls"] == 0
    assert observed["modes"] == []
    assert state["stages"]["question_closure"]["model_calls"] == 0


# ── Пожелание прогона сильнее настройки установки ─────────────────────────

def test_fast_makes_no_calls_even_when_the_installation_asks_for_deep(
    run_pair, monkeypatch
):
    monkeypatch.setenv("STAGE_COMPARISON_AI_MODE", "DEEP")

    state, observed = run_pair("FAST")

    assert observed["calls"] == 0, "ни одного обращения к модели"
    assert observed["modes"] == [], "слой не создавался вовсе"
    assert observed["runtime_modes"] == [], "среда даже не проверялась"
    assert state["stages"]["ai_resolution"]["mode"] == ai_settings.MODE_OFF


def test_deep_is_honoured_even_when_the_installation_is_switched_off(
    run_pair, monkeypatch
):
    """Выключенная установка — это умолчание, а не запрет.

    Запрещает режимы отдельная политика (`allowed_run_modes`), и только она.
    """
    monkeypatch.setenv("STAGE_COMPARISON_AI_MODE", "OFF")

    state, observed = run_pair("DEEP")

    assert observed["modes"] == [ai_settings.MODE_DEEP]
    assert state["stages"]["ai_resolution"]["mode"] == ai_settings.MODE_DEEP


def test_without_a_request_the_installation_setting_still_decides(
    run_pair, monkeypatch
):
    monkeypatch.setenv("STAGE_COMPARISON_AI_MODE", "DEEP")

    _state, observed = run_pair(None)

    assert observed["modes"] == [ai_settings.MODE_DEEP]


def test_a_mode_the_installation_forbids_is_refused_not_downgraded(
    run_pair, monkeypatch
):
    """Тихая деградация хуже отказа: «глубокая проверка» без критика — не она."""
    monkeypatch.setenv("STAGE_COMPARISON_AI_MODE", "STANDARD")
    monkeypatch.setenv("STAGE_COMPARISON_AI_ALLOWED_MODES", "FAST,STANDARD")

    with pytest.raises(ValueError):
        run_pair("DEEP")


# ── Аудитный след прогона описывает ЭТОТ прогон ───────────────────────────

def test_the_runtime_check_is_audited_with_the_mode_of_this_run(
    run_pair, monkeypatch
):
    monkeypatch.setenv("STAGE_COMPARISON_AI_MODE", "STANDARD")

    _state, observed = run_pair("DEEP")

    assert observed["runtime_modes"] == [ai_settings.MODE_DEEP]


def test_a_failing_layer_still_records_the_mode_it_was_running(
    run_pair, monkeypatch
):
    """Отказ слоя не имеет права переписать глубину прогона на «Быстро».

    Артефакт упавшей «глубокой проверки», записанный как режим без моделей,
    объясняет не тот прогон, к которому приложен: разбирать инцидент потом
    не по чему.
    """
    from backend.app.services.stage_comparison import production_store
    from backend.app.services.stage_comparison.ai import (
        resolution as ai_resolution,
    )

    monkeypatch.setenv("STAGE_COMPARISON_AI_MODE", "OFF")

    def explode(self, **kwargs):
        raise RuntimeError("провайдер недоступен")

    monkeypatch.setattr(ai_resolution.AiResolutionLayer, "resolve", explode)

    run_pair("DEEP")

    artifact = production_store.load_artifact(
        "session-1", "pair-1", "ai_resolutions"
    )
    assert artifact["diagnostics"]["layer_error"] == "RuntimeError"
    assert artifact["mode"] == ai_settings.MODE_DEEP
    assert artifact["run_mode"] == ai_settings.MODE_DEEP
    assert artifact["settings"]["mode"] == ai_settings.MODE_DEEP


# ── Путь HTTP ─────────────────────────────────────────────────────────────

def test_the_http_endpoint_hands_the_mode_to_the_run_service():
    """Проверяется провод, а не имя поля: что уехало из обработчика запроса."""
    from backend.app.api.routers import stage_comparison as router_module

    seen: dict[str, object] = {}

    def capture(session_id, pair_id, **kwargs):
        seen.update(kwargs)
        return {"status": "COMPLETED"}

    import asyncio

    request = router_module.ProductionRunRequest(
        input_mode="PAGE", left_pages=[1], right_pages=[1], ai_mode="DEEP",
    )
    with mock.patch.object(
        router_module.production, "run_production_comparison", capture
    ):
        asyncio.run(
            router_module.run_production_comparison("s", "p", request)
        )

    assert seen["ai_mode"] == "DEEP"
