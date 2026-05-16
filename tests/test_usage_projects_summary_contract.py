"""
test_usage_projects_summary_contract.py
---------------------------------------
Контрактная регрессия для /api/usage/projects-summary.

Баг: до фикса /api/usage/projects-summary возвращал по этапам только
сокращённый набор полей (total_tokens, cost_usd, calls, duration_ms) и не
содержал на уровне проекта total_input_tokens/total_output_tokens. Фронтенд
после refreshProjects() перезаписывал детальный projectUsage этим неполным
ответом, и карточки этапов теряли input_tokens/output_tokens/model.

Тест фиксирует ожидаемый контракт сводки:
- на уровне проекта присутствуют total_input_tokens, total_output_tokens,
  total_cost_usd и stages_summary;
- на уровне этапа присутствуют input_tokens, output_tokens, model и cost;
- duration_ms / calls сохранены (как раньше).

Покрывается одновременно backend (новый) и webapp (legacy) — обе реализации
обслуживают один и тот же фронтенд.

Run:
    python -m pytest tests/test_usage_projects_summary_contract.py -v
"""
from __future__ import annotations

import importlib
import sys
from datetime import datetime
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


_REQUIRED_STAGE_FIELDS = (
    "input_tokens",
    "output_tokens",
    "model",
    "cost_usd",
    "duration_ms",
    "calls",
)

_REQUIRED_PROJECT_FIELDS = (
    "total_input_tokens",
    "total_output_tokens",
    "total_cost_usd",
    "stages_summary",
)


def _make_record(stage: str, model: str, in_tok: int, out_tok: int, cost: float = 0.0) -> dict:
    return {
        "timestamp": datetime.now().isoformat(),
        "session_id": None,
        "project_id": "TEST/proj",
        "stage": stage,
        "model": model,
        "cost_usd": cost,
        "cost_usd_notional": 0.0,
        "duration_ms": 1000,
        "duration_api_ms": 800,
        "num_turns": 1,
        "api_calls": 1,
        "is_retry": False,
        "input_tokens": in_tok,
        "output_tokens": out_tok,
        "cache_creation_tokens": 0,
        "cache_read_tokens": 0,
    }


def _seed_tracker(tracker, monkeypatch) -> None:
    """Подменяем приватный state и подавляем фильтр по pipeline_log.json,
    чтобы тест не зависел от файлов на диске."""
    tracker._records = [
        _make_record("text_analysis", "openai/gpt-5.4", 1000, 200, cost=0.01),
        _make_record("block_analysis", "openai/gpt-5.4", 2000, 400, cost=0.02),
        _make_record("findings_merge", "claude-opus-4-7", 500, 100, cost=0.0),
    ]
    monkeypatch.setattr(
        type(tracker), "_get_audit_started_at", staticmethod(lambda _pid: None)
    )
    monkeypatch.setattr(
        type(tracker), "_get_pipeline_durations", staticmethod(lambda _pid: {})
    )
    # Не даём загрузке/сохранению трогать реальный файл usage_data.json.
    monkeypatch.setattr(tracker, "_save", lambda: None)


def _check_summary_shape(summary: dict) -> None:
    assert isinstance(summary, dict) and summary, (
        "get_all_projects_usage должен вернуть непустой dict, когда есть записи"
    )
    pid, entry = next(iter(summary.items()))
    # Project-level: поля для верхней панели и для совместимости с loadProject().
    for field in _REQUIRED_PROJECT_FIELDS:
        assert field in entry, (
            f"project entry {pid!r}: отсутствует поле {field!r}; "
            f"карточки этапов и верхняя панель сломаются. Got: {sorted(entry)}"
        )
    # Числовая проверка — суммы должны соответствовать тестовым записям.
    assert entry["total_input_tokens"] == 1000 + 2000 + 500
    assert entry["total_output_tokens"] == 200 + 400 + 100

    stages = entry["stages_summary"]
    assert "text_analysis" in stages and "block_analysis" in stages and "findings_merge" in stages

    for stage_key, stage in stages.items():
        for field in _REQUIRED_STAGE_FIELDS:
            assert field in stage, (
                f"stages_summary[{stage_key!r}]: отсутствует поле {field!r}; "
                f"карточка этапа потеряет данные. Got: {sorted(stage)}"
            )

    # Конкретные значения по стадии: токены и модель должны попасть в сводку.
    ta = stages["text_analysis"]
    assert ta["input_tokens"] == 1000
    assert ta["output_tokens"] == 200
    assert ta["model"] == "openai/gpt-5.4"

    fm = stages["findings_merge"]
    assert fm["model"] == "claude-opus-4-7"


def test_backend_projects_summary_contract(monkeypatch):
    """backend.app.services.common.usage_service: проверка контракта."""
    mod = importlib.import_module("backend.app.services.common.usage_service")
    tracker = mod.UsageTracker.__new__(mod.UsageTracker)
    tracker._records = []
    tracker._session_reset_at = datetime.now().isoformat()
    _seed_tracker(tracker, monkeypatch)
    _check_summary_shape(tracker.get_all_projects_usage())


def test_webapp_projects_summary_contract(monkeypatch):
    """webapp.services.usage_service (legacy): тот же контракт."""
    mod = importlib.import_module("webapp.services.usage_service")
    tracker = mod.UsageTracker.__new__(mod.UsageTracker)
    tracker._records = []
    tracker._session_reset_at = datetime.now().isoformat()
    _seed_tracker(tracker, monkeypatch)
    _check_summary_shape(tracker.get_all_projects_usage())


@pytest.mark.parametrize(
    "module_path",
    [
        "backend.app.services.common.usage_service",
        "webapp.services.usage_service",
    ],
)
def test_projects_summary_matches_project_usage_fields(module_path, monkeypatch):
    """Поля сводки по проектам должны быть совместимы с per-project usage
    (per-stage), чтобы фронтенд мог использовать любой источник для карточек."""
    mod = importlib.import_module(module_path)
    tracker = mod.UsageTracker.__new__(mod.UsageTracker)
    tracker._records = []
    tracker._session_reset_at = datetime.now().isoformat()
    _seed_tracker(tracker, monkeypatch)

    summary = tracker.get_all_projects_usage()
    detailed = tracker.get_project_usage("TEST/proj")

    assert "TEST/proj" in summary
    s_stages = summary["TEST/proj"]["stages_summary"]
    d_stages = detailed["stages_summary"]
    common_keys = set(s_stages) & set(d_stages)
    assert common_keys, "пустое пересечение этапов между сводкой и детальной"
    for stage_key in common_keys:
        s_stage = s_stages[stage_key]
        d_stage = d_stages[stage_key]
        for field in ("input_tokens", "output_tokens", "model"):
            assert s_stage.get(field) == d_stage.get(field), (
                f"{stage_key}.{field}: summary={s_stage.get(field)!r} "
                f"!= detailed={d_stage.get(field)!r}"
            )
