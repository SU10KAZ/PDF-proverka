from __future__ import annotations

import asyncio

import pytest

from backend.app.models.usage import LLMResult
from backend.app.services import section_optimization_agent_service as agent


def _dossier() -> dict:
    return {
        "candidate": {
            "signal_id": "REPL-1",
            "title": "Тиражировать принятое решение",
            "representative_proposal": "Заменить изделие на унифицированное",
        },
        "source_decisions": [
            {
                "source_ref": "P1:OPT-1",
                "project_id": "P1",
                "current": "Исходное изделие",
                "proposed": "Принятое изделие",
            }
        ],
        "targets": [
            {
                "project_id": "P2",
                "project_name": "Корпус 2",
                "version_id": "v001",
                "rows": [
                    {"row_id": "ROW-2", "page": 12, "name": "Целевое изделие", "quantity": "5"},
                ],
            }
        ],
        "guardrails": ["Не изменять проект автоматически"],
    }


def _raw_review() -> dict:
    return {
        "summary": "Решение требует проверки схемы.",
        "target_assessments": [
            {
                "project_id": "P2",
                "verdict": "needs_graphics",
                "confidence": 0.74,
                "reason": "В таблице отсутствует схема подключения.",
                "target_row_ids": ["ROW-2", "HALLUCINATED"],
                "conditions": ["Сохранить номинал"],
                "missing_data": [],
                "graphics_required": True,
                "graphics_reason": "Проверить подключение",
                "suggested_pages": [12, 999],
                "expert_action": "Проверить лист 12",
            }
        ],
        "cross_project_risks": ["Различие схем"],
        "expert_summary": "Нужна графическая проверка.",
    }


def test_validate_agent_review_keeps_only_supplied_sources():
    review = agent.validate_agent_review(_raw_review(), _dossier())

    assert review["overall_recommendation"] == "needs_graphics"
    assessment = review["target_assessments"][0]
    assert assessment["target_row_ids"] == ["ROW-2"]
    assert assessment["suggested_pages"] == [12]
    assert assessment["graphics_required"] is True


def test_validate_agent_review_rejects_positive_verdict_without_valid_row_reference():
    raw = _raw_review()
    assessment = raw["target_assessments"][0]
    assessment.update({
        "verdict": "applicable",
        "confidence": 0.99,
        "target_row_ids": ["HALLUCINATED"],
        "graphics_required": False,
        "graphics_reason": "",
        "suggested_pages": [12],
    })

    review = agent.validate_agent_review(raw, _dossier())
    result = review["target_assessments"][0]

    assert review["overall_recommendation"] == "needs_data"
    assert result["verdict"] == "needs_data"
    assert result["confidence"] == 0.0
    assert result["target_row_ids"] == []
    assert result["graphics_required"] is False
    assert result["suggested_pages"] == []
    assert "не сослался ни на одну строку" in result["missing_data"][-1]


@pytest.mark.asyncio
async def test_agent_calls_codex_json_runner_and_returns_metrics(monkeypatch):
    monkeypatch.setattr(agent, "_record_usage", lambda *_args: None)
    captured = {}

    async def fake_runner(messages, **kwargs):
        captured["messages"] = messages
        captured["kwargs"] = kwargs
        return LLMResult(
            json_data=_raw_review(),
            model="codex/gpt-test",
            input_tokens=321,
            output_tokens=123,
            duration_ms=456,
        )

    review, metrics = await agent.analyze_replication_dossier(
        _dossier(),
        object_id="obj-1",
        section="EOM",
        replication_id="repl-1",
        runner=fake_runner,
    )

    assert review["target_assessments"][0]["project_id"] == "P2"
    assert metrics["model"] == "codex/gpt-test"
    assert metrics["input_tokens"] == 321
    assert captured["kwargs"]["output_schema"] == agent.AGENT_OUTPUT_SCHEMA
    assert captured["kwargs"]["project_id"] == "obj-1/EOM"
    assert "common purchasing" in captured["messages"][0]["content"]


@pytest.mark.asyncio
async def test_agent_limits_parallel_codex_sessions(monkeypatch):
    monkeypatch.setenv("SECTION_OPTIMIZATION_AGENT_CONCURRENCY", "1")
    monkeypatch.setattr(agent, "_record_usage", lambda *_args: None)
    agent._AGENT_SEMAPHORE = None
    agent._AGENT_SEMAPHORE_LOOP = None
    active = 0
    maximum = 0

    async def fake_runner(_messages, **_kwargs):
        nonlocal active, maximum
        active += 1
        maximum = max(maximum, active)
        await asyncio.sleep(0.01)
        active -= 1
        return LLMResult(json_data=_raw_review(), model="codex/gpt-test")

    await asyncio.gather(*[
        agent.analyze_replication_dossier(
            _dossier(),
            object_id="obj-1",
            section="EOM",
            replication_id=f"repl-{index}",
            runner=fake_runner,
        )
        for index in range(3)
    ])

    assert maximum == 1


def test_discovery_requires_action_and_comparison_even_for_positive_model_verdict():
    dossier = _dossier()
    dossier["source_decisions"] = []
    dossier["candidate"]["kind"] = "type_size_reduction_opportunity"
    dossier["targets"][0]["rows"][0]["type_mark"] = "A"
    dossier["targets"].append({"project_id": "P3", "rows": [{"row_id": "ROW-3", "type_mark": "B"}]})
    raw = _raw_review()
    supplied = raw["target_assessments"][0]
    supplied.update(verdict="applicable", graphics_required=False)
    result = agent.validate_agent_review(raw, dossier)["target_assessments"][0]
    assert result["verdict"] == "needs_data" and result["confidence"] == 0
    supplied.update(proposed_action="Заменить A на B при сохранении номинала", comparison_row_ids=["ROW-2", "ROW-3"])
    result = agent.validate_agent_review(raw, dossier)["target_assessments"][0]
    assert result["verdict"] == "applicable"
    assert result["proposed_action"] == supplied["proposed_action"]
    assert result["comparison_row_ids"] == ["ROW-2", "ROW-3"]
    supplied["comparison_row_ids"].append("INVENTED")
    assert agent.validate_agent_review(raw, dossier)["target_assessments"][0]["verdict"] == "needs_data"
