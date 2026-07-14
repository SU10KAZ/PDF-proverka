from __future__ import annotations

from backend.scripts.run_codex_subscription_vision_benchmark import (
    clean_token_count,
    greedy_match,
    semantic_score,
)
from backend.scripts.run_stage02_codex_block_ab import extract_codex_tokens


def test_token_parser_does_not_append_following_iso_year():
    text = "tokens used\n5,185\n2026-07-11T12:00:00Z"
    assert extract_codex_tokens(text) == 5185


def test_clean_token_count_repairs_legacy_year_suffix():
    assert clean_token_count(51852026) == 5185
    assert clean_token_count(8205) == 8205


def test_semantic_finding_match_uses_shared_concept_and_visible_value():
    baseline = {
        "severity": "РЕКОМЕНДАТЕЛЬНОЕ",
        "category": "documentation",
        "finding": "В узле оставлен незаполненный размер xxx.",
        "value_found": "xxx",
        "recommendation": "Указать фактический размер.",
    }
    candidate = {
        "severity": "РЕКОМЕНДАТЕЛЬНОЕ",
        "category": "documentation",
        "finding": "Размерное обозначение xxx является placeholder.",
        "value_found": "xxx",
        "recommendation": "Заменить шаблон конкретным значением.",
    }

    assert semantic_score(baseline, candidate, "findings") >= 0.38
    matches, missed, extra = greedy_match([baseline], [candidate], "findings")
    assert len(matches) == 1
    assert missed == []
    assert extra == []


def test_different_optimization_directions_are_not_forced_into_one_match():
    baseline = {
        "type": "faster_install",
        "current": "Повторяющиеся узлы воздуховодов собираются на площадке.",
        "proposed": "Применить заводские монтажные модули.",
        "evidence": "Повторяются зеркальные квартирные ветви.",
        "estimated_effect": "Сокращение монтажа",
    }
    unrelated = {
        "type": "lifecycle",
        "current": "Установлены CAV-клапаны.",
        "proposed": "Организовать удаленный мониторинг состояния клапанов.",
        "evidence": "На плане показаны CAV DN100.",
        "estimated_effect": "Снижение обслуживания",
    }

    assert semantic_score(baseline, unrelated, "optimizations") < 0.38
