from __future__ import annotations

from pathlib import Path

import pytest

from backend.app.pipeline.stages.block_analysis.gemma_findings_only import (
    SYSTEM_PROMPT_PROFILE_ASTRA_SHADOW_V2,
    SYSTEM_PROMPT_PROFILE_ASTRA_SHADOW_V3,
    SYSTEM_PROMPT_PROFILE_PRODUCTION,
    build_system_prompt,
)
from backend.scripts.run_astra_prompt_shadow_ab import (
    ReviewedBlock,
    _expert_eval,
    select_reviewed_balanced,
)
from backend.scripts.run_stage02_codex_block_ab import BlockCandidate


def _candidate(index: int, discipline: str) -> BlockCandidate:
    root = Path(f"/tmp/prompt-shadow-{index}")
    return BlockCandidate(
        object_slug="object",
        discipline=discipline,
        document=f"document-{index}",
        version="v001",
        version_dir=root,
        latest_dir=root / "03_analysis" / "latest",
        block_id=f"blk_{index}",
        page=index,
        image_path=root / "block.png",
        image_source_dir="blocks_stage02_100",
        block_record={},
        gpt_findings=[],
        enrichment_source="test",
    )


def _reference(index: int, decision: str) -> dict:
    return {
        "reference_key": f"ref-{index}",
        "finding_id": f"F-{index:03d}",
        "decision": decision,
        "problem": f"Расхождение марки K{index}",
        "description": "На схеме и в спецификации указаны разные значения.",
        "rejection_reason": "" if decision == "accepted" else "Факт опровергнут.",
        "comparison_finding": {
            "severity": "ЭКСПЛУАТАЦИОННОЕ",
            "category": "mark_conflict",
            "finding": f"Расхождение марки K{index}. На схеме и в спецификации указаны разные значения.",
            "norm_quote": None,
            "value_found": f"K{index}",
            "recommendation": "Уточнить марку.",
        },
    }


def test_production_prompt_remains_default_and_unchanged() -> None:
    implicit = build_system_prompt("VK", extended=False)
    explicit = build_system_prompt(
        "VK",
        extended=False,
        prompt_profile=SYSTEM_PROMPT_PROFILE_PRODUCTION,
    )
    assert implicit == explicit
    assert "Большинство блоков корректны" in implicit
    assert "не обязан исчерпывать чек-лист" in implicit


def test_astra_shadow_prompt_is_candidate_first_and_requires_three_passes() -> None:
    prompt = build_system_prompt(
        "VK",
        extended=False,
        prompt_profile=SYSTEM_PROMPT_PROFILE_ASTRA_SHADOW_V2,
    )
    assert "Большинство блоков корректны" not in prompt
    assert "не обязан исчерпывать чек-лист" not in prompt
    assert "формирует кандидатов для отдельного publication gate" in prompt
    assert "1. Внутренняя согласованность блока" in prompt
    assert "2. Согласованность с переданным контекстом" in prompt
    assert "3. Явные пропуски" in prompt
    assert "Минимального\nчисла findings нет" in prompt
    assert prompt.index("обязательно\nвыполни все проходы") < prompt.index(
        "`findings=[]` допустим"
    )


def test_astra_shadow_v3_adds_bounded_discipline_recipe_after_entity_join() -> None:
    prompt = build_system_prompt(
        "EOM",
        extended=False,
        prompt_profile=SYSTEM_PROMPT_PROFILE_ASTRA_SHADOW_V3,
    )
    assert "Протокол сопоставления сущностей и document_retrieval" in prompt
    assert "Дисциплинарный рецепт: ЭОМ/ЭС" in prompt
    assert "аппарат ↔ линия ↔ нагрузка" in prompt
    assert "Не сравнивай аппараты разных линий" in prompt
    assert "Минимального\nчисла findings нет" in prompt
    assert prompt.index("Протокол сопоставления сущностей") < prompt.index(
        "Дисциплинарный рецепт: ЭОМ/ЭС"
    ) < prompt.index("Обязательный протокол полноты")

    structural = build_system_prompt(
        "KJ",
        extended=False,
        prompt_profile=SYSTEM_PROMPT_PROFILE_ASTRA_SHADOW_V3,
    )
    assert "Дисциплинарный рецепт: КЖ/КМ" in structural
    assert "количество × единичная длина/масса" in structural
    assert "Дисциплинарный рецепт: ЭОМ/ЭС" not in structural


def test_unknown_prompt_profile_is_rejected() -> None:
    with pytest.raises(ValueError, match="Unknown system prompt profile"):
        build_system_prompt("VK", extended=False, prompt_profile="unknown")


def test_reviewed_selection_balances_positive_and_negative_blocks() -> None:
    disciplines = ("AR", "KJ", "EOM", "OV", "TX", "AR")
    reviewed = []
    for index in range(12):
        decision = "accepted" if index < 6 else "rejected"
        reviewed.append(
            ReviewedBlock(
                _candidate(index, disciplines[index % len(disciplines)]),
                (_reference(index, decision),),
            )
        )
    selected = select_reviewed_balanced(reviewed, limit=10)
    assert len(selected) == 10
    assert sum(item.has_accepted for item in selected) == 5
    assert sum(not item.has_accepted for item in selected) == 5
    assert len({item.candidate.discipline for item in selected}) >= 4


def test_expert_eval_separates_accepted_rejected_and_unreviewed() -> None:
    references = (_reference(1, "accepted"), _reference(2, "rejected"))
    result = {
        "codex_findings": [
            references[0]["comparison_finding"],
            {
                "severity": "РЕКОМЕНДАТЕЛЬНОЕ",
                "category": "other",
                "finding": "Отдельная новая проблема Z99.",
                "norm_quote": None,
                "value_found": "Z99",
                "recommendation": "Исправить.",
            },
        ]
    }
    evaluation = _expert_eval(result, references, threshold=0.30)
    assert evaluation["matched_accepted"] == 1
    assert evaluation["matched_rejected"] == 0
    assert evaluation["unreviewed_model_findings_count"] == 1
