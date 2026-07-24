from __future__ import annotations

from pathlib import Path

import json

import backend.app.pipeline.stages.prepare.codex_targeted_findings as targeted


SAMPLE_MD = """\
## Page 3
### BLOCK #4 [TEXT]: blk_refs
##### ВЕДОМОСТЬ ССЫЛОЧНЫХ ДОКУМЕНТОВ
| ССЫЛКА | НАИМЕНОВАНИЕ |
| ГОСТ 21.507-81 | Интерьеры |
| ГОСТ 33929-2016 | Полистиролбетон |
###### АББРЕВИАТУРЫ ИСПОЛЬЗУЕМЫЕ В ПРОЕКТЕ
**Д1** - Марка дверей
**СП1** - Марка профильного светильника
| СП-1 | Унитаз подвесной | 1 |
## Page 8
### BLOCK #8 [TEXT]: blk_spec
| D11 | 750 | 2300 | Металлическая дверь |
## Page 9
### BLOCK #9 [IMAGE]: blk_plan
**Summary:** На плане показана дверь Д11.
**Verification:** Возможно, следует проверить Д99.
"""


def _write_md(tmp_path: Path) -> Path:
    path = tmp_path / "sample_results.md"
    path.write_text(SAMPLE_MD, encoding="utf-8")
    return path


def test_norm_status_hints_match_ocr_punctuation(monkeypatch):
    monkeypatch.setattr(
        targeted,
        "_norm_status_rows",
        lambda: ((
            "ГОСТ 21.507-81 (СТ СЭВ 4410-83)",
            {"status": "cancelled", "last_verified": "2026-05-06"},
        ),),
    )

    hints = targeted._norm_status_hints("| ГОСТ 21.507-81 | Интерьеры |")

    assert "ГОСТ 21.507-81" in hints
    assert "status=cancelled" in hints
    assert "verified=2026-05-06" in hints


def test_mark_system_context_contains_global_conflicts(tmp_path):
    context = targeted._mark_system_context(_write_md(tmp_path))

    assert "D11" in context
    assert "Д11" in context
    assert "СП1" in context
    assert "СП-1" in context
    assert "профильного светильника" in context
    assert "Унитаз" in context
    assert "evidence_kind primary_text" in context
    assert "evidence_kind derived_image_description" in context
    assert "Д99" not in context


def test_ai_builds_exhaustive_norm_and_mark_passes(monkeypatch, tmp_path):
    from backend.app.core import config

    md_path = _write_md(tmp_path)
    monkeypatch.setattr(
        config, "FINDING_EVIDENCE_OCR_OBSERVER_ENABLED", True,
    )
    monkeypatch.setattr(targeted, "_get_md_file_path", lambda *_: str(md_path))
    monkeypatch.setattr(targeted, "_read_existing_findings", lambda *_args, **_kw: '{"findings":[]}')

    passes = targeted.build_targeted_findings_passes({"section": "AI"}, "doc")

    assert [item.stage for item in passes] == [
        "alia_docnorm_audit",
        "alia_mark_system_audit",
    ]
    norm_system = passes[0].messages[0]["content"]
    norm_user = passes[0].messages[1]["content"]
    mark_system = passes[1].messages[0]["content"]
    assert "КАЖДУЮ строку" in norm_system
    assert "LOCAL VERIFIED NORM STATUS" in norm_user
    assert "префикс марки -> класс сущности" in mark_system
    assert "D11" in passes[1].messages[1]["content"]
    assert "Д11" in passes[1].messages[1]["content"]


def test_atomicity_guard_splits_distinct_serious_findings_from_same_block(tmp_path):
    stage01_path = tmp_path / "01_blocks_analysis.json"
    stage01_path.write_text(
        json.dumps(
            {
                "block_analyses": [
                    {
                        "block_id": "blk_wall",
                        "page": 15,
                        "findings": [
                            {
                                "id": "G-014",
                                "severity": "КРИТИЧЕСКОЕ",
                                "category": "dimensions",
                                "finding": "Пом. 19: цепочка не сходится.\n\nРекомендация: Исправить цепочку 19.",
                                "value_found": "580, 580, 1155",
                            },
                            {
                                "id": "G-015",
                                "severity": "КРИТИЧЕСКОЕ",
                                "category": "dimensions",
                                "finding": "Пом. 20: цепочка не сходится.\n\nРекомендация: Исправить цепочку 20.",
                                "value_found": "80, 800, 680, 1555",
                            },
                        ],
                    }
                ]
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    merged = {
        "findings": [
            {
                "id": "F-024",
                "severity": "КРИТИЧЕСКОЕ",
                "problem": "Две размерные цепочки не сходятся",
                "source_finding_ids": ["G-014", "G-015"],
            }
        ]
    }

    result = targeted.enforce_stage01_atomicity(merged, stage01_path)

    assert len(result["findings"]) == 2
    assert [item["source_finding_ids"] for item in result["findings"]] == [["G-014"], ["G-015"]]
    assert "1160" in result["findings"][0]["description"]
    assert "1560" in result["findings"][1]["description"]
    assert result["meta"]["stage01_atomicity_guard"] == {
        "split_groups": 1,
        "restored_findings": 2,
    }


def test_atomicity_guard_keeps_same_issue_candidates_merged(tmp_path):
    """Регрессия 23.07: G-038/G-039 превращались в дубли F-005/F-027."""
    stage01_path = tmp_path / "01_blocks_analysis.json"
    stage01_path.write_text(
        json.dumps(
            {
                "block_analyses": [
                    {
                        "block_id": "blk_angle",
                        "page": 10,
                        "findings": [
                            {
                                "id": "G-038",
                                "severity": "ПРОВЕРИТЬ ПО СМЕЖНЫМ",
                                "category": "documentation",
                                "finding": (
                                    "На схеме указаны анкерные болты, а в примечании "
                                    "два дюбель-гвоздя."
                                ),
                                "value_found": (
                                    "«Анкерный болт»; «Опорный уголок крепить "
                                    "двумя дюбель-гвоздями»"
                                ),
                                "comparison_ref": "codex:002",
                                "detector_comparison": {
                                    "relation": "new",
                                    "counterpart_refs": [],
                                    "confidence": 1.0,
                                },
                            },
                            {
                                "id": "G-039",
                                "severity": "ЭКСПЛУАТАЦИОННОЕ",
                                "category": "documentation",
                                "finding": (
                                    "В узле показаны анкерные болты, тогда как общие "
                                    "указания требуют два дюбель-гвоздя."
                                ),
                                "value_found": (
                                    "Анкерный болт; Опорный уголок крепить "
                                    "двумя дюбель-гвоздями."
                                ),
                                "comparison_ref": "codex:001",
                                "detector_comparison": {
                                    "relation": "match",
                                    "counterpart_refs": ["gpt_openrouter:001"],
                                    "confidence": 0.99,
                                },
                            },
                        ],
                    }
                ]
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    merged = {
        "findings": [
            {
                "id": "F-005",
                "severity": "ЭКОНОМИЧЕСКОЕ",
                "problem": "Для опорного уголка заданы два типа крепежа.",
                "source_finding_ids": ["T-005", "G-038", "G-039"],
            }
        ]
    }

    result = targeted.enforce_stage01_atomicity(merged, stage01_path)

    assert result is merged
    assert len(result["findings"]) == 1
    assert result["findings"][0]["source_finding_ids"] == [
        "T-005", "G-038", "G-039",
    ]


def test_atomicity_guard_collapses_duplicate_component_before_split(tmp_path):
    """Два повтора одного дефекта + другой дефект дают две, а не три строки."""
    stage01_path = tmp_path / "01_blocks_analysis.json"
    stage01_path.write_text(
        json.dumps(
            {
                "block_analyses": [
                    {
                        "block_id": "blk_mixed",
                        "page": 16,
                        "findings": [
                            {
                                "id": "G-091",
                                "severity": "ЭКСПЛУАТАЦИОННОЕ",
                                "category": "material",
                                "finding": "Плотность утеплителя указана неверно.",
                                "value_found": "ТЕХНОАКУСТИК 41 кг/м3",
                                "comparison_ref": "gpt:001",
                                "detector_comparison": {
                                    "relation": "match",
                                    "counterpart_refs": ["codex:001"],
                                    "confidence": 0.99,
                                },
                            },
                            {
                                "id": "G-092",
                                "severity": "ЭКОНОМИЧЕСКОЕ",
                                "category": "material",
                                "finding": "Марка утеплителя не соответствует плотности.",
                                "value_found": "Паспортная плотность 43 кг/м3",
                                "comparison_ref": "codex:001",
                                "detector_comparison": {
                                    "relation": "extension",
                                    "counterpart_refs": ["gpt:001"],
                                    "confidence": 0.99,
                                },
                            },
                            {
                                "id": "G-093",
                                "severity": "ЭКСПЛУАТАЦИОННОЕ",
                                "category": "fastening",
                                "finding": "Шаг перфоленты не согласован с армированием.",
                                "value_found": "каждый 3 ряд вместо каждого 2 ряда",
                            },
                        ],
                    }
                ]
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    merged = {
        "findings": [
            {
                "id": "F-016",
                "severity": "ЭКСПЛУАТАЦИОННОЕ",
                "source_finding_ids": ["G-091", "G-092", "G-093"],
            }
        ]
    }

    result = targeted.enforce_stage01_atomicity(merged, stage01_path)

    assert len(result["findings"]) == 2
    assert [item["source_finding_ids"] for item in result["findings"]] == [
        ["G-091", "G-092"],
        ["G-093"],
    ]
    assert result["meta"]["stage01_atomicity_guard"] == {
        "split_groups": 1,
        "restored_findings": 2,
    }


def test_atomicity_guard_keeps_cross_block_dedup_and_recommendations(tmp_path):
    stage01_path = tmp_path / "01_blocks_analysis.json"
    stage01_path.write_text(
        json.dumps(
            {
                "block_analyses": [
                    {
                        "block_id": "b1",
                        "page": 1,
                        "findings": [
                            {"id": "G-001", "severity": "ЭКСПЛУАТАЦИОННОЕ", "finding": "Повтор"},
                            {"id": "G-003", "severity": "РЕКОМЕНДАТЕЛЬНОЕ", "finding": "Опечатка 1"},
                            {"id": "G-004", "severity": "РЕКОМЕНДАТЕЛЬНОЕ", "finding": "Опечатка 2"},
                        ],
                    },
                    {
                        "block_id": "b2",
                        "page": 2,
                        "findings": [
                            {"id": "G-002", "severity": "ЭКСПЛУАТАЦИОННОЕ", "finding": "Повтор"}
                        ],
                    },
                ]
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    merged = {
        "findings": [
            {"id": "F-001", "source_finding_ids": ["G-001", "G-002"]},
            {"id": "F-002", "source_finding_ids": ["G-003", "G-004"]},
        ]
    }

    result = targeted.enforce_stage01_atomicity(merged, stage01_path)

    assert result is merged
    assert len(result["findings"]) == 2
