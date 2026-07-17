from __future__ import annotations

from pathlib import Path

import pytest

from backend.app.models.usage import LLMResult
from backend.app.services import section_optimization_graphics_agent_service as graphics
from backend.app.services.section_optimization_graphics_agent_service import rank_block_candidates


def _dossier() -> dict:
    return {
        "candidate": {
            "title": "Тиражировать принятое решение",
            "representative_proposal": "Унифицировать способ подключения оборудования",
        },
        "source_decisions": [],
        "targets": [{
            "project_id": "P2",
            "project_name": "Корпус 2",
            "version_id": "v002",
            "rows": [{
                "row_id": "ROW-2",
                "page": 22,
                "sheet": "15",
                "name": "Шкаф управления",
                "type_mark": "ШУ-2",
                "quantity": "1",
            }],
        }],
    }


def _assessment() -> dict:
    return {
        "project_id": "P2",
        "project_name": "Корпус 2",
        "version_id": "v002",
        "verdict": "needs_graphics",
        "reason": "В таблице не видна схема подключения.",
        "target_row_ids": ["ROW-2"],
        "conditions": ["Сохранить интерфейсы"],
        "missing_data": [],
        "graphics_required": True,
        "graphics_reason": "Проверить схему подключения шкафа управления.",
        "suggested_pages": [22],
        "expert_action": "Проверить схему",
    }


def _raw_review(evidence_project: str = "P2", evidence_block: str = "B-SCHEME") -> dict:
    return {
        "project_id": "P2",
        "conclusion": "supports_replication",
        "confidence": 0.81,
        "answer": "На схеме виден совместимый интерфейс подключения.",
        "evidence": [{
            "project_id": evidence_project,
            "block_id": evidence_block,
            "page": 8,
            "observation": "Показана требуемая цепь подключения.",
        }],
        "conditions": ["Сохранить обозначенные цепи"],
        "missing_data": [],
        "expert_action": "Сопоставить маркировку перед принятием",
    }


def test_rank_block_candidates_uses_profile_and_page_text():
    catalog = [
        {
            "block_id": "B-SCHEME",
            "page": 8,
            "label": "Принципиальная схема шкафа управления",
            "profile_id": "panel_circuit_scheme",
            "page_text": "цепь подключения шкафа ШУ-2",
        },
        {
            "block_id": "B-ROUTE",
            "page": 12,
            "label": "План размещения шкафа управления",
            "profile_id": "cable_route_plan",
            "page_text": "лотки на кровле",
        },
    ]

    ranked = graphics.rank_block_candidates(
        catalog,
        "Проверить схему подключения шкафа управления",
        target_pages=[22],
        limit=2,
    )

    assert ranked[0]["block_id"] == "B-SCHEME"
    assert ranked[0]["retrieval_score"] > ranked[1]["retrieval_score"]


def test_validate_graphics_review_rejects_hallucinated_evidence():
    selected = [{
        "role": "target",
        "project_id": "P2",
        "version_id": "v002",
        "block_id": "B-SCHEME",
        "page": 8,
        "label": "Схема",
    }]
    review = graphics.validate_graphics_review(
        _raw_review(evidence_project="P9", evidence_block="FAKE"),
        project_id="P2",
        selected_blocks=selected,
    )

    assert review["evidence"] == []
    assert review["conclusion"] == "not_visible"
    assert review["resolved_verdict"] == "needs_data"


@pytest.mark.asyncio
async def test_graphics_agent_attaches_selected_images(monkeypatch, tmp_path):
    monkeypatch.setenv("SECTION_OPTIMIZATION_GRAPHICS_SOURCE_BLOCKS", "0")
    monkeypatch.setattr(graphics, "_record_usage", lambda *_args, **_kwargs: None)
    image_path = tmp_path / "block_B-SCHEME.png"
    image_path.write_bytes(b"not-decoded-by-fake-runner")
    catalog = [{
        "project_id": "P2",
        "version_id": "v002",
        "block_id": "B-SCHEME",
        "page": 8,
        "sheet": "5",
        "label": "Принципиальная схема шкафа управления",
        "profile_id": "panel_circuit_scheme",
        "page_text": "схема подключения ШУ-2",
        "image_path": str(image_path),
        "searchable": "схема подключения ШУ-2",
    }]
    captured = {}

    async def fake_runner(messages, **kwargs):
        captured["messages"] = messages
        captured["kwargs"] = kwargs
        return LLMResult(
            json_data=_raw_review(),
            model="codex/gpt-vision-test",
            input_tokens=100,
            output_tokens=40,
            duration_ms=50,
        )

    review, meta = await graphics.analyze_graphics_assessment(
        _dossier(),
        _assessment(),
        object_id="object-1",
        section="EOM",
        replication_id="repl-1",
        runner=fake_runner,
        catalog_cache={("P2", "v002"): catalog},
    )

    assert captured["kwargs"]["image_paths"] == [str(image_path)]
    assert captured["kwargs"]["output_schema"] == graphics.GRAPHICS_OUTPUT_SCHEMA
    assert review["conclusion"] == "supports_replication"
    assert review["evidence"][0]["block_id"] == "B-SCHEME"
    assert review["selected_blocks"][0]["role"] == "target"
    assert meta["model_calls"] == 1


@pytest.mark.asyncio
async def test_graphics_agent_does_not_call_model_without_relevant_target(monkeypatch):
    monkeypatch.setenv("SECTION_OPTIMIZATION_GRAPHICS_SOURCE_BLOCKS", "0")
    called = False

    async def fake_runner(*_args, **_kwargs):
        nonlocal called
        called = True
        return LLMResult(json_data=_raw_review())

    review, meta = await graphics.analyze_graphics_assessment(
        _dossier(),
        _assessment(),
        object_id="object-1",
        section="EOM",
        replication_id="repl-1",
        runner=fake_runner,
        catalog_cache={("P2", "v002"): []},
    )

    assert called is False
    assert review["conclusion"] == "not_visible"
    assert meta["model_calls"] == 0


def test_rank_block_candidates_limit_zero_disables_selection(tmp_path):
    """limit=0 обязан отключать отбор, а не молча оплачивать один vision-вызов.

    Регресс на `max(1, limit)`: соседняя ручка source_limit честно отключается,
    и оператор, гасящий расход, вправе ждать того же от target_limit.
    """
    catalog = [
        {"block_id": f"B{i}", "page": i, "label": "схема щита", "searchable": "схема щита",
         "project_id": "P", "version_id": "v1", "image_path": str(tmp_path / f"{i}.png")}
        for i in range(1, 4)
    ]
    assert rank_block_candidates(catalog, "схема щита", limit=0) == []
    assert len(rank_block_candidates(catalog, "схема щита", limit=2)) == 2


def test_image_index_matches_what_runner_actually_sends(tmp_path):
    """image_index обязан указывать на тот же блок, что и вложение с этим номером.

    Раннер (_normalize_image_paths) выбрасывает дубликаты и непрочитавшиеся
    файлы. Если не повторить фильтрацию до нумерации, каждый выброс сдвигает
    индексы и модель цитирует чужой block_id, а валидация это принимает —
    она сверяет пару project_id+block_id, но не порядок.
    """
    from backend.app.services.llm.codex_runner import _normalize_image_paths
    from backend.app.services.section_optimization_graphics_agent_service import (
        _align_with_runner_images,
        _public_block,
    )

    ok1, ok2 = tmp_path / "a.png", tmp_path / "b.png"
    ok1.write_bytes(b"x")
    ok2.write_bytes(b"x")
    selected = [
        ("target", {"block_id": "T-1", "image_path": str(ok1)}),
        ("target", {"block_id": "T-2", "image_path": str(tmp_path / "gone.png")}),  # нет файла
        ("target", {"block_id": "T-3", "image_path": str(ok2)}),
        ("source_reference", {"block_id": "S-1", "image_path": str(ok1)}),          # дубль
    ]

    aligned = _align_with_runner_images(selected)
    public = [_public_block(item, role=role, image_index=i)
              for i, (role, item) in enumerate(aligned, start=1)]
    runner_paths = _normalize_image_paths([item["image_path"] for _, item in aligned])

    assert len(public) == len(runner_paths), "промпт заявляет не столько картинок, сколько уйдёт"
    assert [p["block_id"] for p in public] == ["T-1", "T-3"]
    for entry, sent in zip(public, runner_paths):
        source = next(item for _role, item in aligned if item["block_id"] == entry["block_id"])
        assert Path(source["image_path"]).resolve() == sent


def test_page_context_map_prefers_richest_duplicate_page():
    """При дублирующихся номерах страниц побеждает самая полная запись.

    Регресс на наивное `result[page_no] = ...`: часть реальных графов содержит
    список страниц дважды — одна копия со штампом и текстом, другая пустая. На
    живых данных последняя копия оказывалась беднее лучшей в 39 случаях из 54,
    то есть ранжирование блоков для платного vision-вызова теряло контекст.
    """
    from backend.app.services.section_optimization_graphics_agent_service import _page_context_map

    graph = {
        "pages": [
            {"page": 1, "sheet_no_raw": None, "sheet_name": None, "text_blocks": []},
            {"page": 1, "sheet_no_raw": "31.11", "sheet_name": "Схема щита",
             "text_blocks": [{"text": "кабель ВВГнг 3х2.5"}]},
            # обратный порядок: полная запись идёт первой
            {"page": 2, "sheet_no_raw": "31.12", "sheet_name": "План",
             "text_blocks": [{"text": "лоток 200х50"}]},
            {"page": 2, "sheet_no_raw": None, "sheet_name": None, "text_blocks": []},
        ]
    }
    result = _page_context_map(graph)

    assert result[1]["page_text"] == "кабель ВВГнг 3х2.5"
    assert result[1]["sheet"] == "31.11"
    # порядок не должен влиять на исход
    assert result[2]["page_text"] == "лоток 200х50"
    assert result[2]["sheet"] == "31.12"
