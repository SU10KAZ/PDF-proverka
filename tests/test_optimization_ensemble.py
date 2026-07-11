from __future__ import annotations

import asyncio
import json
from pathlib import Path
from types import SimpleNamespace

from backend.app.pipeline.stages.optimization import ensemble


def _item(
    item_id: str,
    *,
    current: str = "Клапан Danfoss DN50",
    proposed: str = "Заменить на допустимый аналог с теми же параметрами",
    item_type: str = "cheaper_analog",
    spec_items: list[str] | None = None,
) -> dict:
    return {
        "id": item_id,
        "section": "ОВ",
        "page": 12,
        "sheet": "Лист 7",
        "spec_items": spec_items or ["Поз. 5 - Клапан Danfoss DN50"],
        "current": current,
        "proposed": proposed,
        "type": item_type,
        "savings_pct": 10,
        "savings_basis": "экспертная оценка",
        "timeline_impact": "без изменений",
        "risks": "Проверить гидравлику",
        "status": "предложение",
        "norm": "",
    }


def _doc(*items: dict) -> dict:
    return {
        "meta": {"project_id": "p1", "project_name": "Test"},
        "items": list(items),
    }


def test_exact_duplicate_is_merged_with_both_providers():
    merged, report = ensemble.merge_optimization_documents(
        _doc(_item("OPT-001")),
        _doc(_item("OPT-901")),
        run_id="run-1",
    )

    assert len(merged["items"]) == 1
    assert report["duplicates_merged"] == 1
    assert merged["items"][0]["detector_summary"] == "claude_codex"
    assert merged["items"][0]["provenance"]["found_by"] == ["claude", "codex"]


def test_same_object_with_different_optimization_actions_is_preserved():
    claude_item = _item(
        "OPT-001",
        current="Воздуховоды имеют 18 типоразмеров",
        proposed="Унифицировать номенклатуру до 10 типоразмеров",
        item_type="simpler_design",
        spec_items=["Поз. 10-28 - Воздуховоды"],
    )
    codex_item = _item(
        "OPT-002",
        current="Воздуховоды собираются отдельными участками на площадке",
        proposed="Перейти на заводские модульные секции для ускорения монтажа",
        item_type="simpler_design",
        spec_items=["Поз. 10-28 - Воздуховоды"],
    )

    merged, report = ensemble.merge_optimization_documents(
        _doc(claude_item), _doc(codex_item), run_id="run-2"
    )

    assert len(merged["items"]) == 2
    assert report["duplicates_merged"] == 0
    assert {item["detector_summary"] for item in merged["items"]} == {"claude", "codex"}


def test_single_provider_result_is_degraded_but_not_lost():
    merged, report = ensemble.merge_optimization_documents(
        _doc(_item("OPT-001")), None, run_id="run-3"
    )

    assert report["status"] == "degraded"
    assert len(merged["items"]) == 1
    assert merged["meta"]["ensemble"]["source_counts"]["claude_only"] == 1


def test_runner_starts_providers_concurrently_and_keeps_raw_outputs(tmp_path, monkeypatch):
    started: list[str] = []
    both_started = asyncio.Event()

    async def fake_run_optimization(project_info, project_id, **kwargs):
        model = kwargs["model_override"]
        started.append(model)
        if len(started) == 2:
            both_started.set()
        await asyncio.wait_for(both_started.wait(), timeout=1)
        output_dir = Path(kwargs["output_dir"])
        output_dir.mkdir(parents=True, exist_ok=True)
        proposed = "Заменить клапан на допустимый аналог"
        payload = _doc(_item("OPT-001", proposed=proposed))
        (output_dir / "optimization.json").write_text(
            json.dumps(payload, ensure_ascii=False), encoding="utf-8"
        )
        return 0, "ok", SimpleNamespace(duration_ms=1)

    monkeypatch.setattr(ensemble.claude_runner, "run_optimization", fake_run_optimization)
    logs: list[str] = []

    async def log(message, *args):
        logs.append(message)

    result = asyncio.run(
        ensemble.run_optimization_ensemble(
            project_info={"section": "ОВ"},
            project_id="p1",
            output_dir=tmp_path,
            version_dir=tmp_path,
            version_id="v001",
            log=log,
        )
    )

    assert result.success is True
    assert result.status == "ok"
    assert len(started) == 2
    assert (tmp_path / "optimization_claude.json").is_file()
    assert (tmp_path / "optimization_codex.json").is_file()
    assert (tmp_path / "optimization_merge_report.json").is_file()
    assert json.loads((tmp_path / "optimization.json").read_text(encoding="utf-8"))["items"][0]["detector_summary"] == "claude_codex"


def test_ensemble_model_is_only_allowed_for_optimization():
    from backend.app.core import config

    model = config.OPTIMIZATION_DUAL_MODEL_ID
    assert config.validate_stage_model_choice("optimization", model) is None
    assert config.validate_stage_model_choice("block_batch", model) is not None


def test_provenance_is_restored_after_corrector_rewrite(tmp_path):
    original, _ = ensemble.merge_optimization_documents(
        _doc(_item("OPT-001")), _doc(_item("OPT-002")), run_id="run-4"
    )
    (tmp_path / "optimization_pre_review.json").write_text(
        json.dumps(original, ensure_ascii=False), encoding="utf-8"
    )
    rewritten = json.loads(json.dumps(original))
    rewritten["items"][0].pop("provenance")
    rewritten["items"][0].pop("detector_summary")
    rewritten["meta"].pop("ensemble")
    (tmp_path / "optimization.json").write_text(
        json.dumps(rewritten, ensure_ascii=False), encoding="utf-8"
    )
    (tmp_path / "optimization_merge_report.json").write_text("{}", encoding="utf-8")

    restored = ensemble.restore_ensemble_provenance(tmp_path)
    saved = json.loads((tmp_path / "optimization.json").read_text(encoding="utf-8"))

    assert restored == 1
    assert saved["items"][0]["detector_summary"] == "claude_codex"
    assert saved["meta"]["ensemble"]["run_id"] == "run-4"


def test_provider_failure_uses_other_result_and_removes_stale_raw_file(tmp_path, monkeypatch):
    (tmp_path / "optimization_codex.json").write_text(
        json.dumps(_doc(_item("OPT-OLD"))), encoding="utf-8"
    )

    async def fake_run_optimization(project_info, project_id, **kwargs):
        if str(kwargs["model_override"]).startswith("codex/"):
            return 1, "codex unavailable", SimpleNamespace(duration_ms=1)
        output_dir = Path(kwargs["output_dir"])
        output_dir.mkdir(parents=True, exist_ok=True)
        (output_dir / "optimization.json").write_text(
            json.dumps(_doc(_item("OPT-001")), ensure_ascii=False), encoding="utf-8"
        )
        return 0, "ok", SimpleNamespace(duration_ms=1)

    monkeypatch.setattr(ensemble.claude_runner, "run_optimization", fake_run_optimization)

    async def log(message, *args):
        return None

    result = asyncio.run(
        ensemble.run_optimization_ensemble(
            project_info={"section": "ОВ"}, project_id="p1",
            output_dir=tmp_path, version_dir=tmp_path, version_id="v001", log=log,
        )
    )

    assert result.success is True
    assert result.status == "degraded"
    assert (tmp_path / "optimization_claude.json").is_file()
    assert not (tmp_path / "optimization_codex.json").exists()
    final = json.loads((tmp_path / "optimization.json").read_text(encoding="utf-8"))
    assert final["items"][0]["detector_summary"] == "claude"
