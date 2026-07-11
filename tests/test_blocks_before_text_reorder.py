"""Тесты разворота порядка block→text (флаг PIPELINE_BLOCKS_BEFORE_TEXT_ENABLED).

Покрывают чистые/детерминированные части: компактный view 02, порядок этапов в
статус-панели и каскадном сбросе, контракты JSON-схем, подстановку {BLOCKS_ANALYSIS_PATH}.
Оркестрация manager/resume проверяется отдельными интеграционными тестами.
"""
import json
from pathlib import Path

import pytest

from backend.app.pipeline.stages.block_analysis.blocks_for_text import (
    BLOCKS_FOR_TEXT_FILENAME,
    build_compact_view,
    write_blocks_for_text_compact,
)

REPO = Path(__file__).resolve().parents[1]


# ─── Компактный view 02 ──────────────────────────────────────────────────────

def _sample_02():
    return {
        "stage": "01_blocks_analysis",
        "block_analyses": [
            {  # чистый блок — без находок и coverage ok → отбрасывается
                "block_id": "CLEAN-1", "page": 1, "sheet": "Лист 1",
                "coverage_status": "ok", "summary": "", "key_values_read": [],
                "findings": [],
            },
            {  # блок с находкой → включается, длинный finding усекается
                "block_id": "BAD-1", "page": 2, "sheet": "Лист 2",
                "coverage_status": "ok",
                "findings": [{
                    "id": "G-001", "severity": "КРИТИЧЕСКОЕ", "category": "cable",
                    "finding": "x" * 5000,
                    "value_found": "y" * 5000,
                    "block_evidence": "BAD-1",
                    "highlight_regions": [{"x": 0.1, "y": 0.1, "w": 0.2, "h": 0.2, "label": "L"}],
                }],
            },
            {  # блок без находок, но coverage не ok → включается (для warning)
                "block_id": "UNCOV-1", "page": 3, "sheet": None,
                "coverage_status": "missing_gemma_enrichment",
                "findings": [],
            },
        ],
    }


def test_compact_view_filters_clean_blocks_and_trims():
    view = build_compact_view(_sample_02())
    assert view["meta"]["total_blocks"] == 3
    assert view["meta"]["blocks_omitted_clean"] == 1  # CLEAN-1 выкинут
    ids = [b["block_id"] for b in view["blocks"]]
    assert "CLEAN-1" not in ids
    assert set(ids) == {"BAD-1", "UNCOV-1"}
    # Длинные строки усечены
    f = view["blocks"][0]["findings"][0]
    assert len(f["finding"]) < 5000 and f["finding"].endswith("…")
    assert len(f["value_found"]) < 5000
    # highlight_regions сохранены (поле из спеки Андрея Ивановича)
    assert f["highlight_regions"]


def test_compact_view_findings_budget():
    many = {"block_analyses": [
        {"block_id": f"B{i}", "page": i, "sheet": None, "coverage_status": "ok",
         "findings": [{"id": "G", "severity": "s", "category": "c", "finding": "f",
                       "value_found": "v", "block_evidence": "e", "highlight_regions": []}]}
        for i in range(700)
    ]}
    view = build_compact_view(many)
    total = sum(len(b["findings"]) for b in view["blocks"])
    assert total <= view["meta"]["max_total_findings"]
    assert view["meta"]["findings_truncated"] is True


def test_write_compact_roundtrip(tmp_path):
    (tmp_path / "01_blocks_analysis.json").write_text(
        json.dumps(_sample_02(), ensure_ascii=False), encoding="utf-8")
    dst = write_blocks_for_text_compact(tmp_path)
    assert dst is not None and dst.name == BLOCKS_FOR_TEXT_FILENAME
    data = json.loads(dst.read_text(encoding="utf-8"))
    assert data["stage"] == "01_blocks_for_text"


def test_write_compact_missing_source(tmp_path):
    assert write_blocks_for_text_compact(tmp_path) is None


# ─── Схемы (публичный контракт) ──────────────────────────────────────────────

@pytest.mark.parametrize("base", ["backend/app/schemas"])
def test_text_analysis_schema_has_new_field(base):
    d = json.loads((REPO / base / "text_analysis.json").read_text(encoding="utf-8"))
    props = d.get("properties", {})
    assert "items_verified_from_blocks" in props
    # Опциональное — НЕ в required
    assert "items_verified_from_blocks" not in d.get("required", [])


@pytest.mark.parametrize("base", ["backend/app/schemas"])
def test_block_batch_schema_drops_legacy_field(base):
    d = json.loads((REPO / base / "block_batch.json").read_text(encoding="utf-8"))
    assert "items_verified_from_stage_01" not in d.get("properties", {})


# ─── Порядок этапов флаг-зависим ─────────────────────────────────────────────

def test_stage_order_flag(monkeypatch):
    from backend.app.services.common import project_service as ps
    from backend.app.core import config as cfg

    monkeypatch.setattr(cfg, "PIPELINE_BLOCKS_BEFORE_TEXT_ENABLED", False, raising=False)
    keys_off = [k for k, _ in ps._get_stage_order()]
    assert keys_off.index("text_analysis") < keys_off.index("block_analysis")

    monkeypatch.setattr(cfg, "PIPELINE_BLOCKS_BEFORE_TEXT_ENABLED", True, raising=False)
    keys_on = [k for k, _ in ps._get_stage_order()]
    assert keys_on.index("block_analysis") < keys_on.index("text_analysis")
    assert keys_on.index("block_retry") < keys_on.index("text_analysis")


def test_audit_logger_cascade_order_flag(monkeypatch):
    from backend.app.services.common import audit_logger as al
    from backend.app.core import config as cfg

    monkeypatch.setattr(cfg, "PIPELINE_BLOCKS_BEFORE_TEXT_ENABLED", True, raising=False)
    keys = al._stage_order_keys()
    assert keys.index("block_analysis") < keys.index("text_analysis")
    assert keys.index("block_retry") < keys.index("text_analysis")


def test_downstream_dependency_text_flag(monkeypatch):
    from backend.app.services.common import project_service as ps
    from backend.app.core import config as cfg

    # OFF: block_analysis done подтверждает text_analysis (старое поведение)
    monkeypatch.setattr(cfg, "PIPELINE_BLOCKS_BEFORE_TEXT_ENABLED", False, raising=False)
    stages = {"block_analysis": {"status": "done"}}
    assert ps._downstream_done(stages, "text_analysis", {}) is True

    # ON: block_analysis done больше НЕ подтверждает text_analysis
    monkeypatch.setattr(cfg, "PIPELINE_BLOCKS_BEFORE_TEXT_ENABLED", True, raising=False)
    assert ps._downstream_done(stages, "text_analysis", {}) is False


# ─── Подстановка {BLOCKS_ANALYSIS_PATH} ──────────────────────────────────────

def test_text_task_substitutes_blocks_path():
    from backend.app.pipeline.stages.prepare.task_builder import build_text_analysis_prompt
    prompt = build_text_analysis_prompt(
        {"project_id": "adhoc", "section": "AR"},
        output_path="/tmp/out",
        md_file_path="/tmp/doc.md",
    )
    assert "{BLOCKS_ANALYSIS_PATH}" not in prompt
    assert "01_blocks_for_text.json" in prompt
