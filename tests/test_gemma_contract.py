"""reserc.md #18 — тесты валидатора gemma_enrichment_contract (раньше без тестов).

validate_gemma_summary — gate готовности обязательного Gemma enrichment. Покрываем
битые summary: schema v1→migration, hash mismatch, duplicate_block_id,
coverage_ratio_mismatch, md_hash_mismatch + happy-path.
"""
from __future__ import annotations

import json

from backend.app.pipeline.stages.gemma_enrichment import gemma_enrichment_contract as c


def _setup(tmp_path):
    """Собрать валидную фикстуру: base index (2 image-блока) + MD + canonical summary."""
    bdir = c.gemma_base_blocks_dir(tmp_path)
    bdir.mkdir(parents=True, exist_ok=True)
    index = {**c.gemma_base_crop_policy(), "blocks": [
        {"block_id": "b1", "block_type": "image"},
        {"block_id": "b2", "block_type": "image"},
    ]}
    c.gemma_base_blocks_index_path(tmp_path).write_text(
        json.dumps(index), encoding="utf-8"
    )
    md = tmp_path / "doc.md"
    md.write_text("# doc\nsome text\n", encoding="utf-8")
    summary = c.build_gemma_summary(
        status="ok", project_dir=tmp_path, md_path=md, model="gemma",
        blocks_total=2, blocks_ok=2, blocks_failed=0,
    )
    return md, summary


def test_valid_summary_passes(tmp_path):
    md, summary = _setup(tmp_path)
    res = c.validate_gemma_summary(tmp_path, md_path=md, summary=summary)
    assert res["valid"] is True, res


def test_schema_v1_triggers_migration(tmp_path):
    md, summary = _setup(tmp_path)
    summary["schema_version"] = 1                       # старая схема
    res = c.validate_gemma_summary(tmp_path, md_path=md, summary=summary)
    assert res["valid"] is False
    assert res["reason_code"] == "schema_mismatch"


def test_blocks_index_hash_mismatch(tmp_path):
    md, summary = _setup(tmp_path)
    summary["base_blocks_index_hash"] = "deadbeef"      # индекс «изменился»
    res = c.validate_gemma_summary(tmp_path, md_path=md, summary=summary)
    assert res["valid"] is False
    assert res["reason_code"] == "blocks_index_hash_mismatch"


def test_duplicate_block_id(tmp_path):
    md, summary = _setup(tmp_path)
    summary["blocks"].append(dict(summary["blocks"][0]))   # дубль b1
    summary["blocks_total"] = len(summary["blocks"])
    res = c.validate_gemma_summary(tmp_path, md_path=md, summary=summary)
    assert res["valid"] is False
    assert res["reason_code"] == "duplicate_block_id"


def test_coverage_ratio_mismatch(tmp_path):
    md, summary = _setup(tmp_path)
    summary["coverage_ratio"] = 0.123                   # неверное значение
    res = c.validate_gemma_summary(tmp_path, md_path=md, summary=summary)
    assert res["valid"] is False
    assert res["reason_code"] == "coverage_ratio_mismatch"


def test_md_hash_mismatch(tmp_path):
    md, summary = _setup(tmp_path)
    md.write_text("# doc\nCHANGED content\n", encoding="utf-8")  # MD изменился
    res = c.validate_gemma_summary(tmp_path, md_path=md, summary=summary)
    assert res["valid"] is False
    assert res["reason_code"] == "md_hash_mismatch"


def test_missing_summary(tmp_path):
    res = c.validate_gemma_summary(tmp_path, summary={})
    assert res["reason_code"] == "missing_summary"


def test_stage_mismatch(tmp_path):
    md, summary = _setup(tmp_path)
    summary["stage"] = "something_else"
    res = c.validate_gemma_summary(tmp_path, md_path=md, summary=summary)
    assert res["reason_code"] == "stage_mismatch"


def test_coverage_ratio_math():
    assert c.coverage_ratio(2, 2) == 1.0
    assert c.coverage_ratio(0, 0) == 0.0
    assert c.coverage_ratio(1, 3) == round(1 / 3, 6)
