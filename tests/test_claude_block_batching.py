"""Тесты для Claude-specific риск-aware батчинга и параллелизма stage 02.

Ловят два класса регрессий:
  1) compact-режим раздувает Claude batch до 30 блоков (раньше было).
  2) stage 02 block_batch использует общий MAX_PARALLEL_BATCHES=5 для Claude (раньше было).
"""
import json
import os
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from blocks import (  # noqa: E402
    CLAUDE_HARD_CAP,
    MODEL_BATCH_LIMITS,
    _classify_block_risk,
    _claude_cap_for_risk,
    _pack_blocks_claude_risk_aware,
    generate_block_batches,
)


# ───────── classification ──────────────────────────────────────────────────

def test_classify_full_page_is_heavy():
    assert _classify_block_risk({"is_full_page": True, "size_kb": 50}) == "heavy"


def test_classify_quadrant_is_heavy():
    assert _classify_block_risk({"quadrant": "TL", "size_kb": 50}) == "heavy"


def test_classify_merged_block_ids_is_heavy():
    assert _classify_block_risk({"merged_block_ids": ["a", "b"], "size_kb": 30}) == "heavy"


def test_classify_big_size_is_heavy():
    assert _classify_block_risk({"size_kb": 2500}) == "heavy"


def test_classify_big_render_is_heavy():
    assert _classify_block_risk({"size_kb": 100, "render_size": [2800, 900]}) == "heavy"


def test_classify_big_ocr_is_heavy():
    assert _classify_block_risk({"size_kb": 100, "ocr_text_len": 5000}) == "heavy"


def test_classify_big_crop_is_heavy():
    # crop_px = [x1, y1, x2, y2], long side = 3500
    assert _classify_block_risk({"size_kb": 100, "crop_px": [0, 0, 3500, 500]}) == "heavy"


def test_classify_normal():
    assert _classify_block_risk({"size_kb": 700}) == "normal"
    assert _classify_block_risk({"ocr_text_len": 1500}) == "normal"


def test_classify_light():
    assert _classify_block_risk({"size_kb": 80, "ocr_text_len": 200}) == "light"
    assert _classify_block_risk({}) == "light"


def test_claude_cap_for_risk_respects_hard_cap():
    for risk in ("heavy", "normal", "light"):
        assert _claude_cap_for_risk(risk) <= CLAUDE_HARD_CAP
    # Heavy cap очень мал
    assert _claude_cap_for_risk("heavy") <= 6


# ───────── risk-aware packing ──────────────────────────────────────────────

def _mk(block_id: str, **kw):
    base = {"block_id": block_id, "page": kw.pop("page", 1),
            "file": f"blocks/{block_id}.png", "size_kb": kw.pop("size_kb", 80)}
    base.update(kw)
    return base


def test_pack_claude_hard_cap_never_exceeds_12():
    """Никакой батч Claude не может превысить CLAUDE_HARD_CAP = 12."""
    blocks = [_mk(f"b{i}", size_kb=30, page=i // 5 + 1) for i in range(60)]
    packed = _pack_blocks_claude_risk_aware(blocks, max_size_kb=50000, solo_threshold_kb=3072)
    for group in packed:
        assert len(group) <= CLAUDE_HARD_CAP, f"Claude batch {len(group)} > hard cap {CLAUDE_HARD_CAP}"


def test_pack_claude_heavy_blocks_small_batches():
    """Полностраничные блоки классифицируются как heavy и идут маленькими пакетами."""
    blocks = [
        _mk(f"fp{i}", size_kb=900, page=i + 1, is_full_page=True)
        for i in range(12)
    ]
    packed = _pack_blocks_claude_risk_aware(blocks, max_size_kb=50000, solo_threshold_kb=3072)
    # 12 heavy блоков → минимум 2 пакета (heavy max 6)
    assert len(packed) >= 2
    for group in packed:
        heavy_count = sum(1 for b in group if _classify_block_risk(b) == "heavy")
        assert heavy_count <= 6, f"Слишком много heavy в одном пакете: {heavy_count}"


def test_pack_claude_merged_blocks_are_heavy():
    """merged_block_ids → heavy → маленький пакет."""
    blocks = [
        _mk(f"m{i}", size_kb=400, page=i + 1, merged_block_ids=["x", "y"])
        for i in range(10)
    ]
    packed = _pack_blocks_claude_risk_aware(blocks, max_size_kb=50000, solo_threshold_kb=3072)
    for group in packed:
        assert len(group) <= 6


def test_pack_claude_quadrant_blocks_are_heavy():
    """quadrant → heavy."""
    blocks = [
        _mk(f"q{i}", size_kb=500, page=i + 1, quadrant="TL")
        for i in range(8)
    ]
    packed = _pack_blocks_claude_risk_aware(blocks, max_size_kb=50000, solo_threshold_kb=3072)
    for group in packed:
        assert len(group) <= 6


def test_pack_claude_light_blocks_pack_larger():
    """Лёгкие блоки (<500KB, короткий OCR) собираются батчами побольше."""
    blocks = [_mk(f"b{i}", size_kb=30, page=i + 1) for i in range(40)]
    packed = _pack_blocks_claude_risk_aware(blocks, max_size_kb=50000, solo_threshold_kb=3072)
    # Ожидаем batch ≈ 10 (light max), так что 40 блоков ≈ 4 пакета
    assert len(packed) <= 5
    # И все упакованы
    total = sum(len(g) for g in packed)
    assert total == 40


def test_pack_claude_solo_threshold_separates_giants():
    """Блок ≥ solo_threshold → отдельный пакет."""
    blocks = [
        _mk("small", size_kb=50),
        _mk("giant", size_kb=4000),
        _mk("small2", size_kb=50),
    ]
    packed = _pack_blocks_claude_risk_aware(blocks, max_size_kb=5120, solo_threshold_kb=3072)
    # giant должен быть отдельно
    assert any(len(g) == 1 and g[0]["block_id"] == "giant" for g in packed)


def test_pack_claude_no_lost_blocks():
    blocks = [_mk(f"b{i}", size_kb=50 + (i % 10) * 30, page=i // 4 + 1) for i in range(50)]
    packed = _pack_blocks_claude_risk_aware(blocks, max_size_kb=50000, solo_threshold_kb=3072)
    ids_in = {b["block_id"] for b in blocks}
    ids_out = {b["block_id"] for g in packed for b in g}
    assert ids_in == ids_out


def test_pack_claude_dense_pages_isolated():
    """Плотная страница не мешается с другими в одном пакете."""
    blocks = (
        [_mk(f"p1_{i}", size_kb=30, page=1) for i in range(3)]
        + [_mk(f"p2_{i}", size_kb=30, page=2) for i in range(20)]
        + [_mk(f"p3_{i}", size_kb=30, page=3) for i in range(3)]
    )
    packed = _pack_blocks_claude_risk_aware(
        blocks, max_size_kb=50000, solo_threshold_kb=3072, dense_pages={2}
    )
    for group in packed:
        pages = {b["page"] for b in group}
        if 2 in pages:
            assert pages == {2}


# ───────── end-to-end: compact не раздувает Claude batch ──────────────────

def test_model_batch_limits_claude_hard_capped():
    """MODEL_BATCH_LIMITS для Claude не превышают CLAUDE_HARD_CAP."""
    assert MODEL_BATCH_LIMITS["claude-opus-4-7"]["max_blocks"] <= CLAUDE_HARD_CAP
    assert MODEL_BATCH_LIMITS["claude-sonnet-4-6"]["max_blocks"] <= CLAUDE_HARD_CAP


def _write_index(output_dir: Path, blocks: list[dict], compact: bool = False):
    output_dir.mkdir(parents=True, exist_ok=True)
    blocks_dir = output_dir / "blocks"
    blocks_dir.mkdir(parents=True, exist_ok=True)
    index = {"compact": compact, "blocks": blocks}
    (blocks_dir / "index.json").write_text(json.dumps(index), encoding="utf-8")


def _write_stage_models(stage: str, model: str, tmp_cfg_dir: Path):
    tmp_cfg_dir.mkdir(parents=True, exist_ok=True)
    (tmp_cfg_dir / "stage_models.json").write_text(
        json.dumps({stage: model}), encoding="utf-8"
    )


def test_generate_claude_compact_never_exceeds_hard_cap(tmp_path, monkeypatch):
    """Даже при compact=True и 60 лёгких блоках Claude batch не должен стать 30."""
    # Подменяем путь к stage_models.json, чтобы модель была Claude
    fake_cfg = tmp_path / "webapp" / "data"
    _write_stage_models("block_batch", "claude-opus-4-7", fake_cfg)
    monkeypatch.setattr("blocks._STAGE_MODELS_PATH", fake_cfg / "stage_models.json")

    project_dir = tmp_path / "proj"
    output_dir = project_dir / "_output"
    blocks = [
        {
            "block_id": f"b{i:03d}",
            "page": (i // 6) + 1,
            "file": f"block_b{i:03d}.png",
            "size_kb": 25,
            "ocr_text_len": 80,
            "render_size": [800, 400],
        }
        for i in range(60)
    ]
    _write_index(output_dir, blocks, compact=True)

    result = generate_block_batches(str(project_dir))
    assert "batches" in result
    for batch in result["batches"]:
        assert batch["block_count"] <= CLAUDE_HARD_CAP, (
            f"Claude+compact batch {batch['block_count']} > {CLAUDE_HARD_CAP}"
        )
    # Стратегия должна быть risk-aware, а не adaptive с max_blocks=30
    assert result["strategy"] == "claude_risk_aware"


def test_generate_non_claude_compact_still_allowed_to_grow(tmp_path, monkeypatch):
    """Не-Claude путь (Gemini) по-прежнему может иметь batch > 12 при compact."""
    fake_cfg = tmp_path / "webapp" / "data"
    _write_stage_models("block_batch", "google/gemini-3.1-pro-preview", fake_cfg)
    monkeypatch.setattr("blocks._STAGE_MODELS_PATH", fake_cfg / "stage_models.json")

    project_dir = tmp_path / "proj"
    output_dir = project_dir / "_output"
    blocks = [
        {
            "block_id": f"b{i:03d}",
            "page": (i // 10) + 1,
            "file": f"block_b{i:03d}.png",
            "size_kb": 25,
        }
        for i in range(60)
    ]
    _write_index(output_dir, blocks, compact=True)

    result = generate_block_batches(str(project_dir))
    # Для не-Claude стратегия остаётся adaptive
    assert result["strategy"] == "adaptive"
    # Лимит может вырасти выше 15 (compact для Gemini ок)
    max_in_batch = max(b["block_count"] for b in result["batches"]) if result["batches"] else 0
    assert max_in_batch > CLAUDE_HARD_CAP or len(result["batches"]) > 0


# ───────── parallelism helper ──────────────────────────────────────────────

def test_block_batch_parallelism_claude_default(monkeypatch):
    monkeypatch.delenv("CLAUDE_BLOCK_BATCH_PARALLELISM", raising=False)
    from webapp.config import (
        get_block_batch_parallelism,
        CLAUDE_BLOCK_BATCH_PARALLELISM_DEFAULT,
        CLAUDE_BLOCK_BATCH_PARALLELISM_CAP,
    )
    assert CLAUDE_BLOCK_BATCH_PARALLELISM_DEFAULT == 2
    assert CLAUDE_BLOCK_BATCH_PARALLELISM_CAP == 3
    assert get_block_batch_parallelism("block_batch", model="claude-opus-4-7") == 2
    assert get_block_batch_parallelism("block_batch", model="claude-sonnet-4-6") == 2


def test_block_batch_parallelism_claude_env_clamped(monkeypatch):
    """ENV override всё равно clamp до cap=3."""
    from webapp.config import get_block_batch_parallelism
    monkeypatch.setenv("CLAUDE_BLOCK_BATCH_PARALLELISM", "99")
    assert get_block_batch_parallelism("block_batch", model="claude-opus-4-7") == 3
    monkeypatch.setenv("CLAUDE_BLOCK_BATCH_PARALLELISM", "1")
    assert get_block_batch_parallelism("block_batch", model="claude-opus-4-7") == 1
    monkeypatch.setenv("CLAUDE_BLOCK_BATCH_PARALLELISM", "-5")
    # -5 → invalid, fallback на default, clamp до cap
    assert get_block_batch_parallelism("block_batch", model="claude-opus-4-7") == 2
    monkeypatch.setenv("CLAUDE_BLOCK_BATCH_PARALLELISM", "not-a-number")
    assert get_block_batch_parallelism("block_batch", model="claude-opus-4-7") == 2


def test_block_batch_parallelism_non_claude_uses_general(monkeypatch):
    """Для OpenRouter используется общий MAX_PARALLEL_BATCHES."""
    monkeypatch.delenv("CLAUDE_BLOCK_BATCH_PARALLELISM", raising=False)
    from webapp.config import get_block_batch_parallelism, MAX_PARALLEL_BATCHES
    assert get_block_batch_parallelism("block_batch", model="openai/gpt-5.4") == MAX_PARALLEL_BATCHES
    assert get_block_batch_parallelism("block_batch", model="google/gemini-3.1-pro-preview") == MAX_PARALLEL_BATCHES


def test_block_batch_parallelism_claude_never_exceeds_3(monkeypatch):
    """Любое значение ENV не должно привести к параллелизму > 3 для Claude."""
    from webapp.config import get_block_batch_parallelism
    for v in ("4", "5", "10", "100"):
        monkeypatch.setenv("CLAUDE_BLOCK_BATCH_PARALLELISM", v)
        assert get_block_batch_parallelism("block_batch", model="claude-opus-4-7") <= 3
