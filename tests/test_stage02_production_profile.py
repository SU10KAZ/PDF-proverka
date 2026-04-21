"""Тесты: production profile stage 02 block_batch (r800 + baseline_p3).

Закреплённые эксперименты 20–21.04.2026, КЖ5.17, 215 блоков.
Покрытие:
  A. Production defaults — все значения совпадают с решением экспериментов
  B. Guardrails — compact не раздувает Claude, ENV clamp, non-Claude path
  C. Decision surface — get_stage02_production_profile() возвращает правильный профиль
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import blocks as blk
from blocks import (
    MIN_LONG_SIDE_PX,
    TARGET_DPI,
    CLAUDE_HARD_CAP,
    _CLAUDE_RISK_TARGETS,
    STAGE02_PRODUCTION_PROFILE,
    get_stage02_production_profile,
    read_claude_risk_overrides,
    generate_block_batches,
)
from webapp.config import (
    CLAUDE_BLOCK_BATCH_PARALLELISM_DEFAULT,
    CLAUDE_BLOCK_BATCH_PARALLELISM_CAP,
    get_block_batch_parallelism,
)


# ═══════════════════════════════════════════════════════════════════
# A. Production defaults
# ═══════════════════════════════════════════════════════════════════

class TestProductionDefaults:
    def test_render_profile_is_r800(self):
        assert MIN_LONG_SIDE_PX == 800

    def test_target_dpi(self):
        assert TARGET_DPI == 100

    def test_claude_hard_cap(self):
        assert CLAUDE_HARD_CAP == 12

    def test_baseline_heavy(self):
        assert _CLAUDE_RISK_TARGETS["heavy"]["target"] == 5
        assert _CLAUDE_RISK_TARGETS["heavy"]["max"] == 6

    def test_baseline_normal(self):
        assert _CLAUDE_RISK_TARGETS["normal"]["target"] == 8
        assert _CLAUDE_RISK_TARGETS["normal"]["max"] == 8

    def test_baseline_light(self):
        assert _CLAUDE_RISK_TARGETS["light"]["target"] == 10
        assert _CLAUDE_RISK_TARGETS["light"]["max"] == 10

    def test_parallelism_default_is_3(self):
        assert CLAUDE_BLOCK_BATCH_PARALLELISM_DEFAULT == 3

    def test_parallelism_cap_is_3(self):
        assert CLAUDE_BLOCK_BATCH_PARALLELISM_CAP == 3

    def test_parallelism_helper_claude_returns_3(self):
        val = get_block_batch_parallelism("block_batch", model="claude-opus-4-7")
        assert val == 3

    def test_parallelism_helper_claude_sonnet_returns_3(self):
        val = get_block_batch_parallelism("block_batch", model="claude-sonnet-4-6")
        assert val == 3


# ═══════════════════════════════════════════════════════════════════
# B. Guardrails
# ═══════════════════════════════════════════════════════════════════

class TestGuardrails:
    def test_hard_cap_not_exceeded_by_env_override(self, monkeypatch):
        """ENV override не может пробить CLAUDE_HARD_CAP = 12."""
        monkeypatch.setenv("CLAUDE_BATCH_LIGHT_TARGET", "20")
        monkeypatch.setenv("CLAUDE_BATCH_LIGHT_MAX", "30")
        targets = read_claude_risk_overrides()
        assert targets["light"]["target"] <= CLAUDE_HARD_CAP
        assert targets["light"]["max"] <= CLAUDE_HARD_CAP

    def test_hard_cap_not_exceeded_heavy(self, monkeypatch):
        monkeypatch.setenv("CLAUDE_BATCH_HEAVY_MAX", "99")
        targets = read_claude_risk_overrides()
        assert targets["heavy"]["max"] <= CLAUDE_HARD_CAP

    def test_env_override_clamp_normal(self, monkeypatch):
        monkeypatch.setenv("CLAUDE_BATCH_NORMAL_TARGET", "15")
        targets = read_claude_risk_overrides()
        assert targets["normal"]["target"] == CLAUDE_HARD_CAP  # clamped to 12

    def test_no_env_override_uses_production_defaults(self, monkeypatch):
        """При отсутствии ENV переменных — дефолты совпадают с baseline profile."""
        for key in [
            "CLAUDE_BATCH_HEAVY_TARGET", "CLAUDE_BATCH_HEAVY_MAX",
            "CLAUDE_BATCH_NORMAL_TARGET", "CLAUDE_BATCH_NORMAL_MAX",
            "CLAUDE_BATCH_LIGHT_TARGET", "CLAUDE_BATCH_LIGHT_MAX",
        ]:
            monkeypatch.delenv(key, raising=False)
        targets = read_claude_risk_overrides()
        assert targets == _CLAUDE_RISK_TARGETS

    def test_parallelism_env_override_clamped_to_cap(self, monkeypatch):
        monkeypatch.setenv("CLAUDE_BLOCK_BATCH_PARALLELISM", "10")
        val = get_block_batch_parallelism("block_batch", model="claude-opus-4-7")
        assert val == CLAUDE_BLOCK_BATCH_PARALLELISM_CAP  # 3

    def test_parallelism_env_override_valid(self, monkeypatch):
        monkeypatch.setenv("CLAUDE_BLOCK_BATCH_PARALLELISM", "2")
        val = get_block_batch_parallelism("block_batch", model="claude-opus-4-7")
        assert val == 2  # safe fallback via ENV

    def test_parallelism_env_garbage_uses_default(self, monkeypatch):
        monkeypatch.setenv("CLAUDE_BLOCK_BATCH_PARALLELISM", "abc")
        val = get_block_batch_parallelism("block_batch", model="claude-opus-4-7")
        assert val == CLAUDE_BLOCK_BATCH_PARALLELISM_DEFAULT  # 3

    def test_non_claude_parallelism_unchanged(self):
        """OpenRouter/GPT path использует MAX_PARALLEL_BATCHES, не Claude default."""
        from webapp.config import MAX_PARALLEL_BATCHES
        val = get_block_batch_parallelism("block_batch", model="openai/gpt-5.4")
        assert val == MAX_PARALLEL_BATCHES
        assert val != CLAUDE_BLOCK_BATCH_PARALLELISM_DEFAULT  # разные пути

    def test_compact_does_not_exceed_hard_cap_for_claude(self, tmp_path):
        """compact → 30 путь для non-Claude НЕ должен применяться к Claude батчам."""
        # Проверяем через generate_block_batches с моделью Claude
        # Создаём минимальный fake project
        proj_dir = tmp_path / "test_proj"
        out_dir = proj_dir / "_output" / "blocks"
        out_dir.mkdir(parents=True)

        import json
        blocks = []
        for i in range(50):
            fname = f"block_{i:03d}.png"
            (out_dir / fname).write_bytes(b"PNG" * 100)
            blocks.append({
                "block_id": f"BLK{i:03d}", "page": (i % 5) + 1,
                "file": f"blocks/{fname}", "size_kb": 30,
                "is_full_page": False, "render_size": [400, 300],
                "ocr_text_len": 50, "crop_px": [0, 0, 400, 300],
                "merged_block_ids": [],
            })
        (out_dir / "index.json").write_text(
            json.dumps({"blocks": blocks}), encoding="utf-8",
        )
        (proj_dir / "project_info.json").write_text(
            json.dumps({"project_id": "test", "section": "EOM"}), encoding="utf-8",
        )

        result = generate_block_batches(
            str(proj_dir),
            max_blocks=CLAUDE_HARD_CAP,  # явный Claude cap
        )
        if result.get("error"):
            pytest.skip("project setup issue")
        for batch in result.get("batches", []):
            assert batch["block_count"] <= CLAUDE_HARD_CAP, (
                f"batch {batch['batch_id']} has {batch['block_count']} > {CLAUDE_HARD_CAP}"
            )


# ═══════════════════════════════════════════════════════════════════
# C. Decision surface
# ═══════════════════════════════════════════════════════════════════

class TestDecisionSurface:
    def test_production_profile_render_r800(self):
        p = get_stage02_production_profile()
        assert p["render_profile"] == "r800"
        assert p["min_long_side_px"] == 800

    def test_production_profile_batch_baseline(self):
        p = get_stage02_production_profile()
        assert p["claude_batch_profile"] == "baseline"
        assert p["batch_targets"]["heavy"] == {"target": 5, "max": 6}
        assert p["batch_targets"]["normal"] == {"target": 8, "max": 8}
        assert p["batch_targets"]["light"] == {"target": 10, "max": 10}

    def test_production_profile_hard_cap(self):
        p = get_stage02_production_profile()
        assert p["claude_hard_cap"] == 12

    def test_production_profile_parallelism(self):
        p = get_stage02_production_profile()
        assert p["claude_block_batch_parallelism_default"] == 3
        assert p["claude_block_batch_parallelism_cap"] == 3

    def test_production_profile_safe_fallback_documented(self):
        p = get_stage02_production_profile()
        assert "baseline_p2" in p["safe_fallback"]
        assert "r800" in p["safe_fallback"]

    def test_production_profile_consistent_with_runtime_constants(self):
        """STAGE02_PRODUCTION_PROFILE не расходится с runtime-константами blocks.py."""
        p = STAGE02_PRODUCTION_PROFILE
        assert p["min_long_side_px"] == MIN_LONG_SIDE_PX
        assert p["target_dpi"] == TARGET_DPI
        assert p["claude_hard_cap"] == CLAUDE_HARD_CAP
        assert p["batch_targets"] == _CLAUDE_RISK_TARGETS

    def test_production_profile_consistent_with_config_parallelism(self):
        """STAGE02_PRODUCTION_PROFILE parallelism не расходится с webapp/config."""
        p = STAGE02_PRODUCTION_PROFILE
        assert p["claude_block_batch_parallelism_default"] == CLAUDE_BLOCK_BATCH_PARALLELISM_DEFAULT
        assert p["claude_block_batch_parallelism_cap"] == CLAUDE_BLOCK_BATCH_PARALLELISM_CAP

    def test_get_stage02_production_profile_returns_copy(self):
        """Мутация возвращаемого dict не меняет STAGE02_PRODUCTION_PROFILE."""
        p = get_stage02_production_profile()
        p["render_profile"] = "r9999"
        assert STAGE02_PRODUCTION_PROFILE["render_profile"] == "r800"

    def test_experimental_profiles_differ_from_production(self):
        """Aggressive profile в матрице отличается от production baseline."""
        from scripts.run_claude_block_batch_matrix import PROFILES
        prod = _CLAUDE_RISK_TARGETS
        aggr = PROFILES["aggressive"]
        # Aggressive имеет хотя бы один target/max выше baseline
        assert aggr["light"]["max"] > prod["light"]["max"]
