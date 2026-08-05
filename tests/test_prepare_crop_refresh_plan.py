"""«Подготовить данные» = кропы по ссылкам + CTX.

Раньше пропажа даже одного PNG выставляла `--force` — и crop_blocks перекачивал
по crop_url ВЕСЬ проект. Теперь force остаётся только для реального рассинхрона
политики рендера, а недостающие PNG просто докачиваются (существующие
переиспользуются веткой `[EXISTS]` в crop_blocks).
"""
from __future__ import annotations

import json
from pathlib import Path

from backend.app.pipeline.stages.gemma_enrichment.gemma_enrichment_contract import (
    stage02_crop_policy,
)
from backend.app.pipeline.stages.prepare import prepare_service


def _write_index(blocks_dir: Path, block_ids: list[str], *, policy: dict) -> None:
    blocks_dir.mkdir(parents=True, exist_ok=True)
    (blocks_dir / "index.json").write_text(json.dumps({
        "total_blocks": len(block_ids),
        "profile": policy["profile"],
        "dpi": policy["dpi"],
        "min_long_side": policy["min_long_side"],
        "compact": policy["compact"],
        "skip_small": policy["skip_small"],
        "blocks": [
            {"block_id": bid, "file": f"block_{bid}.png", "page": 1}
            for bid in block_ids
        ],
    }), encoding="utf-8")


def _write_png(blocks_dir: Path, block_id: str) -> None:
    (blocks_dir / f"block_{block_id}.png").write_bytes(b"\x89PNG\r\n\x1a\n" + b"0" * 4096)


def test_missing_pngs_are_topped_up_without_full_recrop(tmp_path):
    policy = stage02_crop_policy()
    blocks_dir = tmp_path / "blocks_stage02_100"
    _write_index(blocks_dir, ["AAA", "BBB", "CCC"], policy=policy)
    _write_png(blocks_dir, "AAA")
    _write_png(blocks_dir, "BBB")
    # block_CCC.png отсутствует

    force_crop, missing = prepare_service._plan_crop_refresh(blocks_dir, policy)

    assert force_crop is False, "пропавший PNG не должен запускать перекачку всего проекта"
    assert missing, "недостающий PNG должен быть виден в отчёте"


def test_policy_mismatch_forces_full_recrop(tmp_path):
    policy = stage02_crop_policy()
    blocks_dir = tmp_path / "blocks_stage02_100"
    _write_index(blocks_dir, ["AAA"], policy={**policy, "dpi": 300, "profile": "gemma_300"})
    _write_png(blocks_dir, "AAA")

    force_crop, _missing = prepare_service._plan_crop_refresh(blocks_dir, policy)

    assert force_crop is True, "PNG чужого профиля переиспользовать нельзя"


def test_no_index_means_plain_crop(tmp_path):
    policy = stage02_crop_policy()
    blocks_dir = tmp_path / "blocks_stage02_100"
    blocks_dir.mkdir(parents=True)
    _write_png(blocks_dir, "AAA")  # PNG есть, index.json нет

    force_crop, missing = prepare_service._plan_crop_refresh(blocks_dir, policy)

    assert force_crop is False
    assert missing == []


def test_crop_args_reflect_plan(tmp_path):
    policy = stage02_crop_policy()
    args_topup = prepare_service._build_crop_args(
        tmp_path, force=False, policy=policy, output_dir=tmp_path / "blocks_stage02_100",
    )
    args_force = prepare_service._build_crop_args(
        tmp_path, force=True, policy=policy, output_dir=tmp_path / "blocks_stage02_100",
    )

    assert "--force" not in args_topup
    assert "--force" in args_force
    assert "--no-skip-small" in args_topup  # stage02 policy: skip_small=False
