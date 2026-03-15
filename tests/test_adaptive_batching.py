"""Тесты для адаптивной пакетировки блоков."""
import pytest
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from blocks import _pack_blocks_adaptive, _make_batch_entry


def _make_block(block_id: str, size_kb: int = 100, page: int = 1):
    return {
        "block_id": block_id,
        "page": page,
        "file": f"blocks/{block_id}.png",
        "size_kb": size_kb,
    }


def test_make_batch_entry():
    blocks = [_make_block("b1", 100), _make_block("b2", 200)]
    entry = _make_batch_entry(1, blocks)  # (batch_id, blocks_list)
    assert entry["batch_id"] == 1
    assert entry["block_count"] == 2
    assert len(entry["blocks"]) == 2


def test_pack_adaptive_single_large_block():
    """Блок > SOLO_BLOCK_THRESHOLD → один в пакете."""
    blocks = [_make_block("b1", 4000)]
    # _pack_blocks_adaptive returns list[list[dict]]
    packed = _pack_blocks_adaptive(blocks, max_size_kb=5120, max_blocks=15)
    assert len(packed) == 1
    assert len(packed[0]) == 1


def test_pack_adaptive_respects_max_blocks():
    """Не более max_blocks блоков в одном пакете."""
    blocks = [_make_block(f"b{i}", 50) for i in range(20)]
    packed = _pack_blocks_adaptive(blocks, max_size_kb=50000, max_blocks=5)
    for group in packed:
        assert len(group) <= 5


def test_pack_adaptive_respects_max_size():
    """Не более max_size_kb в одном пакете."""
    blocks = [_make_block(f"b{i}", 1500) for i in range(10)]
    packed = _pack_blocks_adaptive(blocks, max_size_kb=5120, max_blocks=15)
    for group in packed:
        total_kb = sum(b.get("size_kb", 0) for b in group)
        assert total_kb <= 5120 + 1500  # допуск на последний блок


def test_pack_adaptive_no_lost_blocks():
    """Все блоки должны попасть в какой-то пакет."""
    blocks = [_make_block(f"b{i}", 100 + i * 50) for i in range(15)]
    packed = _pack_blocks_adaptive(blocks, max_size_kb=5120, max_blocks=15)
    all_ids = [b["block_id"] for group in packed for b in group]
    assert sorted(all_ids) == sorted([f"b{i}" for i in range(15)])


def test_pack_adaptive_empty():
    packed = _pack_blocks_adaptive([], max_size_kb=5120, max_blocks=15)
    assert packed == []


def test_pack_adaptive_mixed_sizes():
    """Микс маленьких и больших блоков."""
    blocks = [
        _make_block("small1", 50),
        _make_block("small2", 80),
        _make_block("big1", 4500),
        _make_block("small3", 60),
        _make_block("big2", 3500),
    ]
    packed = _pack_blocks_adaptive(blocks, max_size_kb=5120, max_blocks=15)
    assert len(packed) >= 2  # big blocks force separate batches
    all_ids = [b["block_id"] for group in packed for b in group]
    assert sorted(all_ids) == sorted(["small1", "small2", "big1", "small3", "big2"])
