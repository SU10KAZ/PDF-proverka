"""Страж пересмотра оптимизаций: предложения не должны исчезать.

Агентный корректор оптимизаций уже ловили на тихой потере данных (замер 07-07:
ЭО1 — 14 предложений, отрецензировано 7, остальные молча удалены; всего 41
удаление). Пересмотр по нормам трогает тот же файл той же LLM, поэтому вход
защищён проверкой: каждый исходный id обязан остаться на месте.
"""
import json

import pytest

from backend.app.pipeline.stages.norms.runner import _optimization_intact
from backend.app.core.config import OPTIMIZATION_NORM_FIX_TASK_TEMPLATE


def _write(path, items):
    path.write_text(
        json.dumps({"meta": {"total_items": len(items)}, "items": items}, ensure_ascii=False),
        encoding="utf-8",
    )
    return path


def _items(*ids):
    return [{"id": i, "proposed": f"предложение {i}", "norm": "СП 30.13330.2020"} for i in ids]


def test_unchanged_file_is_intact(tmp_path):
    a = _write(tmp_path / "optimization.json", _items("OPT-001", "OPT-002"))
    b = _write(tmp_path / "backup.json", _items("OPT-001", "OPT-002"))
    assert _optimization_intact(a, b) is True


def test_revised_content_is_intact_while_ids_survive(tmp_path):
    """Пересмотр обязан менять формулировки — это не потеря данных."""
    revised = _items("OPT-001", "OPT-002")
    revised[0]["proposed"] = "переписано под актуальную норму"
    revised[0]["norm_outcome"] = "revised"
    a = _write(tmp_path / "optimization.json", revised)
    b = _write(tmp_path / "backup.json", _items("OPT-001", "OPT-002"))
    assert _optimization_intact(a, b) is True


def test_deleted_item_is_caught(tmp_path):
    a = _write(tmp_path / "optimization.json", _items("OPT-001"))
    b = _write(tmp_path / "backup.json", _items("OPT-001", "OPT-002"))
    assert _optimization_intact(a, b) is False


def test_all_items_wiped_is_caught(tmp_path):
    a = _write(tmp_path / "optimization.json", [])
    b = _write(tmp_path / "backup.json", _items("OPT-001", "OPT-002"))
    assert _optimization_intact(a, b) is False


def test_renamed_id_is_caught(tmp_path):
    """Смена id рвёт связь с решениями эксперта — это тоже потеря."""
    a = _write(tmp_path / "optimization.json", _items("OPT-001", "OPT-999"))
    b = _write(tmp_path / "backup.json", _items("OPT-001", "OPT-002"))
    assert _optimization_intact(a, b) is False


def test_added_item_is_allowed(tmp_path):
    """Добавление не теряет данных: инвариант — «не удалять», а не «не менять»."""
    a = _write(tmp_path / "optimization.json", _items("OPT-001", "OPT-002", "OPT-003"))
    b = _write(tmp_path / "backup.json", _items("OPT-001", "OPT-002"))
    assert _optimization_intact(a, b) is True


def test_corrupt_json_is_caught(tmp_path):
    a = tmp_path / "optimization.json"
    a.write_text("{битый", encoding="utf-8")
    b = _write(tmp_path / "backup.json", _items("OPT-001"))
    assert _optimization_intact(a, b) is False


def test_missing_file_is_caught(tmp_path):
    b = _write(tmp_path / "backup.json", _items("OPT-001"))
    assert _optimization_intact(tmp_path / "нет-такого.json", b) is False


def test_prompt_forbids_deletion_and_web(tmp_path):
    """Инвариант держится не только кодом-стражем, но и промптом."""
    text = OPTIMIZATION_NORM_FIX_TASK_TEMPLATE.read_text(encoding="utf-8")
    assert "УДАЛЯТЬ предложения" in text
    assert "WebSearch" in text and "Запрещено" in text
    # Три исхода пересмотра — иначе модель просто поправит ссылку и не переосмыслит.
    for outcome in ("still_valid", "revised", "obsolete"):
        assert outcome in text
    assert "{OPTIMIZATIONS_TO_FIX}" in text
