"""Статус нормы проставляется предложениям детерминированно.

Без этого шага «✓ норма проверена» не появлялась бы никогда: поля писал только
этап пересмотра (3c), а он запускается лишь при плохой норме. Живой прогон
13АВ-РД-ВК1-К1 V1 (16.07.2026): все 18 норм действуют → пересмотр пропущен →
24 предложения остались без признака, хотя их нормы проверены.
"""
import json

import pytest

from backend.app.pipeline.stages.norms.runner import enrich_optimization_norm_status


def _setup(tmp_path, items, checks):
    (tmp_path / "optimization.json").write_text(
        json.dumps({"items": items}, ensure_ascii=False), encoding="utf-8")
    (tmp_path / "norm_checks.json").write_text(
        json.dumps({"checks": checks}, ensure_ascii=False), encoding="utf-8")
    return tmp_path


def _read(tmp_path):
    return json.loads((tmp_path / "optimization.json").read_text(encoding="utf-8"))["items"]


def test_active_norm_marks_item_ok(tmp_path):
    _setup(tmp_path,
           [{"id": "OPT-001", "norm": "СП 30.13330.2020"}],
           [{"norm_as_cited": "СП 30.13330.2020", "status": "active",
             "affected_optimizations": ["OPT-001"]}])

    assert enrich_optimization_norm_status(tmp_path) == 1
    it = _read(tmp_path)[0]
    assert it["norm_verified"] is True
    assert it["norm_status"] == "ok"


def test_unconfirmed_norm_marks_warning_with_reason(tmp_path):
    _setup(tmp_path,
           [{"id": "OPT-001", "norm": "ФЗ №123"}],
           [{"norm_as_cited": "ФЗ №123", "status": "not_found",
             "affected_optimizations": ["OPT-001"]}])

    assert enrich_optimization_norm_status(tmp_path) == 1
    it = _read(tmp_path)[0]
    assert it["norm_status"] == "warning"
    assert "not_found" in it["norm_revision"]["revision_reason"]


def test_item_without_recognised_norms_stays_silent(tmp_path):
    """«Не проверено» и «проверено, всё хорошо» — разные вещи."""
    _setup(tmp_path,
           [{"id": "OPT-001", "norm": ""}, {"id": "OPT-002", "norm": "СП 30.13330.2020"}],
           [{"norm_as_cited": "СП 30.13330.2020", "status": "active",
             "affected_optimizations": ["OPT-002"]}])

    assert enrich_optimization_norm_status(tmp_path) == 1
    items = {i["id"]: i for i in _read(tmp_path)}
    assert "norm_verified" not in items["OPT-001"]
    assert items["OPT-002"]["norm_status"] == "ok"


def test_revision_verdict_is_not_overwritten(tmp_path):
    """Вердикт пересмотра сильнее: он видел текст нормы через MCP."""
    _setup(tmp_path,
           [{"id": "OPT-001", "norm": "СП 30.13330.2020", "norm_verified": True,
             "norm_status": "revised", "norm_outcome": "obsolete"}],
           [{"norm_as_cited": "СП 30.13330.2020", "status": "active",
             "affected_optimizations": ["OPT-001"]}])

    assert enrich_optimization_norm_status(tmp_path) == 0
    it = _read(tmp_path)[0]
    assert it["norm_status"] == "revised"
    assert it["norm_outcome"] == "obsolete"


def test_one_bad_norm_among_good_wins(tmp_path):
    """Предложение цитирует несколько норм — плохая перевешивает."""
    _setup(tmp_path,
           [{"id": "OPT-001", "norm": "СП 30.13330.2020; ГОСТ 8944-75"}],
           [{"norm_as_cited": "СП 30.13330.2020", "status": "active",
             "affected_optimizations": ["OPT-001"]},
            {"norm_as_cited": "ГОСТ 8944-75", "status": "not_found",
             "affected_optimizations": ["OPT-001"]}])

    assert enrich_optimization_norm_status(tmp_path) == 1
    assert _read(tmp_path)[0]["norm_status"] == "warning"


def test_missing_files_are_noop(tmp_path):
    assert enrich_optimization_norm_status(tmp_path) == 0


def test_corrupt_json_is_noop_not_crash(tmp_path):
    (tmp_path / "optimization.json").write_text("{битый", encoding="utf-8")
    (tmp_path / "norm_checks.json").write_text('{"checks": []}', encoding="utf-8")
    assert enrich_optimization_norm_status(tmp_path) == 0


def test_nothing_written_when_no_matches(tmp_path):
    """Файл не переписывается впустую."""
    p = _setup(tmp_path, [{"id": "OPT-001"}], [])
    before = (p / "optimization.json").read_text(encoding="utf-8")
    assert enrich_optimization_norm_status(p) == 0
    assert (p / "optimization.json").read_text(encoding="utf-8") == before
