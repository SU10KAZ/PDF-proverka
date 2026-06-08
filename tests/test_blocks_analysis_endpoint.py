"""
Тесты эндпоинта GET /api/tiles/{project_id}/blocks/analysis
(backend.app.api.routers.blocks.get_blocks_analysis).

Регрессия, которую закрывают эти тесты:
текущий production-режим Stage 02 (`findings_only_gemma_pair` / single_block)
пишет единый merged `02_blocks_analysis.json` и НЕ пишет legacy
`block_batch_*.json`. Старый эндпоинт читал только legacy-источники, поэтому
`blocks_map` оказывался пустым и ВСЕ блоки из `blocks_gemma_100/index.json`
классифицировались как `skipped` («Без значимого содержимого»), хотя аудит
завершён и у большинства блоков есть findings.

Проверяем:
* `02_blocks_analysis.json` — основной источник (single-block формат);
* legacy fallback `block_batch_*.json` (старые batched-проекты без merged-файла);
* v4 fallback `typed_facts_batch_*.json`;
* приоритет: при наличии `02_blocks_analysis.json` legacy не используется;
* отсутствие любых источников анализа не ломает эндпоинт (всё → skipped).

Запуск:
    python -m pytest tests/test_blocks_analysis_endpoint.py -v
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from backend.app.api.routers import blocks
from backend.app.main import app

client = TestClient(app)

PROJECT_ID = "AR/133-23-ГК-АР1"
URL = f"/api/tiles/{PROJECT_ID}/blocks/analysis"


def _write(path: Path, payload) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def _setup_project(
    tmp_path: Path,
    *,
    index_blocks: list[dict],
    blocks_analysis=None,
    block_batch=None,
    typed_facts=None,
    findings=None,
) -> Path:
    """Раскладывает _output так, как ждёт эндпоинт, и возвращает output_dir.

    gemma_blocks_index_path(output_dir.parent) → <project>/_output/blocks_gemma_100/index.json,
    поэтому output_dir обязан называться `_output`.
    """
    output_dir = tmp_path / "project" / "_output"
    output_dir.mkdir(parents=True, exist_ok=True)

    if blocks_analysis is not None:
        _write(output_dir / "02_blocks_analysis.json", blocks_analysis)
    if block_batch is not None:
        _write(output_dir / "block_batch_001.json", block_batch)
    if typed_facts is not None:
        _write(output_dir / "typed_facts_batch_001.json", typed_facts)
    if findings is not None:
        _write(output_dir / "03_findings.json", findings)

    _write(output_dir / "blocks_gemma_100" / "index.json", {"blocks": index_blocks})
    return output_dir


def _patch_output(monkeypatch, output_dir: Path) -> None:
    monkeypatch.setattr(blocks, "_version_output", lambda pid, vid: output_dir)


def test_single_block_02_blocks_analysis_is_primary(tmp_path, monkeypatch):
    """Новый формат: 02_blocks_analysis.json → блоки классифицируются, не skipped."""
    output_dir = _setup_project(
        tmp_path,
        blocks_analysis={
            "stage02_mode": "findings_only_gemma_pair",
            "block_analyses": [
                {"block_id": "A", "page": 5, "findings": [{"id": "f1"}]},  # inline findings
                {"block_id": "B", "page": 6, "findings": []},              # referenced in 03_findings
                {"block_id": "C", "page": 7, "findings": []},              # truly no findings
            ],
        },
        findings={"findings": [{"source_block_ids": ["B"]}]},
        # D есть только в index.json — реально не анализировался → skipped
        index_blocks=[
            {"block_id": "A"},
            {"block_id": "B"},
            {"block_id": "C"},
            {"block_id": "D"},
        ],
    )
    _patch_output(monkeypatch, output_dir)

    resp = client.get(URL)
    assert resp.status_code == 200
    data = resp.json()
    counts = data["counts"]

    assert counts["has_findings"] == 2, counts   # A (inline) + B (через 03_findings)
    assert counts["no_findings"] == 1, counts    # C
    assert counts["skipped"] == 1, counts        # D
    assert data["blocks"]["A"]["status"] == "has_findings"
    assert data["blocks"]["B"]["status"] == "has_findings"
    assert data["blocks"]["C"]["status"] == "no_findings"
    assert data["blocks"]["D"]["status"] == "skipped"


def test_02_blocks_analysis_takes_precedence_over_legacy(tmp_path, monkeypatch):
    """При наличии 02_blocks_analysis.json legacy block_batch_*.json игнорируется."""
    output_dir = _setup_project(
        tmp_path,
        blocks_analysis={"block_analyses": [{"block_id": "A", "findings": []}]},
        block_batch={"block_analyses": [{"block_id": "B", "findings": [{"id": "x"}]}]},
        index_blocks=[{"block_id": "A"}, {"block_id": "B"}],
    )
    _patch_output(monkeypatch, output_dir)

    data = client.get(URL).json()
    # A пришёл из 02 → no_findings; B был только в legacy-batch → не учитывается → skipped
    assert data["blocks"]["A"]["status"] == "no_findings"
    assert data["blocks"]["B"]["status"] == "skipped"
    assert data["counts"]["no_findings"] == 1
    assert data["counts"]["skipped"] == 1


def test_legacy_block_batch_fallback(tmp_path, monkeypatch):
    """Старые batched-проекты без 02_blocks_analysis.json: читаем block_batch_*.json."""
    output_dir = _setup_project(
        tmp_path,
        block_batch={
            "block_analyses": [
                {"block_id": "A", "findings": [{"id": "f"}]},
                {"block_id": "B", "findings": []},
            ]
        },
        index_blocks=[{"block_id": "A"}, {"block_id": "B"}],
    )
    _patch_output(monkeypatch, output_dir)

    data = client.get(URL).json()
    assert data["counts"]["has_findings"] == 1
    assert data["counts"]["no_findings"] == 1
    assert data["counts"]["skipped"] == 0


def test_v4_typed_facts_fallback(tmp_path, monkeypatch):
    """v4 fallback: typed_facts_batch_*.json, когда нет ни 02, ни block_batch."""
    output_dir = _setup_project(
        tmp_path,
        typed_facts={
            "entity_mentions": [
                {
                    "source_context": {"block_id": "A", "page": 5},
                    "entity_type": "breaker",
                    "normalized_label": "QF1",
                    "attributes": [],
                }
            ]
        },
        index_blocks=[{"block_id": "A"}, {"block_id": "B"}],
    )
    _patch_output(monkeypatch, output_dir)

    data = client.get(URL).json()
    # A проанализирован (из typed_facts), без 03_findings → no_findings; B → skipped
    assert data["blocks"]["A"]["status"] == "no_findings"
    assert data["blocks"]["B"]["status"] == "skipped"
    assert data["counts"]["skipped"] == 1


def test_no_analysis_sources_does_not_crash(tmp_path, monkeypatch):
    """Нет ни одного источника анализа → всё skipped, но 200 и без падения."""
    output_dir = _setup_project(
        tmp_path,
        index_blocks=[{"block_id": "A"}, {"block_id": "B"}],
    )
    _patch_output(monkeypatch, output_dir)

    resp = client.get(URL)
    assert resp.status_code == 200
    data = resp.json()
    assert data["counts"]["skipped"] == 2
    assert data["counts"]["has_findings"] == 0
    assert data["counts"]["no_findings"] == 0


def test_real_project_v2_counts(monkeypatch):
    """Acceptance на реальных данных: проект 133-23-ГК-АР1 V2 → 45 / 6 / 0.

    Пропускается, если данные проекта отсутствуют в этом окружении.
    """
    root = Path(__file__).resolve().parents[1]
    output_dir = (
        root
        / "projects"
        / '213. Мосфильмовская 31А "King&Sons"'
        / "AR"
        / "133-23-ГК-АР1(main)"
        / "133-23-ГК-АР1.pdf"
        / "_output"
    )
    if not (output_dir / "02_blocks_analysis.json").exists():
        pytest.skip("реальные данные проекта недоступны в этом окружении")

    _patch_output(monkeypatch, output_dir)
    data = client.get(URL).json()
    counts = data["counts"]
    assert counts["has_findings"] == 45, counts
    assert counts["no_findings"] == 6, counts
    assert counts["skipped"] == 0, counts
