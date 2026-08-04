"""Тесты выдачи полного профильного Markdown в /blocks/llm-text.

Shadow-пакет ar_ceiling_lighting пишется backfill-скриптом в
block_vector_graphs/<block_id>.ar_ceiling_lighting.json; endpoint отдаёт
его поле markdown как profiled_graph_markdown_full. Stage 01/02 этот
файл видеть не должны (load_prepared_package ищет строго <block_id>.json).
"""
from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pytest

from backend.app.api.routers import blocks as blocks_router
from backend.app.services.common import version_service
from backend.app.pipeline.stages.block_grounding.block_profile_registry import (
    load_prepared_package)

FULL_MD = "\n".join(
    ["# План потолков и освещения — поквартирное описание", ""]
    + [f"## Квартира {n}\n\nтекст квартиры {n}" for n in range(700, 710)]
    + ["### Помещение 6.709.1 — Жилая комната",
       "Вывод под люстру, группы 7.",
       "`одноклавишный выключатель группы 7 → группа 7 → вывод под люстру (6.709.1)`"]
)


def _shadow_package(block_id: str, *, status: str = "complete", warnings=None) -> dict:
    return {
        "schema_version": 6,
        "block_id": block_id,
        "page": 104,
        "source_kind": "structured_architecture",
        "profile_id": "ar_ceiling_lighting",
        "profile_version": "test",
        "status": status,
        "markdown": FULL_MD,
        "user_text": None,
        "graph": None,
        "warnings": list(warnings or []),
        "conflicts": [{"type": "GEOMETRY_CONFLICT", "what": "тест"}],
        "validation": {"apartments_total": 10},
        "source_pdf": "document.pdf",
        "source_sha256": "0" * 64,
        "provenance": {"llm": False, "ocr": False},
    }


@pytest.fixture
def env(tmp_path, monkeypatch):
    version_dir = tmp_path / "v001"
    output_dir = version_dir / "03_analysis" / "latest"
    (output_dir / "block_vector_graphs").mkdir(parents=True)
    (version_dir / "02_work").mkdir(parents=True)

    def fake_resolve(project_id, version_id=None):
        return {"output_dir": output_dir, "version_dir": version_dir}

    monkeypatch.setattr(version_service, "resolve_project_version_context", fake_resolve)
    return output_dir


def _call(block_id: str, page: int | None = 104) -> dict:
    return asyncio.run(blocks_router.get_block_llm_text(
        "P-TEST", block_id, request=None, version_id="v001", page=page))


def _write_shadow(output_dir: Path, block_id: str, package: dict) -> Path:
    path = output_dir / "block_vector_graphs" / f"{block_id}.ar_ceiling_lighting.json"
    path.write_text(json.dumps(package, ensure_ascii=False), encoding="utf-8")
    return path


def test_endpoint_returns_full_markdown(env):
    _write_shadow(env, "B-1", _shadow_package("B-1"))
    payload = _call("B-1")
    assert payload["profiled_graph_markdown_full"] == FULL_MD
    assert payload["profile_shadow"]["profile_id"] == "ar_ceiling_lighting"
    assert payload["profile_shadow"]["status"] == "complete"
    assert payload["profile_shadow"]["conflict_count"] == 1


def test_markdown_not_truncated_first_and_last_apartment(env):
    _write_shadow(env, "B-1", _shadow_package("B-1"))
    md = _call("B-1")["profiled_graph_markdown_full"]
    assert len(md) == len(FULL_MD)
    assert "## Квартира 700" in md
    assert "## Квартира 709" in md
    assert "### Помещение 6.709.1" in md


def test_endpoint_does_not_leak_local_path(env):
    path = _write_shadow(env, "B-1", _shadow_package("B-1"))
    payload = _call("B-1")
    dumped = json.dumps(payload, ensure_ascii=False, default=str)
    assert str(path) not in dumped
    assert str(env) not in dumped


def test_missing_package_returns_null_not_500(env):
    payload = _call("B-NO-SHADOW")
    assert payload["profiled_graph_markdown_full"] is None
    assert payload["profile_shadow"] is None


def test_partial_status_returns_warnings(env):
    _write_shadow(env, "B-1", _shadow_package(
        "B-1", status="partial",
        warnings=["LIGHTING_LEGEND_NOT_PARSED: легенда не разобрана"]))
    payload = _call("B-1")
    assert payload["profile_shadow"]["status"] == "partial"
    assert any("LIGHTING_LEGEND_NOT_PARSED" in w
               for w in payload["profile_shadow"]["warnings"])


def test_alien_block_id_does_not_get_neighbors_markdown(env):
    _write_shadow(env, "B-1", _shadow_package("B-1"))
    payload = _call("B-2")
    assert payload["profiled_graph_markdown_full"] is None


def test_block_id_mismatch_inside_package_rejected(env):
    # файл назван как B-1, но block_id внутри чужой → не отдаём
    _write_shadow(env, "B-1", _shadow_package("B-OTHER"))
    payload = _call("B-1")
    assert payload["profiled_graph_markdown_full"] is None


def test_path_traversal_block_id_is_sanitized(env, tmp_path):
    secret = tmp_path / "secret.ar_ceiling_lighting.json"
    secret.write_text(json.dumps(_shadow_package("../secret")), encoding="utf-8")
    payload = _call("../secret")
    assert payload["profiled_graph_markdown_full"] is None


def test_stage01_prepared_package_does_not_see_shadow(env):
    """Production-читатель Stage 01/02 ищет строго <block_id>.json —
    shadow-артефакт не подхватывается, аудит не меняется."""
    _write_shadow(env, "B-1", _shadow_package("B-1"))
    assert load_prepared_package(env, "B-1") is None


def test_existing_contract_fields_preserved(env):
    _write_shadow(env, "B-1", _shadow_package("B-1"))
    payload = _call("B-1")
    for key in ("system_prompt", "user_text", "singleline_graph_markdown",
                "block_graph_package", "profiled_graph", "profiled_graph_display",
                "vector_text", "text_groups"):
        assert key in payload, f"поле {key} исчезло из контракта"
