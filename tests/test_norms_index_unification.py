"""reserc.md #34 — единый источник индекса норм.

Было: external_provider (статусы) дефолтил на НЕсуществующий backend/.../tools/
status_index.json → пустой индекс; _native_verify (цитаты пунктов) хардкодил
внешний /home/coder/projects/Norms/tools. Стало: оба берут in-repo norms/tools
(authoritative 565), env-override сохранён, при расхождении путей — warning.
"""
from __future__ import annotations

import logging
from pathlib import Path

from backend.app.pipeline.stages.norms import _native_verify as nv
from backend.app.pipeline.stages.norms import external_provider as ep


def test_status_and_paragraph_indexes_share_norms_tools_root():
    tools = nv._default_norms_tools_path()
    assert tools.name == "tools" and tools.parent.name == "norms"
    # дефолтный status_index лежит в том же norms/tools
    assert ep._DEFAULT_STATUS_INDEX == tools / "status_index.json"


def test_inrepo_index_exists_and_is_authoritative_565():
    # in-repo индекс реально существует и это authoritative 565 (а не битый путь).
    assert ep._DEFAULT_STATUS_INDEX.exists(), "in-repo status_index.json отсутствует"
    import json
    meta = json.loads(ep._DEFAULT_STATUS_INDEX.read_text(encoding="utf-8")).get("meta", {})
    assert meta.get("total") == 565


def test_native_default_no_longer_hardcodes_external():
    # Прежний хардкод /home/coder/projects/Norms/tools больше не дефолт.
    assert nv._default_norms_tools_path() != Path("/home/coder/projects/Norms/tools")


def test_divergence_warning_fires(monkeypatch, caplog):
    monkeypatch.setattr(nv, "NORMS_TOOLS_PATH", Path("/some/other/place/tools"))
    with caplog.at_level(logging.WARNING):
        nv._warn_if_index_paths_diverge()
    assert any("#34" in r.getMessage() for r in caplog.records)


def test_no_divergence_no_warning(monkeypatch, caplog):
    # Когда оба пути совпадают — предупреждения нет.
    same = Path(ep.NORMS_STATUS_INDEX_PATH).resolve().parent
    monkeypatch.setattr(nv, "NORMS_TOOLS_PATH", same)
    with caplog.at_level(logging.WARNING):
        nv._warn_if_index_paths_diverge()
    assert not any("#34" in r.getMessage() for r in caplog.records)
