"""reserc.md #15 / #3 / #12 — удаление мёртвого legacy block-batch конвейера.

Production-режим block_batch ЗАЛОЧЕН на единственный выбор
`findings_only_gemma_pair` (single-block GPT-5.4 + gemma-enrichment), пишущий
01_blocks_analysis.json напрямую. Поэтому legacy-ветки batch-конвейера
(generate batches → run batches → merge → _retry_batch_split) были провабельно
недостижимы и удалены из manager.py.

Этот тест — guard: пока block_batch залочен на единственный режим, мёртвые
ветки не могут «ожить»; и удалённые символы не возвращаются незаметно.
"""
from __future__ import annotations

import inspect

from backend.app.core import config
from backend.app.pipeline import manager as mgr


def test_block_batch_mode_locked_to_findings_only():
    """Единственный допустимый режim — findings_only_gemma_pair.
    Если кто-то добавит сюда второй режим, ему придётся осознанно вернуть и
    legacy-обработку (этот тест упадёт и заставит подумать)."""
    assert config.STAGE_BATCH_MODE_CHOICES["block_batch"] == ["findings_only_gemma_pair"]
    assert config.get_stage_batch_mode("block_batch") == "findings_only_gemma_pair"


def test_legacy_batch_methods_removed():
    """Осиротевший _retry_batch_split удалён из менеджера."""
    assert not hasattr(mgr.PipelineManager, "_retry_batch_split")


def test_live_block_analysis_path_intact():
    """Живой путь (findings_only single-block) и block_retry на месте."""
    assert hasattr(mgr.PipelineManager, "_run_block_analysis_findings_only")
    # _run_block_retry остаётся: он вызывается безусловно в _run_ocr_pipeline
    assert hasattr(mgr.PipelineManager, "_run_block_retry")


def test_no_legacy_batch_loop_markers_in_source():
    """Legacy batch-loop маркеры (генерация/слияние пакетов через blocks.py)
    больше не присутствуют в manager.py."""
    src = inspect.getsource(mgr)
    assert "ЭТАП 4: Генерация пакетов блоков" not in src
    assert "block_batches.json не создан" not in src
    assert "Слияние block_batch_*.json" not in src
