"""reserc.md #33 — детерминированный corrector не чанкуется.

Чанкование детерминированного (Python-only, single-pass) corrector давало N
лишних полных перезаписей findings и усекало 03_findings_review.json до
последнего чанка. Теперь чанкуем только агентный путь.
"""
from __future__ import annotations

from backend.app.pipeline.stages.findings_review.runner import _should_chunk_corrector


def test_deterministic_never_chunks_even_when_large():
    # Детерминированный путь: один проход независимо от числа замечаний.
    assert _should_chunk_corrector(total_issues=999, deterministic=True, chunk_size=12) is False
    assert _should_chunk_corrector(total_issues=1, deterministic=True, chunk_size=12) is False


def test_agentic_chunks_above_threshold():
    # Агентный (legacy) путь: чанкуем при превышении порога.
    assert _should_chunk_corrector(total_issues=13, deterministic=False, chunk_size=12) is True
    assert _should_chunk_corrector(total_issues=12, deterministic=False, chunk_size=12) is False
    assert _should_chunk_corrector(total_issues=5, deterministic=False, chunk_size=12) is False
