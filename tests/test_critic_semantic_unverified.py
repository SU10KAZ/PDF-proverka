"""reserc.md #91 — видимая деградация семантического критика.

При сбое/непарсе LLM-батча кандидаты по fail-soft остаются pass. Раньше это было
видно только как bool llm_failed. Теперь _run_semantic_pass возвращает число
непроверенных кандидатов (unverified) → попадает в meta.semantic_unverified.
"""
from __future__ import annotations

import asyncio

from backend.app.pipeline.stages.findings_review import deterministic_critic as dc


def _run(coro):
    return asyncio.run(coro)


def _candidates(n):
    return [{"id": f"F-{i:03d}", "finding": f"замечание {i}"} for i in range(1, n + 1)]


def test_unverified_counts_failed_batch():
    idx = dc.build_index({}, {})

    async def _boom(_prompt):
        raise RuntimeError("llm down")

    merged, failed, unverified = _run(
        dc._run_semantic_pass(_candidates(3), idx, _boom, 12, None)
    )
    assert failed is True
    assert unverified == 3          # все 3 кандидата остались без проверки
    assert merged == {}


def test_unverified_zero_on_success():
    idx = dc.build_index({}, {})

    async def _ok(_prompt):
        return (
            '{"verdicts":[{"finding_id":"F-001","verdict":"weak_evidence","reason":"r"},'
            '{"finding_id":"F-002","verdict":"pass","reason":"ok"}]}'
        )

    merged, failed, unverified = _run(
        dc._run_semantic_pass(_candidates(2), idx, _ok, 12, None)
    )
    assert failed is False
    assert unverified == 0
    assert "F-001" in merged


def test_unverified_counts_unparsable_batch():
    idx = dc.build_index({}, {})

    async def _garbage(_prompt):
        return "не json вообще"

    merged, failed, unverified = _run(
        dc._run_semantic_pass(_candidates(2), idx, _garbage, 12, None)
    )
    assert failed is True
    assert unverified == 2


def test_result_meta_exposes_semantic_unverified():
    r = dc.DeterministicCriticResult(findings_total=5, semantic_unverified=4)
    meta = r.to_review_dict("p")["meta"]
    assert meta["semantic_unverified"] == 4
