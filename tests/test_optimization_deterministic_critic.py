"""Тесты детерминированной структурной аугментации критика оптимизаций.

Инварианты:
  · вердикт у КАЖДОГО item (покрытие 100%, в т.ч. неотрецензированных);
  · семантические не-pass агентного критика НЕ перебиваются структуркой;
  · unrealistic_savings — basis-aware (расчёт не флагуется).
"""
import asyncio
import json

from backend.app.pipeline.stages.optimization.deterministic_critic import (
    augment_reviews,
    run_deterministic_critic_augment,
    structural_verdict,
)


def _item(iid, **kw):
    base = {"id": iid, "spec_items": ["Поз. 1 — X"], "page": 5,
            "savings_pct": 10, "savings_basis": "расчёт"}
    base.update(kw)
    return base


# ─── структурные правила ─────────────────────────────────────────────────────

def test_no_traceability_when_spec_empty():
    v = structural_verdict(_item("OPT-001", spec_items=[]))
    assert v and v[0] == "no_traceability"


def test_no_traceability_when_page_missing():
    v = structural_verdict(_item("OPT-001", page=None))
    assert v and v[0] == "no_traceability"


def test_unrealistic_savings_expert_basis():
    v = structural_verdict(_item("OPT-001", savings_pct=70, savings_basis="экспертная оценка"))
    assert v and v[0] == "unrealistic_savings"


def test_savings_not_flagged_when_calculated():
    """basis-aware: 70% с основанием «расчёт» НЕ флагуется."""
    v = structural_verdict(_item("OPT-001", savings_pct=70, savings_basis="расчёт"))
    assert v is None


def test_clean_item_none():
    assert structural_verdict(_item("OPT-001")) is None


# ─── слияние ─────────────────────────────────────────────────────────────────

def test_coverage_every_item_gets_verdict():
    items = [_item(f"OPT-{i:03d}") for i in range(1, 6)]
    existing = {"OPT-001": {"item_id": "OPT-001", "verdict": "pass"}}  # только 1 из 5
    reviews, res = augment_reviews(items, existing)
    assert len(reviews) == 5
    assert res.coverage_added == 4          # 4 неотрецензированных закрыты
    assert all(r.get("verdict") for r in reviews)


def test_semantic_negative_not_overridden():
    """Агентный technical_issue на item с плохой traceability остаётся technical_issue."""
    items = [_item("OPT-001", spec_items=[], page=None)]  # структурно = no_traceability
    existing = {"OPT-001": {"item_id": "OPT-001", "verdict": "technical_issue",
                            "details": "реальная тех-проблема"}}
    reviews, res = augment_reviews(items, existing)
    assert reviews[0]["verdict"] == "technical_issue"
    assert reviews[0]["source"] == "agentic"


def test_agentic_pass_flips_to_structural():
    items = [_item("OPT-001", savings_pct=80, savings_basis="экспертная оценка")]
    existing = {"OPT-001": {"item_id": "OPT-001", "verdict": "pass"}}
    reviews, res = augment_reviews(items, existing)
    assert reviews[0]["verdict"] == "unrealistic_savings"
    assert reviews[0]["source"] == "deterministic"
    assert res.structural_added == 1


def test_conflicting_finding_id_preserved():
    items = [_item("OPT-001")]
    existing = {"OPT-001": {"item_id": "OPT-001", "verdict": "conflicts_with_finding",
                            "conflicting_finding_id": "F-008"}}
    reviews, _ = augment_reviews(items, existing)
    assert reviews[0]["conflicting_finding_id"] == "F-008"


# ─── I/O ─────────────────────────────────────────────────────────────────────

def test_run_writes_full_coverage(tmp_path):
    opt = {"meta": {}, "items": [_item(f"OPT-{i:03d}") for i in range(1, 4)]}
    (tmp_path / "optimization.json").write_text(json.dumps(opt), encoding="utf-8")
    review = {"meta": {}, "reviews": [{"item_id": "OPT-001", "verdict": "pass"}]}
    (tmp_path / "optimization_review.json").write_text(json.dumps(review), encoding="utf-8")

    res = asyncio.run(run_deterministic_critic_augment(tmp_path))
    assert res.error is None
    out = json.loads((tmp_path / "optimization_review.json").read_text(encoding="utf-8"))
    assert len(out["reviews"]) == 3
    assert out["meta"]["total_reviewed"] == 3


def test_run_without_agentic_review(tmp_path):
    """Нет optimization_review.json вообще → структурка даёт полное покрытие."""
    opt = {"meta": {}, "items": [_item("OPT-001", spec_items=[])]}
    (tmp_path / "optimization.json").write_text(json.dumps(opt), encoding="utf-8")
    res = asyncio.run(run_deterministic_critic_augment(tmp_path))
    assert res.items_total == 1
    out = json.loads((tmp_path / "optimization_review.json").read_text(encoding="utf-8"))
    assert out["reviews"][0]["verdict"] == "no_traceability"
