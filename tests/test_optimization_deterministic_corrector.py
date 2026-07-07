"""Тесты детерминированного корректора оптимизаций.

Ключевые инварианты (замер 07-07):
  · ничего не удаляется (len до == len после);
  · неотрецензированные item'ы сохраняются как pass (guard против потери данных);
  · conflicts_with_finding / unrealistic_savings правятся, а не удаляются.
"""
import asyncio
import json

import pytest

from backend.app.pipeline.stages.optimization.deterministic_corrector import (
    build_verdict_map,
    correct_items,
    iter_opt_items,
    run_deterministic_corrector,
)


def _items(n):
    return [
        {"id": f"OPT-{i:03d}", "proposed": f"предложение {i}", "savings_pct": 10}
        for i in range(1, n + 1)
    ]


def _review(pairs):
    """pairs: [(item_id, verdict, extra_dict)]"""
    reviews = []
    for iid, verdict, extra in pairs:
        r = {"item_id": iid, "verdict": verdict, "details": f"деталь {verdict}"}
        r.update(extra or {})
        reviews.append(r)
    return {"meta": {}, "reviews": reviews}


# ─── guard: неотрецензированные не теряются ──────────────────────────────────

def test_unreviewed_items_kept_as_pass():
    """Критик обрёк 3 из 5 — остальные 2 ДОЛЖНЫ сохраниться (не удаляться)."""
    items = _items(5)
    review = _review([
        ("OPT-001", "pass", {}),
        ("OPT-002", "pass", {}),
        ("OPT-003", "pass", {}),
    ])
    vmap = build_verdict_map(review)
    new_items, res = correct_items(items, vmap)

    assert len(new_items) == 5          # ничего не потеряно
    assert res.deleted == 0
    assert res.unreviewed_kept == 2     # OPT-004, OPT-005
    ids = {i["id"] for i in new_items}
    assert ids == {f"OPT-{i:03d}" for i in range(1, 6)}


def test_never_deletes_on_negative_verdicts():
    items = _items(4)
    review = _review([
        ("OPT-001", "conflicts_with_finding", {"conflicting_finding_id": "F-008"}),
        ("OPT-002", "too_vague", {}),
        ("OPT-003", "technical_issue", {}),
        ("OPT-004", "vendor_violation", {}),
    ])
    vmap = build_verdict_map(review)
    new_items, res = correct_items(items, vmap)

    assert len(new_items) == 4
    assert res.deleted == 0
    assert res.corrected == 4


# ─── конкретные правки ───────────────────────────────────────────────────────

def test_unrealistic_savings_capped_not_deleted():
    items = [{"id": "OPT-001", "savings_pct": 70, "savings_basis": "экспертная оценка"}]
    review = _review([("OPT-001", "unrealistic_savings", {})])
    new_items, res = correct_items(items, build_verdict_map(review), savings_cap=50)

    it = new_items[0]
    assert it["savings_pct"] == 50
    assert it["savings_pct_original"] == 70
    assert res.savings_capped == 1
    assert "corrector_note" in it


def test_conflict_blocks_savings_and_links_finding():
    items = [{"id": "OPT-001", "savings_pct": 15}]
    review = _review([("OPT-001", "conflicts_with_finding",
                       {"conflicting_finding_id": "F-005"})])
    new_items, res = correct_items(items, build_verdict_map(review))

    it = new_items[0]
    assert it["blocked_by_finding"] == "F-005"
    assert it["savings_pct"] == 0
    assert it["savings_pct_original"] == 15
    assert it["requires_review"] is True
    assert res.conflicts_blocked == 1


def test_vendor_violation_flags_review():
    items = [{"id": "OPT-001", "savings_pct": 10}]
    review = _review([("OPT-001", "vendor_violation", {})])
    new_items, res = correct_items(items, build_verdict_map(review))

    assert new_items[0]["requires_review"] is True
    assert new_items[0]["corrected_by"] == "deterministic"
    assert res.flagged_review == 1


def test_pass_item_unchanged():
    items = [{"id": "OPT-001", "savings_pct": 10, "proposed": "x"}]
    new_items, res = correct_items(items, build_verdict_map(_review([("OPT-001", "pass", {})])))
    assert new_items[0] == items[0]        # без правок
    assert "corrected_by" not in new_items[0]


def test_note_idempotent():
    """Повторный прогон не дублирует corrector_note."""
    items = [{"id": "OPT-001", "savings_pct": 70}]
    review = _review([("OPT-001", "unrealistic_savings", {})])
    once, _ = correct_items(items, build_verdict_map(review), savings_cap=50)
    twice, _ = correct_items(once, build_verdict_map(review), savings_cap=50)
    assert once[0]["corrector_note"] == twice[0]["corrector_note"]
    assert twice[0]["savings_pct"] == 50
    assert twice[0]["savings_pct_original"] == 70   # не перезатёрт срезанным


# ─── I/O ─────────────────────────────────────────────────────────────────────

def test_run_writes_and_backs_up(tmp_path):
    opt = {"meta": {"total_items": 3}, "items": _items(3)}
    (tmp_path / "optimization.json").write_text(json.dumps(opt), encoding="utf-8")
    review = _review([
        ("OPT-001", "pass", {}),
        ("OPT-002", "unrealistic_savings", {}),
    ])
    (tmp_path / "optimization_review.json").write_text(json.dumps(review), encoding="utf-8")

    res = asyncio.run(run_deterministic_corrector(tmp_path, savings_cap=50))

    assert res.error is None
    assert res.items_total == 3
    assert res.unreviewed_kept == 1     # OPT-003 без вердикта
    # бэкап создан
    assert (tmp_path / "optimization_pre_review.json").exists()
    # итог: 3 item'а на месте
    out = json.loads((tmp_path / "optimization.json").read_text(encoding="utf-8"))
    assert len(iter_opt_items(out)) == 3
    assert out["meta"]["corrector"]["deleted"] == 0


def test_missing_files_failsoft(tmp_path):
    res = asyncio.run(run_deterministic_corrector(tmp_path))
    assert res.error is not None
