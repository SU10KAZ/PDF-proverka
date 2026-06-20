"""reserc.md #96 — eval-харнесс качества аудита против экспертного эталона.

Чистые метрики precision/acceptance по decisions_log (accepted/rejected вердикты
эксперта). Recall честно не вычисляется (нет ground-truth пропусков) — проверяем,
что харнесс это явно декларирует, а не выдаёт фейковое число.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts import eval_harness as eh

ROOT = Path(__file__).resolve().parents[1]


def _e(decision, **kw):
    d = {"expert_decision": decision, "item_type": "finding", "section": "AR",
         "severity": "КРИТИЧЕСКОЕ", "category": "x", "object_id": "o1",
         "source_project": "P1", "customer_confirmed": False}
    d.update(kw)
    return d


def test_precision_basic():
    entries = [_e("accepted"), _e("accepted"), _e("rejected"), _e("rejected"),
               _e("rejected")]
    m = eh.compute_eval_metrics(entries)
    assert m["total"] == 5
    assert m["accepted"] == 2 and m["rejected"] == 3
    assert m["reviewed"] == 5
    assert m["precision"] == round(2 / 5, 4)


def test_other_no_verdict_counted():
    entries = [_e("accepted"), _e(""), _e(None), _e("pending")]
    m = eh.compute_eval_metrics(entries)
    assert m["accepted"] == 1 and m["rejected"] == 0
    assert m["reviewed"] == 1
    assert m["other_no_verdict"] == 3
    assert m["precision"] == 1.0


def test_empty_dataset_no_crash():
    m = eh.compute_eval_metrics([])
    assert m["total"] == 0
    assert m["precision"] is None
    assert m["customer_confirm_rate"] is None


def test_breakdown_by_section_and_type():
    entries = [
        _e("accepted", section="AR"), _e("rejected", section="AR"),
        _e("accepted", section="EOM", item_type="optimization"),
    ]
    m = eh.compute_eval_metrics(entries)
    assert m["by_section"]["AR"]["total"] == 2
    assert m["by_section"]["AR"]["precision"] == 0.5
    assert m["by_section"]["EOM"]["precision"] == 1.0
    assert m["by_item_type"]["optimization"]["total"] == 1


def test_customer_confirm_rate_and_responses():
    entries = [_e("accepted", customer_confirmed=True, customer_response="Внесено"),
               _e("rejected", customer_response="Отклонено"),
               _e("rejected", customer_response="")]
    m = eh.compute_eval_metrics(entries)
    assert m["customer_confirmed"] == 1
    assert m["customer_confirm_rate"] == round(1 / 3, 4)
    assert m["customer_response_distribution"]["Внесено"] == 1
    assert m["customer_response_distribution"]["—"] == 1  # пустой → «—»


def test_top_bottom_projects_respect_min_sample():
    # P_big: 10 замечаний (выборка достаточна), P_small: 1 (отсеивается)
    entries = ([_e("accepted", source_project="P_big") for _ in range(6)]
               + [_e("rejected", source_project="P_big") for _ in range(4)]
               + [_e("accepted", source_project="P_small")])
    m = eh.compute_eval_metrics(entries, min_sample=5)
    names = {p["source_project"] for p in m["top_projects"]}
    assert "P_big" in names
    assert "P_small" not in names  # выборка < min_sample


def test_recall_note_is_explicit():
    m = eh.compute_eval_metrics([_e("accepted")])
    assert "recall" in m["recall_note"].lower()
    assert "не вычисляется" in m["recall_note"].lower()


def test_load_entries_handles_shapes(tmp_path):
    p = tmp_path / "d.json"
    p.write_text(json.dumps({"entries": [_e("accepted")]}), encoding="utf-8")
    assert len(eh.load_entries(p)) == 1
    p.write_text(json.dumps([_e("rejected")]), encoding="utf-8")
    assert len(eh.load_entries(p)) == 1


def test_format_report_runs():
    m = eh.compute_eval_metrics([_e("accepted"), _e("rejected")])
    txt = eh.format_report(m)
    assert "EVAL-ХАРНЕСС" in txt
    assert "precision" in txt.lower()


@pytest.mark.skipif(not (ROOT / "knowledge_base" / "decisions_log.json").exists(),
                    reason="live decisions_log отсутствует")
def test_smoke_on_live_decisions_log():
    """Дымовой прогон на реальном эталоне: метрики вменяемы."""
    entries = eh.load_entries(ROOT / "knowledge_base" / "decisions_log.json")
    assert len(entries) > 0
    m = eh.compute_eval_metrics(entries)
    assert m["total"] == len(entries)
    assert m["reviewed"] == m["accepted"] + m["rejected"]
    assert 0.0 <= (m["precision"] or 0.0) <= 1.0
    assert m["accepted"] + m["rejected"] + m["other_no_verdict"] == m["total"]
