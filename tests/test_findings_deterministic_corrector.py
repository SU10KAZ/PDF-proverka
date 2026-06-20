"""Тесты детерминированного corrector замечаний.

Главный инвариант: ни одно замечание не удаляется. phantom-блоки чистятся,
page/sheet чинятся, no_evidence/contradicts_text понижаются в
ПРОВЕРИТЬ_ПО_СМЕЖНЫМ. Также сквозной тест critic → corrector.
"""
import asyncio
import json

from backend.app.pipeline.stages.findings_review import deterministic_critic as dc
from backend.app.pipeline.stages.findings_review import deterministic_corrector as dcorr


def _blocks(*ids_pages):
    return {
        "block_analyses": [
            {"block_id": bid, "page": page, "sheet": sheet, "label": "схема",
             "findings": [{"finding": text}] if text else []}
            for (bid, page, sheet, text) in ids_pages
        ]
    }


def _graph(*pages):
    return {"pages": [
        {"page": p, "sheet_no_raw": sheet, "text_blocks": [{"text": txt}]}
        for (p, sheet, txt) in pages
    ]}


def _review(*items):
    return {"reviews": [
        {"finding_id": fid, "verdict": v, "reason": reason}
        for (fid, v, reason) in items
    ]}


def _run(coro):
    return asyncio.run(coro)


def test_no_finding_is_ever_deleted():
    findings = {"findings": [
        {"id": "F-001", "severity": "КРИТИЧЕСКОЕ", "page": 4},
        {"id": "F-002", "severity": "КРИТИЧЕСКОЕ", "page": 4,
         "evidence": [{"type": "image", "block_id": "GHOST-XXXX-YYY", "page": 4}]},
        {"id": "F-003", "severity": "ЭКОНОМИЧЕСКОЕ", "page": 99,
         "evidence": [{"type": "image", "block_id": "B1-AAAA-BBB", "page": 4}]},
    ]}
    review = _review(
        ("F-001", "no_evidence", "нет ссылок"),
        ("F-002", "phantom_block", "фантом"),
        ("F-003", "page_mismatch", "не та страница"),
    )
    blocks = _blocks(("B1-AAAA-BBB", 4, "Л4", "x"))
    data, result = dcorr.correct_findings(findings, review, blocks, {})
    fl = dc.iter_findings(data)
    assert len(fl) == 3  # ничего не удалено
    assert result.findings_total == 3


def test_no_evidence_critical_flagged_not_downgraded():
    # reserc.md #31: критичное замечание без доказательств НЕ понижаем молча —
    # помечаем на ручную проверку, severity сохраняем.
    findings = {"findings": [{"id": "F-001", "severity": "КРИТИЧЕСКОЕ", "page": 4}]}
    review = _review(("F-001", "no_evidence", "нет"))
    data, result = dcorr.correct_findings(findings, review, {}, {})
    f = dc.iter_findings(data)[0]
    assert f["severity"] == "КРИТИЧЕСКОЕ"          # severity сохранён
    assert f["requires_human_review"] is True       # помечено на ручную проверку
    assert result.flagged_human == 1
    assert result.downgraded == 0
    assert f["corrected_by"] == "deterministic"
    assert "no_evidence" in f["corrector_note"]


def test_no_evidence_economic_flagged_not_downgraded():
    findings = {"findings": [{"id": "F-001", "severity": "ЭКОНОМИЧЕСКОЕ", "page": 4}]}
    review = _review(("F-001", "no_evidence", "нет"))
    data, result = dcorr.correct_findings(findings, review, {}, {})
    f = dc.iter_findings(data)[0]
    assert f["severity"] == "ЭКОНОМИЧЕСКОЕ"
    assert f["requires_human_review"] is True
    assert result.flagged_human == 1


def test_no_evidence_noncritical_downgraded():
    # Непротектированные severity (эксплуатационное/рекомендательное) — понижаем
    # в ПРОВЕРИТЬ_ПО_СМЕЖНЫМ как раньше.
    findings = {"findings": [{"id": "F-001", "severity": "ЭКСПЛУАТАЦИОННОЕ", "page": 4}]}
    review = _review(("F-001", "no_evidence", "нет"))
    data, result = dcorr.correct_findings(findings, review, {}, {})
    f = dc.iter_findings(data)[0]
    assert f["severity"] == dcorr.CROSS_CHECK_SEVERITY
    assert result.downgraded == 1
    assert result.flagged_human == 0
    assert "no_evidence" in f["corrector_note"]


def test_phantom_block_cleaned():
    findings = {"findings": [{
        "id": "F-001", "severity": "КРИТИЧЕСКОЕ", "page": 4,
        "evidence": [
            {"type": "image", "block_id": "GHOST-XXXX-YYY", "page": 4},
            {"type": "image", "block_id": "REAL-AAAA-BBB", "page": 4},
        ],
        "related_block_ids": ["GHOST-XXXX-YYY", "REAL-AAAA-BBB"],
    }]}
    review = _review(("F-001", "phantom_block", "фантом"))
    blocks = _blocks(("REAL-AAAA-BBB", 4, None, "x"))
    data, result = dcorr.correct_findings(findings, review, blocks, {})
    f = dc.iter_findings(data)[0]
    ev_ids = [e["block_id"] for e in f["evidence"]]
    assert ev_ids == ["REAL-AAAA-BBB"]
    assert f["related_block_ids"] == ["REAL-AAAA-BBB"]
    assert result.phantom_cleaned == 1


def test_phantom_block_all_removed_critical_flagged():
    # Фантом-блоки убраны, evidence не осталось → no_evidence. Критичное —
    # помечаем на ручную проверку, не понижаем (#31).
    findings = {"findings": [{
        "id": "F-001", "severity": "КРИТИЧЕСКОЕ", "page": 4,
        "evidence": [{"type": "image", "block_id": "GHOST-XXXX-YYY", "page": 4}],
        "related_block_ids": ["GHOST-XXXX-YYY"],
    }]}
    review = _review(("F-001", "phantom_block", "фантом"))
    blocks = _blocks(("REAL-AAAA-BBB", 4, None, "x"))
    data, result = dcorr.correct_findings(findings, review, blocks, {})
    f = dc.iter_findings(data)[0]
    assert f["severity"] == "КРИТИЧЕСКОЕ"               # severity сохранён
    assert f["requires_human_review"] is True
    assert result.flagged_human == 1
    assert result.phantom_cleaned == 1


def test_phantom_block_all_removed_noncritical_downgrades():
    findings = {"findings": [{
        "id": "F-001", "severity": "РЕКОМЕНДАТЕЛЬНОЕ", "page": 4,
        "evidence": [{"type": "image", "block_id": "GHOST-XXXX-YYY", "page": 4}],
        "related_block_ids": ["GHOST-XXXX-YYY"],
    }]}
    review = _review(("F-001", "phantom_block", "фантом"))
    blocks = _blocks(("REAL-AAAA-BBB", 4, None, "x"))
    data, result = dcorr.correct_findings(findings, review, blocks, {})
    f = dc.iter_findings(data)[0]
    assert f["severity"] == dcorr.CROSS_CHECK_SEVERITY  # не осталось evidence → понижено
    assert result.downgraded == 1


def test_page_mismatch_fixed_from_evidence():
    findings = {"findings": [{
        "id": "F-001", "severity": "КРИТИЧЕСКОЕ", "page": 99,
        "evidence": [{"type": "image", "block_id": "B1-AAAA-BBB", "page": 4}],
    }]}
    review = _review(("F-001", "page_mismatch", "не та страница"))
    blocks = _blocks(("B1-AAAA-BBB", 4, "Л4", "x"))
    data, result = dcorr.correct_findings(findings, review, blocks, {})
    f = dc.iter_findings(data)[0]
    assert f["page"] == 4
    assert f["sheet"] == "Л4"
    assert result.page_fixed == 1


def test_pass_untouched():
    findings = {"findings": [{"id": "F-001", "severity": "КРИТИЧЕСКОЕ", "page": 4,
                              "related_block_ids": ["B1-AAAA-BBB"]}]}
    review = _review(("F-001", "pass", "ок"))
    data, result = dcorr.correct_findings(findings, review, _blocks(("B1-AAAA-BBB", 4, None, "x")), {})
    f = dc.iter_findings(data)[0]
    assert f["severity"] == "КРИТИЧЕСКОЕ"
    assert "corrector_note" not in f
    assert result.corrected == 0


def test_norm_quote_preserved():
    findings = {"findings": [{
        "id": "F-001", "severity": "КРИТИЧЕСКОЕ", "page": 4,
        "norm_quote": "СП 256... п.7.1", "norm": "СП 256",
    }]}
    review = _review(("F-001", "no_evidence", "нет"))
    data, _ = dcorr.correct_findings(findings, review, {}, {})
    f = dc.iter_findings(data)[0]
    assert f["norm_quote"] == "СП 256... п.7.1"  # сохранено


# ─── Сквозной critic → corrector ─────────────────────────────────────────────

def test_critic_then_corrector_roundtrip(tmp_path):
    findings = {"findings": [
        {"id": "F-001", "severity": "КРИТИЧЕСКОЕ", "page": 4, "related_block_ids": ["B1-AAAA-BBB"]},
        {"id": "F-002", "severity": "КРИТИЧЕСКОЕ", "page": 4},  # no_evidence
        {"id": "F-003", "severity": "ЭКОНОМИЧЕСКОЕ", "page": 4,
         "evidence": [{"type": "image", "block_id": "GHOST-ZZZZ-WWW", "page": 4}]},  # phantom
    ]}
    (tmp_path / "03_findings.json").write_text(json.dumps(findings), encoding="utf-8")
    (tmp_path / "02_blocks_analysis.json").write_text(
        json.dumps(_blocks(("B1-AAAA-BBB", 4, "Л4", "x"))), encoding="utf-8")
    (tmp_path / "document_graph.json").write_text(
        json.dumps(_graph((4, "Л4", "txt"))), encoding="utf-8")

    crit = _run(dc.run_deterministic_critic(tmp_path, llm_call=None, write=True))
    assert crit.findings_total == 3
    assert crit.deterministic_issues == 2  # F-002 no_evidence, F-003 phantom

    corr = _run(dcorr.run_deterministic_corrector(tmp_path, write=True))
    assert corr.findings_total == 3

    final = json.loads((tmp_path / "03_findings.json").read_text())
    by_id = {f["id"]: f for f in final["findings"]}
    assert by_id["F-001"]["severity"] == "КРИТИЧЕСКОЕ"           # pass — без изменений
    # #31: критичное no_evidence и экономический phantom-без-остатка —
    # помечены на ручную проверку, severity сохранён (не понижены молча).
    assert by_id["F-002"]["severity"] == "КРИТИЧЕСКОЕ"
    assert by_id["F-002"]["requires_human_review"] is True
    assert by_id["F-003"]["severity"] == "ЭКОНОМИЧЕСКОЕ"
    assert by_id["F-003"]["requires_human_review"] is True
    assert len(final["findings"]) == 3  # ничего не потеряно


def test_missing_review_returns_error(tmp_path):
    (tmp_path / "03_findings.json").write_text(json.dumps({"findings": []}), encoding="utf-8")
    res = _run(dcorr.run_deterministic_corrector(tmp_path, write=True))
    assert res.error is not None
