"""Тесты детерминированного critic замечаний.

Покрывают структурные проверки 1/2/4, консервативность page_mismatch,
точечный LLM-проход (3/5) и его fail-soft, а также контракт review-файла.
"""
import asyncio
import json

from backend.app.pipeline.stages.findings_review import deterministic_critic as dc


def _blocks(*ids_pages):
    """(block_id, page, sheet, finding_text) -> 02_blocks_analysis.json-подобный dict."""
    return {
        "block_analyses": [
            {
                "block_id": bid,
                "page": page,
                "sheet": sheet,
                "label": "схема",
                "findings": [{"finding": text}] if text else [],
            }
            for (bid, page, sheet, text) in ids_pages
        ]
    }


def _graph(*pages):
    return {
        "pages": [
            {"page": p, "sheet_no_raw": sheet, "text_blocks": [{"text": txt}]}
            for (p, sheet, txt) in pages
        ]
    }


def _run(coro):
    return asyncio.run(coro)


# ─── Проверка 1: evidence_presence ───────────────────────────────────────────

def test_no_evidence():
    findings = {"findings": [{"id": "F-001", "page": 4, "description": "нет ссылок"}]}
    reviews, candidates, result, _ = dc.review_structural(findings, {}, {})
    assert result.findings_total == 1
    assert reviews[0]["verdict"] == "no_evidence"
    assert candidates == []


# ─── Проверка 2: block_exists ────────────────────────────────────────────────

def test_phantom_block():
    findings = {"findings": [{
        "id": "F-001", "page": 4,
        "evidence": [{"type": "image", "block_id": "GHOST-XXXX-YYY", "page": 4}],
    }]}
    blocks = _blocks(("REAL-AAAA-BBB", 4, None, "что-то"))
    reviews, candidates, result, _ = dc.review_structural(findings, blocks, {})
    assert reviews[0]["verdict"] == "phantom_block"
    assert candidates == []


# ─── Проверка 4: page_sheet_correct ──────────────────────────────────────────

def test_page_mismatch_tight_evidence():
    findings = {"findings": [{
        "id": "F-001", "page": 14,
        "evidence": [{"type": "image", "block_id": "B1-AAAA-BBB", "page": 4}],
    }]}
    blocks = _blocks(("B1-AAAA-BBB", 4, None, "x"))
    reviews, _c, _r, _ = dc.review_structural(findings, blocks, {})
    assert reviews[0]["verdict"] == "page_mismatch"


def test_page_mismatch_skipped_when_evidence_is_broad():
    """Широкий разброс evidence (>2 страниц) → НЕ флагуем page_mismatch."""
    evid = [{"type": "image", "block_id": f"B{i}-AAAA-BBB", "page": i} for i in (4, 5, 6, 9)]
    findings = {"findings": [{"id": "F-001", "page": 14, "evidence": evid}]}
    blocks = _blocks(*[(f"B{i}-AAAA-BBB", i, None, "x") for i in (4, 5, 6, 9)])
    reviews, candidates, _r, _ = dc.review_structural(findings, blocks, {})
    # структурно чисто → кандидат на LLM (а без LLM станет pass)
    assert reviews == []
    assert len(candidates) == 1


def test_page_list_matches_evidence():
    findings = {"findings": [{
        "id": "F-001", "page": [27, 29],
        "evidence": [
            {"type": "image", "block_id": "B1-AAAA-BBB", "page": 27},
            {"type": "image", "block_id": "B2-AAAA-BBB", "page": 29},
        ],
    }]}
    blocks = _blocks(("B1-AAAA-BBB", 27, None, "x"), ("B2-AAAA-BBB", 29, None, "y"))
    reviews, candidates, _r, _ = dc.review_structural(findings, blocks, {})
    assert reviews == []
    assert len(candidates) == 1  # прошёл структурно


def test_pass_via_related_block_ids():
    findings = {"findings": [{
        "id": "F-001", "page": 4,
        "related_block_ids": ["B1-AAAA-BBB"],
    }]}
    blocks = _blocks(("B1-AAAA-BBB", 4, None, "x"))
    reviews, candidates, _r, _ = dc.review_structural(findings, blocks, {})
    assert reviews == []
    assert len(candidates) == 1


# ─── Семантический LLM-проход (3/5) ──────────────────────────────────────────

def test_semantic_flags_weak_evidence(tmp_path):
    findings = {"findings": [{
        "id": "F-001", "page": 4, "description": "claim",
        "related_block_ids": ["B1-AAAA-BBB"],
    }]}
    (tmp_path / "03_findings.json").write_text(json.dumps(findings), encoding="utf-8")
    (tmp_path / "02_blocks_analysis.json").write_text(
        json.dumps(_blocks(("B1-AAAA-BBB", 4, None, "x"))), encoding="utf-8")
    (tmp_path / "document_graph.json").write_text(json.dumps(_graph((4, "1", "txt"))), encoding="utf-8")

    async def fake_llm(prompt):
        return json.dumps({"verdicts": [{"finding_id": "F-001", "verdict": "weak_evidence", "reason": "no"}]})

    res = _run(dc.run_deterministic_critic(tmp_path, llm_call=fake_llm, write=True))
    assert res.semantic_issues == 1
    assert res.reviews[0]["verdict"] == "weak_evidence"
    assert res.llm_used is True

    review = json.loads((tmp_path / "03_findings_review.json").read_text())
    assert review["meta"]["total_reviewed"] == 1
    assert review["meta"]["verdicts"].get("weak_evidence") == 1
    assert review["reviews"][0]["finding_id"] == "F-001"


def test_semantic_failsoft_on_llm_error(tmp_path):
    findings = {"findings": [{
        "id": "F-001", "page": 4, "related_block_ids": ["B1-AAAA-BBB"],
    }]}
    (tmp_path / "03_findings.json").write_text(json.dumps(findings), encoding="utf-8")
    (tmp_path / "02_blocks_analysis.json").write_text(
        json.dumps(_blocks(("B1-AAAA-BBB", 4, None, "x"))), encoding="utf-8")

    async def boom(prompt):
        raise RuntimeError("ngrok timeout")

    res = _run(dc.run_deterministic_critic(tmp_path, llm_call=boom, write=True))
    # fail-soft: кандидат остаётся pass, файл всё равно записан
    assert res.llm_failed is True
    assert res.reviews[0]["verdict"] == "pass"
    assert (tmp_path / "03_findings_review.json").exists()


# ─── Контракт результата / I/O ───────────────────────────────────────────────

def test_review_file_schema_no_llm(tmp_path):
    findings = {"findings": [
        {"id": "F-001", "page": 4, "related_block_ids": ["B1-AAAA-BBB"]},
        {"id": "F-002", "page": 4, "description": "нет evidence"},
    ]}
    (tmp_path / "03_findings.json").write_text(json.dumps(findings), encoding="utf-8")
    (tmp_path / "02_blocks_analysis.json").write_text(
        json.dumps(_blocks(("B1-AAAA-BBB", 4, None, "x"))), encoding="utf-8")

    res = _run(dc.run_deterministic_critic(tmp_path, llm_call=None, write=True))
    assert res.findings_total == 2
    assert res.deterministic_issues == 1  # F-002 no_evidence
    assert res.llm_used is False

    review = json.loads((tmp_path / "03_findings_review.json").read_text())
    assert review["meta"]["total_reviewed"] == 2
    assert review["meta"]["verdicts"]["pass"] == 1
    assert review["meta"]["verdicts"]["no_evidence"] == 1
    ids = [r["finding_id"] for r in review["reviews"]]
    assert ids == ["F-001", "F-002"]  # сохранён исходный порядок


def test_missing_findings_returns_error(tmp_path):
    res = _run(dc.run_deterministic_critic(tmp_path, llm_call=None, write=True))
    assert res.error is not None
    assert not (tmp_path / "03_findings_review.json").exists()


def test_chunk_input_filename(tmp_path):
    chunk = {"findings": [{"id": "F-007", "page": 4, "related_block_ids": ["B1-AAAA-BBB"]}]}
    (tmp_path / "03_findings_review_input_001.json").write_text(json.dumps(chunk), encoding="utf-8")
    (tmp_path / "02_blocks_analysis.json").write_text(
        json.dumps(_blocks(("B1-AAAA-BBB", 4, None, "x"))), encoding="utf-8")

    res = _run(dc.run_deterministic_critic(
        tmp_path, llm_call=None, write=True,
        findings_filename="03_findings_review_input_001.json",
        review_filename="03_findings_review_001.json",
    ))
    assert res.findings_total == 1
    assert (tmp_path / "03_findings_review_001.json").exists()
