"""Тесты для session-level плоского списка text-LLM изменений.

Покрывают:
  - build_flat: агрегирует несколько `text_llm_diff.json`
  - корректный учёт done/not_run/error/skipped pairs в summary
  - сортировка по pair/page/severity
  - location resolved через text_location (MD page marker)
  - HTTP endpoint GET /api/stage-comparison/sessions/{sid}/text-llm-diff-flat
"""
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import pytest


@pytest.fixture(autouse=True)
def _tmp_comparison_root(tmp_path, monkeypatch):
    root = tmp_path / "comparison_test"
    root.mkdir()
    monkeypatch.setenv("COMPARISON_ROOT", str(root))
    yield root


def _make_pair(session_id: str, pair_id: str, *,
               left_md: Path | None, right_md: Path | None) -> dict:
    from backend.app.services.stage_comparison import paths as paths_mod
    meta = {"id": session_id, "pair_order": [pair_id], "warnings": [],
            "created_at": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")}
    paths_mod.session_json_path(session_id).write_text(
        json.dumps(meta, ensure_ascii=False), encoding="utf-8")
    pair = {
        "id": pair_id, "status": "matched",
        "left":  {"filename": "left.pdf",  "md_path": (str(left_md) if left_md else None)},
        "right": {"filename": "right.pdf", "md_path": (str(right_md) if right_md else None)},
    }
    paths_mod.pair_json_path(session_id, pair_id).write_text(
        json.dumps(pair, ensure_ascii=False), encoding="utf-8")
    return pair


def _make_pair_multi(session_id: str, pairs: list[dict]) -> None:
    from backend.app.services.stage_comparison import paths as paths_mod
    meta = {"id": session_id, "pair_order": [p["id"] for p in pairs], "warnings": [],
            "created_at": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")}
    paths_mod.session_json_path(session_id).write_text(
        json.dumps(meta, ensure_ascii=False), encoding="utf-8")
    for p in pairs:
        paths_mod.pair_json_path(session_id, p["id"]).write_text(
            json.dumps(p, ensure_ascii=False), encoding="utf-8")


def _write_text_llm_diff(session_id: str, pair_id: str, payload: dict) -> None:
    from backend.app.services.stage_comparison import paths as paths_mod
    path = paths_mod.text_llm_diff_path(session_id, pair_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def _write_md_with_pages(tmp_path: Path, name: str, pages: dict[int, str]) -> Path:
    parts = []
    for pno, text in sorted(pages.items()):
        parts.append(f"## СТРАНИЦА {pno}")
        parts.append(text)
    p = tmp_path / name
    p.write_text("\n\n".join(parts), encoding="utf-8")
    return p


# ─── 1. Базовая агрегация done-пары ──────────────────────────────────────


def test_flat_aggregates_done_pair(tmp_path):
    from backend.app.services.stage_comparison import text_llm_flat

    left = _write_md_with_pages(tmp_path, "L.md", {1: "alpha quote here", 2: "page two"})
    right = _write_md_with_pages(tmp_path, "R.md", {1: "alpha quote here updated", 2: "page two"})
    _make_pair("flat_s1", "p1", left_md=left, right_md=right)
    _write_text_llm_diff("flat_s1", "p1", {
        "version": 1, "status": "done",
        "left_md_path": str(left), "right_md_path": str(right),
        "changes": [
            {"id": "c1", "type": "changed", "category": "material",
             "severity": "high", "title": "Изменение материала",
             "summary": "Описание",
             "old_value": "X", "new_value": "Y",
             "evidence_left":  {"quote": "alpha quote here"},
             "evidence_right": {"quote": "alpha quote here updated"},
             "requires_human_review": True},
        ],
    })

    result = text_llm_flat.build_flat("flat_s1")
    assert result["summary"]["total_pairs"] == 1
    assert result["summary"]["done_pairs"] == 1
    assert result["summary"]["total_changes"] == 1
    assert result["summary"]["by_severity"]["high"] == 1
    assert result["summary"]["requires_human_review"] == 1
    items = result["items"]
    assert len(items) == 1
    it = items[0]
    assert it["pair_id"] == "p1"
    assert it["title"] == "Изменение материала"
    assert it["severity"] == "high"
    assert it["sheet"] == "Лист 1"
    assert it["left_page"] == 1
    assert it["right_page"] == 1
    assert it["location_method"] == "md_page_marker"


# ─── 2. not_run / error / skipped учёт ───────────────────────────────────


def test_flat_counts_not_run_and_error_pairs(tmp_path):
    from backend.app.services.stage_comparison import text_llm_flat

    pairs_meta = [
        {"id": "p_done",  "status": "matched",
         "left": {"filename": "a.pdf"}, "right": {"filename": "b.pdf"}},
        {"id": "p_err",   "status": "matched",
         "left": {"filename": "c.pdf"}, "right": {"filename": "d.pdf"}},
        {"id": "p_skip",  "status": "matched",
         "left": {"filename": "e.pdf"}, "right": {"filename": "f.pdf"}},
        {"id": "p_notrun","status": "matched",
         "left": {"filename": "g.pdf"}, "right": {"filename": "h.pdf"}},
    ]
    _make_pair_multi("flat_s2", pairs_meta)
    _write_text_llm_diff("flat_s2", "p_done", {
        "version": 1, "status": "done", "changes": [
            {"id": "c1", "type": "changed", "severity": "low", "title": "ok"},
        ],
    })
    _write_text_llm_diff("flat_s2", "p_err", {
        "version": 1, "status": "error", "error": "boom",
    })
    _write_text_llm_diff("flat_s2", "p_skip", {
        "version": 1, "status": "missing_md",
    })
    # p_notrun: вообще нет файла text_llm_diff.json

    result = text_llm_flat.build_flat("flat_s2")
    s = result["summary"]
    assert s["total_pairs"] == 4
    assert s["done_pairs"] == 1
    assert s["error_pairs"] == 1
    assert s["skipped_pairs"] == 1
    assert s["not_run_pairs"] == 1
    assert s["total_changes"] == 1
    # Только done item попал в items
    assert len(result["items"]) == 1
    assert result["items"][0]["pair_id"] == "p_done"


# ─── 3. Сортировка ───────────────────────────────────────────────────────


def test_flat_sorts_by_pair_then_page_then_severity(tmp_path):
    from backend.app.services.stage_comparison import text_llm_flat

    left = _write_md_with_pages(tmp_path, "L.md",
                                 {1: "alpha", 2: "beta", 3: "gamma"})
    right = _write_md_with_pages(tmp_path, "R.md",
                                  {1: "alpha", 2: "beta", 3: "gamma"})
    _make_pair("flat_s3", "p1", left_md=left, right_md=right)
    _write_text_llm_diff("flat_s3", "p1", {
        "version": 1, "status": "done",
        "left_md_path": str(left), "right_md_path": str(right),
        "changes": [
            {"id": "cZ", "type": "changed", "severity": "low",
             "title": "page3 low",
             "evidence_left": {"quote": "gamma"}, "evidence_right": {"quote": "gamma"}},
            {"id": "cA", "type": "changed", "severity": "high",
             "title": "page1 high",
             "evidence_left": {"quote": "alpha"}, "evidence_right": {"quote": "alpha"}},
            {"id": "cM", "type": "changed", "severity": "medium",
             "title": "page2 medium",
             "evidence_left": {"quote": "beta"}, "evidence_right": {"quote": "beta"}},
        ],
    })
    items = text_llm_flat.build_flat("flat_s3")["items"]
    titles = [it["title"] for it in items]
    assert titles == ["page1 high", "page2 medium", "page3 low"], titles


# ─── 4. HTTP endpoint ────────────────────────────────────────────────────


def test_flat_http_endpoint(tmp_path):
    from fastapi.testclient import TestClient
    from backend.app.main import app
    from backend.app.services.stage_comparison import text_llm_flat as _  # noqa: F401

    left = _write_md_with_pages(tmp_path, "L.md", {1: "X"})
    right = _write_md_with_pages(tmp_path, "R.md", {1: "Y"})
    _make_pair("flat_s4", "p1", left_md=left, right_md=right)
    _write_text_llm_diff("flat_s4", "p1", {
        "version": 1, "status": "done",
        "left_md_path": str(left), "right_md_path": str(right),
        "changes": [{"id": "c1", "type": "changed", "severity": "medium",
                     "title": "T", "summary": "S",
                     "evidence_left": {"quote": "X"}, "evidence_right": {"quote": "Y"}}],
    })
    client = TestClient(app)
    r = client.get("/api/stage-comparison/sessions/flat_s4/text-llm-diff-flat")
    assert r.status_code == 200, r.text
    payload = r.json()
    assert payload["session_id"] == "flat_s4"
    assert payload["summary"]["total_pairs"] == 1
    assert payload["summary"]["done_pairs"] == 1
    assert payload["summary"]["total_changes"] == 1
    assert len(payload["items"]) == 1


def test_flat_http_endpoint_unknown_session():
    from fastapi.testclient import TestClient
    from backend.app.main import app

    client = TestClient(app)
    r = client.get("/api/stage-comparison/sessions/__nope__/text-llm-diff-flat")
    assert r.status_code == 404
