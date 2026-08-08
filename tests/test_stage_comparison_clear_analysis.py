"""Тесты «Очистить анализ» по выбранным парам (clear_analysis).

Покрытие (ТЗ):
  1. clear-analysis создаёт backup;
  2. удаляет comparison_result.json;
  3. удаляет expert_review-решения пары (session-level, по ключам);
  4. удаляет v2_review_status.json;
  5. удаляет v2_excluded_changes.json (excluded/review status);
  6. НЕ удаляет page_enriched.json (large-sheet Qwen);
  7. НЕ удаляет OCR result.json;
  8. НЕ удаляет исходный PDF и enriched MD;
  9. НЕ очищает пару с running job (warning);
 10. batch clear работает только по выбранным pair_ids;
 + endpoint: ok/cleared_pairs; 400 на пустых pair_ids.

Никаких внешних API, Qwen/Opus не вызываются. Файлы — в tmp_path.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from backend.app.services.stage_comparison import paths as paths_mod
from backend.app.services.stage_comparison import store as store_mod
from backend.app.services.stage_comparison import expert_review as er_mod
from backend.app.services.stage_comparison import unified_analysis_jobs as uaj_mod
from backend.app.services.stage_comparison import clear_analysis as ca_mod

SID = "sess1"


def _write(p: Path, content: str = "{}") -> Path:
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(content, encoding="utf-8")
    return p


@pytest.fixture
def env(tmp_path, monkeypatch):
    monkeypatch.setenv("COMPARISON_ROOT", str(tmp_path / "comparison"))
    session = {"id": SID, "pairs": [
        {"id": "p1", "status": "active"},
        {"id": "p2", "status": "active"},
    ]}
    monkeypatch.setattr(store_mod, "get_session",
                        lambda s: session if s == SID else None)
    # no active jobs by default
    monkeypatch.setattr(uaj_mod, "find_active_session_job", lambda s: None)
    return tmp_path


def _seed_pair(pid: str) -> dict:
    """Создать аналитические + сохраняемые артефакты для пары. Вернуть пути."""
    paths = {
        "comparison_result": _write(
            paths_mod.enriched_comparison_result_path(SID, pid),
            json.dumps({"status": "done", "changes": [{"id": "c1"}]})),
        "raw": _write(paths_mod.enriched_comparison_raw_path(SID, pid), "raw"),
        "job": _write(paths_mod.enriched_comparison_job_path(SID, pid), "{}"),
        "v2_status": _write(paths_mod.v2_review_status_path(SID, pid),
                            json.dumps({"c1": "confirmed"})),
        "v2_excluded": _write(paths_mod.v2_excluded_changes_path(SID, pid),
                              json.dumps(["c9"])),
        "graphic_diffs": _write(paths_mod.graphic_diffs_path(SID, pid), "[]"),
        "block_eq": _write(paths_mod.block_equivalence_report_path(SID, pid), "{}"),
        # preserved
        "page_enriched": _write(paths_mod.large_sheet_artifact_path(
            SID, pid, "right", 1, "page_enriched.json"),
            json.dumps({"circuits": [1, 2, 3]})),
        "enriched_md_left": _write(
            paths_mod.text_enrichment_md_path(SID, pid, "left"), "# left"),
        "enriched_md_right": _write(
            paths_mod.text_enrichment_md_path(SID, pid, "right"), "# right"),
        "ocr_result": _write(paths_mod.pair_dir(SID, pid) / "result.json",
                             json.dumps({"blocks": []})),
        "pdf": _write(paths_mod.pair_dir(SID, pid) / "source.pdf", "%PDF-1.7"),
    }
    return paths


def _seed_expert_review():
    _write(paths_mod.expert_review_path(SID), json.dumps({
        "version": 2, "updated_at": "t",
        "decisions": {"p1::c1": {"verdict": "accepted"},
                      "p2::c2": {"verdict": "rejected"}},
    }))


# ─── service scenarios ──────────────────────────────────────────────────────

def test_1_creates_backup(env):
    p = _seed_pair("p1")
    res = ca_mod.clear_pair_analysis(SID, "p1")
    assert res["backup_path"]
    bdir = Path(res["backup_path"])
    assert bdir.is_dir() and "_backup_before_clear_analysis_" in bdir.name
    # backed-up copy present
    assert (bdir / "enriched_comparison" / "comparison_result.json").exists()


def test_2_removes_comparison_result(env):
    p = _seed_pair("p1")
    assert p["comparison_result"].exists()
    ca_mod.clear_pair_analysis(SID, "p1")
    assert not p["comparison_result"].exists()
    assert not p["raw"].exists()
    assert not p["job"].exists()


def test_3_removes_expert_review_keys_for_pair_only(env):
    _seed_pair("p1")
    _seed_expert_review()
    res = ca_mod.clear_pair_analysis(SID, "p1")
    assert res["expert_review_removed_keys"] == 1
    data = json.loads(paths_mod.expert_review_path(SID).read_text())
    assert "p1::c1" not in data["decisions"]
    assert "p2::c2" in data["decisions"]  # other pair untouched


def test_4_removes_v2_review_status(env):
    p = _seed_pair("p1")
    ca_mod.clear_pair_analysis(SID, "p1")
    assert not p["v2_status"].exists()


def test_5_removes_v2_excluded_changes(env):
    p = _seed_pair("p1")
    ca_mod.clear_pair_analysis(SID, "p1")
    assert not p["v2_excluded"].exists()


def test_6_preserves_page_enriched(env):
    p = _seed_pair("p1")
    ca_mod.clear_pair_analysis(SID, "p1", clear_enrichment=True)
    assert p["page_enriched"].exists()  # large-sheet Qwen artifact preserved


def test_7_preserves_ocr_result_json(env):
    p = _seed_pair("p1")
    ca_mod.clear_pair_analysis(SID, "p1", clear_enrichment=True)
    assert p["ocr_result"].exists()


def test_8_preserves_pdf_and_enriched_md(env):
    p = _seed_pair("p1")
    ca_mod.clear_pair_analysis(SID, "p1", clear_enrichment=True)
    assert p["pdf"].exists()
    assert p["enriched_md_left"].exists()
    assert p["enriched_md_right"].exists()


def test_9_skips_pair_with_running_job(env, monkeypatch):
    p = _seed_pair("p1")
    monkeypatch.setattr(uaj_mod, "find_active_session_job",
                        lambda s: {"status": "running", "items": [{"pair_id": "p1"}]})
    out = ca_mod.clear_pairs_analysis(SID, ["p1"])
    assert out["cleared_pairs"] == 0
    assert out["skipped"] == [{"pair_id": "p1",
                               "reason": "pair has running job, cancel first"}]
    # files intact
    assert p["comparison_result"].exists()
    assert p["v2_status"].exists()


def test_10_batch_only_selected_pairs(env):
    p1 = _seed_pair("p1")
    p2 = _seed_pair("p2")
    _seed_expert_review()
    out = ca_mod.clear_pairs_analysis(SID, ["p1"])
    assert out["cleared_pairs"] == 1
    # p1 cleared
    assert not p1["comparison_result"].exists()
    # p2 untouched
    assert p2["comparison_result"].exists()
    assert p2["v2_status"].exists()
    data = json.loads(paths_mod.expert_review_path(SID).read_text())
    assert "p2::c2" in data["decisions"]


def test_clear_enrichment_removes_derived_diffs(env):
    p = _seed_pair("p1")
    ca_mod.clear_pair_analysis(SID, "p1", clear_enrichment=True)
    assert not p["graphic_diffs"].exists()
    assert not p["block_eq"].exists()
    # but enrichment preserved
    assert p["page_enriched"].exists()


def test_default_off_does_not_remove_derived_diffs(env):
    p = _seed_pair("p1")
    ca_mod.clear_pair_analysis(SID, "p1")  # clear_enrichment default False
    assert p["graphic_diffs"].exists()
    assert p["block_eq"].exists()


def test_unknown_pair_skipped(env):
    out = ca_mod.clear_pairs_analysis(SID, ["pX"])
    assert out["cleared_pairs"] == 0
    assert out["skipped"] == [{"pair_id": "pX", "reason": "pair not in session"}]


# ─── endpoint ───────────────────────────────────────────────────────────────

def _client():
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from backend.app.api.routers import stage_comparison as router_mod
    app = FastAPI()
    app.include_router(router_mod.router)
    return TestClient(app)


def test_endpoint_ok(env):
    p = _seed_pair("p1")
    client = _client()
    r = client.post(f"/api/stage-comparison/sessions/{SID}/pairs/clear-analysis",
                    json={"pair_ids": ["p1"]})
    assert r.status_code == 200
    body = r.json()
    assert body["ok"] is True
    assert body["cleared_pairs"] == 1
    assert body["backup_paths"]
    assert not p["comparison_result"].exists()


def test_endpoint_empty_pair_ids_400(env):
    client = _client()
    r = client.post(f"/api/stage-comparison/sessions/{SID}/pairs/clear-analysis",
                    json={"pair_ids": []})
    assert r.status_code == 400


def test_endpoint_unknown_session_404(env, monkeypatch):
    client = _client()
    r = client.post("/api/stage-comparison/sessions/NOPE/pairs/clear-analysis",
                    json={"pair_ids": ["p1"]})
    assert r.status_code == 404
