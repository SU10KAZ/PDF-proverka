# -*- coding: utf-8 -*-
"""Тесты write-layer operator review для Pipeline V2 Exclusion Preview v2.

Endpoints:
  GET    /api/stage-comparison/pipeline-v2/{sid}/exclusion-review-overrides?pair_id=
  PUT    /api/stage-comparison/pipeline-v2/{sid}/exclusion-review-overrides?pair_id=
  DELETE /api/stage-comparison/pipeline-v2/{sid}/exclusion-review-overrides/{did}?pair_id=

Покрытие spec-кейсов:
  1.  GET missing → пустой ok (decisions=[]);
  2.  PUT создаёт artifact;
  3.  PUT upsert идемпотентно (один и тот же item_id → обновление, без дубля);
  4.  невалидный decision → 422;
  5.  path traversal → 400;
  6.  пишет ТОЛЬКО exclusion_review_overrides.json, соседние артефакты не трогает;
  7.  exclusion-preview-v2 endpoint включает operator_review в items и summary;
  8.  модуль без vision/Qwen/Opus/jobs (mark-only гарантия);
  9.  DELETE удаляет решение, повторный → deleted=false;
  10. atomic write, валидный JSON, нет .tmp;
  11. все 5 valid decisions принимаются;
  12. comment опционален — отсутствует/null → не ломает.

Никаких реальных LLM/Qwen/Opus/network.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient


@pytest.fixture()
def comparison_root(tmp_path, monkeypatch):
    root = tmp_path / "comparison_excl_review"
    root.mkdir()
    monkeypatch.setenv("COMPARISON_ROOT", str(root))
    yield root


@pytest.fixture()
def client(comparison_root):
    from backend.app.api.routers import stage_comparison as router_mod
    app = FastAPI()
    app.include_router(router_mod.router)
    return TestClient(app)


SID = "ba413a93c5754f6c"
PID = "pf06effb7"
EP = f"/api/stage-comparison/pipeline-v2/{SID}/exclusion-review-overrides"
XPP = f"/api/stage-comparison/pipeline-v2/{SID}/exclusion-preview-v2"


def _pv2_dir(root: Path) -> Path:
    d = root / "sessions" / SID / "pairs" / PID / "pipeline_v2"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _ov_path(root: Path) -> Path:
    return _pv2_dir(root) / "exclusion_review_overrides.json"


def _decision(**over):
    base = {
        "exclusion_item_id":  "xp_bp::EYMU::PNNH",
        "left_block_id":      "EYMU",
        "right_block_id":     "PNNH",
        "left_entity_label":  "ВРУ-3",
        "right_entity_label": "ВРУ-2",
        "preview_classification": "candidate_exclude",
        "preview_severity":   "high",
        "operator_decision":  "reject_exclude",
        "comment":            "smoke test",
    }
    base.update(over)
    return base


# ─── 1: GET missing → empty ok ───────────────────────────────────────────────

def test_1_get_missing_empty_ok(client, comparison_root):
    r = client.get(EP, params={"pair_id": PID})
    assert r.status_code == 200
    d = r.json()
    assert d["status"] == "ok"
    assert d["kind"].endswith("exclusion_review_overrides")
    assert d["decisions"] == []
    assert d["session_id"] == SID and d["pair_id"] == PID


# ─── 2: PUT creates file ─────────────────────────────────────────────────────

def test_2_put_creates_file(client, comparison_root):
    assert not _ov_path(comparison_root).exists()
    r = client.put(EP, params={"pair_id": PID},
                   json={"decision": _decision(), "created_by": "igor"})
    assert r.status_code == 200
    d = r.json()
    assert d["ok"] is True and d["created"] is True
    assert d["decision"]["operator_decision"] == "reject_exclude"
    dec_id = d["decision"]["decision_id"]
    assert dec_id.startswith("xrd_")
    assert d["summary"]["total"] == 1 and d["summary"]["reject_exclude"] == 1
    assert _ov_path(comparison_root).is_file()


# ─── 3: PUT upsert idempotent ────────────────────────────────────────────────

def test_3_put_upsert_idempotent(client, comparison_root):
    r1 = client.put(EP, params={"pair_id": PID},
                    json={"decision": _decision()})
    did1 = r1.json()["decision"]["decision_id"]

    # та же identita, другое решение → update, тот же decision_id
    r2 = client.put(EP, params={"pair_id": PID},
                    json={"decision": _decision(operator_decision="needs_review",
                                                comment="updated")})
    d2 = r2.json()
    assert d2["created"] is False
    assert d2["decision"]["decision_id"] == did1
    assert d2["decision"]["operator_decision"] == "needs_review"
    assert d2["decision"]["comment"] == "updated"

    # нет дублей
    got = client.get(EP, params={"pair_id": PID}).json()
    assert len(got["decisions"]) == 1


# ─── 4: invalid decision → 422 ───────────────────────────────────────────────

def test_4_invalid_decision_422(client, comparison_root):
    r = client.put(EP, params={"pair_id": PID},
                   json={"decision": _decision(operator_decision="make_it_explode")})
    assert r.status_code == 422
    # файл не создан
    assert not _ov_path(comparison_root).exists()


# ─── 5: path traversal → 400 ─────────────────────────────────────────────────

def test_5_path_traversal_rejected(client, comparison_root):
    bad = "/api/stage-comparison/pipeline-v2/..%2f..%2fetc/exclusion-review-overrides"
    r = client.get(bad, params={"pair_id": PID})
    assert r.status_code in (400, 404)

    # traversal в pair_id
    r2 = client.put(EP, params={"pair_id": "../../escape"},
                    json={"decision": _decision()})
    assert r2.status_code in (400, 422)


# ─── 6: writes only target artifact ──────────────────────────────────────────

def test_6_writes_only_target(client, comparison_root):
    d = _pv2_dir(comparison_root)
    # подложить соседние артефакты
    (d / "exclusion_preview_v2_report.json").write_text("{}", encoding="utf-8")
    (d / "entity_alignment_preview_report.json").write_text("{}", encoding="utf-8")
    (d / "block_link_preview_report.json").write_text("{}", encoding="utf-8")
    before = {p.name: p.read_bytes() for p in d.iterdir()}

    client.put(EP, params={"pair_id": PID},
               json={"decision": _decision()})

    after = {p.name: p.read_bytes() for p in d.iterdir()}
    new_files = set(after) - set(before)
    assert new_files == {"exclusion_review_overrides.json"}
    for name, content in before.items():
        assert after[name] == content, f"{name} должен быть byte-identical"


# ─── 7: exclusion-preview-v2 includes operator_review ────────────────────────

def test_7_exclusion_preview_includes_operator_review(client, comparison_root):
    d = _pv2_dir(comparison_root)
    item_id = "xp_bp::EYMU::PNNH"
    report = {
        "version": 1,
        "kind": "stage_comparison_pipeline_v2_exclusion_preview",
        "status": "ok",
        "generated_at": "2026-06-12T10:00:00",
        "summary": {
            "items_total": 1, "candidate_exclude": 1, "review_only": 0,
            "keep": 0, "link_validation_required": 0,
            "repeated_reject_transitions": 0, "manual_vision_conflict": 0,
        },
        "items": [{
            "item_id": item_id,
            "left_block_id": "EYMU", "right_block_id": "PNNH",
            "left_entity_label": "ВРУ-3", "right_entity_label": "ВРУ-2",
            "classification": "candidate_exclude", "severity": "high",
            "confidence": 0.82, "reasons": ["repeated_reject"],
            "risk_flags": [], "recommended_action": "review_and_exclude",
            # mark-only инварианты (должны быть srtipped)
            "use_as_grounded_fact": True, "auto_apply": True,
            "enforce_allowed": True,
        }],
        "warnings": [],
    }
    (d / "exclusion_preview_v2_report.json").write_text(
        json.dumps(report, ensure_ascii=False), encoding="utf-8")

    # ДО override — operator_review отсутствует или status=none
    before = client.get(XPP, params={"pair_id": PID}).json()
    assert before["status"] == "ok" and before["available"]
    it_before = before["items"][0]
    assert it_before.get("operator_review", {}).get("status", "none") == "none"
    # mark-only strip сработал
    assert it_before.get("use_as_grounded_fact") is False
    assert it_before.get("auto_apply") is False
    assert it_before.get("enforce_allowed") is False

    # записать решение оператора
    client.put(EP, params={"pair_id": PID},
               json={"decision": _decision(comment="confirmed bad mapping")})

    # ПОСЛЕ override — operator_review должен быть в item
    after = client.get(XPP, params={"pair_id": PID}).json()
    it_after = after["items"][0]
    rev = it_after.get("operator_review", {})
    assert rev["status"] == "reviewed"
    assert rev["operator_decision"] == "reject_exclude"
    assert rev["comment"] == "confirmed bad mapping"
    assert rev["decision_id"].startswith("xrd_")

    # summary тоже содержит operator_review
    summ = after["summary"]
    assert summ["operator_review"]["total"] == 1
    assert summ["operator_review"]["reject_exclude"] == 1


# ─── 8: модуль без vision/Qwen/Opus/jobs ─────────────────────────────────────

def test_8_no_model_or_job_imports():
    import ast
    import inspect
    from backend.app.services.stage_comparison import (
        pipeline_v2_exclusion_review_overrides as mod)
    tree = ast.parse(inspect.getsource(mod))
    imported: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported += [a.name for a in node.names]
        elif isinstance(node, ast.ImportFrom):
            imported.append(node.module or "")
    forbidden_substr = ("vision", "qwen", "opus", "llm", "subprocess",
                        "httpx", "requests", "jobs", "graphic_llm",
                        "md_enrichment", "unified")
    for name in imported:
        low = name.lower()
        assert not any(f in low for f in forbidden_substr), \
            f"unexpected import: {name}"
    # единственный внутренний импорт — paths
    internal = [n for n in imported if n.startswith("backend.")]
    assert internal == [
        "backend.app.services.stage_comparison.paths"], internal


# ─── 9: DELETE removes decision ──────────────────────────────────────────────

def test_9_delete_removes(client, comparison_root):
    r = client.put(EP, params={"pair_id": PID},
                   json={"decision": _decision()})
    did = r.json()["decision"]["decision_id"]

    dr = client.delete(f"{EP}/{did}", params={"pair_id": PID})
    assert dr.status_code == 200
    d = dr.json()
    assert d["ok"] is True and d["deleted"] is True
    assert d["summary"]["total"] == 0

    # GET подтверждает пустой список
    assert client.get(EP, params={"pair_id": PID}).json()["decisions"] == []

    # повторный DELETE — deleted=false, не 404
    dr2 = client.delete(f"{EP}/{did}", params={"pair_id": PID})
    assert dr2.status_code == 200 and dr2.json()["deleted"] is False


# ─── 10: atomic write, valid JSON, no .tmp ───────────────────────────────────

def test_10_atomic_write_valid_json(client, comparison_root):
    client.put(EP, params={"pair_id": PID},
               json={"decision": _decision()})
    client.put(EP, params={"pair_id": PID},
               json={"decision": _decision(
                   exclusion_item_id="xp_bp::AA::BB",
                   left_block_id="AA", right_block_id="BB",
                   left_entity_label="ЩР-1", right_entity_label="ЩР-1",
                   operator_decision="keep")})
    path = _ov_path(comparison_root)
    parsed = json.loads(path.read_text(encoding="utf-8"))
    assert parsed["kind"] == "stage_comparison_pipeline_v2_exclusion_review_overrides"
    assert len(parsed["decisions"]) == 2
    assert parsed["history"]  # история не пустая
    # нет .tmp-файлов
    assert not list(path.parent.glob("*.tmp"))


# ─── 11: all 5 valid decisions accepted ──────────────────────────────────────

def test_11_all_valid_decisions(client, comparison_root):
    valid = ["approve_exclude", "reject_exclude", "needs_review",
             "keep", "run_link_validation"]
    for i, dec in enumerate(valid):
        item_id = f"xp_bp::B{i}::C{i}"
        r = client.put(EP, params={"pair_id": PID},
                       json={"decision": _decision(
                           exclusion_item_id=item_id,
                           left_block_id=f"B{i}", right_block_id=f"C{i}",
                           operator_decision=dec)})
        assert r.status_code == 200, f"decision {dec!r} должен быть 200, got {r.status_code}"
        assert r.json()["decision"]["operator_decision"] == dec

    got = client.get(EP, params={"pair_id": PID}).json()
    assert got["summary"]["total"] == len(valid)
    assert got["summary"]["approve_exclude"] == 1
    assert got["summary"]["reject_exclude"] == 1
    assert got["summary"]["needs_review"] == 1
    assert got["summary"]["keep"] == 1
    assert got["summary"]["run_link_validation"] == 1


# ─── 12: comment optional ────────────────────────────────────────────────────

def test_12_comment_optional(client, comparison_root):
    # без comment
    r1 = client.put(EP, params={"pair_id": PID},
                    json={"decision": _decision(comment=None)})
    assert r1.status_code == 200
    d1 = r1.json()["decision"]
    assert d1.get("comment") is None or d1.get("comment") == ""

    # update с пустой строкой
    r2 = client.put(EP, params={"pair_id": PID},
                    json={"decision": _decision(comment="")})
    assert r2.status_code == 200

    # файл по-прежнему валидный JSON
    path = _ov_path(comparison_root)
    parsed = json.loads(path.read_text(encoding="utf-8"))
    assert parsed["decisions"]
