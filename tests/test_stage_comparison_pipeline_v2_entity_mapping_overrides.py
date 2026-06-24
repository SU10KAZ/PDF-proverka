# -*- coding: utf-8 -*-
"""Тесты ручных override'ов выравнивания сущностей Pipeline V2 (write-слой).

Endpoints:
  GET    /api/stage-comparison/pipeline-v2/{sid}/entity-mapping-overrides?pair_id=
  PUT    /api/stage-comparison/pipeline-v2/{sid}/entity-mapping-overrides?pair_id=
  DELETE /api/stage-comparison/pipeline-v2/{sid}/entity-mapping-overrides/{mid}?pair_id=

Покрытие spec-кейсов:
  1.  GET missing → пустой ok (mappings=[]);
  2.  PUT создаёт overrides-файл;
  3.  PUT upsert (идемпотентно по идентичности пары);
  4.  невалидный decision → 422;
  5.  path traversal → 400;
  6.  пишет ТОЛЬКО target artifact;
  7.  entity-alignment endpoint включает manual_mapping в карточке + summary;
  8.  модуль не импортирует vision/Qwen/Opus/jobs (нет model-вызовов);
  9.  DELETE удаляет override;
  10. atomic write / валидный JSON.

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
    root = tmp_path / "comparison_entity_mapping"
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
EP = f"/api/stage-comparison/pipeline-v2/{SID}/entity-mapping-overrides"
EAP = f"/api/stage-comparison/pipeline-v2/{SID}/entity-alignment-preview"


def _pv2_dir(root: Path) -> Path:
    d = root / "sessions" / SID / "pairs" / PID / "pipeline_v2"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _ov_path(root: Path) -> Path:
    return _pv2_dir(root) / "entity_mapping_overrides.json"


def _mapping(**over):
    base = {"left_entity_label": "ВРУ-3", "right_entity_label": "ВРУ-2",
            "left_block_id": "EYMU", "right_block_id": "PNNH",
            "left_page_number": 27, "right_page_number": 26,
            "source_classification": "scope_reorganized",
            "manual_decision": "confirmed_reorganized",
            "comment": "smoke"}
    base.update(over)
    return base


# ─── 1: GET missing → empty ok ───────────────────────────────────────────────

def test_1_get_missing_empty_ok(client, comparison_root):
    r = client.get(EP, params={"pair_id": PID})
    assert r.status_code == 200
    d = r.json()
    assert d["status"] == "ok" and d["kind"].endswith("entity_mapping_overrides")
    assert d["mappings"] == [] and d["session_id"] == SID and d["pair_id"] == PID


# ─── 2: PUT creates file ─────────────────────────────────────────────────────

def test_2_put_creates_file(client, comparison_root):
    assert not _ov_path(comparison_root).exists()
    r = client.put(EP, params={"pair_id": PID}, json={"mapping": _mapping(),
                                                      "created_by": "igor"})
    assert r.status_code == 200
    d = r.json()
    assert d["ok"] is True and d["created"] is True
    assert d["override"]["manual_decision"] == "confirmed_reorganized"
    assert d["summary"] == {"total": 1, "confirmed": 1, "rejected": 0, "no_match": 0}
    assert _ov_path(comparison_root).is_file()


# ─── 3: PUT upsert idempotent ────────────────────────────────────────────────

def test_3_put_upsert_idempotent(client, comparison_root):
    r1 = client.put(EP, params={"pair_id": PID}, json={"mapping": _mapping()})
    mid1 = r1.json()["override"]["mapping_id"]
    # та же пара, другое решение → update, тот же mapping_id, не дубликат
    r2 = client.put(EP, params={"pair_id": PID},
                    json={"mapping": _mapping(manual_decision="confirmed_rename")})
    d2 = r2.json()
    assert d2["created"] is False
    assert d2["override"]["mapping_id"] == mid1
    assert d2["override"]["manual_decision"] == "confirmed_rename"
    got = client.get(EP, params={"pair_id": PID}).json()
    assert len(got["mappings"]) == 1   # не дублируется


# ─── 4: invalid decision → 422 ───────────────────────────────────────────────

def test_4_invalid_decision_422(client, comparison_root):
    r = client.put(EP, params={"pair_id": PID},
                   json={"mapping": _mapping(manual_decision="bogus_decision")})
    assert r.status_code == 422
    # файл не создан (валидация до записи)
    assert not _ov_path(comparison_root).exists()


# ─── 5: path traversal → 400 ─────────────────────────────────────────────────

def test_5_path_traversal_rejected(client, comparison_root):
    bad = "/api/stage-comparison/pipeline-v2/..%2f..%2fetc/entity-mapping-overrides"
    r = client.get(bad, params={"pair_id": PID})
    assert r.status_code in (400, 404)
    # валидный sid, traversal в pair_id
    r2 = client.put(EP, params={"pair_id": "../../escape"},
                    json={"mapping": _mapping()})
    assert r2.status_code in (400, 422)


# ─── 6: writes only target artifact ──────────────────────────────────────────

def test_6_writes_only_target(client, comparison_root):
    d = _pv2_dir(comparison_root)
    # подложить «соседние» артефакты — они не должны измениться
    (d / "entity_alignment_preview_report.json").write_text("{}", encoding="utf-8")
    (d / "block_link_preview_report.json").write_text("{}", encoding="utf-8")
    before = {p.name: p.read_bytes() for p in d.iterdir()}
    client.put(EP, params={"pair_id": PID}, json={"mapping": _mapping()})
    after = {p.name: p.read_bytes() for p in d.iterdir()}
    # появился ровно один новый файл — overrides; остальные не тронуты
    new = set(after) - set(before)
    assert new == {"entity_mapping_overrides.json"}
    for name, content in before.items():
        assert after[name] == content   # соседние артефакты byte-identical


# ─── 7: entity-alignment endpoint includes manual_mapping ────────────────────

def test_7_entity_alignment_includes_manual_mapping(client, comparison_root):
    d = _pv2_dir(comparison_root)
    report = {
        "version": 1, "kind": "stage_comparison_pipeline_v2_entity_alignment_preview",
        "status": "ok",
        "summary": {"graphic_pairs_total": 1, "same_entity_likely": 0,
                    "possible_rename": 0, "scope_reorganized": 1,
                    "mismatch_likely": 0, "link_validation_candidate": 0,
                    "needs_manual_mapping": 1, "unpaired_left": 0, "unpaired_right": 0},
        "pairs": [{"pair_key": "EYMU__PNNH", "left_block_id": "EYMU",
                   "right_block_id": "PNNH", "left_entity_label": "ВРУ-3",
                   "right_entity_label": "ВРУ-2", "entity_family": "ВРУ",
                   "classification": "scope_reorganized", "confidence": 0.6,
                   "reasons": [], "risk_flags": [],
                   "recommended_action": "manual_mapping", "evidence": {}}],
        "unpaired_entities": {"left": [], "right": []}, "warnings": [],
    }
    (d / "entity_alignment_preview_report.json").write_text(
        json.dumps(report, ensure_ascii=False), encoding="utf-8")
    # до override
    before = client.get(EAP, params={"pair_id": PID}).json()
    assert before["pairs"][0]["manual_mapping"]["status"] == "none"
    assert "manual_mapping" not in before["summary"]
    # записать override
    client.put(EP, params={"pair_id": PID}, json={"mapping": _mapping()})
    after = client.get(EAP, params={"pair_id": PID}).json()
    mm = after["pairs"][0]["manual_mapping"]
    assert mm["status"] == "mapped" and mm["decision"] == "confirmed_reorganized"
    assert mm["comment"] == "smoke" and mm["mapping_id"]
    assert after["summary"]["manual_mapping"]["confirmed"] == 1


# ─── 8: модуль без vision/Qwen/Opus/jobs ─────────────────────────────────────

def test_8_no_model_or_job_imports():
    # Гарантия mark-only: модуль импортирует только stdlib + paths, никаких
    # vision/Qwen/Opus/jobs/network. Проверяем РЕАЛЬНЫЕ import'ы через ast
    # (а не упоминания в docstring).
    import ast
    import inspect
    from backend.app.services.stage_comparison import (
        pipeline_v2_entity_mapping_overrides as mod)
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


# ─── 9: DELETE removes override ──────────────────────────────────────────────

def test_9_delete_removes(client, comparison_root):
    r = client.put(EP, params={"pair_id": PID}, json={"mapping": _mapping()})
    mid = r.json()["override"]["mapping_id"]
    dr = client.delete(f"{EP}/{mid}", params={"pair_id": PID})
    assert dr.status_code == 200
    d = dr.json()
    assert d["ok"] is True and d["deleted"] is True
    assert d["summary"]["total"] == 0
    assert client.get(EP, params={"pair_id": PID}).json()["mappings"] == []
    # повторный delete — deleted=false, не падает
    dr2 = client.delete(f"{EP}/{mid}", params={"pair_id": PID})
    assert dr2.status_code == 200 and dr2.json()["deleted"] is False


# ─── 10: atomic write / valid JSON ───────────────────────────────────────────

def test_10_atomic_write_valid_json(client, comparison_root):
    client.put(EP, params={"pair_id": PID}, json={"mapping": _mapping()})
    client.put(EP, params={"pair_id": PID},
               json={"mapping": _mapping(left_block_id="6XLX", right_block_id=None,
                                         right_entity_label=None,
                                         manual_decision="no_match")})
    path = _ov_path(comparison_root)
    parsed = json.loads(path.read_text(encoding="utf-8"))   # валидный JSON
    assert parsed["kind"] == "stage_comparison_pipeline_v2_entity_mapping_overrides"
    assert len(parsed["mappings"]) == 2
    assert len(parsed["no_match"]) == 1   # derived view
    assert parsed["history"]              # история не пустая
    # нет осиротевших .tmp файлов
    assert not list(path.parent.glob("*.tmp"))
