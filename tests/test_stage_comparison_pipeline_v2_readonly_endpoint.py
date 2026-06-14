# -*- coding: utf-8 -*-
"""Тесты read-only endpoint'а Pipeline V2 UI payload.

GET /api/stage-comparison/pipeline-v2/{session_id}/ui-payload (+ ?pair_id=…)

Покрываемые spec-кейсы:
  1.  отдаёт готовый pipeline_v2_ui_payload.json;
  2.  строит payload из summary + diff + explanation, если готового нет;
  3.  not_found, если артефактов нет (+ available_pairs discovery);
  4.  не вызывает Qwen/Opus/LLM/job-функции (сеть заблокирована, source scan);
  5.  read-only: не создаёт новых файлов/директорий;
  6.  битый artifact → fail-soft JSON, не 500;
  7.  5 секций UI payload сохраняются в порядке контракта.

Никаких реальных LLM/network/live backend.
"""
from __future__ import annotations

import importlib
import json
from pathlib import Path

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.app.services.stage_comparison import pipeline_v2_dry_run as dr
from backend.app.services.stage_comparison import pipeline_v2_ui_payload as ui


@pytest.fixture()
def comparison_root(tmp_path, monkeypatch):
    # корень НЕ праймится sessions/.gitkeep'ом: сервис обязан резолвить пути
    # через «чистые» *_path() без mkdir — снапшоты дерева охраняют и root
    root = tmp_path / "comparison_pv2_endpoint"
    root.mkdir()
    monkeypatch.setenv("COMPARISON_ROOT", str(root))
    yield root


@pytest.fixture()
def client(comparison_root):
    from backend.app.api.routers import stage_comparison as router_mod
    app = FastAPI()
    app.include_router(router_mod.router)
    return TestClient(app)


def _svc():
    from backend.app.services.stage_comparison import pipeline_v2_payload_service
    return pipeline_v2_payload_service


# ─── builders (синтетика в форме реальных артефактов) ────────────────────────


def _delta(did, *, verdict_subject="organization", old="ARTEL", new="ИНПАД"):
    return {
        "delta_id": did, "delta_type": "changed", "entity_type": "stamp_field",
        "semantic_group": "stamp", "left_entity_id": "el", "right_entity_id": "er",
        "left_block_id": "L1", "right_block_id": "R1", "block_match_id": "bm_1",
        "page_numbers": {"left": 1, "right": 1}, "subject": verdict_subject,
        "field": "value", "old_value": old, "new_value": new,
        "change_summary": f"stamp_field: {old} → {new}", "confidence": 0.85,
        "evidence": {"left": {"quote": old}, "right": {"quote": new}},
        "match": {"method": "subject_type", "score": 1.0, "reasons": []},
        "quality_flags": [],
    }


def _expl(did, *, status="explained", verdict="accept", show=True, risk="medium",
          raw_status="ok", graphic_context=None):
    return {
        "explanation_id": f"expl_{did}", "delta_id": did, "mode": "explain_and_critic",
        "summary": f"Объяснение {did}", "engineering_meaning": "…",
        "contractor_impact": "…", "risk_level": risk,
        "groundedness": {"verdict": "grounded", "reason": "",
                         "uses_left_evidence": True, "uses_right_evidence": True},
        "critic": {"verdict": verdict, "reason": "", "should_show_to_engineer": show},
        "graphic_context": graphic_context or {
            "readiness": "medium", "needs_vision_enrichment": False,
            "manual_review_recommended": False, "notes": []},
        "input_delta": {"delta_type": "changed", "entity_type": "stamp_field",
                        "old_value": "ARTEL", "new_value": "ИНПАД"},
        "model": {"provider": "mock", "raw_status": raw_status, "error": None},
        "quality_flags": [], "status": status,
    }


def _reports():
    """Diff + explanation отчёты, дающие все 5 секций ненулевой логикой."""
    deltas = [_delta("d_conf"), _delta("d_review"), _delta("d_weak"),
              _delta("d_noise"), _delta("d_fail")]
    expls = [
        _expl("d_conf"),
        _expl("d_review", status="needs_human_review", verdict="needs_human_review"),
        _expl("d_weak", status="needs_human_review", verdict="possible_weak_graphic",
              graphic_context={"readiness": "not_usable",
                               "needs_vision_enrichment": True,
                               "manual_review_recommended": True, "notes": []}),
        _expl("d_noise", status="needs_human_review", verdict="possible_ocr_noise",
              show=False, risk="none"),
        _expl("d_fail", status="failed", verdict="needs_human_review",
              raw_status="failed"),
    ]
    diff = {"version": 1, "kind": "stage_comparison_pipeline_v2_entity_diff",
            "summary": {"deltas_total": len(deltas)}, "deltas": deltas,
            "matched_entity_pairs": [], "unmatched_left_entities": [],
            "unmatched_right_entities": [], "block_summaries": [], "warnings": []}
    de = {"version": 1, "kind": "stage_comparison_pipeline_v2_delta_explanation",
          "summary": {"deltas_total": len(deltas), "selected_total": len(expls)},
          "selection": {"strategy": "all",
                        "selected_delta_ids": [e["delta_id"] for e in expls]},
          "explanations": expls, "coverage_notes": [], "warnings": []}
    return diff, de


def _summary(diff, de):
    return {
        "version": 1, "kind": "stage_comparison_pipeline_v2_dry_run_summary",
        "status": "ok",
        "artifacts": {"summary_json": "pipeline_v2_summary.json"},
        "inputs": {}, "stages": {"entity_diff": {"deltas_total": 5}},
        "graphic_descriptor": {"left_graphic_blocks_total": 1,
                               "right_graphic_blocks_total": 1,
                               "by_readiness": {"high": 2}},
        "delta_explanation": {},
        "delta_sections": dr.build_delta_sections(diff, de),
        "warnings": [], "next_recommended_stage": "delta_explanation",
    }


_FIVE_KEYS = ["confirmed_changes", "needs_review", "weak_graphic_review",
              "likely_noise_hidden_by_default", "llm_failed_or_skipped"]

_SID = "sess1234abcd"


def _art_dir(root: Path, sid: str = _SID, pair_id=None) -> Path:
    base = root / "sessions" / sid
    if pair_id:
        base = base / "pairs" / pair_id
    d = base / "pipeline_v2"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _seed_artifacts(root: Path, *, sid: str = _SID, pair_id=None,
                    with_ready=False, with_diff=True, with_de=True):
    diff, de = _reports()
    d = _art_dir(root, sid, pair_id)
    (d / "pipeline_v2_summary.json").write_text(
        json.dumps(_summary(diff, de), ensure_ascii=False), encoding="utf-8")
    if with_diff:
        (d / "entity_diff_report.json").write_text(
            json.dumps(diff, ensure_ascii=False), encoding="utf-8")
    if with_de:
        (d / "delta_explanation_report.json").write_text(
            json.dumps(de, ensure_ascii=False), encoding="utf-8")
    if with_ready:
        payload = ui.build_pipeline_v2_ui_payload(_summary(diff, de), diff, de)
        payload["headline"]["deltas_total"] = 777  # маркер «готовый с диска»
        (d / "pipeline_v2_ui_payload.json").write_text(
            json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    return d


def _tree_snapshot(root: Path) -> set:
    return {str(p.relative_to(root)) for p in root.rglob("*")}


def _url(sid: str = _SID, pair_id=None):
    base = f"/api/stage-comparison/pipeline-v2/{sid}/ui-payload"
    return base + (f"?pair_id={pair_id}" if pair_id else "")


# ─── 1: готовый payload ──────────────────────────────────────────────────────


def test_1_returns_ready_payload(client, comparison_root):
    _seed_artifacts(comparison_root, with_ready=True)
    r = client.get(_url())
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "ok"
    assert body["available"] is True
    assert body["source"] == "ready_payload"
    assert body["payload"]["kind"] == ui.PAYLOAD_KIND
    # именно файл с диска, а не пересборка
    assert body["payload"]["headline"]["deltas_total"] == 777
    assert body["artifacts_dir"] == f"sessions/{_SID}/pipeline_v2"


# ─── 2: сборка из артефактов ─────────────────────────────────────────────────


def test_2_builds_from_artifacts_when_no_ready_payload(client, comparison_root):
    _seed_artifacts(comparison_root)
    r = client.get(_url())
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "ok"
    assert body["source"] == "built_from_artifacts"
    hl = body["payload"]["headline"]
    assert hl["deltas_total"] == 5
    assert hl["confirmed_total"] == 1
    assert hl["failed_or_skipped_total"] == 1


def test_2b_partial_when_reports_missing(client, comparison_root):
    _seed_artifacts(comparison_root, with_diff=False, with_de=False)
    r = client.get(_url())
    body = r.json()
    assert r.status_code == 200
    assert body["status"] == "partial"
    assert body["available"] is True
    # counters сохранены из summary.delta_sections
    assert body["payload"]["headline"]["confirmed_total"] == 1


# ─── 3: not_found + discovery ────────────────────────────────────────────────


def test_3_not_found_without_artifacts(client, comparison_root):
    (comparison_root / "sessions" / _SID).mkdir(parents=True)
    r = client.get(_url())
    assert r.status_code == 200
    body = r.json()
    assert body == {
        "status": "not_found", "available": False, "session_id": _SID,
        "pair_id": None, "source": None,
        "message": "Pipeline V2 artifacts not found for this session.",
        "payload": None, "warnings": [], "available_pairs": [],
    }


def test_3b_unknown_session_is_not_found_and_creates_nothing(client, comparison_root):
    before = _tree_snapshot(comparison_root)
    r = client.get(_url("nosuchsession"))
    assert r.status_code == 200
    assert r.json()["status"] == "not_found"
    assert _tree_snapshot(comparison_root) == before


def test_3c_pair_level_artifacts_and_discovery(client, comparison_root):
    _seed_artifacts(comparison_root, pair_id="p1")
    # session-level нет → not_found, но пары с артефактами перечислены
    body = client.get(_url()).json()
    assert body["status"] == "not_found"
    assert body["available_pairs"] == ["p1"]
    # pair-level отдаётся по ?pair_id=
    body2 = client.get(_url(pair_id="p1")).json()
    assert body2["status"] == "ok"
    assert body2["pair_id"] == "p1"
    assert body2["artifacts_dir"] == f"sessions/{_SID}/pairs/p1/pipeline_v2"


def test_3d_invalid_session_id_is_400(client, comparison_root):
    # literal ".." httpx нормализует в URL, поэтому шлём percent-encoded
    r = client.get("/api/stage-comparison/pipeline-v2/%2E%2E/ui-payload")
    assert r.status_code == 400
    # traversal-куски в pair_id вычищаются _safe_id: ../../etc → etc
    # (нейтрализовано, не ошибка), а полностью невалидный id → 400
    r2 = client.get(_url(pair_id="%2E%2E"))
    assert r2.status_code == 400


# ─── 4: не вызывает LLM/jobs ─────────────────────────────────────────────────


def test_4_no_network_during_discovery(comparison_root, monkeypatch):
    import socket

    _seed_artifacts(comparison_root, with_ready=True)

    def _boom(*a, **k):  # pragma: no cover
        raise AssertionError("network access attempted by readonly service")

    # сервис (вся логика endpoint'а) работает при полностью убитой сети;
    # сам TestClient через socket-patch гонять нельзя — его anyio-portal
    # использует socketpair для своих внутренних нужд
    monkeypatch.setattr(socket, "socket", _boom)
    monkeypatch.setattr(socket, "create_connection", _boom)
    assert _svc().discover_pipeline_v2_payload(_SID)["status"] == "ok"


def test_4b_service_has_no_llm_or_job_imports():
    svc = _svc()
    src = Path(svc.__file__).read_text(encoding="utf-8")
    # Запрещены КОНКРЕТНЫЕ provider/job/network модули и invocation-паттерны.
    # Голые подстроки "qwen"/"opus"/"jobs"/"queue" исключены: они дают
    # false-positive на легитимных strip-key'ах (``raw_qwen_description`` —
    # ключ, который сервис ВЫРЕЗАЕТ) и в докстрингах вида «не создаёт jobs / не
    # трогает queue». Реальную инвокацию ловят специфичные токены ниже
    # (md_enrichment_jobs / unified_analysis_jobs / pipeline_queue / graphic_llm
    # / text_llm / llm_runner / ClaudeCodeProvider / subprocess / httpx / …).
    for forbidden in ("graphic_llm", "text_llm", "llm_runner",
                      "ClaudeCodeProvider", "claude -p",
                      "import qwen", "qwen_runner", "opus_runner",
                      "md_enrichment_jobs", "unified_analysis_jobs",
                      "pipeline_queue", "subprocess",
                      "httpx", "requests", "urllib"):
        assert forbidden.lower() not in src.lower(), \
            f"service references {forbidden!r}"


def test_4c_endpoint_never_invokes_dry_run(client, comparison_root, monkeypatch):
    _seed_artifacts(comparison_root)

    def _boom(*a, **k):  # pragma: no cover
        raise AssertionError("endpoint must not run pipeline v2")

    monkeypatch.setattr(dr, "run_pipeline_v2_dry_run", _boom)
    assert client.get(_url()).json()["status"] == "ok"


# ─── 5: read-only ────────────────────────────────────────────────────────────


def test_5_endpoint_creates_no_files(client, comparison_root):
    _seed_artifacts(comparison_root)          # есть артефакты → build path
    _seed_artifacts(comparison_root, pair_id="p1", with_ready=True)
    before = _tree_snapshot(comparison_root)
    for url in (_url(), _url(pair_id="p1"), _url("nosuchsession"),
                _url(pair_id="nopair")):
        assert client.get(url).status_code == 200
    assert _tree_snapshot(comparison_root) == before


# ─── 6: битые артефакты → fail-soft, не 500 ──────────────────────────────────


def test_6_broken_summary_gives_error_json_not_500(client, comparison_root):
    d = _art_dir(comparison_root)
    (d / "pipeline_v2_summary.json").write_text("{broken json", encoding="utf-8")
    r = client.get(_url())
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "error"
    assert body["available"] is False
    assert any("pipeline_v2_summary.json" in w for w in body["warnings"])


def test_6b_broken_ready_payload_falls_back_to_build(client, comparison_root):
    _seed_artifacts(comparison_root)
    d = _art_dir(comparison_root)
    (d / "pipeline_v2_ui_payload.json").write_text("{broken", encoding="utf-8")
    body = client.get(_url()).json()
    assert body["status"] == "partial"          # warning о битом ready-файле
    assert body["source"] == "built_from_artifacts"
    assert any("pipeline_v2_ui_payload.json" in w for w in body["warnings"])


def test_6c_ready_payload_with_wrong_kind_is_rebuilt(client, comparison_root):
    _seed_artifacts(comparison_root)
    d = _art_dir(comparison_root)
    (d / "pipeline_v2_ui_payload.json").write_text(
        json.dumps({"kind": "something_else"}), encoding="utf-8")
    body = client.get(_url()).json()
    assert body["source"] == "built_from_artifacts"
    assert any("unexpected kind" in w for w in body["warnings"])


def test_6d_broken_optional_report_degrades_to_partial(client, comparison_root):
    _seed_artifacts(comparison_root)
    d = _art_dir(comparison_root)
    (d / "entity_diff_report.json").write_text("[not a dict", encoding="utf-8")
    body = client.get(_url()).json()
    assert body["status"] == "partial"
    assert body["available"] is True
    assert any("entity_diff_report.json" in w for w in body["warnings"])


# ─── 7: контракт секций сохраняется ──────────────────────────────────────────


def test_7_sections_contract_preserved(client, comparison_root):
    _seed_artifacts(comparison_root)
    payload = client.get(_url()).json()["payload"]
    assert [s["key"] for s in payload["sections"]] == _FIVE_KEYS
    by_key = {s["key"]: s for s in payload["sections"]}
    assert by_key["confirmed_changes"]["default_visible"] is True
    assert by_key["weak_graphic_review"]["display_hint"] == "warning"
    assert by_key["likely_noise_hidden_by_default"]["default_visible"] is False
    assert by_key["llm_failed_or_skipped"]["show_in_diagnostics"] is True
    counts = {k: by_key[k]["count"] for k in _FIVE_KEYS}
    assert counts == {"confirmed_changes": 1, "needs_review": 1,
                      "weak_graphic_review": 1,
                      "likely_noise_hidden_by_default": 1,
                      "llm_failed_or_skipped": 1}


# ─── сервис напрямую (unit) ──────────────────────────────────────────────────


def test_service_resolves_dirs_without_mkdir(comparison_root):
    svc = _svc()
    p = svc.pipeline_v2_artifacts_dir("freshsid")
    assert not p.exists()
    assert not (comparison_root / "sessions" / "freshsid").exists()
    p2 = svc.pipeline_v2_artifacts_dir("freshsid", "freshpair")
    assert not p2.exists()
    # root-уровень тоже не материализуется: ни sessions/, ни .gitkeep
    assert not (comparison_root / "sessions").exists()
    assert not (comparison_root / ".gitkeep").exists()


def test_service_does_not_create_nonexistent_root(tmp_path, monkeypatch):
    """GET к свежему COMPARISON_ROOT не должен материализовать дерево."""
    ghost = tmp_path / "ghost_comparison_root"      # НЕ создаём
    monkeypatch.setenv("COMPARISON_ROOT", str(ghost))
    svc = _svc()
    out = svc.discover_pipeline_v2_payload("somesid")
    assert out["status"] == "not_found"
    assert not ghost.exists()


def test_service_invalid_ids_raise_value_error(comparison_root):
    svc = _svc()
    with pytest.raises(ValueError):
        svc.discover_pipeline_v2_payload("..")
    with pytest.raises(ValueError):
        svc.discover_pipeline_v2_payload(_SID, "###")
    # traversal-куски НЕ дают выход из sessions-дерева: ../../etc → etc
    p = svc.pipeline_v2_artifacts_dir(_SID, "../../etc")
    assert "sessions" in p.parts and ".." not in str(p)


# ─── kill-тесты по адверсариальному ревью ────────────────────────────────────


@pytest.mark.parametrize("with_diff,with_de", [(False, True), (True, False)])
def test_2c_partial_when_single_report_missing(client, comparison_root,
                                               with_diff, with_de):
    _seed_artifacts(comparison_root, with_diff=with_diff, with_de=with_de)
    body = client.get(_url()).json()
    assert body["status"] == "partial"
    assert body["available"] is True


def test_3e_available_pairs_only_with_artifacts(client, comparison_root):
    """Discovery перечисляет ТОЛЬКО пары с реальными артефактами."""
    sessions = comparison_root / "sessions" / _SID / "pairs"
    # p_art: только summary; p_readyonly: только готовый payload
    _seed_artifacts(comparison_root, pair_id="p_art",
                    with_diff=False, with_de=False)
    d = _art_dir(comparison_root, pair_id="p_readyonly")
    (d / "pipeline_v2_ui_payload.json").write_text("{}", encoding="utf-8")
    # p_emptydir: пустой pipeline_v2/; p_nodir: пара без pipeline_v2/
    (sessions / "p_emptydir" / "pipeline_v2").mkdir(parents=True)
    (sessions / "p_nodir").mkdir(parents=True)
    (sessions / "p_nodir" / "pair.json").write_text("{}", encoding="utf-8")
    body = client.get(_url()).json()
    assert body["status"] == "not_found"
    assert body["available_pairs"] == ["p_art", "p_readyonly"]


def test_3f_pair_level_not_found_includes_available_pairs(client, comparison_root):
    _seed_artifacts(comparison_root, pair_id="p1")
    body = client.get(_url(pair_id="nopair")).json()
    assert body["status"] == "not_found"
    assert body["pair_id"] == "nopair"
    assert body["available_pairs"] == ["p1"]


@pytest.mark.parametrize("fname", [
    "entity_diff_report.json", "delta_explanation_report.json",
    "left_graphic_descriptor_report.json",
    "right_graphic_descriptor_report.json",
])
def test_6e_any_broken_optional_report_warns(client, comparison_root, fname):
    _seed_artifacts(comparison_root)
    d = _art_dir(comparison_root)
    (d / fname).write_text("{broken", encoding="utf-8")
    body = client.get(_url()).json()
    assert body["status"] == "partial"
    assert any(fname in w for w in body["warnings"])


def test_6f_builder_exception_is_failsoft_error(client, comparison_root,
                                                monkeypatch):
    _seed_artifacts(comparison_root, with_diff=False, with_de=False)
    svc = _svc()

    def _boom(*a, **k):
        raise RuntimeError("builder exploded")

    monkeypatch.setattr(svc, "build_pipeline_v2_ui_payload", _boom)
    r = client.get(_url())
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "error"
    assert body["available"] is False
    assert any("RuntimeError" in w for w in body["warnings"])


def test_6g_summary_valid_json_but_not_object_is_error(client, comparison_root):
    d = _art_dir(comparison_root)
    (d / "pipeline_v2_summary.json").write_text("[1, 2, 3]", encoding="utf-8")
    body = client.get(_url()).json()
    assert body["status"] == "error"
    assert any("expected JSON object" in w for w in body["warnings"])


def test_6h_ready_payload_non_object_falls_back(client, comparison_root):
    _seed_artifacts(comparison_root)
    d = _art_dir(comparison_root)
    (d / "pipeline_v2_ui_payload.json").write_text('"oops"', encoding="utf-8")
    body = client.get(_url()).json()
    assert body["source"] == "built_from_artifacts"
    assert any("expected JSON object" in w for w in body["warnings"])


def test_6i_nan_in_artifacts_sanitized_not_500(client, comparison_root):
    """NaN/Infinity из artifact-JSON не валят сериализацию ответа."""
    _seed_artifacts(comparison_root)
    d = _art_dir(comparison_root)
    diff = json.loads((d / "entity_diff_report.json").read_text(encoding="utf-8"))
    diff["deltas"][0]["confidence"] = float("nan")
    (d / "entity_diff_report.json").write_text(
        json.dumps(diff, ensure_ascii=False, allow_nan=True), encoding="utf-8")
    r = client.get(_url())
    assert r.status_code == 200
    body = r.json()
    assert body["available"] is True
    assert any("non-finite" in w for w in body["warnings"])
    # ответ — строгий JSON (повторная сериализация без allow_nan не падает)
    json.dumps(body, allow_nan=False)


def test_endpoint_runs_discover_off_event_loop(client, comparison_root,
                                               monkeypatch):
    """run_in_threadpool обязателен: sync-диск в event loop = watchdog-риск."""
    import asyncio
    from backend.app.api.routers import stage_comparison as router_mod

    seen = []

    def _fake(session_id, pair_id=None):
        try:
            asyncio.get_running_loop()
            seen.append(True)        # выполняется В event loop — плохо
        except RuntimeError:
            seen.append(False)       # threadpool worker — правильно
        return {"status": "not_found", "available": False}

    monkeypatch.setattr(router_mod.pipeline_v2_payload_mod,
                        "discover_pipeline_v2_payload", _fake)
    assert client.get(_url()).status_code == 200
    assert seen == [False]
