"""Production comparison catalog: generic discovery of accepted, frozen results.

Zero model calls: live results are produced by the REAL V3 engine through the
production run endpoint with FakeProvider; sealed snapshots are synthetic and
bound to the real pair only by sha256 of both source PDFs.
"""
from __future__ import annotations

import hashlib
import json
import os
import shutil
from pathlib import Path

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.tests.project_change_v3 import generic_fixture as gf

REPO = Path(__file__).resolve().parents[3]
HM_FIXTURES = REPO / "backend/app/data/human_mapping_fixtures"
SECOND_PAIR = "generic_pair_second_run"
SEALED_PAIR = "sealedpairkey0000000000"
CORPUS = Path("/home/coder/auditmanager/corpus-audits")


def sha(path) -> str:
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def _add_pair(env, pair_id: str) -> Path:
    """A second comparison pair of the SAME session over the SAME two documents."""
    from backend.app.services.stage_comparison import paths as paths_mod

    source = json.loads(paths_mod.pair_json_path(env["session_id"], gf.PAIR_ID).read_text(encoding="utf-8"))
    target = paths_mod.pair_json_path(env["session_id"], pair_id)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps({**source, "id": pair_id}, ensure_ascii=False), encoding="utf-8")
    meta_path = paths_mod.session_json_path(env["session_id"])
    meta = json.loads(meta_path.read_text(encoding="utf-8"))
    meta["pair_order"] = [*meta.get("pair_order", []), pair_id]
    meta_path.write_text(json.dumps(meta, ensure_ascii=False), encoding="utf-8")
    return paths_mod.production_dir(env["session_id"], pair_id)


def _run(env, pair_id: str = gf.PAIR_ID) -> dict:
    from backend.app.services.project_change_v3.provider import FakeProvider, set_test_provider

    set_test_provider(FakeProvider(handlers=gf.fake_handlers(pair_id)))
    url = f"/api/stage-comparison/sessions/{env['session_id']}/pairs/{pair_id}/production/run"
    response = env["client"].post(url, json={"input_mode": "DOCUMENT"})
    assert response.status_code == 200, response.text
    state = response.json()
    assert state["status"] in {"COMPLETED", "REVIEW"} and state["model_calls"] == 0
    return state


def _registry(tmp_path, snapshots=()) -> Path:
    path = tmp_path / "catalog_sources.json"
    path.write_text(json.dumps({"schema": "project-comparison-catalog-sources/1",
                                "sealed_snapshots": list(snapshots)}), encoding="utf-8")
    return path


@pytest.fixture
def env(tmp_path, monkeypatch):
    comparison_root = tmp_path / "comparison"
    comparison_root.mkdir()
    monkeypatch.setenv("COMPARISON_ROOT", str(comparison_root))
    monkeypatch.setenv("PROJECT_COMPARISON_ENGINE", "v3")
    monkeypatch.setenv("PROJECT_COMPARISON_V3_ALLOW_INFERENCE", "0")
    monkeypatch.setenv("PROJECT_COMPARISON_V3_FORCE_UNAVAILABLE", "1")
    built = gf.build_comparison(tmp_path)
    from backend.app.services.project_change_catalog import catalog
    from backend.app.services.project_change_v3 import scope
    from backend.app.services.project_change_v3.provider import reset_test_provider

    monkeypatch.setattr(scope, "_object_stage_paths",
                        lambda: {gf.OBJECT_ID: (built["stage_1"], built["stage_2"])})
    monkeypatch.setattr(catalog, "_objects", lambda: {gf.OBJECT_ID: "Тестовый объект"})
    registry = _registry(tmp_path)
    monkeypatch.setattr(catalog, "SOURCES_REGISTRY", registry)
    monkeypatch.setattr(catalog, "APP_DATA", tmp_path / "data")
    catalog.clear_cache()
    from backend.app.api.routers import project_comparison_catalog, stage_comparison

    app = FastAPI()
    app.include_router(stage_comparison.router)
    app.include_router(project_comparison_catalog.router)
    try:
        yield {**built, "client": TestClient(app), "root": comparison_root, "tmp": tmp_path,
               "catalog": catalog, "monkeypatch": monkeypatch}
    finally:
        reset_test_provider()
        catalog.clear_cache()


def _build(env, **kwargs):
    return env["catalog"].build_catalog(include_diagnostics=True, **kwargs)


# ─── live V3 result discovery ────────────────────────────────────────────────

def test_live_v3_result_is_discovered_generically(env):
    state = _run(env)
    catalog = _build(env)
    [entry] = catalog["entries"]
    result = json.loads((env["root"] / "sessions" / env["session_id"] / "pairs" / gf.PAIR_ID / "production"
                         / "project_change_v3_result.json").read_text(encoding="utf-8"))
    assert entry["result_source"] == "LIVE_RUN" and entry["result_status"] == "COMPLETED_FROZEN"
    assert (entry["object_id"], entry["session_id"], entry["pair_id"], entry["run_id"]) == (
        gf.OBJECT_ID, env["session_id"], gf.PAIR_ID, state["run_id"])
    # Counts come from the persisted result and the published mapping.
    assert entry["counts"]["projectchanges"] == len(result["projectchanges"]) == 1
    assert entry["counts"]["unresolved_hints"] == len(result["unresolved_hints"]) == 1
    assert entry["counts"]["semantic_regions"] == 2
    assert entry["human_mapping"] == {"available": True, "regions": 2, "data_source": "PUBLISHED_BY_RUN",
                                      "url": f"/human-mapping/?object={gf.OBJECT_ID}&comparison={gf.PAIR_ID}",
                                      "reason": None}
    # Model provenance is the recorded one.
    assert entry["engine"]["model"] == result["provenance"]["model"]
    assert entry["engine"]["engine_version"] == result["provenance"]["engine_version"]
    # Source identity: sha256 of the pair's real PDFs.
    binding = entry["provenance"]["source_binding"]
    assert binding["old_sha256"] == entry["documents"]["old"]["pdf_sha256"] == sha(env["left"]["pdf_path"])
    assert binding["new_sha256"] == entry["documents"]["new"]["pdf_sha256"] == sha(env["right"]["pdf_path"])
    assert entry["open"] == {
        "available": True, "reason": None, "object_id": gf.OBJECT_ID, "session_id": env["session_id"],
        "pair_id": gf.PAIR_ID, "source_run_id": state["run_id"], "tab": "diffs",
        "presentation_api": f"/api/stage-comparison/objects/{gf.OBJECT_ID}/project-changes",
        "pair_changes_api": f"/api/stage-comparison/sessions/{env['session_id']}/pairs/{gf.PAIR_ID}/production/changes"}
    assert entry["is_primary"] and entry["primary_rule"] == "SINGLE_RESULT"
    assert catalog["model_calls"] == 0 and catalog["diagnostics"] == []


def test_catalog_is_deterministic_and_read_only(env):
    _run(env)
    before = {str(p): sha(p) for p in sorted(env["root"].rglob("*")) if p.is_file()}
    first, second = _build(env), _build(env)
    assert first == second and first["catalog_revision"] == second["catalog_revision"]
    after = {str(p): sha(p) for p in sorted(env["root"].rglob("*")) if p.is_file()}
    assert before == after


def test_model_display_uses_result_provenance_not_prompt_text(env):
    from backend.app.services.project_change_catalog.catalog import model_display

    assert model_display("claude-opus-5") == "Claude Opus 5"
    assert model_display("gpt-6-astra") == "GPT-6 Astra"
    assert model_display("") == ""
    _run(env)
    path = env["root"] / "sessions" / env["session_id"] / "pairs" / gf.PAIR_ID / "production" / "project_change_v3_result.json"
    result = json.loads(path.read_text(encoding="utf-8"))
    stat = path.stat()
    result["provenance"].update(model="claude-opus-5", reasoning="xhigh",
                                engine_variant="ProjectChange V3 / Opus (prompts mention GPT-6 Astra)")
    path.write_text(json.dumps(result, ensure_ascii=False), encoding="utf-8")
    os.utime(path, ns=(stat.st_atime_ns, stat.st_mtime_ns))
    [entry] = _build(env)["entries"]
    assert entry["engine"]["model"] == "claude-opus-5"
    assert entry["engine"]["model_display"] == "Claude Opus 5" and entry["engine"]["reasoning"] == "xhigh"


# ─── failed / partial exclusion ─────────────────────────────────────────────

def _write_state(production: Path, **state) -> None:
    production.mkdir(parents=True, exist_ok=True)
    (production / "state.json").write_text(json.dumps({
        "kind": "stage_comparison_production_state", "schema_version": 1, "version": 1,
        "engine": "projectchange_v3", "engine_version": "3.5.2",
        "provenance": {"engine": "projectchange_v3", "model": "gpt-6-astra", "reasoning": "xhigh"}, **state,
    }, ensure_ascii=False), encoding="utf-8")


def test_failed_cancelled_and_partial_astra_like_runs_are_not_catalog_results(env):
    _run(env)
    good = _build(env)["entries"][0]["catalog_entry_id"]
    # Production-shaped failures of an Astra run (transport, cancel, partial Mapper/Miner, no Dedupe).
    cases = {
        "astra_transport_failed": dict(status="FAILED", reason_code="v3_transport_failed", run_id="a" * 32),
        "astra_failed": dict(status="FAILED", reason_code="v3_miner_validation_failed", run_id="b" * 32),
        "astra_cancelled": dict(status="CANCELLED", reason_code="v3_cancelled", run_id="c" * 32),
        "astra_partial_running": dict(status="RUNNING", reason_code="v3_running", run_id="d" * 32,
                                      current_stage="MINING", message="V3: поиск изменений, регион 1 из 15"),
    }
    for pair_id, state in cases.items():
        production = _add_pair(env, pair_id)
        _write_state(production, pair_id=pair_id, session_id=env["session_id"], **state)
        # Partial checkpoint artifacts next to the state: a Mapper map, a Miner checkpoint, a frozen=false marker.
        work = production / "project_change_v3"
        (work / "miner_checkpoints").mkdir(parents=True)
        (work / "miner_checkpoints" / f"project_change_v3_miner_checkpoint_{state['run_id']}.json").write_text(
            json.dumps({"run_id": state["run_id"], "accepted_regions": ["A-R001"]}), encoding="utf-8")
        (production / "project_change_v3_semantic_map.json").write_text(
            json.dumps({"pair": pair_id, "regions": [{"region_id": "A-R001"}]}), encoding="utf-8")
        (production / "ASTRA_RESULT_FREEZE.json").write_text(
            json.dumps({"ASTRA_DEV5_RESULT_FROZEN": False, "final_result_sha256": None}), encoding="utf-8")
    # A terminal state whose result belongs to ANOTHER run is not a result either.
    production = _add_pair(env, "astra_result_of_other_run")
    _write_state(production, status="REVIEW", reason_code="v3_completed", run_id="e" * 32)
    other = json.loads((env["root"] / "sessions" / env["session_id"] / "pairs" / gf.PAIR_ID / "production"
                        / "project_change_v3_result.json").read_text(encoding="utf-8"))
    (production / "project_change_v3_result.json").write_text(
        json.dumps({**other, "pair_id": "astra_result_of_other_run", "run_id": "f" * 32}), encoding="utf-8")

    catalog = _build(env)
    assert [e["catalog_entry_id"] for e in catalog["entries"]] == [good]
    diagnostics = {d["pair_id"]: d for d in catalog["diagnostics"]}
    assert diagnostics["astra_transport_failed"]["label"] == "TRANSPORT_FAILED"
    assert diagnostics["astra_failed"]["label"] == "FAILED"
    assert diagnostics["astra_cancelled"]["label"] == "CANCELLED"
    assert diagnostics["astra_partial_running"]["label"] == "PARTIAL"
    assert diagnostics["astra_result_of_other_run"]["reason"] == "RUN_NOT_PUBLISHED:REVIEW"
    for d in catalog["diagnostics"]:
        # Diagnostics never carry cards or result counts.
        assert not {"counts", "projectchanges", "items", "entries"} & set(d)
    assert catalog["diagnostics"][0]["model_display"] == "GPT-6 Astra"
    # Diagnostics are served only on request.
    assert "diagnostics" not in env["catalog"].build_catalog()


@pytest.mark.skipif(not CORPUS.is_dir(), reason="research corpus not on this host")
def test_real_incomplete_astra_dev5_artifacts_are_never_completed_results(env):
    """The REAL sealed Astra DEV5 attempts (FROZEN=false), placed where a production run lives."""
    _run(env)
    good = _build(env)["entries"][0]["catalog_entry_id"]
    freezes = sorted(CORPUS.glob("2026092*_dev5_astra_*/ASTRA_RESULT_FREEZE.json"))
    assert freezes, "expected sealed Astra DEV5 attempts"
    for index, freeze in enumerate(freezes):
        record = json.loads(freeze.read_text(encoding="utf-8"))
        assert record["ASTRA_DEV5_RESULT_FROZEN"] is False
        production = _add_pair(env, f"real_astra_attempt_{index}")
        production.mkdir(parents=True, exist_ok=True)
        shutil.copy(freeze, production / "ASTRA_RESULT_FREEZE.json")
        research_state = freeze.parent / "state.json"
        if research_state.is_file():
            shutil.copy(research_state, production / "state.json")
        for checkpoint in (freeze.parent / "artifacts").glob("miner_checkpoints/*.json"):
            (production / "project_change_v3" / "miner_checkpoints").mkdir(parents=True, exist_ok=True)
            shutil.copy(checkpoint, production / "project_change_v3" / "miner_checkpoints" / checkpoint.name)
        semantic = freeze.parent / "artifacts" / "project_change_v3_semantic_map.json"
        if semantic.is_file():
            shutil.copy(semantic, production / "project_change_v3_semantic_map.json")
    catalog = _build(env)
    assert [e["catalog_entry_id"] for e in catalog["entries"]] == [good]
    assert not any("astra" in json.dumps(e).lower() for e in catalog["entries"])


# ─── multiple runs per pair ─────────────────────────────────────────────────

def test_multiple_frozen_runs_of_the_same_pair_do_not_collide(env):
    first = _run(env)
    _add_pair(env, SECOND_PAIR)
    second = _run(env, SECOND_PAIR)
    catalog = _build(env)
    entries = catalog["entries"]
    assert len(entries) == 2 and len({e["catalog_entry_id"] for e in entries}) == 2
    assert {e["run_id"] for e in entries} == {first["run_id"], second["run_id"]}
    assert len({e["pair_group_key"] for e in entries}) == 1
    [group] = catalog["groups"]
    assert sorted(group["entry_ids"]) == sorted(e["catalog_entry_id"] for e in entries)
    primary = [e for e in entries if e["is_primary"]]
    assert len(primary) == 1 and primary[0]["catalog_entry_id"] == group["primary_entry_id"]
    # Deterministic product rule (no quality ranking): the most recently frozen run.
    assert primary[0]["run_id"] == second["run_id"] and primary[0]["primary_rule"] == "LATEST_FROZEN"
    assert all(e["variants_in_pair"] == 2 for e in entries)


# ─── sealed snapshot discovery / source hash binding ────────────────────────

def _snapshot(env, *, old_sha: str, new_sha: str, cards: int = 3, default: bool = False, engine_model="gpt-6-astra"):
    from backend.app.services.project_change_preview.service import SNAPSHOT_OBJECT

    data = env["tmp"] / "data"
    root = data / "pcsnap_test"
    (root / "documents").mkdir(parents=True, exist_ok=True)
    prefix = f"/api/project-change-preview/objects/{SNAPSHOT_OBJECT}/evidence/"
    items = [{"id": f"pcv3_{i}", "status": "REVIEW", "effective_decision": None, "cipher": "Тест",
              "source_run_id": "projectchange_v3_production_snapshot", "summary_ru": f"Изменение {i}",
              "evidence": [{"id": f"pev_{i}", "pair_id": SEALED_PAIR, "side": side, "page": 1,
                            "image_url": prefix + f"pev_{i}/crop",
                            "document": {"id": code, "label": code, "pdf_path": "x", "version": "v001"}}
                           for side, code in (("OLD", "GEN-OV-OLD"), ("NEW", "GEN-OV-NEW"))]}
             for i in range(cards)]
    files = {
        "presentation.json": {
            "envelope": {"schema_version": "project-change-view/1", "object_id": SNAPSHOT_OBJECT,
                         "origin": "PRODUCTION", "mode": "PREVIEW", "decision_revision": 0,
                         "source_run_id": "projectchange_v3_production_snapshot", "items": items,
                         "capabilities": {"decisions": False, "history": False}, "summary": {}},
            "pairs": {SEALED_PAIR: {"pair": {"id": SEALED_PAIR,
                                             "left": {"document_code": "GEN-OV-OLD"},
                                             "right": {"document_code": "GEN-OV-NEW"}}}},
            "documents": {f"{SEALED_PAIR}:old": {"file": "documents/o.pdf", "source_sha256": old_sha, "embargo_pages": []},
                          f"{SEALED_PAIR}:new": {"file": "documents/n.pdf", "source_sha256": new_sha, "embargo_pages": []}},
            "evidence": {}},
        "source-manifest.json": {"engine": {"engine": "projectchange_v3", "engine_version": "3.0.0",
                                            "model": engine_model, "reasoning": "xhigh",
                                            "mapper_prompt_version": "m" * 64}},
        "source-receipts.json": {"finals": {"A": {"pair_key": SEALED_PAIR, "sha256": "ab" * 32, "count": cards}}},
    }
    receipts = {}
    for name, value in files.items():
        (root / name).write_text(json.dumps(value, ensure_ascii=False), encoding="utf-8")
        receipts[name] = sha(root / name)
    (root / "MANIFEST.json").write_text(json.dumps({
        "schema": "project-change-production-snapshot/1", "object_id": SNAPSHOT_OBJECT,
        "decision_mode": "READ_ONLY", "auto_refresh": False,
        "source_run_id": "projectchange_v3_production_snapshot", "files": receipts}), encoding="utf-8")
    from backend.app.services.project_change_preview import service as preview_service

    env["monkeypatch"].setattr(preview_service, "OBJECT", gf.OBJECT_ID)
    spec = {"snapshot_dir": "pcsnap_test", "accepted_at": "2026-09-19T09:10:56+03:00",
            "accepted_at_source": "test", **({"default": True} if default else {})}
    registry = _registry(env["tmp"], [spec])
    return registry, data


def _hm_identity(env, old_sha, new_sha):
    from backend.app.services.human_mapping_production import fixture_binding

    fixtures = env["tmp"] / "hm" / "human_mapping_fixtures"
    fixtures.mkdir(parents=True)
    shutil.copy(HM_FIXTURES / "UI_DATA_PAIR_A.json", fixtures / "UI_DATA_PAIR_A.json")
    record = {"schema": "human-mapping-fixture-source-identity/1", "fixtures_dir": fixtures.name, "fixtures": [{
        "fixture_alias": "A", "pair_key": SEALED_PAIR, "label": "Тест", "ui_data": "UI_DATA_PAIR_A.json",
        "ui_data_sha256": sha(fixtures / "UI_DATA_PAIR_A.json"), "regions": 999,
        "old": {"pdf_sha256": old_sha}, "new": {"pdf_sha256": new_sha}}]}
    path = fixtures.parent / "human_mapping_fixture_sources.json"
    path.write_text(json.dumps(record), encoding="utf-8")
    env["monkeypatch"].setattr(fixture_binding, "IDENTITY", path)
    fixture_binding.clear_cache()


def test_sealed_snapshot_binds_to_real_pair_by_source_pdf_sha256(env):
    old, new = sha(env["left"]["pdf_path"]), sha(env["right"]["pdf_path"])
    registry, data = _snapshot(env, old_sha=old, new_sha=new, cards=3)
    _hm_identity(env, old, new)
    catalog = _build(env, registry=registry, data_root=data)
    [entry] = catalog["entries"]
    assert entry["result_source"] == "SEALED_SNAPSHOT" and entry["result_status"] == "COMPLETED_FROZEN"
    assert entry["counts"]["projectchanges"] == 3 and entry["counts"]["unresolved_hints"] is None
    # Region count is read from the mapping data (25), never from the identity record (999).
    assert entry["human_mapping"]["available"] and entry["human_mapping"]["regions"] == 25
    assert entry["counts"]["semantic_regions"] == 25
    assert entry["engine"]["model_display"] == "GPT-6 Astra" and entry["engine"]["engine_version"] == "3.0.0"
    assert (entry["session_id"], entry["pair_id"]) == (env["session_id"], gf.PAIR_ID)
    assert entry["provenance"]["source_binding"]["old_sha256"] == old
    assert entry["provenance"]["snapshot"]["sealed_pair_key"] == SEALED_PAIR
    assert entry["open"]["available"] and entry["open"]["source_run_id"] == "projectchange_v3_production_snapshot"
    assert entry["frozen_at"] == "2026-09-19T06:10:56+00:00"
    assert entry["title"] == "GEN-OV-NEW"


def test_same_file_names_and_codes_with_different_bytes_never_bind(env):
    old = sha(env["left"]["pdf_path"])
    registry, data = _snapshot(env, old_sha=old, new_sha="0" * 64)
    catalog = _build(env, registry=registry, data_root=data)
    assert catalog["entries"] == []
    assert [d["reason"] for d in catalog["diagnostics"]] == ["NO_REAL_PAIR_WITH_IDENTICAL_SOURCE_PDFS"]


def test_live_result_whose_source_pdf_changed_is_not_a_catalog_result(env):
    _run(env)
    right = Path(env["right"]["pdf_path"])
    stat = right.stat()
    right.write_bytes(right.read_bytes() + b"\n% changed after the run\n")
    os.utime(right, ns=(stat.st_atime_ns, stat.st_mtime_ns))  # same name, same mtime, different bytes
    catalog = _build(env)
    assert catalog["entries"] == []
    assert catalog["diagnostics"][0]["reason"] == "SOURCE_PDF_CHANGED_AFTER_RUN"


def test_snapshot_and_live_run_on_the_same_pair_are_separate_variants(env):
    state = _run(env)
    old, new = sha(env["left"]["pdf_path"]), sha(env["right"]["pdf_path"])
    registry, data = _snapshot(env, old_sha=old, new_sha=new)
    catalog = _build(env, registry=registry, data_root=data)
    by_source = {e["result_source"]: e for e in catalog["entries"]}
    assert set(by_source) == {"LIVE_RUN", "SEALED_SNAPSHOT"}
    assert by_source["LIVE_RUN"]["pair_group_key"] == by_source["SEALED_SNAPSHOT"]["pair_group_key"]
    # The pair's change list shows the live run; the snapshot is kept, not overwritten.
    assert by_source["SEALED_SNAPSHOT"]["open"] == {**by_source["SEALED_SNAPSHOT"]["open"],
                                                    "available": False, "reason": "PAIR_VIEW_SHOWS_LIVE_RUN"}
    assert by_source["SEALED_SNAPSHOT"]["human_mapping"]["reason"] == "PAIR_HM_BELONGS_TO_OTHER_RUN"
    assert by_source["LIVE_RUN"]["is_primary"] and by_source["LIVE_RUN"]["primary_rule"] == "SHOWN_BY_PAIR_VIEW"
    assert by_source["LIVE_RUN"]["run_id"] == state["run_id"]
    # Explicit accepted metadata overrides the product rule.
    registry, data = _snapshot(env, old_sha=old, new_sha=new, default=True)
    env["catalog"].clear_cache()
    catalog = _build(env, registry=registry, data_root=data)
    snapshot = next(e for e in catalog["entries"] if e["result_source"] == "SEALED_SNAPSHOT")
    assert snapshot["is_primary"] and snapshot["primary_rule"] == "EXPLICIT_DEFAULT"


# ─── truth leakage / API / UTF-8 ────────────────────────────────────────────

def test_evaluation_markers_never_reach_a_catalog_entry(env):
    from backend.app.services.project_change_catalog import catalog as mod

    _run(env)
    catalog = _build(env)
    raw = json.dumps(catalog, ensure_ascii=False)
    assert not mod.TRUTH_TOKENS.search(raw) and not mod.TRUTH_KEYS.search(raw)
    # A result carrying evaluation vocabulary is rejected into diagnostics.
    old, new = sha(env["left"]["pdf_path"]), sha(env["right"]["pdf_path"])
    registry, data = _snapshot(env, old_sha=old, new_sha=new, engine_model="gpt-6-astra-SUPPORTED")
    env["catalog"].clear_cache()
    catalog = _build(env, registry=registry, data_root=data)
    assert all(e["result_source"] == "LIVE_RUN" for e in catalog["entries"])
    assert any(d["reason"].startswith("EVALUATION_MARKER_PRESENT") for d in catalog["diagnostics"])


def test_api_schema_and_utf8(env):
    _run(env)
    client = env["client"]
    response = client.get("/api/project-comparison/catalog")
    assert response.status_code == 200 and response.headers["content-type"].startswith("application/json")
    body = response.json()
    assert body["schema"] == "project-comparison-catalog/1" and body["model_calls"] == 0
    assert "diagnostics" not in body
    [entry] = body["entries"]
    required = {"schema", "catalog_entry_id", "result_source", "result_status", "object_id", "object_name",
                "session_id", "pair_id", "run_id", "pair_group_key", "title", "documents", "engine", "frozen_at",
                "frozen_at_source", "counts", "human_mapping", "open", "provenance", "is_primary",
                "primary_rule", "variants_in_pair"}
    assert required <= set(entry)
    assert {"label", "engine", "engine_version", "provider", "model", "model_display", "reasoning"} <= set(entry["engine"])
    assert entry["object_name"] == "Тестовый объект" and "?" not in entry["object_name"]
    assert "Тестовый объект".encode("utf-8") in response.content
    assert client.get("/api/project-comparison/catalog?diagnostics=true").json()["diagnostics"] == []
    one = client.get(f"/api/project-comparison/catalog/entries/{entry['catalog_entry_id']}")
    assert one.status_code == 200 and one.json() == entry
    assert client.get("/api/project-comparison/catalog/entries/pcc1_" + "0" * 32).status_code == 404
    assert client.get("/api/project-comparison/catalog/entries/../../etc").status_code in {400, 404}
    assert client.get("/api/project-comparison/catalog/entries/not-an-id").status_code == 400


def test_catalog_code_does_not_hardcode_results():
    source = (REPO / "backend/app/services/project_change_catalog/catalog.py").read_text(encoding="utf-8")
    ui = (REPO / "frontend/static/js/project-comparison-catalog.js").read_text(encoding="utf-8")
    for text in (source, ui):
        for token in ("АР1", "ИОС4.2", "ИОС2.1", "p290a06df79", "p11ad4a09d9", "p7b37b4e31b",
                      "a631b49aaaac4db0af66a495c155c629", "ad0a31a342a666082f2ef66a", "corpus-audits"):
            assert token not in text
