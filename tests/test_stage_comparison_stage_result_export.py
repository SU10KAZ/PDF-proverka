from __future__ import annotations

import json

import pytest

from backend.app.services.stage_comparison import production_orchestrator as orchestrator


def _install_artifacts(monkeypatch):
    state = {
        "run_id": "run-42",
        "status": "PARTIAL",
        "input_signature": "sha256:generation",
        "analysis_config": {"ai_mode": "FAST"},
        "selection": {"input_mode": "DOCUMENT", "ai_mode": "FAST"},
        "stages": {
            key: {"status": "COMPLETED", "reason_code": f"reason-{key}"}
            for keys in orchestrator.PRODUCTION_STAGE_RESULT_STATE_KEYS.values()
            for key in keys
        },
        "sheet_suggestions": {"suggestions": [{"left_page": 1, "right_page": 2}]},
    }
    artifacts = {
        name: {
            "kind": f"kind-{name}",
            "schema_version": "v1",
            "input_signature": f"sha256:{name}",
            "evidence": {"path": "/home/coder/private/source.pdf"},
            "provenance": {"sources": [name]},
            "reason_code": f"reason-{name}",
            "diagnostic_message": "Authorization: Bearer secret-token-value",
            "path_map": {"/var/lib/audit/private-item": {"available": True}},
            "api_key": "must-not-leak",
            "image_base64": "must-not-leak-binary",
        }
        for names in orchestrator.PRODUCTION_STAGE_RESULT_ARTIFACTS.values()
        for name in names
    }
    artifacts["state"] = state

    monkeypatch.setattr(
        orchestrator.store,
        "get_pair_for_production",
        lambda *_: {
            "id": "pair-1",
            "left": {"pdf_path": "/srv/docs/left.pdf"},
            "right": {"pdf_path": "/srv/docs/right.pdf"},
        },
    )
    monkeypatch.setattr(
        orchestrator.production_store,
        "load_artifact",
        lambda _session, _pair, name: artifacts.get(name),
    )
    monkeypatch.setattr(
        orchestrator.production_store,
        "save_artifact",
        lambda *_args, **_kwargs: pytest.fail("stage result export must be read-only"),
    )
    return artifacts


@pytest.mark.parametrize("stage_id", tuple(orchestrator.PRODUCTION_STAGE_RESULT_ARTIFACTS))
def test_each_stage_exports_only_its_owned_backend_outputs(monkeypatch, stage_id):
    _install_artifacts(monkeypatch)

    result = orchestrator.get_production_stage_result("session-1", "pair-1", stage_id)

    assert result["stage"]["id"] == stage_id
    assert result["run_id"] == "run-42"
    assert result["status"] == "COMPLETED"
    assert set(result["outputs"]["artifacts"]) == set(
        orchestrator.PRODUCTION_STAGE_RESULT_ARTIFACTS[stage_id]
    )
    assert set(result["outputs"]["stage_state"]) == set(
        orchestrator.PRODUCTION_STAGE_RESULT_STATE_KEYS[stage_id]
    )


def test_stage_export_contains_diagnostics_and_redacts_unsafe_values(monkeypatch):
    _install_artifacts(monkeypatch)

    result = orchestrator.get_production_stage_result("session-1", "pair-1", "content")
    selection = orchestrator.get_production_stage_result(
        "session-1", "pair-1", "selection"
    )
    serialized = json.dumps(result, ensure_ascii=False)
    serialized_selection = json.dumps(selection, ensure_ascii=False)

    assert set(result) == {
        "schema_version",
        "stage",
        "run_id",
        "status",
        "inputs",
        "outputs",
        "evidence_provenance",
        "reasons",
        "diagnostics",
    }
    assert result["inputs"]["upstream_artifacts"]["sheet_relations"] == {
        "kind": "kind-sheet_relations",
        "schema_version": "v1",
        "input_signature": "sha256:sheet_relations",
    }
    assert result["evidence_provenance"]["included_in_outputs"] is True
    assert any(
        path.endswith(".evidence")
        for path in result["evidence_provenance"]["json_paths"]
    )
    assert any(path.endswith(".reason_code") for path in result["reasons"])
    assert "must-not-leak" not in serialized
    assert "secret-token-value" not in serialized
    assert "/home/coder" not in serialized
    assert "/var/lib/audit" not in serialized
    assert "/srv/docs" not in serialized_selection
    assert {item["reason"] for item in result["diagnostics"]["omissions"]} >= {
        "secret",
        "binary_data",
        "absolute_server_path",
    }


def test_unknown_stage_is_rejected_without_starting_work(monkeypatch):
    monkeypatch.setattr(
        orchestrator.store,
        "get_pair_for_production",
        lambda *_: pytest.fail("invalid stage must fail before reading the pair"),
    )
    monkeypatch.setattr(
        orchestrator,
        "run_production_comparison",
        lambda *_args, **_kwargs: pytest.fail("copy must never start a producer"),
    )

    with pytest.raises(ValueError, match="unsupported production stage"):
        orchestrator.get_production_stage_result("session-1", "pair-1", "unknown")
