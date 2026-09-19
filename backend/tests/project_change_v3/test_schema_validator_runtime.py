"""The real jsonschema validator on the V3 provider path (skipped where it is not installed).

Runs in the release venv (jsonschema==4.23.0 from requirements-projectchange-v3-runtime.lock.txt).
The fail-closed guard for a runtime WITHOUT the package lives in
test_oversize_transport.py::test_missing_schema_validator_refuses_before_any_model_call.
Zero model calls: the Codex CLI is fake_codex.py.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

jsonschema = pytest.importorskip("jsonschema")

from backend.app.services.project_change_v3.contracts import DEDUPE_SCHEMA, MAP_SCHEMA, MINER_SCHEMA  # noqa: E402
from backend.app.services.project_change_v3.provider import CodexProvider, ProviderError  # noqa: E402

FAKE = Path(__file__).with_name("fake_codex.py")


def test_installed_version_is_the_locked_one():
    from importlib.metadata import version

    assert version("jsonschema") == "4.23.0"


@pytest.mark.parametrize("schema", [MAP_SCHEMA, MINER_SCHEMA, DEDUPE_SCHEMA], ids=["map", "miner", "dedupe"])
def test_v3_schemas_are_valid_for_the_validator(schema):
    jsonschema.validators.validator_for(schema).check_schema(schema)


@pytest.fixture
def fake_cli(tmp_path, monkeypatch):
    log, answer = tmp_path / "codex.log", tmp_path / "answer.json"
    monkeypatch.setenv("STAGE_COMPARISON_AI_CODEX_BIN", str(FAKE))
    monkeypatch.setenv("STAGE_COMPARISON_AI_ENV_ALLOWLIST", "FAKE_CODEX_LOG,FAKE_CODEX_RESPONSE,FAKE_CODEX_MODE")
    monkeypatch.setenv("FAKE_CODEX_LOG", str(log))
    monkeypatch.setenv("FAKE_CODEX_RESPONSE", str(answer))
    monkeypatch.delenv("FAKE_CODEX_MODE", raising=False)
    return answer


def _dedupe_call(provider):
    return provider.complete(stage="DEDUPE", call_id="c", pair_id="P", prompt="p", data={"pair": "P"},
                             schema=DEDUPE_SCHEMA, images=[])


def test_provider_validates_answers_with_the_real_validator(fake_cli):
    fake_cli.write_text(json.dumps({"pair": "P", "decisions": [], "notes": []}), encoding="utf-8")
    provider = CodexProvider()
    assert _dedupe_call(provider) == {"pair": "P", "decisions": [], "notes": []}
    assert provider.last_transport["usage"]["output_tokens"] == 10

    fake_cli.write_text(json.dumps({"pair": "P", "decisions": "not-a-list"}), encoding="utf-8")
    with pytest.raises(ProviderError) as info:
        _dedupe_call(provider)
    assert info.value.code == "schema_invalid"
