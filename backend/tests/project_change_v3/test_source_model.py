import pytest
from backend.app.services.project_change_v3 import source_model, contracts, run_storage


@pytest.mark.parametrize('key', list(contracts.MODEL_PROFILES))
def test_saved_identity_survives_default_changes(monkeypatch, key):
    p = contracts.MODEL_PROFILES[key]
    monkeypatch.setattr(contracts, 'DEFAULT_PROFILE_KEY', 'opus55')
    assert source_model.from_state({'provenance': {'model': p.model, 'provider': p.provider}}) == p


@pytest.mark.parametrize('provenance', [{}, {'model': 'unknown'},
    {'model': 'gpt-6-astra', 'provider': 'claude_code_cli_subscription'},
    {'model': 'gpt-6-astra', 'provider': 'codex_cli_subscription', 'model_profile': 'opus55'},
    {'model': 'gpt-6-astra', 'provider': 'codex_cli_subscription', 'reasoning': 'low'}])
def test_missing_or_conflicting_model_never_defaults(provenance):
    with pytest.raises(ValueError):
        source_model.from_state({'provenance': provenance})


def test_report_reads_exact_source_run(monkeypatch, tmp_path):
    calls = []
    monkeypatch.setattr(run_storage, 'run_dir', lambda s, p, r: calls.append((s, p, r)) or tmp_path)
    monkeypatch.setattr(run_storage, 'read', lambda p: {'run_id': 'historical', 'provenance': {
        'model': 'gpt-6.1-sol', 'provider': 'codex_cli_subscription', 'model_profile': 'sol61'}})
    assert source_model.for_run('s', 'p', 'historical').key == 'sol61'
    assert calls == [('s', 'p', 'historical')]
    with pytest.raises(ValueError):
        source_model.for_run('s', 'p', 'current')
