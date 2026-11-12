"""Simulated-provider contract tests; do not claim measured model performance."""
import asyncio
import copy
import json
from pathlib import Path
import sys
import pytest
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from experiments.auditmanager_stage4_20261110.reference_retention_workflow import run_reference_retention, selection_request

BASE = Path(__file__).parent

@pytest.fixture
def case():
    trace = json.loads((BASE / 'AR_calculation_reference_trace.json').read_text())
    bundle = json.loads((BASE / 'ar_rope_bundle_source_evidence/bundle.json').read_text())
    entry = json.loads((BASE / 'ar_rope_current_release_evidence/adjudication/candidate.json').read_text())['entry']
    quote = trace['source_quote']
    selection = {'reference_dispositions': [dict(reference='КМ8', role='calculation_context', block_id=quote['block_id'], quote=quote['quote'], disposition='required', reason='The accepted question asks for calculation context explicitly referenced by this source.')]}
    return trace['approved_user_fields'], entry['response'], bundle['md'], selection


def test_selection_packet_contains_sources_not_expected_answer(case):
    fields, response, md, _ = case
    request = selection_request(fields, response, md)
    assert 'КМ8' not in request['messages'][0]['content']
    payload = json.loads(request['messages'][1]['content'])
    assert set(payload) == {'approved_user_fields','untrusted_source_review','authoritative_text_blocks','source_md_sha256'}

@pytest.mark.parametrize('repair_succeeds', [True, False])
def test_bounded_repair_never_releases_or_mutates_original(case, repair_succeeds):
    fields, response, md, selection = case
    before = copy.deepcopy(fields)
    phases = []
    async def invoke(request, phase):
        phases.append(phase)
        if phase == 'reference_selection': return selection
        output = copy.deepcopy(fields)
        if repair_succeeds: output['solution'] += ' Проверить применимое расчётное обоснование по существующей ссылке КМ8.'
        return output
    result = asyncio.run(run_reference_retention(fields,response,md,invoke=invoke))
    assert phases == ['reference_selection', 'reference_repair']
    assert result['status'] == ('candidate_requires_semantic_review' if repair_succeeds else 'repair_unresolved')
    assert fields == before == result['original_fields']
    assert not result['semantic_acceptance'] and not result['automatic_release']


def test_empty_selection_is_not_success(case):
    fields, response, md, _ = case
    async def invoke(request, phase):
        assert phase == 'reference_selection'
        return {'reference_dispositions': []}
    result = asyncio.run(run_reference_retention(fields,response,md,invoke=invoke))
    assert result['status'] == 'not_assessed'


def test_fabricated_reference_rejected_before_repair(case):
    fields, response, md, selection = case
    selection['reference_dispositions'][0]['quote'] = 'См. несуществующий КМ8'
    async def invoke(request, phase):
        assert phase == 'reference_selection'
        return selection
    with pytest.raises(ValueError, match='reference_not_in_bound_quote'):
        asyncio.run(run_reference_retention(fields,response,md,invoke=invoke))


def test_live_comparison_prevents_provider_creation(tmp_path, monkeypatch):
    from experiments.auditmanager_stage4_20261110 import run_ar_reference_retention_candidate as runner
    monkeypatch.setattr(runner, 'observe', lambda: {'collection_status': 'running', 'runs': []})
    def forbidden(*args, **kwargs):
        pytest.fail('Provider must not be created during the comparison')
    monkeypatch.setattr(runner, 'direct_cli_executor', forbidden)
    with pytest.raises(RuntimeError, match='comparison_in_progress'):
        asyncio.run(runner.main(tmp_path, True))
    assert (tmp_path / 'selection_request.json').exists()
    assert not list(tmp_path.rglob('attempt.json'))
