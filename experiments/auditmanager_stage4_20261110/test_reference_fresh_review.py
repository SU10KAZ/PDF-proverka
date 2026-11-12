"""Executor safety and admission tests with simulated provider responses."""
import asyncio
from copy import deepcopy
import json
import pytest
from experiments.auditmanager_stage4_20261110.test_reference_retention_admission_bridge import case
from experiments.auditmanager_stage4_20261110.reference_retention_admission_bridge import prepare_reference_adjudication
from experiments.auditmanager_stage4_20261110.run_reference_fresh_review import run


@pytest.fixture
def inputs(case):
    _, entry, workflow, md = case
    preparation = prepare_reference_adjudication(*case)
    # Mocked calls exercise orchestration only, not graphic/semantic quality.
    preparation['request']['image_paths'] = []
    preparation['request']['image_bindings'] = []
    decision = deepcopy(entry['decision'])
    decision['approved_fields'] = workflow['candidate_fields']
    decision['context_reference_obligations'] = preparation['required_obligations']
    return preparation, entry, md, decision


def execute(inputs, root, invoke, collection=None):
    return asyncio.run(run(*inputs[:3], root, invoke=invoke, executor_id='simulated-only',
                          collection=collection or {'collection_status': 'completed', 'runs': []}))


def test_recorded_response_reused_without_second_dispatch(inputs, tmp_path):
    calls = []
    async def invoke(request, phase):
        calls.append(phase)
        return {'json_data': inputs[3], 'is_error': False}
    first = execute(inputs, tmp_path, invoke)
    assert first['status'] == 'candidate_ready'
    assert not first['semantic_acceptance'] and not first['automatic_release']
    assert execute(inputs, tmp_path, invoke) == first
    assert len(calls) == 1


def test_silent_reference_removal_is_held(inputs, tmp_path):
    inputs[3].pop('context_reference_obligations')
    async def invoke(request, phase):
        return {'json_data': inputs[3]}
    report = execute(inputs, tmp_path, invoke)
    assert report['status'] == 'held'
    assert report['reason'] == 'reference_selection_incomplete'
    assert 'entry' not in report


def test_active_comparison_blocks_dispatch(inputs, tmp_path):
    async def forbidden(*args):
        pytest.fail('must not dispatch')
    with pytest.raises(RuntimeError, match='comparison_in_progress'):
        execute(inputs, tmp_path, forbidden, {'collection_status': 'running', 'runs': []})
    assert not (tmp_path/'attempt.json').exists()


def test_interrupted_attempt_is_not_repeated(inputs, tmp_path):
    calls = []
    async def fail(*args):
        calls.append(1)
        raise RuntimeError('interrupted')
    with pytest.raises(RuntimeError, match='interrupted'):
        execute(inputs, tmp_path, fail)
    with pytest.raises(FileExistsError):
        execute(inputs, tmp_path, fail)
    assert len(calls) == 1


def test_changed_saved_response_is_rejected(inputs, tmp_path):
    async def invoke(*args):
        return {'json_data': inputs[3]}
    execute(inputs, tmp_path, invoke)
    (tmp_path/'result.json').write_text(json.dumps({'json_data': {}}))
    with pytest.raises(ValueError, match='stale_fresh_review_result'):
        execute(inputs, tmp_path, invoke)


def test_reference_retention_does_not_bypass_normal_admission(inputs, tmp_path):
    inputs[3]['claim_reviews'] = []
    async def invoke(*args):
        return {'json_data': inputs[3]}
    report = execute(inputs, tmp_path, invoke)
    assert report['status'] == 'held'
    assert report['admission_receipts'][0]['reason'] == 'incomplete_claim_review'


def test_stale_source_stops_before_dispatch(inputs, tmp_path):
    async def forbidden(*args):
        pytest.fail('must not dispatch')
    changed = (inputs[0], inputs[1], inputs[2] + '\nchanged', inputs[3])
    with pytest.raises(ValueError, match='stale_fresh_review_input'):
        execute(changed, tmp_path, forbidden)
    assert not (tmp_path/'attempt.json').exists()
