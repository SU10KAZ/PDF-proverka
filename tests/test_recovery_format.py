import asyncio
from copy import deepcopy
import json

import pytest

from backend.app.pipeline.stages.prepare.codex_targeted_findings import CodexTargetedPass
from backend.app.pipeline.stages.prepare.recovery_format import build_recovery_format_followup
from backend.app.pipeline.stages.prepare.recovery_context import build_context_followup


def request():
    candidate = {'origin': 'text', 'pointer': '/text_findings/0', 'source_id': 'T-1'}
    packet = {'candidate': candidate, 'candidate_group': [candidate],
              'authoritative_md': '### BLOCK [TEXT]: b1\nValue 12\n',
              'allowed_source_refs': ['b1'], 'attached_image_block_ids_in_order': [],
              'existing_findings': []}
    return CodexTargetedPass('candidate_recovery_test', 'recovery.json', [
        {'role': 'system', 'content': 'Inspect sources'},
        {'role': 'user', 'content': json.dumps(packet)}])


def answer():
    return {'findings': [{'id': 'T-1', 'problem': 'Check value 12',
                          'source_finding_ids': ['T-1'], 'report_section': 'questions'}],
            'candidate_decisions': [{'origin': 'text', 'pointer': '/text_findings/0',
                                    'decision': 'retained', 'source_refs': ['b1']}]}


def test_repair_keeps_evidence_unassigned_and_is_bounded():
    from backend.app.pipeline.stages.prepare.candidate_recovery import validate_recovery_payload
    original = request()
    before = deepcopy(original.messages)
    payload = validate_recovery_payload(original, answer())
    followup, receipt = build_recovery_format_followup(original, payload)
    assert receipt['status'] == 'scheduled' and not receipt['evidence_inferred']
    packet = json.loads(followup.messages[-1]['content'])
    assert packet['authoritative_md'] == json.loads(original.messages[-1]['content'])['authoritative_md']
    rejected = packet['recovery_format_repair']['rejected_findings'][0]['finding']
    assert 'evidence' not in rejected
    assert original.messages == before
    assert followup.image_paths == original.image_paths
    assert build_recovery_format_followup(followup, payload)[0] is None
    assert build_context_followup(followup, payload)[0] is None
    # No automatic repair for missing graphics or missing candidate identity.
    for reason in ('graphic_source_unavailable', 'missing_candidate_link'):
        assert build_recovery_format_followup(original, {
            'recovery_pending': [{'reason': reason, 'finding': rejected}]})[0] is None


@pytest.mark.parametrize('valid_correction', [True, False])
def test_pipeline_repairs_once_without_bypassing_admission(monkeypatch, tmp_path, valid_correction):
    from backend.app.services.llm import claude_runner
    from backend.app.models.usage import LLMResult
    from backend.app.pipeline.stages.prepare import codex_targeted_findings as targeted
    calls = []

    async def fake_stage(**kwargs):
        calls.append(kwargs['stage'])
        payload = answer()
        if kwargs['stage'].endswith('_format') and valid_correction:
            payload['findings'][0]['evidence'] = [
                {'block_id': 'b1', 'type': 'text', 'quote': 'Value 12'}]
        return 0, 'ok', LLMResult(json_data=payload, model='codex/gpt-6-astra')

    monkeypatch.setattr(targeted, 'build_targeted_findings_passes', lambda *a: [request()])
    monkeypatch.setattr(claude_runner, '_codex_targeted_findings_enabled', lambda: True)
    monkeypatch.setattr(claude_runner, '_run_codex_json_stage', fake_stage)
    monkeypatch.setattr(claude_runner, '_save_audit_trail', lambda *a, **k: None)
    result = asyncio.run(claude_runner._run_codex_targeted_findings_merge(
        project_info={}, project_id='test', model='codex/gpt-6-astra',
        base_result=LLMResult(json_data={'findings': []}), base_text='base', on_output=None,
        output_dir=tmp_path, version_dir=tmp_path, version_id='v001'))
    assert result[0] == 0
    assert calls == ['candidate_recovery_test', 'candidate_recovery_test_format']
    output = result[2].json_data
    assert len(output['findings']) == int(valid_correction)
    reports = output['candidate_recovery_reports']
    assert reports[0]['pending'][0]['reason'] == 'missing_inspected_source_reference'
    assert reports[0]['format_repair']['status'] == 'scheduled'
    assert reports[1]['format_repair']['status'] == 'format_repair_limit'
    if valid_correction:
        assert output['findings'][0]['evidence'][0]['quote'] == 'Value 12'
        assert output['findings'][0]['recovery_semantic_verified'] is False
    else:
        assert reports[1]['pending'][0]['reason'] == 'missing_inspected_source_reference'
