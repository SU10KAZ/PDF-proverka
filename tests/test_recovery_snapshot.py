import copy
import json

from backend.app.pipeline.stages.prepare.codex_targeted_findings import (
    CodexTargetedPass, combine_findings_with_targeted,
)
from backend.app.pipeline.stages.prepare.candidate_recovery import validate_recovery_payload
from backend.app.pipeline.stages.prepare.recovery_snapshot import refresh_recovery_snapshot


def test_current_snapshot_links_duplicate_but_preserves_other_candidate_atom():
    base = {'findings': [{'id': 'F-004', 'problem': 'Original useful finding'}]}
    first = {'findings': [{'problem': 'Two drives share a tag',
                          'source_finding_ids': ['G-1'], 'evidence': [{'block_id': 'g'}]}]}
    done = [('candidate_recovery_first', first)]
    packet = {'candidate': {'origin': 'graphic', 'pointer': '/0', 'source_id': 'G-2'},
              'candidate_group': [{'origin': 'graphic', 'pointer': '/0', 'source_id': 'G-2'},
                                  {'origin': 'graphic', 'pointer': '/1', 'source_id': 'G-3'}],
              'existing_findings': [], 'allowed_source_refs': ['g'],
              'attached_image_block_ids_in_order': ['g'], 'authoritative_md': 'Unchanged'}
    request = CodexTargetedPass('candidate_recovery_second', 'second.json',
                               [{'role': 'system', 'content': 'Inspect sources'},
                                {'role': 'user', 'content': json.dumps(packet)}], ('g.png',))
    original = copy.deepcopy((base, done, request))
    fresh = refresh_recovery_snapshot(request, base, done)
    p = json.loads(fresh.messages[-1]['content'])
    assert [f['id'] for f in p['existing_findings']] == ['F-004', 'F-005']
    assert p['authoritative_md'] == packet['authoritative_md']
    assert fresh.image_paths == request.image_paths
    response = {'candidate_decisions': [{'origin': 'graphic', 'pointer': '/0',
        'decision': 'duplicate', 'target_finding_ids': ['F-005'],
        'source_refs': ['g'], 'reason': 'Same two drives and conflicting tag'}],
        'findings': [{'problem': 'Independent thermostat conflict',
                      'source_finding_ids': ['G-2', 'G-3'], 'evidence': [{'block_id': 'g'}]}]}
    valid = validate_recovery_payload(fresh, response)
    combined = combine_findings_with_targeted(base, done + [(fresh.stage, valid)])
    assert len(combined['findings']) == 3
    assert combined['findings'][0] == base['findings'][0]
    assert combined['findings'][1]['source_finding_ids'] == ['G-1', 'G-2']
    assert combined['findings'][2]['id'] == 'F-006'
    assert combined['findings'][2]['problem'] == 'Independent thermostat conflict'
    assert (base, done, request) == original


def test_non_recovery_request_unchanged():
    request = CodexTargetedPass('object_operations_test', 'x.json', [])
    assert refresh_recovery_snapshot(request, {}, []) is request


def test_serial_runner_dispatches_latest_snapshot(monkeypatch, tmp_path):
    import asyncio
    from backend.app.services.llm import claude_runner
    from backend.app.models.usage import LLMResult
    from backend.app.pipeline.stages.prepare import codex_targeted_findings as targeted

    passes = []
    for n in (1, 2):
        c = {'origin': 'text', 'pointer': '/text_findings/' + str(n), 'source_id': 'T-' + str(n)}
        p = {'candidate': c, 'candidate_group': [c], 'existing_findings': [],
             'allowed_source_refs': ['t'], 'attached_image_block_ids_in_order': [],
             'authoritative_md': '### BLOCK [TEXT]: t\nObject source'}
        passes.append(CodexTargetedPass('candidate_recovery_' + str(n), str(n) + '.json',
            [{'role': 'system', 'content': 'Inspect sources'},
             {'role': 'user', 'content': json.dumps(p)}]))
    observed = []

    async def fake_stage(**kwargs):
        p = json.loads(kwargs['messages'][-1]['content'])
        observed.append([f['id'] for f in p['existing_findings']])
        c = p['candidate']
        decision = {'origin': c['origin'], 'pointer': c['pointer'],
                    'source_refs': ['t'], 'reason': 'Same object and claim'}
        if c['source_id'] == 'T-1':
            payload = {'findings': [{'problem': 'Source-backed conflict',
                'source_finding_ids': ['T-1'], 'evidence': [{'block_id': 't'}]}],
                'candidate_decisions': [{**decision, 'decision': 'retained'}]}
        else:
            payload = {'findings': [], 'candidate_decisions': [
                {**decision, 'decision': 'duplicate', 'target_finding_ids': ['F-001']}]}
        return 0, 'ok', LLMResult(json_data=payload, model='codex/gpt-6-astra')

    monkeypatch.setattr(targeted, 'build_targeted_findings_passes', lambda *a: passes)
    monkeypatch.setattr(claude_runner, '_codex_targeted_findings_enabled', lambda: True)
    monkeypatch.setattr(claude_runner, '_run_codex_json_stage', fake_stage)
    monkeypatch.setattr(claude_runner, '_save_audit_trail', lambda *a, **k: None)
    result = asyncio.run(claude_runner._run_codex_targeted_findings_merge(
        project_info={}, project_id='test', model='codex/gpt-6-astra',
        base_result=LLMResult(json_data={'findings': []}), base_text='base', on_output=None,
        output_dir=tmp_path, version_dir=tmp_path, version_id='v001'))
    assert observed == [[], ['F-001']]
    finding, = result[2].json_data['findings']
    assert finding['source_finding_ids'] == ['T-1', 'T-2']
    assert finding['recovery_semantic_verified'] is False
