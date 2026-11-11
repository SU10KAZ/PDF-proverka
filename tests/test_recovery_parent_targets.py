from copy import deepcopy
from backend.app.pipeline.stages.findings_merge.recovery_parent_targets import carry_parent_targets, bind_parent_targets
from backend.app.pipeline.stages.findings_merge.recovery_addenda import prepare_addenda
from backend.app.pipeline.stages.prepare.codex_targeted_findings import combine_findings_with_targeted


def test_followup_targets_its_parent_despite_other_request_using_same_number():
    parent = {'findings': [{'id': 'F-036', 'problem': 'Pressure range', 'description': 'Initial observation'}]}
    other = {'findings': [{'id': 'F-036', 'problem': 'Different cabinet', 'description': 'Unrelated'}]}
    claim = {'id': 'part2', 'claim': 'Specification confirmed', 'reason': 'Inspected table',
             'decision': 'needs_data', 'source_refs': ['t'], 'target_finding_ids': ['F-036']}
    candidate = {'origin': 'graphic', 'pointer': '/g', 'source_id': 'G-1',
                 'pending_subclaims': [{'claim': deepcopy(claim)}]}
    decision = {'origin': 'graphic', 'pointer': '/g', 'subclaims': [deepcopy(claim)]}
    packet = {'existing_findings': [], 'candidate': candidate, 'candidate_group': [candidate],
              'previous_decisions': [decision], 'allowed_source_refs': ['t']}
    carry_parent_targets(packet, 'candidate_recovery_parent', parent)
    alias = packet['existing_findings'][0]['id']
    assert alias == 'parent:candidate_recovery_parent:F-036'
    receipt = deepcopy(packet['previous_decisions'][0])
    receipt['subclaims'][0]['decision'] = 'retained'
    updates, pending = prepare_addenda(packet, [receipt])
    assert not pending and updates
    followup = {'findings': [], 'candidate_decisions': [receipt],
        'recovery_addenda_updates': updates, 'recovery_existing_target_ids': [alias],
        'recovery_parent_target_bindings': packet['parent_target_bindings']}
    base = {'findings': [{'id': 'F-035', 'problem': 'Base'}]}
    merged = combine_findings_with_targeted(base, [('candidate_recovery_other', other),
        ('candidate_recovery_parent', parent), ('candidate_recovery_parent_context', followup)])
    target = merged['findings'][-1]
    assert target['id'] == 'F-037'
    assert 'Specification confirmed' in target['description']
    assert merged['findings'][-2]['description'] == 'Unrelated'
    report = merged['candidate_recovery_reports'][-1]
    assert report['candidate_decisions'][0]['subclaims'][0]['target_finding_ids'] == ['F-037']
    assert report['parent_target_binding']['resolved'] == {alias: 'F-037'}
    # A later mutation of inputs cannot retroactively alter the sealed caller data.
    assert followup['candidate_decisions'][0]['subclaims'][0]['target_finding_ids'] == [alias]


def test_unseen_parent_or_signature_mismatch_never_falls_back_to_global_number():
    binding = {'alias': 'parent:one:F-036', 'parent_stage': 'one', 'parent_local_id': 'F-036',
               'source_signature': 'expected', 'alias_signature': 'alias'}
    for registry in ({}, {('one', 'F-036'): [{'source_signature': 'changed', 'final_id': 'F-036'}]}):
        payload = {'recovery_parent_target_bindings': [binding], 'recovery_existing_target_ids': [binding['alias']]}
        result = bind_parent_targets(payload, registry, [{'id': 'F-036'}])
        assert not result['resolved'] and result['pending']
        assert payload['recovery_existing_target_ids'] == [binding['alias']]
