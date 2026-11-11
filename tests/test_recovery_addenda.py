from copy import deepcopy
from backend.app.pipeline.stages.findings_merge.recovery_addenda import prepare_addenda, apply_addenda
from backend.app.pipeline.stages.findings_merge.subclaim_coverage import pending_subclaims


def fixture():
    key = ('graphic', '/block_analyses/0/findings/0')
    claim = {'id': 'range', 'claim': 'PDS4 range is inconsistent', 'reason': 'drawing and schedule disagree',
             'decision': 'needs_data', 'source_refs': ['g', 't'], 'target_finding_ids': ['F-006']}
    receipt = {'origin': key[0], 'pointer': key[1], 'decision': 'needs_data', 'subclaims': [claim]}
    final = {'findings': [{'id': 'F-006', 'description': 'Confirmed quantity conflict. Graphic not yet checked.',
                          'problem': 'Pressure switch specification', 'source_finding_ids': ['G-011']}],
             'candidate_decisions': [deepcopy(receipt)]}
    candidate = {'origin': key[0], 'pointer': key[1], 'source_id': 'G-011',
                 'pending_subclaims': pending_subclaims(final, {'g', 't'})[key]}
    packet = {'candidate': candidate, 'candidate_group': [candidate], 'existing_findings': deepcopy(final['findings']),
              'allowed_source_refs': ['g', 't']}
    resolved = deepcopy(receipt)
    resolved['decision'] = resolved['subclaims'][0]['decision'] = 'retained'
    return final, packet, resolved, key


def test_recovered_claim_closes_only_after_it_reaches_description_without_losing_original():
    final, packet, resolved, key = fixture()
    original = deepcopy(final)
    final['candidate_recovery_reports'] = [{'candidate_decisions': [resolved]}]
    assert pending_subclaims(final, {'g', 't'})[key][0]['reason'] == 'subclaim_not_emitted'
    updates, pending = prepare_addenda(packet, [resolved])
    assert not pending
    assert len(updates) == 1
    assert len(apply_addenda(final['findings'], updates)['applied']) == 1
    finding = final['findings'][0]
    assert original['findings'][0]['description'] in finding['description']
    assert resolved['subclaims'][0]['claim'] in finding['description']
    assert finding['recovery_original_description'] == original['findings'][0]['description']
    assert finding['recovery_addenda'][0]['semantic_verified'] is False
    assert set(finding['source_block_ids']) == {'g', 't'}
    assert {e['block_id'] for e in finding['evidence']} == {'g', 't'}
    assert not pending_subclaims(final, {'g', 't'})
    snapshot = deepcopy(finding)
    apply_addenda(final['findings'], updates)
    # Replaying the same claim must not repeat user-visible content.
    assert finding['description'] == snapshot['description']
    assert len(finding['recovery_addenda']) == 1
    finding['description'] = finding['recovery_original_description']
    assert pending_subclaims(final, {'g', 't'})[key][0]['reason'] == 'subclaim_not_emitted'


def test_changed_target_and_unknown_sources_remain_unpublished():
    final, packet, resolved, key = fixture()
    updates, _ = prepare_addenda(packet, [resolved])
    final['findings'][0]['problem'] = 'Different object'
    result = apply_addenda(final['findings'], updates)
    assert not result['applied']
    assert result['pending'][0]['reason'] == 'addendum_target_changed'
    resolved['subclaims'][0]['source_refs'] = ['unsupplied']
    updates, pending = prepare_addenda(packet, [resolved])
    assert not updates and pending


def test_multiple_addenda_from_same_snapshot_and_later_snapshot_preserve_each_other():
    final, packet, resolved, _ = fixture()
    second = deepcopy(resolved)
    second['subclaims'][0].update(id='balance', claim='Graphic count is two plus two')
    packet['candidate']['pending_subclaims'].append({'claim': {'id': 'balance'}})
    first_updates, _ = prepare_addenda(packet, [resolved])
    second_updates, _ = prepare_addenda(packet, [second])
    assert len(apply_addenda(final['findings'], first_updates + second_updates)['applied']) == 2
    packet['existing_findings'] = deepcopy(final['findings'])
    third = deepcopy(resolved)
    third['subclaims'][0].update(id='scope', claim='Same cabinet confirmed')
    packet['candidate']['pending_subclaims'].append({'claim': {'id': 'scope'}})
    third_updates, _ = prepare_addenda(packet, [third])
    assert len(apply_addenda(final['findings'], third_updates)['applied']) == 1
    assert len(final['findings'][0]['recovery_addenda']) == 3


def test_new_finding_publication_survives_request_local_renumbering():
    import json
    from backend.app.pipeline.stages.prepare.candidate_recovery import validate_recovery_payload
    from backend.app.pipeline.stages.prepare.codex_targeted_findings import CodexTargetedPass, combine_findings_with_targeted
    final, packet, resolved, key = fixture()
    packet['attached_image_block_ids_in_order'] = ['g']
    new = {'id': 'F-007', 'problem': 'New range conflict', 'description': 'Range summary',
           'source_finding_ids': ['G-011'], 'evidence': [{'block_id': 'g'}]}
    resolved['subclaims'][0]['target_finding_ids'] = ['F-007']
    request = CodexTargetedPass('candidate_recovery_new', 'out.json', [{'role': 'user', 'content': json.dumps(packet)}])
    raw = {'findings': [new], 'candidate_decisions': [resolved]}
    validated = validate_recovery_payload(request, raw)
    assert len(validated['recovery_new_finding_addenda']) == 1
    assert not validated['recovery_addenda_updates']
    final['findings'].append({'id': 'F-007', 'problem': 'Other request', 'description': 'Unrelated'})
    merged = combine_findings_with_targeted(final, [(request.stage, validated)])
    assert merged['findings'][-1]['id'] == 'F-008'
    assert merged['findings'][-1]['recovery_addenda'][0]['target_id'] == 'F-008'
    assert resolved['subclaims'][0]['claim'] in merged['findings'][-1]['description']
    assert merged['findings'][-2]['description'] == 'Unrelated'
    assert not pending_subclaims(merged, {'g', 't'})
    merged['findings'][-1]['description'] = 'Range summary'
    assert pending_subclaims(merged, {'g', 't'})[key][0]['reason'] == 'subclaim_not_emitted'


def test_new_publication_rejects_colliding_ids_and_changed_normalized_target():
    from backend.app.pipeline.stages.findings_merge.recovery_addenda import publish_new_finding_addenda
    final, packet, resolved, _ = fixture()
    original = deepcopy(final['findings'][0])
    updates, _ = prepare_addenda(packet, [resolved])
    changed = {**original, 'id': 'F-020', 'description': 'Different conclusion'}
    result = publish_new_finding_addenda([changed], updates, {'F-006': ['F-020']}, [original])
    assert not result['applied'] and result['pending'][0]['reason'] == 'new_addendum_target_changed'
    packet['existing_findings'].append(deepcopy(original))
    updates, pending = prepare_addenda(packet, [resolved])
    assert not updates and pending
