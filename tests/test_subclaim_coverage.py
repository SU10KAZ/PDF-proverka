import json
from copy import deepcopy
from backend.app.pipeline.stages.findings_merge.subclaim_coverage import pending_subclaims


def receipt(action='needs_data'):
    return {'origin': 'graphic', 'pointer': '/block_analyses/0/findings/0',
            'decision': 'retained', 'subclaims': [
                {'id': 'range', 'claim': 'range on drawing', 'reason': 'inspect drawing',
                 'decision': action, 'source_refs': ['g'], 'required_source_refs': ['g'],
                 'target_finding_ids': ['F-1']}]}


def final(r):
    return {'findings': [{'id': 'F-1', 'source_finding_ids': ['G-1']}],
            'candidate_decisions': [r]}


def test_parent_retention_or_later_omission_does_not_clear_obligation():
    data = final(receipt())
    data['candidate_recovery_reports'] = [{'candidate_decisions': [
        {'origin': 'graphic', 'pointer': '/block_analyses/0/findings/0', 'decision': 'duplicate'}]}]
    before = deepcopy(data)
    assert pending_subclaims(data, {'g'})
    assert data == before
    data['candidate_recovery_reports'].append({'candidate_decisions': [receipt('duplicate')]})
    assert pending_subclaims(data, {'g'})[('graphic', '/block_analyses/0/findings/0')][0]['reason'] == 'subclaim_not_emitted'


def test_conflicting_unknown_and_unlinked_resolutions_remain_pending():
    r = receipt('retained')
    assert pending_subclaims(final(r), {'g'}) == {}
    assert pending_subclaims(final(r), {'other'})
    data = final(r)
    data['findings'] = []
    assert pending_subclaims(data, {'g'})
    data = final(r)
    data['candidate_decisions'].append(deepcopy(r))
    assert pending_subclaims(data, {'g'})
    r['subclaims'].append(deepcopy(r['subclaims'][0]))
    assert pending_subclaims(final(r), {'g'})


def test_legacy_prose_is_not_parsed_as_subclaim_certification():
    r = receipt()
    del r['subclaims']
    r['reason'] = 'Some graphic details are not checked'
    assert pending_subclaims(final(r), {'g'}) == {}


def test_linked_graphic_pending_claim_gets_image_without_mutating_existing_finding(tmp_path, monkeypatch):
    import backend.app.pipeline.stages.prepare.candidate_recovery as recovery
    crops = tmp_path / 'crops'
    crops.mkdir()
    (crops / 'g.png').write_bytes(b'inference not dispatched')
    (crops / 'index.json').write_text(json.dumps({'blocks': [{'block_id': 'g', 'file': 'g.png'}]}))
    monkeypatch.setattr(recovery, 'resolve_blocks_dir', lambda _: crops)
    data = final(receipt())
    payloads = {'02_text_analysis.json': {}, '01_blocks_analysis.json': {
        'block_analyses': [{'block_id': 'g', 'findings': [{'id': 'G-1'}]}]},
        '03_findings.json': data, 'document_graph.json': {
            'pages': [{'page': 1, 'image_blocks': [{'id': 'g'}]}]}}
    for name, payload in payloads.items():
        (tmp_path / name).write_text(json.dumps(payload))
    md = tmp_path / 'document.md'
    md.write_text('## Page 1\n### BLOCK [GRAPHIC]: g\nNavigation only\n')
    passes = recovery.build_recovery_passes(tmp_path, md)
    assert len(passes) == 1
    packet = json.loads(passes[0].messages[-1]['content'])
    assert packet['candidate_group'][0]['pending_subclaims'][0]['claim']['id'] == 'range'
    assert packet['existing_findings'] == data['findings']
    assert passes[0].image_paths == (crops / 'g.png',)
    data['candidate_recovery_reports'] = [{'candidate_decisions': [receipt('retained')]}]
    (tmp_path / '03_findings.json').write_text(json.dumps(data))
    assert len(recovery.build_recovery_passes(tmp_path, md)) == 1  # Retention receipt alone has not published the recovered claim.
