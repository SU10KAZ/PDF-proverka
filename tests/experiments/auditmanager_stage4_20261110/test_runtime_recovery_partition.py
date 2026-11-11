from copy import deepcopy
import hashlib
import json
import pytest
from experiments.auditmanager_stage4_20261110.split_runtime_recovery_images import partition


def packet(tmp_path):
    paths = [tmp_path / 'small.png', tmp_path / 'large.png']
    paths[0].write_bytes(b'small fixture')
    with paths[1].open('wb') as stream:
        stream.truncate(16_000_001)
    hashes = {str(p): hashlib.sha256(p.read_bytes()).hexdigest() for p in paths}
    candidate = {'origin': 'graphic', 'pointer': '/block_analyses/0/findings/0',
                 'source_id': 'G-1', 'pending_subclaims': [
                     {'claim': {'id': 'a', 'required_source_refs': ['a']}},
                     {'claim': {'id': 'b', 'required_source_refs': ['b']}}]}
    payload = {'candidate': candidate, 'candidate_group': [candidate],
               'existing_findings': [{'id': 'F-1', 'description': 'preserve'}],
               'attached_image_block_ids_in_order': ['a', 'b'],
               'unavailable_image_block_ids': [], 'allowed_source_refs': ['a', 'b', 't']}
    return {'stage': 'candidate_recovery_test', 'output_filename': 'test.json',
            'image_hashes': hashes, 'messages': [{'role': 'system', 'content': 'test'},
            {'role': 'user', 'content': json.dumps(payload)}]}


def test_partition_preserves_each_claim_once_and_removes_unattached_image_authority(tmp_path):
    original = packet(tmp_path)
    before = deepcopy(original)
    parts = partition(original)
    assert original == before
    assert len(parts) == 2
    claims = []
    for part in parts:
        content = json.loads(part['messages'][-1]['content'])
        assert len(part['image_hashes']) == 1
        assert content['existing_findings'] == [{'id': 'F-1', 'description': 'preserve'}]
        attached = set(content['attached_image_block_ids_in_order'])
        withheld = set(content['partition_scope']['withheld_image_ids'])
        assert attached.isdisjoint(withheld)
        assert set(content['allowed_source_refs']) == attached | {'t'}
        assert set(content['unavailable_image_block_ids']) == withheld
        claims += [o['claim']['id'] for c in content['candidate_group'] for o in c['pending_subclaims']]
    assert sorted(claims) == ['a', 'b']


def test_cross_part_or_missing_decomposition_is_refused_instead_of_dropped(tmp_path):
    original = packet(tmp_path)
    content = json.loads(original['messages'][-1]['content'])
    content['candidate_group'][0]['pending_subclaims'][0]['claim']['required_source_refs'] = ['a', 'b']
    original['messages'][-1]['content'] = json.dumps(content)
    with pytest.raises(ValueError, match='Cross-part'):
        partition(original)
    content['candidate_group'][0]['pending_subclaims'] = []
    original['messages'][-1]['content'] = json.dumps(content)
    with pytest.raises(ValueError, match='without explicit subclaims'):
        partition(original)
