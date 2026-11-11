"""Partition an oversized recovery packet by explicit pending image obligations."""
from copy import deepcopy
import json
from pathlib import Path

from .run_primary_text_runtime_probe import sha, write


def partition(packet):
    payload = json.loads(packet['messages'][-1]['content'])
    paths = list(packet['image_hashes'])
    ids = payload['attached_image_block_ids_in_order']
    if len(paths) != len(ids) or len(set(ids)) != len(ids):
        raise ValueError('Ambiguous image attachment order')
    chunks, current, size = [], [], 0
    for bid, path in zip(ids, paths):
        path = Path(path)
        if sha(path) != packet['image_hashes'][str(path)]:
            raise ValueError('Image changed')
        length = path.stat().st_size
        if length > 24_000_000:
            raise ValueError('Single image exceeds bound')
        if current and size + length > 16_000_000:
            chunks.append(current); current = []; size = 0
        current.append((bid, str(path))); size += length
        if length > 16_000_000:
            chunks.append(current); current = []; size = 0
    if current:
        chunks.append(current)
    if len(chunks) < 2:
        raise ValueError('No partition necessary')
    assignments = [[] for _ in chunks]
    expected = set()
    for candidate in payload['candidate_group']:
        if not candidate.get('pending_subclaims'):
            raise ValueError('Candidate without explicit subclaims cannot be partitioned safely')
        for obligation in candidate.get('pending_subclaims', []):
            claim = obligation.get('claim', {})
            cid = claim.get('id')
            refs = claim.get('required_source_refs')
            if not isinstance(cid, str) or not isinstance(refs, list):
                raise ValueError('Explicit claim identity and required sources needed')
            key = (candidate['origin'], candidate['pointer'], cid)
            if key in expected:
                raise ValueError('Duplicate subclaim identity')
            expected.add(key)
            graphic = set(refs).intersection(ids)
            matches = [i for i, chunk in enumerate(chunks) if graphic and graphic <= {b for b, _ in chunk}]
            if len(matches) != 1:
                raise ValueError('Cross-part or missing image dependency needs separate context handling')
            assignments[matches[0]].append((candidate, obligation))
    result, assigned = [], set()
    for index, (chunk, obligations) in enumerate(zip(chunks, assignments)):
        if not obligations:
            raise ValueError('Image part has no explicit obligations')
        content = deepcopy(payload)
        members = {}
        for original, obligation in obligations:
            key = (original['origin'], original['pointer'])
            if key not in members:
                members[key] = {**deepcopy(original), 'pending_subclaims': []}
            members[key]['pending_subclaims'].append(deepcopy(obligation))
            assigned.add((*key, obligation['claim']['id']))
        content['candidate_group'] = list(members.values())
        content['candidate'] = content['candidate_group'][0]
        supplied = {bid for bid, _ in chunk}
        withheld = set(ids) - supplied
        content['attached_image_block_ids_in_order'] = [bid for bid, _ in chunk]
        content['unavailable_image_block_ids'] = sorted(set(content['unavailable_image_block_ids']) | withheld)
        content['allowed_source_refs'] = [ref for ref in content['allowed_source_refs'] if ref not in withheld]
        content['partition_scope'] = {'part': index + 1, 'parts': len(chunks),
            'withheld_image_ids': sorted(withheld), 'only_listed_pending_subclaims': True}
        child = deepcopy(packet)
        child['stage'] += f'_part{index + 1:02d}'
        child['output_filename'] = child['stage'] + '.json'
        child['image_hashes'] = {path: packet['image_hashes'][path] for _, path in chunk}
        child['messages'][0]['content'] += '''
PARTITION SCOPE OVERRIDE: Review ONLY the pending_subclaims listed for each
candidate in this packet. Reuse their IDs exactly and omit all other subclaims;
omission preserves earlier obligations. A candidate-level decision cannot close
parts assigned to another packet. Unattached images are NOT inspected. Do not
infer their contents from MD navigation summaries. Keep existing supported
findings and do not duplicate them. Report only supported additions for the
assigned claims; unresolved dependencies remain needs_data.
'''
        child['messages'][-1]['content'] = json.dumps(content, ensure_ascii=False)
        result.append(child)
    if assigned != expected:
        raise ValueError('Partition lost obligations')
    return result


def prepare(source, destination):
    if not source.is_file():
        raise ValueError('Source packet absent')
    packet = json.loads(source.read_text())
    children = partition(packet)
    destination.mkdir(parents=True, exist_ok=False)
    rows = []
    for child in children:
        path = destination / (child['stage'] + '.json')
        write(path, child)
        payload = json.loads(child['messages'][-1]['content'])
        size = sum(Path(p).stat().st_size for p in child['image_hashes'])
        chars = sum(len(m['content']) for m in child['messages'])
        budget = 24_000_000 if len(child['image_hashes']) == 1 else 16_000_000
        rows.append({'path': str(path), 'sha256': sha(path), 'image_bytes': size,
                     'message_characters': chars, 'transport_size_ready': size <= budget and chars < 1_000_000,
                     'candidate_ids': [c['source_id'] for c in payload['candidate_group']],
                     'assigned_subclaims': [{'origin': c['origin'], 'pointer': c['pointer'], 'id': o['claim']['id']}
                        for c in payload['candidate_group'] for o in c['pending_subclaims']]})
    write(destination / 'preparation.json', {'status': 'prepared_not_dispatched',
        'source_packet': str(source), 'source_packet_sha256': sha(source), 'requests': rows,
        'history_supplied': False, 'model_calls': 0, 'production_acceptance': False})
    return rows


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('source', type=Path)
    parser.add_argument('destination', type=Path)
    args = parser.parse_args()
    print(json.dumps(prepare(args.source.resolve(), args.destination.resolve()), ensure_ascii=False))
