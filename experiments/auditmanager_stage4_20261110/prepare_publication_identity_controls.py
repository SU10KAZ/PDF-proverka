"""Compare existing editorial candidate on readable identity controls; no gold input."""
import argparse
import hashlib
from copy import deepcopy
import json
from pathlib import Path

from backend.app.pipeline.stages.findings_merge.publication_reconciliation import (
    build_packet, fingerprint, EDITORIAL_PROVENANCE_INSTRUCTIONS, IDENTITY_SCOPE_INSTRUCTIONS,
)
from .run_primary_text_runtime_probe import sha, write
from .run_runtime_probe_recovery import verify_packet


def prepare(output, *, scope_candidate=False):
    instructions = IDENTITY_SCOPE_INSTRUCTIONS if scope_candidate else EDITORIAL_PROVENANCE_INSTRUCTIONS
    parents = [
        (Path('/tmp/auditmanager_publication_complete_graphic_details_20261112/preparation.json'), ['F-082', 'F-083']),
        (Path('/tmp/auditmanager_publication_remaining84_20261112/preparation.json'), ['F-085', 'F-071'] if scope_candidate else ['F-085']),
    ]
    result = None
    requests = []
    output.mkdir(parents=True, exist_ok=False)
    for parent, ids in parents:
        manifest = json.loads(parent.read_text())
        sources = manifest['sources']
        for name in ('sealed_findings', 'md'):
            if sha(Path(sources[name + '_path'])) != sources[name + '_sha256']:
                raise ValueError('Source changed')
        findings = json.loads(Path(sources['sealed_findings_path']).read_text())['findings']
        if result is None:
            result = {'sources': deepcopy(sources), 'requests': requests, 'parent_preparations': [],
                      'history_supplied': False, 'engineer_answers_supplied': False,
                      'coordinator_review_supplied': False,
                      'selection': 'DEV identity controls with unchanged source context; no reference answers. Original purpose is untrusted.',
                      'dispatched': False, 'production_acceptance': False}
        elif result['sources'] != sources:
            raise ValueError('Mixed source versions')
        result['parent_preparations'].append({'path': str(parent), 'sha256': sha(parent)})
        for fid in ids:
            row = next(r for r in manifest['requests'] if r['finding_id'] == fid)
            request = verify_packet(row)
            old_request = deepcopy(request)
            envelope = json.loads(request['messages'][-1]['content'])
            old_packet = envelope['packet']
            finding = next(f for f in findings if f['id'] == fid)
            packet = build_packet(finding, old_packet['text_sources'], old_packet['attached_graphic_refs'], contract_version=3)
            assert packet['input_units'] == old_packet['input_units']
            # Keep image aliases, attachment order, full-grid geometry and context limits.
            envelope.update(packet=packet, packet_sha256=fingerprint(packet))
            request['stage'] = f'publication_identity_{fid[2:]}'
            request['messages'] = [
                {'role': 'system', 'content': instructions},
                {'role': 'user', 'content': json.dumps(envelope, ensure_ascii=False)},
            ]
            assert request['image_hashes'] == old_request['image_hashes']
            path = output / (request['stage'] + '.json')
            write(path, request)
            new = {**deepcopy(row), 'path': str(path), 'sha256': sha(path),
                   'baseline_request_path': row['path'], 'baseline_request_sha256': row['sha256']}
            verify_packet(new)
            requests.append(new)
    result['candidate_instruction_sha256'] = hashlib.sha256(instructions.encode()).hexdigest()
    result['preparer_sha256'] = sha(Path(__file__))
    write(output / 'preparation.json', result)
    return result


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('output', type=Path)
    parser.add_argument('--scope-candidate', action='store_true')
    args = parser.parse_args()
    result = prepare(args.output.resolve(), scope_candidate=args.scope_candidate)
    print(json.dumps({'packets': len(result['requests']), 'instruction_sha256': result['candidate_instruction_sha256']}, ensure_ascii=False))
