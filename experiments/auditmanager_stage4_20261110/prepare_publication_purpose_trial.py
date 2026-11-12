"""Prepare DEV controls for purpose-aware publication, preserving source packets."""
from copy import deepcopy
import json
from pathlib import Path

from backend.app.pipeline.stages.findings_merge.publication_reconciliation import (
    build_packet, fingerprint, PURPOSE_AWARE_INSTRUCTIONS,
)
from .run_primary_text_runtime_probe import sha, write
from .run_runtime_probe_recovery import verify_packet


def prepare(destination, selected_ids=None):
    base = Path('/tmp/auditmanager_publication_remaining84_20261112/preparation.json')
    followup = Path('/tmp/auditmanager_publication_F008_context1_20261112/preparation.json')
    parents = [(base, ['F-004', 'F-010', 'F-012', 'F-020']), (followup, ['F-008'])]
    if selected_ids is not None:
        allowed = {fid for _, ids in parents for fid in ids}
        if not selected_ids or not set(selected_ids) <= allowed:
            raise ValueError('Unknown or empty control selection')
        parents = [(path, [fid for fid in ids if fid in selected_ids]) for path, ids in parents]
    destination.mkdir(parents=True, exist_ok=False)
    result = None
    requests = []
    for parent, ids in parents:
        prepared = json.loads(parent.read_text())
        sources = prepared['sources']
        for name in ('sealed_findings', 'md'):
            assert sha(Path(sources[name + '_path'])) == sources[name + '_sha256']
        findings = json.loads(Path(sources['sealed_findings_path']).read_text())['findings']
        if result is None:
            result = {'history_supplied': False, 'engineer_answers_supplied': False,
                      'coordinator_review_supplied': False, 'sources': sources,
                      'selection': 'DEV-selected routing controls; prior purpose from current-run finding only; same source context as each selected baseline. Not blind evaluation.',
                      'production_acceptance': False, 'requests': requests,
                      'parent_preparations': [], 'dispatched': False}
        result['parent_preparations'].append({'path': str(parent), 'sha256': sha(parent)})
        for fid in ids:
            row = next(r for r in prepared['requests'] if r['finding_id'] == fid)
            request = verify_packet(row)
            old = json.loads(request['messages'][-1]['content'])
            finding = next(f for f in findings if f['id'] == fid)
            packet = build_packet(finding, old['packet']['text_sources'],
                                  old['packet']['attached_graphic_refs'], contract_version=3)
            assert packet['input_units'] == old['packet']['input_units']
            envelope = {key: deepcopy(old[key]) for key in
                        ('context_complete', 'unavailable_linked_sources', 'context_limit') if key in old}
            envelope.update(packet=packet, packet_sha256=fingerprint(packet))
            # Do not inherit previous model response or context-followup answer hints.
            request = deepcopy(request)
            request['stage'] = f"publication_purpose_{fid[2:]}"
            request['messages'] = [{'role': 'system', 'content': PURPOSE_AWARE_INSTRUCTIONS},
                                   {'role': 'user', 'content': json.dumps(envelope, ensure_ascii=False)}]
            path = destination / (request['stage'] + '.json')
            write(path, request)
            new = {**deepcopy(row), 'path': str(path), 'sha256': sha(path),
                   'baseline_request_path': row['path'], 'baseline_request_sha256': row['sha256']}
            verify_packet(new)
            requests.append(new)
    write(destination / 'preparation.json', result)
    return result


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('destination', type=Path)
    parser.add_argument('--finding-id', action='append')
    args = parser.parse_args()
    result = prepare(args.destination.resolve(), args.finding_id)
    print(json.dumps([{'finding_id': r['finding_id'], 'path': r['path'],
                       'images': len(verify_packet(r)['image_hashes'])} for r in result['requests']], ensure_ascii=False))
