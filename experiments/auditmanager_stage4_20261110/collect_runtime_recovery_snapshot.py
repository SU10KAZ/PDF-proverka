"""Replay sealed completed recovery responses locally, including a live queue prefix.

This is a structural diagnostic, never an accepted project audit. No model calls.
"""
import argparse
from collections import Counter
import json
import re
from pathlib import Path
import subprocess

from .run_primary_text_runtime_probe import sha
from .run_runtime_probe_recovery import verify_packet


def collect(base_review, result_roots, output):
    from backend.app.pipeline.stages.prepare.candidate_recovery import validate_recovery_payload
    from backend.app.pipeline.stages.prepare.codex_targeted_findings import CodexTargetedPass, combine_findings_with_targeted
    from backend.app.pipeline.stages.findings_merge.subclaim_coverage import pending_subclaims

    base = json.loads(base_review.read_text())
    source = Path(base['response_path'])
    if sha(source) != base['response_sha256']:
        raise ValueError('Base response seal differs')
    final = json.loads(source.read_text())['json_data']
    request_path = source.parent / 'request.json'
    if sha(request_path) != base['request_sha256']:
        raise ValueError('Base request seal differs')
    body = json.loads(request_path.read_text())['messages'][1]['content']
    blocks_text, rest = body.split('\n\n## 01_blocks_analysis.json:\n\n', 1)[1].split('\n\n## Primary source manifest\n\n', 1)
    manifest = json.loads(rest.split('\n\n## Authoritative primary TEXT blocks', 1)[0])
    known = {re.search(r'\]:\s*(\S+)', item['heading'])[1] for item in manifest['included_text_spans']}
    known.update(b['block_id'] for b in json.loads(blocks_text).get('block_analyses', []) if isinstance(b.get('block_id'), str))
    payloads, sources, snapshots, seen = [], [], [], set()
    for root in result_roots:
        # Read once: later progress cannot silently change this snapshot.
        status_bytes = (root / 'status.json').read_bytes()
        status = json.loads(status_bytes)
        snapshots.append({'root': str(root), 'status': status['status'],
                          'completed_packets': status['completed_packets'],
                          'planned_ready': status['planned_ready']})
        for row in status['attempts']:
            if not row['success']:
                continue
            if row['stage'] in seen:
                raise ValueError('Repeated successful stage')
            seen.add(row['stage'])
            request = verify_packet({'path': row['request_path'], 'sha256': row['request_sha256']})
            response_path = Path(row['response_path'])
            if sha(response_path) != row['response_sha256']:
                raise ValueError('Recovery response seal differs')
            response = json.loads(response_path.read_text())
            packet = json.loads(request['messages'][-1]['content'])
            known.update(packet['allowed_source_refs'])
            targeted = CodexTargetedPass(stage=request['stage'], output_filename=request['output_filename'],
                messages=request['messages'], image_paths=tuple(request['image_hashes']))
            validated = validate_recovery_payload(targeted, response['json_data'])
            payloads.append((request['stage'], validated))
            sources.append(row)
    merged = combine_findings_with_targeted(final, payloads)
    pending = pending_subclaims(merged, known, base['coverage'])
    reports = merged.get('candidate_recovery_reports', [])
    summary = {'successful_packets_replayed': len(payloads), 'base_findings': len(final.get('findings', [])),
        'merged_findings': len(merged['findings']), 'pending_candidates': len(pending),
        'pending_subclaims': sum(map(len, pending.values())),
        'pending_reasons': dict(Counter(r['reason'] for rows in pending.values() for r in rows)),
        'published_addenda': sum(len(r.get('addenda_publication', {}).get('applied', [])) for r in reports),
        'target_binding_repairs': sum(len(r.get('finding_target_binding', {}).get('repairs', [])) for r in reports),
        'unresolved_target_bindings': sum(len(r.get('finding_target_binding', {}).get('unresolved', [])) for r in reports)}
    artifact = {'status': 'structural_snapshot_unadjudicated', 'summary': summary,
        'git_commit': subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip(),
        'base_review': str(base_review), 'base_review_sha256': sha(base_review),
        'queue_snapshots': snapshots, 'sources': sources, 'combined': merged,
        'pending_subclaims': [{'origin': k[0], 'pointer': k[1], 'claims': v} for k, v in pending.items()],
        'production_acceptance': False, 'limits': [
            'Running queues contribute only the completed prefix, not a complete project result.',
            'Publication and address checks do not establish factual truth or engineering usefulness.',
            'Known false symbol finding may remain in this diagnostic; do not publish as accepted report.']}
    with output.open('x') as stream:
        json.dump(artifact, stream, ensure_ascii=False, indent=2)
        stream.write('\n')
    return summary


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('base_review', type=Path)
    parser.add_argument('output', type=Path)
    parser.add_argument('result_roots', type=Path, nargs='+')
    args = parser.parse_args()
    print(json.dumps(collect(args.base_review, args.result_roots, args.output), ensure_ascii=False))
