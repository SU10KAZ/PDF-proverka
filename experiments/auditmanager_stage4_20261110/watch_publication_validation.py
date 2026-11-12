"""Validate completed publication responses locally; never dispatch model calls."""
import argparse
from collections import Counter
import json
import os
from pathlib import Path
import time

from .evaluate_publication_reconciliation import evaluate
from .run_primary_text_runtime_probe import sha, write


def collect(root, output):
    preparation = root / 'preparation.json'
    prepared = json.loads(preparation.read_text())
    state = json.loads((output / 'status.json').read_text())
    if sha(preparation) != state['preparation_sha256']:
        raise ValueError('Preparation changed')
    mapping = {}
    for row in prepared['requests']:
        request = json.loads(Path(row['path']).read_text())
        stage = request['stage']
        if stage in mapping:
            raise ValueError('Duplicate request stage')
        mapping[stage] = row['finding_id']
    successes = [a for a in state['attempts'] if a['success']]
    rows = []
    for attempt in successes:
        stage = attempt['stage']
        fid = mapping[stage]
        receipt_path = output / 'evaluations' / stage / 'publication_validation.json'
        receipt = json.loads(receipt_path.read_text()) if receipt_path.exists() else None
        if receipt is None or receipt.get('response_sha256') != attempt['response_sha256']:
            receipt = evaluate(root, output, fid)
        rows.append({'finding_id': fid, 'stage': stage,
                     'status': receipt['status'], 'error': receipt.get('error'),
                     'receipt_path': str(receipt_path), 'receipt_sha256': sha(receipt_path)})
    result = {'status': 'local_validation_snapshot', 'observed_at_unix': time.time(),
              'observed_collection_status': state['status'],
              'prepared_requests': len(prepared['requests']),
              'successful_responses': len(successes), 'evaluated_responses': len(rows),
              'counts': dict(Counter(r['status'] for r in rows)), 'rows': rows,
              'semantic_review_complete': False, 'production_acceptance': False,
              'model_calls_by_this_tool': 0}
    write(output / 'publication_batch_validation.json', result)
    return result, state


def watch(root, output, enabled):
    while True:
        result, state = collect(root, output)
        print(json.dumps({k: result[k] for k in ('observed_collection_status', 'successful_responses', 'counts')}, ensure_ascii=False), flush=True)
        if not enabled or state['status'] != 'running':
            return
        # A stale status file is not a live job. Never restart it from here.
        try:
            cmdline = Path(f"/proc/{state['pid']}/cmdline").read_bytes().replace(b'\0', b' ').decode()
        except (FileNotFoundError, ProcessLookupError):
            raise RuntimeError('Collection process missing; inspect existing run, do not redispatch')
        if 'run_runtime_probe_recovery' not in cmdline or str(output) not in cmdline:
            raise RuntimeError('Collection PID no longer identifies this run')
        time.sleep(10)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('root', type=Path)
    parser.add_argument('output', type=Path)
    parser.add_argument('--watch', action='store_true')
    args = parser.parse_args()
    watch(args.root.resolve(), args.output.resolve(), args.watch)
