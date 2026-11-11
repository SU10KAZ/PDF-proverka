"""Validate a sealed publication response without changing the original run."""
import argparse
import json
from pathlib import Path

from backend.app.pipeline.stages.findings_merge.publication_reconciliation import (
    InvalidPublication, reconcile_publication,
)
from .run_primary_text_runtime_probe import sha, write
from .run_runtime_probe_recovery import verify_packet


def evaluate(root, output):
    preparation = root / 'preparation.json'
    prepared = json.loads(preparation.read_text())
    state = json.loads((output / 'status.json').read_text())
    if state['preparation_sha256'] != sha(preparation):
        raise ValueError('Preparation changed')
    if state['status'] != 'completed_unadjudicated':
        raise ValueError('Collection is not successfully terminal')
    sources = prepared['sources']
    source_file = Path(sources['sealed_findings_path'])
    if sha(source_file) != sources['sealed_findings_sha256'] or sha(Path(sources['md_path'])) != sources['md_sha256']:
        raise ValueError('Primary input changed')
    if len(prepared['requests']) != 1:
        raise ValueError('Single packet evaluator only')
    request = verify_packet(prepared['requests'][0])
    envelope = json.loads(request['messages'][-1]['content'])
    packet = envelope['packet']
    findings = json.loads(source_file.read_text())['findings']
    matches = [f for f in findings if f.get('id') == packet['finding_id']]
    if len(matches) != 1:
        raise ValueError('Ambiguous original finding')
    attempts = [a for a in state['attempts'] if a['success']]
    if len(attempts) != 1:
        raise ValueError('Ambiguous model response')
    attempt = attempts[0]
    if attempt['request_sha256'] != prepared['requests'][0]['sha256']:
        raise ValueError('Response belongs to another request')
    response_file = Path(attempt['response_path'])
    if sha(response_file) != attempt['response_sha256']:
        raise ValueError('Model response changed')
    response = json.loads(response_file.read_text())['json_data']
    receipt = {'request_sha256': attempt['request_sha256'],
               'response_sha256': attempt['response_sha256'],
               'selection': prepared['selection'], 'semantic_verified': False,
               'production_acceptance': False}
    try:
        projection = reconcile_publication(matches[0], packet, response)
    except InvalidPublication as exc:
        receipt.update(status='invalid_publication_requires_inspection', error=str(exc))
        write(output / 'publication_validation.json', receipt)
        return receipt
    destination = output / 'publication_projection.json'
    write(destination, projection)
    receipt.update(status='structurally_valid_requires_source_review',
                   projection_path=str(destination), projection_sha256=sha(destination),
                   counts={key: len(projection[key]) for key in
                           ('technical_findings', 'questions', 'withdrawn', 'pending')})
    write(output / 'publication_validation.json', receipt)
    return receipt


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('root', type=Path)
    parser.add_argument('output', type=Path)
    args = parser.parse_args()
    print(json.dumps(evaluate(args.root.resolve(), args.output.resolve()), ensure_ascii=False, indent=2))
