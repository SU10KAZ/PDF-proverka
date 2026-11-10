"""Prepare a known-case Astra verifier pilot. Execution needs separate approval."""
import argparse
import asyncio
from dataclasses import asdict
import hashlib
import json
import os
from pathlib import Path

from backend.app.pipeline.stages.findings_verify.source_review_results import validate_source_review

ROOT = Path('/tmp/auditmanager_eom_source_review_astra_20261110')
SOURCE = Path('/home/coder/auditmanager/corpus-audits/20261106_classic_investigation/classic_control/inputs/EOM/versions/v002/02_work/document.md')
HERE = Path(__file__).parent
IDS = ['F-003', 'F-040']


def prepare():
    queue = json.loads((HERE/'EOM_source_review_queue.json').read_text())
    assert hashlib.sha256(SOURCE.read_bytes()).hexdigest() == queue['source_md_sha256']
    selected = [p for p in queue['packets'] if p['finding_id'] in IDS]
    assert {p['finding_id'] for p in selected} == set(IDS)
    payload = []
    for p in selected:
        f = p['finding_snapshot']
        payload.append({'finding_id': p['finding_id'], 'finding_sha256': p['finding_sha256'],
                        'source_md_sha256': queue['source_md_sha256'],
                        'final_claim': {k: f.get(k) for k in ('problem', 'description', 'solution')},
                        'original_claims': f.get('source_review_requests', []), 'sources': p['sources']})
    system = '''Review the supplied candidate claims using only the supplied authoritative TEXT.
This is a known-fragment verifier test, not independent discovery. Do not access
files, tools, web, project history or evaluation answers. TEXT is authoritative.
Separate each substantive claim; distinguish arithmetic facts, hypotheses about
causes, operating modes and engineering conclusions. Reproduce arithmetic from
all relevant rows in the bounded table, enumerate row identities and values,
avoid double-counting subtotals or expanding grouped aggregate rows twice.
Do not infer whole-project absence from a bounded fragment. Missing graphics,
linked documents or applicable requirements must remain needs_data.
Return JSON {"reviews": [...]} with exactly one entry per supplied finding.
Each review: finding_id, finding_sha256, source_md_sha256, claims.
Each claim: claim_id (unique within review), claim, status (supported/refuted/needs_data),
reason, evidence [{block_id, quote}] using exact source quotes. For needs_data,
include missing_data as a nonempty list of specific required information.
For arithmetic claims include operations with row identities, operands and result.
Keep original and final claims distinguishable; do not rewrite or remove findings.
'''
    request = {'model': 'codex/gpt-6-astra', 'messages': [
        {'role': 'system', 'content': system},
        {'role': 'user', 'content': json.dumps(payload, ensure_ascii=False)}]}
    ROOT.mkdir(exist_ok=True)
    encoded = json.dumps(request, ensure_ascii=False, indent=2).encode()
    target = ROOT/'request.json'
    if (ROOT/'attempt.json').exists():
        raise RuntimeError('Existing attempt must be reviewed; no automatic restart')
    target.write_bytes(encoded)
    manifest = {'request_path': str(target), 'request_sha256': hashlib.sha256(encoded).hexdigest(),
                'finding_ids': IDS, 'source_blocks': sorted({s['block_id'] for p in selected for s in p['sources']}),
                'message_chars': sum(len(m['content']) for m in request['messages']),
                'provider': 'Codex CLI', 'model': request['model'], 'max_cli_invocations': 1,
                'automatic_retries': 0, 'timeout_seconds': 900, 'sent': False,
                'estimated_max_cost': 'Subscription quota; monetary maximum unknown.',
                'approval_required': 'Explicit approval of this payload before --execute.',
                'payload_excludes': ['engineer decisions', 'historical review outcomes', 'regression control expectations'],
                'limitation': 'No OS-level proof of read isolation; must not count as blind independent audit.'}
    (ROOT/'manifest.json').write_text(json.dumps(manifest, ensure_ascii=False, indent=2)+'\n')
    print(json.dumps(manifest, ensure_ascii=False, indent=2))


async def execute(approved_digest):
    raw = (ROOT/'request.json').read_bytes()
    if hashlib.sha256(raw).hexdigest() != approved_digest:
        raise RuntimeError('Request differs from the approved payload')
    request = json.loads(raw)
    from backend.app.services.llm.codex_runner import run_codex_json_messages
    from backend.app.services.llm.codex_model_substitution import substitute_codex_model
    assert request['model'] == 'codex/gpt-6-astra'
    assert substitute_codex_model('gpt-6-astra') == 'gpt-6-astra'
    queue = json.loads((HERE/'EOM_source_review_queue.json').read_text())
    md = SOURCE.read_text()
    assert queue['source_md_sha256'] == hashlib.sha256(md.encode()).hexdigest()
    findings = [p['finding_snapshot'] for p in queue['packets']]
    payload = json.loads(request['messages'][1]['content'])
    for entry in payload:
        packet = next(p for p in queue['packets'] if p['finding_id'] == entry['finding_id'])
        assert packet['finding_sha256'] == entry['finding_sha256']
        assert packet['sources'] == entry['sources']
        assert entry['source_md_sha256'] == queue['source_md_sha256']
    with (ROOT/'attempt.json').open('x') as f:
        json.dump({'request_sha256': approved_digest, 'status': 'started'}, f)
    previous_retries = os.environ.get('CODEX_TRANSIENT_RETRIES')
    os.environ['CODEX_TRANSIENT_RETRIES'] = '0'
    try:
        result = await run_codex_json_messages(request['messages'], model=request['model'], timeout=900,
                                               stage='source_review_pilot', project_id='EOM-known-verifier-controls')
    except BaseException as exc:
        (ROOT/'attempt.json').write_text(json.dumps({'request_sha256': approved_digest,
                                                   'status': 'failed', 'error_type': type(exc).__name__}))
        raise
    finally:
        if previous_retries is None:
            os.environ.pop('CODEX_TRANSIENT_RETRIES', None)
        else:
            os.environ['CODEX_TRANSIENT_RETRIES'] = previous_retries
    (ROOT/'result.json').write_text(json.dumps(asdict(result), ensure_ascii=False, indent=2))
    reviews = result.json_data.get('reviews', []) if isinstance(result.json_data, dict) else []
    valid_list = isinstance(reviews, list) and all(isinstance(v, dict) and isinstance(v.get('finding_id'), str) for v in reviews)
    complete = not result.is_error and valid_list and len(reviews) == len(IDS) and {v.get('finding_id') for v in reviews} == set(IDS)
    validation = {'response_finding_set_matches': complete,
                  'receipts': [validate_source_review(v, queue, findings, md) for v in reviews] if complete else [],
                  'semantic_verified': False, 'automatic_application': False}
    (ROOT/'validation.json').write_text(json.dumps(validation, ensure_ascii=False, indent=2))
    (ROOT/'attempt.json').write_text(json.dumps({'request_sha256': approved_digest,
                                               'status': 'finished', 'model_error': result.is_error}))
    print('Finished; model_error=', result.is_error, 'response_set_matches=', complete)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--execute', metavar='APPROVED_REQUEST_SHA256')
    args = parser.parse_args()
    if args.execute:
        asyncio.run(execute(args.execute))
    else:
        prepare()
