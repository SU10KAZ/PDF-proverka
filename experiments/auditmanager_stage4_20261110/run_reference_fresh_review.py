"""One recorded adjudication after reference repair; never releases a report."""
import argparse
import asyncio
from copy import deepcopy
import hashlib
import json
import os
from pathlib import Path
from .run_itp_pipe_operations import immutable
from .observe_current_method_collection import observe
from .reference_retention_review_gate import validate_reference_review
from backend.app.pipeline.stages.findings_verify.source_review_evidence import evidence_digest
from backend.app.pipeline.stages.findings_verify.source_review_admission import apply_review_admissions
from backend.app.pipeline.stages.findings_verify.source_review_workflow import direct_cli_executor


async def run(preparation, entry, md, root, *, invoke, executor_id, collection):
    root = Path(root)
    root.mkdir(parents=True, exist_ok=True)
    if preparation.get('status') != 'prepared_for_fresh_semantic_adjudication':
        raise ValueError('reference_review_not_prepared')
    request = preparation['request']
    payload = json.loads(request['messages'][1]['content'])
    if (preparation['original_response_sha256'] != evidence_digest(entry['response'])
            or payload['untrusted_review'] != entry['response']
            or entry['response'].get('source_md_sha256') != hashlib.sha256(md.encode()).hexdigest()):
        raise ValueError('stale_fresh_review_input')
    binding = {'preparation': evidence_digest(preparation), 'entry': evidence_digest(entry),
               'md': evidence_digest(md), 'executor_id': executor_id}
    immutable(root/'binding.json', binding)
    immutable(root/'request.json', request)
    result_path = root/'result.json'
    if result_path.exists():
        result = json.loads(result_path.read_text())
        if json.loads((root/'result_binding.json').read_text()) != {
                'request_sha256': evidence_digest(request), 'result_sha256': evidence_digest(result)}:
            raise ValueError('stale_fresh_review_result')
    else:
        if collection['collection_status'] == 'running' or any(r['process']['live'] for r in collection['runs']):
            raise RuntimeError('comparison_in_progress')
        images = request.get('image_bindings', [])
        if request.get('image_paths', []) != [i['path'] for i in images]:
            raise ValueError('unbound_review_images')
        for item in images:
            if hashlib.sha256(Path(item['path']).read_bytes()).hexdigest() != item['sha256']:
                raise ValueError('changed_review_image')
        with (root/'attempt.json').open('x') as stream:
            json.dump({'request_sha256': evidence_digest(request), 'executor_id': executor_id}, stream)
        result = await invoke(request, 'reference_fresh_adjudication')
        immutable(result_path, result)
        immutable(root/'result_binding.json', {
            'request_sha256': evidence_digest(request), 'result_sha256': evidence_digest(result)})
    if result.get('is_error') or not isinstance(result.get('json_data'), dict):
        raise ValueError('unusable_fresh_review_result')
    decision = result['json_data']
    original = next(p['finding_snapshot'] for p in entry['queue']['packets']
                    if p['finding_id'] == entry['response']['finding_id'])
    report = {'semantic_acceptance': False, 'automatic_release': False}
    try:
        validate_reference_review(preparation, decision, entry['response'], original, md)
    except ValueError as exc:
        report.update(status='held', reason=str(exc))
    else:
        candidate = deepcopy(entry)
        candidate['decision'] = decision
        _, receipts = apply_review_admissions({'findings': [original]}, [candidate], md)
        report.update(status='candidate_ready' if receipts[0]['status'] == 'applied' else 'held',
                      admission_receipts=receipts, entry=candidate)
    immutable(root/'candidate.json', report)
    return report


async def main(args):
    base = Path(__file__).parent
    preparation = json.loads((args.pilot_root/'fresh_adjudication_preparation.json').read_text())
    entry = json.loads((base/'ar_rope_current_release_evidence/adjudication/candidate.json').read_text())['entry']
    md = json.loads((base/'ar_rope_bundle_source_evidence/bundle.json').read_text())['md']
    if not args.execute:
        print(json.dumps({'status': 'prepared_only', 'message_chars': sum(len(m['content']) for m in preparation['request']['messages']),
                          'images': len(preparation['request']['image_paths']), 'maximum_calls': 1}))
        return
    os.environ['CODEX_TRANSIENT_RETRIES'] = '0'
    executor = direct_cli_executor('codex/gpt-6-astra', project_id='AR-reference-fresh-review', reasoning_effort='high')
    report = await run(preparation, entry, md, args.output, invoke=executor,
                       executor_id='codex/gpt-6-astra:high', collection=observe())
    print(json.dumps({'status': report['status'], 'automatic_release': False}))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--pilot-root', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--execute', action='store_true')
    asyncio.run(main(parser.parse_args()))
