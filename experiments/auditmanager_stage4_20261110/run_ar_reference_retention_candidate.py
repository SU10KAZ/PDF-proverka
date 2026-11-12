"""Prepare or explicitly execute a known-source reference selection/repair pilot."""
import argparse
import asyncio
import hashlib
import json
import os
from pathlib import Path
from .reference_retention_workflow import selection_request, run_reference_retention
from .reference_retention_admission_bridge import prepare_reference_adjudication
from .run_itp_pipe_operations import immutable
from .observe_current_method_collection import observe
from backend.app.pipeline.stages.findings_verify.source_review_workflow import direct_cli_executor
from backend.app.pipeline.stages.findings_verify.source_review_evidence import evidence_digest

BASE = Path(__file__).resolve().parent
DEFAULT_ROOT = Path('/tmp/auditmanager_ar_reference_retention_candidate_v002_20261112')


def prepare(root):
    root.mkdir(parents=True, exist_ok=True)
    source = BASE / 'ar_rope_bundle_source_evidence/bundle.json'
    candidate = BASE / 'ar_rope_current_release_evidence/adjudication/candidate.json'
    bundle = json.loads(source.read_text())
    entry = json.loads(candidate.read_text())['entry']
    fields, response, md = entry['decision']['approved_fields'], entry['response'], bundle['md']
    request = selection_request(fields, response, md)
    inputs = [source, candidate, Path(__file__), BASE/'reference_retention_workflow.py', BASE/'reference_retention_candidate.py', BASE/'reference_retention_admission_bridge.py', BASE/'ar_rope_current_release_evidence/adjudication/request.json']
    binding = {'model': 'codex/gpt-6-astra', 'reasoning_effort': 'high',
        'maximum_new_calls': 2, 'automatic_retries': 0, 'known_source_control': True,
        'prior_model_review_supplied': True, 'engineer_answers_supplied': False,
        'expected_reference_or_control_answer_supplied': False,
        'message_chars': sum(len(m['content']) for m in request['messages']),
        'image_count': 0, 'request_sha256': evidence_digest(request),
        'inputs': {str(p): hashlib.sha256(p.read_bytes()).hexdigest() for p in inputs}}
    immutable(root/'selection_request.json', request)
    immutable(root/'binding.json', binding)
    return fields, response, md, binding


async def main(root, execute):
    fields, response, md, binding = prepare(root)
    print(json.dumps({k:v for k,v in binding.items() if k != 'inputs'}, ensure_ascii=False), flush=True)
    if not execute:
        return
    # Avoid quota/time contamination of the currently frozen comparison.
    current = observe()
    if current['collection_status'] == 'running' or any(r['process']['live'] for r in current['runs']):
        raise RuntimeError('comparison_in_progress: candidate prepared but not dispatched')
    os.environ['CODEX_TRANSIENT_RETRIES'] = '0'
    executor = direct_cli_executor('codex/gpt-6-astra', project_id='AR-reference-retention-candidate', reasoning_effort='high')
    async def invoke(request, phase):
        for path, expected in binding['inputs'].items():
            if hashlib.sha256(Path(path).read_bytes()).hexdigest() != expected:
                raise ValueError('candidate_inputs_changed')
        work = root / phase
        work.mkdir(exist_ok=True)
        immutable(work/'request.json', request)
        digest = evidence_digest(request)
        if (work/'result.json').exists():
            result = json.loads((work/'result.json').read_text())
            expected = {'request_sha256': digest, 'result_sha256': evidence_digest(result)}
            if json.loads((work/'result_binding.json').read_text()) != expected:
                raise ValueError('stale_candidate_result')
        else:
            with (work/'attempt.json').open('x') as f:
                json.dump({'request_sha256': digest, 'model': binding['model'], 'reasoning_effort': binding['reasoning_effort']}, f)
            result = await executor(request, phase)
            immutable(work/'result.json', result)
            immutable(work/'result_binding.json', {'request_sha256': digest, 'result_sha256': evidence_digest(result)})
        if result.get('is_error') or not isinstance(result.get('json_data'), dict):
            raise ValueError('unusable_provider_result')
        return result['json_data']
    result = await run_reference_retention(fields, response, md, invoke=invoke)
    immutable(root/'candidate_result.json', result)
    if result['status'] in ('candidate_requires_semantic_review', 'literal_retention_verified'):
        entry = json.loads((BASE/'ar_rope_current_release_evidence/adjudication/candidate.json').read_text())['entry']
        prior_request = json.loads((BASE/'ar_rope_current_release_evidence/adjudication/request.json').read_text())
        bridge = prepare_reference_adjudication(prior_request, entry, result, md)
        immutable(root/'fresh_adjudication_preparation.json', bridge)
        print(json.dumps({'fresh_adjudication_status': bridge['status'], 'dispatched': False}), flush=True)
    print(json.dumps({'status': result['status'], 'semantic_acceptance': False, 'automatic_release': False}), flush=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--root', type=Path, default=DEFAULT_ROOT)
    parser.add_argument('--execute', action='store_true')
    args = parser.parse_args()
    asyncio.run(main(args.root, args.execute))
