"""Same-input adjudication controls; only generic output policy changes.

Saved source reviews are untrusted inputs. No previous adjudication, engineer
answer or coordinator verdict enters either request.
"""
import argparse
import asyncio
import json
import os
from pathlib import Path
from backend.app.pipeline.stages.findings_verify.source_review_workflow import prepare_adjudication_request, direct_cli_executor
from backend.app.pipeline.stages.findings_verify.source_review_admission import apply_review_admissions
from backend.app.pipeline.stages.findings_verify.source_review_evidence import evidence_digest
from .run_primary_text_runtime_probe import sha, write
from .run_publication_f071_source_review import immutable


def prepare(root):
    root.mkdir(exist_ok=True)
    cases = []
    for number in ('083', '071'):
        source = Path(f'/tmp/auditmanager_publication_f{number}_source_review_20261112')
        saved = json.loads((source / 'workflow/candidate.json').read_text())['entry']
        wire = json.loads((source / 'workflow/00_review/request.json').read_text())['request']
        response = json.loads((source / 'workflow/00_review/result.json').read_text())['json_data']
        assert response == saved['response']
        request = prepare_adjudication_request(wire, response)
        assert request['image_paths'] == wire['image_paths']
        assert request['image_bindings'] == wire['image_bindings']
        payload = json.loads(request['messages'][1]['content'])
        assert payload['source_packet'] == json.loads(wire['messages'][1]['content'])
        case = root / ('F-' + number); case.mkdir(exist_ok=True)
        immutable(case / 'request.json', request)
        binding = {'source_root': str(source), 'request_sha256': sha(case / 'request.json'),
            'source_review_sha256': sha(source / 'workflow/00_review/result.json'),
            'images': {p: sha(Path(p)) for p in request['image_paths']},
            'model': 'codex/gpt-6-astra', 'reasoning_effort': 'high',
            'history_or_engineer_answers_supplied': False, 'same_source_review_and_context': True,
            'message_chars': sum(len(m['content']) for m in request['messages'])}
        immutable(case / 'binding.json', binding)
        cases.append((case, request, saved['queue'], response, binding))
    return cases


async def main(root, execute):
    cases = prepare(root)
    print('PREPARED', [(p.name, b['message_chars'], len(b['images'])) for p, _, _, _, b in cases], flush=True)
    if not execute:
        return
    os.environ['CODEX_TRANSIENT_RETRIES'] = '0'
    invoke = direct_cli_executor('codex/gpt-6-astra', project_id='identity-adjudication-pair')
    write(root / 'process.json', {'status': 'running', 'pid': os.getpid()})
    for case, request, queue, response, binding in cases:
        for path, expected in binding['images'].items():
            assert sha(Path(path)) == expected
        attempt = case / 'attempt.json'
        with attempt.open('x') as stream:
            json.dump({'status': 'started', 'request_sha256': binding['request_sha256']}, stream)
        try:
            result = await invoke(request, 'adjudication')
            write(case / 'result.json', result)
            if result.get('is_error') or not isinstance(result.get('json_data'), dict):
                raise ValueError('unusable_adjudication_result')
            entry = {'queue': queue, 'response': response, 'decision': result['json_data']}
            source = Path(binding['source_root'])
            finding = json.loads((source / 'finding.json').read_text())
            manifest = json.loads((source / 'manifest.json').read_text())
            md = Path(manifest['md_path']); assert sha(md) == manifest['md_sha256']
            output, receipts = apply_review_admissions({'findings': [finding]}, [entry], md.read_text())
            write(case / 'candidate.json', {'entry': entry, 'receipts': receipts, 'semantic_acceptance': False})
            write(case / 'local_output.json', output)
            write(attempt, {'status': 'finished', 'request_sha256': binding['request_sha256'],
                            'result_sha256': evidence_digest(result)})
            print(case.name, receipts, flush=True)
        except Exception as exc:
            write(root / 'process.json', {'status': 'failed', 'pid': os.getpid(), 'case': case.name, 'error': str(exc)})
            raise
    write(root / 'process.json', {'status': 'finished_unreviewed', 'pid': os.getpid()})


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('root', type=Path)
    parser.add_argument('--execute', action='store_true')
    args = parser.parse_args()
    asyncio.run(main(args.root.resolve(), args.execute))
