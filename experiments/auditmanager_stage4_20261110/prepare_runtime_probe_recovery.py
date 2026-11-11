"""Build source-backed recovery packets from a sealed runtime response; no calls."""
import argparse
import json
from pathlib import Path
import shutil

from .run_primary_text_runtime_probe import OLD, SOURCES, sha, write
from .observe_baseline_collection import process


def prepare(root, destination):
    from backend.app.pipeline.stages.prepare.candidate_recovery import build_recovery_passes
    state = json.loads((root / 'status.json').read_text())
    if state.get('status') != 'completed_unadjudicated':
        raise ValueError('Wait for the terminal merge response')
    if process(state['pid'], 'run_primary_text_runtime_probe')['live'] is not False:
        raise ValueError('Probe process still active')
    preparation = json.loads((root / 'preparation.json').read_text())
    if sha(root / 'request.json') != preparation['request_sha256']:
        raise ValueError('Request changed')
    successful = [a for a in state['attempts'] if not a['is_error'] and a['json_object']]
    if len(successful) != 1:
        raise ValueError('Expected one successful response')
    attempt = successful[0]
    response_path = root / f"attempt_{attempt['attempt']}.json"
    if sha(response_path) != attempt['sha256']:
        raise ValueError('Response changed')
    final = json.loads(response_path.read_text())['json_data']
    destination.mkdir(parents=True, exist_ok=False)
    output = destination / 'work'
    output.mkdir()
    for name in ('02_text_analysis.json', '01_blocks_analysis.json'):
        shutil.copy2(root / 'input_analyses' / name, output / name)
    write(output / '03_findings.json', final)
    source = OLD / 'inputs/AK/versions/v001/03_analysis/runs/comparison'
    shutil.copy2(source / 'document_graph.json', output / 'document_graph.json')
    shutil.copytree(source / 'blocks_stage02_100', output / 'blocks_stage02_100')
    md = destination / 'document.md'
    shutil.copy2(SOURCES / '02_work/document.md', md)
    if sha(md) != preparation['md_sha256']:
        raise ValueError('MD differs from merge sources')
    passes = build_recovery_passes(output, md)
    requests = destination / 'requests'
    requests.mkdir()
    rows = []
    for targeted in passes:
        payload = json.loads(targeted.messages[-1]['content'])
        images = {str(p): sha(p) for p in targeted.image_paths}
        packet = {'stage': targeted.stage, 'output_filename': targeted.output_filename,
                  'messages': targeted.messages, 'image_hashes': images,
                  'model': 'codex/gpt-6-astra', 'reasoning_effort': 'high', 'allowed_tools': ''}
        path = requests / f'{targeted.stage}.json'
        write(path, packet)
        chars = sum(len(m['content']) for m in targeted.messages)
        image_bytes = sum(Path(p).stat().st_size for p in images)
        rows.append({'path': str(path), 'sha256': sha(path), 'message_characters': chars,
                     'images': len(images), 'image_bytes': image_bytes,
                     'candidate_ids': [c['source_id'] for c in payload['candidate_group']],
                     'pending_subclaims': sum(len(c.get('pending_subclaims', [])) for c in payload['candidate_group']),
                     'transport_size_ready': chars < 1_000_000 and image_bytes <= 16_000_000})
    receipt = {'status': 'recovery_prepared_not_dispatched', 'source_response_sha256': sha(response_path),
               'source_request_sha256': preparation['request_sha256'], 'requests': rows,
               'model_calls': 0, 'history_supplied': False, 'production_acceptance': False,
               'limits': ['Source-backed requests are not completed reviews.',
                          'Existing linked findings are context; they are not automatically verified.',
                          'Oversized image packets must be addressed before dispatch.']}
    write(destination / 'preparation.json', receipt)
    return {'packets': len(rows), 'size_ready': sum(r['transport_size_ready'] for r in rows),
            'candidates': sum(len(r['candidate_ids']) for r in rows)}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('root', type=Path)
    parser.add_argument('destination', type=Path)
    args = parser.parse_args()
    print(json.dumps(prepare(args.root.resolve(), args.destination.resolve())))
