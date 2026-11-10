"""Real EOM prescan -> source selection -> model -> final receipt control.

Historical findings remain local and are used only to test preservation after
sealing the answer. No engineer answers or expected verdicts enter the request.
"""
import argparse
import asyncio
from copy import deepcopy
from dataclasses import asdict
import hashlib
import json
import os
from pathlib import Path

from backend.app.pipeline.stages.findings_verify.source_review_evidence import evidence_digest
from backend.app.pipeline.stages.text_analysis.md_prescan import augment_text_analysis_file
from backend.app.pipeline.stages.prepare.candidate_recovery import build_recovery_passes, validate_recovery_payload
from backend.app.pipeline.stages.prepare.codex_targeted_findings import combine_findings_with_targeted
from backend.app.pipeline.stages.findings_merge.candidate_coverage import write_candidate_coverage

ROOT = Path('/tmp/auditmanager_eom_prescan_recovery_control_20261110')
BASE = Path('/home/coder/auditmanager/corpus-audits/20261106_classic_investigation/classic_control/inputs/EOM/versions/v002')


def prepare():
    ROOT.mkdir(exist_ok=True)
    md = BASE / '02_work/document.md'
    source = BASE / '03_analysis/runs/control'
    crops = ROOT / 'blocks_stage02_100'
    if not crops.exists():
        crops.symlink_to(source / 'blocks_stage02_100', target_is_directory=True)
    for name, value in [('02_text_analysis.json', {'text_findings': []}),
                        ('01_blocks_analysis.json', {}), ('03_findings.json', {'findings': []})]:
        (ROOT / name).write_text(json.dumps(value))
    (ROOT / 'document_graph.json').write_bytes((source / 'document_graph.json').read_bytes())
    augment_text_analysis_file(ROOT / '02_text_analysis.json', md)
    requests = build_recovery_passes(ROOT, md)
    request, = [r for r in requests if json.loads(r.messages[-1]['content'])['candidate']['origin'] == 'prescan']
    payload = {'messages': request.messages, 'image_paths': [str(p) for p in request.image_paths]}
    manifest = {'request_sha256': evidence_digest(payload),
                'source_md_sha256': hashlib.sha256(md.read_bytes()).hexdigest(),
                'images': [{'path': str(p), 'sha256': hashlib.sha256(p.read_bytes()).hexdigest()} for p in request.image_paths],
                'model': 'codex/gpt-6-astra', 'reasoning_effort': 'high', 'automatic_retries': 0,
                'message_chars': sum(len(m['content']) for m in request.messages),
                'history_supplied': False, 'expected_verdict_supplied': False,
                'known_source_control_not_blind': True}
    for name, data in [('request.json', payload), ('manifest.json', manifest)]:
        path = ROOT / name
        if path.exists():
            assert json.loads(path.read_text()) == data, 'Frozen request changed'
        else:
            path.write_text(json.dumps(data, ensure_ascii=False, indent=2) + '\n')
    return request, manifest


async def execute():
    os.environ['CODEX_TRANSIENT_RETRIES'] = '0'
    request, manifest = prepare()
    from backend.app.services.llm.codex_runner import run_codex_json_messages
    from backend.app.services.llm.codex_model_substitution import substitute_codex_model
    assert substitute_codex_model('gpt-6-astra') == 'gpt-6-astra'
    result_path = ROOT / 'result.json'
    if result_path.exists():
        result = json.loads(result_path.read_text())
    else:
        with (ROOT / 'attempt.json').open('x') as f:
            json.dump({'status': 'started', 'request_sha256': manifest['request_sha256']}, f)
        print('START', manifest['message_chars'], 'chars', len(request.image_paths), 'images', flush=True)
        result = asdict(await run_codex_json_messages(request.messages, model='codex/gpt-6-astra',
            reasoning_effort='high', timeout=900, stage='candidate_recovery',
            project_id='EOM-prescan-control', image_paths=request.image_paths, allowed_tools=''))
        result_path.write_text(json.dumps(result, ensure_ascii=False, indent=2) + '\n')
        (ROOT / 'attempt.json').write_text(json.dumps({'status': 'finished', 'is_error': result['is_error']}))
    assert not result['is_error'] and isinstance(result.get('json_data'), dict), 'Unusable response; inspect before retry'
    assert hashlib.sha256((BASE / '02_work/document.md').read_bytes()).hexdigest() == manifest['source_md_sha256']
    assert all(hashlib.sha256(Path(i['path']).read_bytes()).hexdigest() == i['sha256'] for i in manifest['images'])
    validated = validate_recovery_payload(request, result['json_data'])
    base = json.loads((BASE / '03_analysis/runs/control/03_findings.json').read_text())
    original = deepcopy(base)
    combined = combine_findings_with_targeted(base, [(request.stage, validated)])
    assert base == original and combined['findings'][:len(base['findings'])] == base['findings']
    (ROOT / '03_findings.json').write_text(json.dumps(combined, ensure_ascii=False, indent=2) + '\n')
    coverage = write_candidate_coverage(ROOT)
    report = {'baseline_findings_preserved': len(base['findings']),
              'new_transport_rows': len(combined['findings']) - len(base['findings']),
              'candidate_decisions': validated.get('candidate_decisions'),
              'recovery_pending': validated['recovery_pending'],
              'decision_coverage': coverage['decision_coverage'],
              'semantic_verified': False, 'production_accepted': False,
              'duration_ms': result['duration_ms'], 'request_sha256': manifest['request_sha256']}
    (ROOT / 'summary.json').write_text(json.dumps(report, ensure_ascii=False, indent=2) + '\n')
    print('COMPLETE', json.dumps(report, ensure_ascii=False), flush=True)


async def followup():
    """Execute the ordinary one-hop retrieval without adding expected answers."""
    os.environ['CODEX_TRANSIENT_RETRIES'] = '0'
    from backend.app.pipeline.stages.prepare.codex_targeted_findings import CodexTargetedPass
    from backend.app.pipeline.stages.prepare.recovery_context import build_context_followup
    from backend.app.services.llm.codex_runner import run_codex_json_messages
    frozen = json.loads((ROOT / 'request.json').read_text())
    manifest = json.loads((ROOT / 'manifest.json').read_text())
    assert evidence_digest(frozen) == manifest['request_sha256']
    packet = json.loads(frozen['messages'][-1]['content'])
    key = hashlib.sha256((packet['candidate']['origin'] + packet['candidate']['pointer']).encode()).hexdigest()[:16]
    initial = CodexTargetedPass('candidate_recovery_' + key, '03_candidate_recovery_' + key + '.json',
                               frozen['messages'], tuple(Path(p) for p in frozen['image_paths']))
    previous = json.loads((ROOT / 'result.json').read_text())
    request, retrieval = build_context_followup(initial, previous['json_data'])
    folder = ROOT / 'followup'; folder.mkdir(exist_ok=True)
    (folder / 'retrieval.json').write_text(json.dumps(retrieval, ensure_ascii=False, indent=2))
    if request is None:
        print('NO_FOLLOWUP', retrieval['status']); return
    data = {'messages': request.messages, 'image_paths': [str(p) for p in request.image_paths]}
    binding = {'request_sha256': evidence_digest(data),
               'images': [{'path': str(p), 'sha256': hashlib.sha256(p.read_bytes()).hexdigest()} for p in request.image_paths]}
    for name, value in [('request.json', data), ('binding.json', binding)]:
        path = folder / name
        if path.exists(): assert json.loads(path.read_text()) == value
        else: path.write_text(json.dumps(value, ensure_ascii=False, indent=2))
    output = folder / 'result.json'
    if output.exists(): result = json.loads(output.read_text())
    else:
        with (folder / 'attempt.json').open('x') as f: json.dump({'status': 'started', **binding}, f)
        print('START_FOLLOWUP', sum(len(m['content']) for m in request.messages), 'chars', len(request.image_paths), 'images', flush=True)
        result = asdict(await run_codex_json_messages(request.messages, model='codex/gpt-6-astra',
            reasoning_effort='high', timeout=900, stage='candidate_recovery',
            project_id='EOM-prescan-followup-control', image_paths=request.image_paths, allowed_tools=''))
        output.write_text(json.dumps(result, ensure_ascii=False, indent=2))
        (folder / 'attempt.json').write_text(json.dumps({'status': 'finished', 'is_error': result['is_error']}))
    assert not result['is_error'] and isinstance(result['json_data'], dict)
    assert all(hashlib.sha256(Path(i['path']).read_bytes()).hexdigest() == i['sha256'] for i in binding['images'])
    assert hashlib.sha256((BASE / '02_work/document.md').read_bytes()).hexdigest() == manifest['source_md_sha256']
    base = json.loads((BASE / '03_analysis/runs/control/03_findings.json').read_text())
    combined = combine_findings_with_targeted(base, [
        (initial.stage, validate_recovery_payload(initial, previous['json_data'])),
        (request.stage, validate_recovery_payload(request, result['json_data']))])
    assert combined['findings'][:len(base['findings'])] == base['findings']
    (ROOT / '03_findings.json').write_text(json.dumps(combined, ensure_ascii=False, indent=2))
    coverage = write_candidate_coverage(ROOT)
    summary = {'baseline_findings_preserved': len(base['findings']),
               'new_transport_rows': len(combined['findings']) - len(base['findings']),
               'decision_coverage': coverage['decision_coverage'], 'semantic_verified': False,
               'duration_ms': result['duration_ms'], 'request_sha256': binding['request_sha256']}
    (folder / 'summary.json').write_text(json.dumps(summary, ensure_ascii=False, indent=2))
    print('COMPLETE_FOLLOWUP', json.dumps(summary, ensure_ascii=False), flush=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--execute', action='store_true')
    parser.add_argument('--followup', action='store_true')
    args = parser.parse_args()
    if args.followup:
        asyncio.run(followup())
    elif args.execute:
        asyncio.run(execute())
    else:
        _, manifest = prepare()
        print(json.dumps(manifest, ensure_ascii=False, indent=2))
