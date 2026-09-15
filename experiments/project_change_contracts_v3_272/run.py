"""Frozen sequential Codex experiment: exactly 12 cases, no semantic retries."""
import asyncio
from collections import Counter
import json
from pathlib import Path
import shutil
import sys

from .common import CODE, PREVIOUS, OUT, CASES, read, sha, ref, write, git_commit, previous_hashes
from .allocation import VERSION as PACKAGE_VERSION
from .normalization import VERSION as NORMALIZATION_VERSION, normalize
from experiments.project_change_contracts_v2_272.sufficiency import VERSION as SUFFICIENCY_VERSION


def provider_module():
    sys.path.insert(0, str(OUT / 'runtime_deps'))
    from experiments.project_change_semantic_codex_272 import provider
    return provider


def exact_prompt(row):
    packet = read(row['semantic_packet']['path'])
    labels = []
    for side in ('old', 'new'):
        for e in packet['evidence'][side]:
            if e.get('raster'):
                i = len(labels)
                labels.append(dict(image_index=i + 1, file=f'image_{i:02d}.png', evidence_id=e['evidence_id'], side=side, page=e['page'], bbox=e['bbox']))
    return ((OUT / 'PROMPT.txt').read_text() + '\n\nProcess only the supplied evidence. Return the final JSON directly.\n'
        + 'Attached images are ordered as this source manifest:\n' + json.dumps(labels, ensure_ascii=False)
        + '\n\nUNTRUSTED PACKET DATA:\n' + json.dumps(read(row['model_input']['path']), ensure_ascii=False))


def freeze():
    if (OUT / 'ARCHITECTURE_FIX_FREEZE.json').exists():
        raise ValueError('Already frozen')
    assert read(OUT / 'LOCAL_REGRESSION.json')['status'] == 'PASS'
    assert read(OUT / 'LOCAL_TESTS.json')['status'] == 'PASS'
    assert read(OUT / 'A2_PACKAGE_REGRESSION.json')['status'] == 'PASS'
    assert read(OUT / 'D2_MECHANICAL_REPLAY.json')['status'] == 'PASS'
    rows = read(OUT / 'PACKAGE_INDEX.json')['packages']
    assert [(r['pair_index'], r['case_id']) for r in rows] == CASES
    deps = OUT / 'runtime_deps'
    if not deps.exists():
        shutil.copytree(PREVIOUS / 'runtime_deps', deps, ignore=shutil.ignore_patterns('__pycache__', '*.pyc'))
    provider = provider_module()
    runtime = provider.runtime_identity()
    assert runtime['model'] == 'gpt-6-astra' and runtime['reasoning'] == 'xhigh' and runtime['provider'] == 'codex_chatgpt'
    prompt_files, leakage = [], []
    for row in rows:
        prompt = exact_prompt(row)
        assert len(prompt) <= 60000, (row['key'], len(prompt))
        data = read(row['model_input']['path'])
        banned = ['source_truth', 'expected_answer', 'previous_verdict', 'source-audit:', 'regression_target', 'truth_origin', 'S_FP01']
        text = json.dumps(data, ensure_ascii=False)
        assert not any(token in text for token in banned)
        # Labels/previous outputs/analyst prose are absent by construction: only
        # neutral query, source evidence and generic v2 contracts are transmitted.
        path = OUT / 'packages' / row['key'] / 'EXACT_PROMPT.txt'
        write(path, prompt)
        prompt_files.append(ref(path))
        leakage.append(dict(case_token=row['case_token'], status='PASS', model_input=ref(row['model_input']['path']),
                            neutral_query_from_previous=True, previous_output_in_model_context=False))
    write(OUT / 'INPUT_LEAKAGE_AUDIT.json', dict(status='PASS', cases=leakage, known_dev=True, blind=False))
    code_files = [p for name in ('project_change_contracts_v3_272', 'project_change_contracts_v2_272', 'project_change_contracts_272',
        'project_change_semantic_codex_272', 'project_change_semantic_272', 'project_change_272')
        for p in (CODE / 'experiments' / name).glob('*.py')]
    code_files.append(Path(__file__).with_name('prompt.txt'))
    dirty = __import__('subprocess').check_output(['git', '-C', str(CODE), 'status', '--porcelain', '--',
        'experiments/project_change_contracts_v3_272'], text=True)
    if dirty:
        raise RuntimeError('Commit v3 implementation before freeze')
    artifact_files = [p for p in OUT.rglob('*') if p.is_file() and '__pycache__' not in p.parts and p.suffix != '.pyc']
    manifest = dict(schema='CONTROLLED_V3_MANIFEST/2', code_commit=git_commit(), cases=[dict(pair_index=p, case_id=c) for p, c in CASES],
        model='gpt-6-astra', reasoning='xhigh', provider='codex_chatgpt', raster_budget=8,
        maximum_calls=12, automatic_retries=0, parallelism=1, known_dev=True, blind=False,
        truth_open_condition='12 successful schema-valid isolated case calls',
        package_version=PACKAGE_VERSION, normalization_version=NORMALIZATION_VERSION, sufficiency_version=SUFFICIENCY_VERSION,
        prior_run=ref(PREVIOUS / 'CONTROLLED_V2_MANIFEST.json'))
    write(OUT / 'CONTROLLED_V3_MANIFEST.json', manifest)
    frozen = dict(schema='ARCHITECTURE_FIX_FREEZE/2', code_commit=git_commit(), runtime=runtime,
        manifest=ref(OUT / 'CONTROLLED_V3_MANIFEST.json'), prompt=ref(OUT / 'PROMPT.txt'),
        config_hash=__import__('hashlib').sha256(json.dumps(runtime, sort_keys=True).encode()).hexdigest(),
        package_hashes={r['key']: r['package_hash'] for r in rows}, exact_prompts=prompt_files,
        code_files=[ref(p) for p in code_files], artifact_files=[ref(p) for p in artifact_files],
        normalization_contract_version=NORMALIZATION_VERSION, evidence_sufficiency_version=SUFFICIENCY_VERSION)
    write(OUT / 'ARCHITECTURE_FIX_FREEZE.json', frozen)
    for path in artifact_files + [OUT / 'ARCHITECTURE_FIX_FREEZE.json', OUT / 'CONTROLLED_V3_MANIFEST.json']:
        path.chmod(0o444)
    verify()
    print('FREEZE VERIFIED: exactly 12 isolated Codex calls authorized by task; no calls yet', flush=True)


def verify():
    freeze = read(OUT / 'ARCHITECTURE_FIX_FREEZE.json')
    if git_commit() != freeze['code_commit']:
        raise ValueError('Code commit changed during frozen run')
    for r in freeze['code_files'] + freeze['artifact_files'] + [freeze['manifest']]:
        if sha(r['path']) != r['sha256']:
            raise ValueError('Frozen file drift: ' + r['path'])
    if read(OUT / 'PREVIOUS_RUN_HASHES.json') != previous_hashes():
        raise ValueError('Previous controlled run changed')
    if provider_module().runtime_identity() != freeze['runtime']:
        raise ValueError('Provider runtime changed')
    rows = read(OUT / 'PACKAGE_INDEX.json')['packages']
    if [(r['pair_index'], r['case_id']) for r in rows] != CASES:
        raise ValueError('Fixed case set changed')
    return freeze, rows


def collect():
    rows = read(OUT / 'PACKAGE_INDEX.json')['packages']
    completed, usage = [], Counter()
    for row in rows:
        path = OUT / 'calls' / row['case_token'] / 'SUCCESS.json'
        if path.exists():
            receipt = read(path)
            assert sha(receipt['normalized_path']) == receipt['normalized_sha256']
            completed.append(row['key'])
            for item in receipt['usage']:
                usage.update({k: v for k, v in item.items() if isinstance(v, int)})
    invocations = list((OUT / 'calls').glob('*/attempt_*/INVOCATION.json'))
    return dict(completed=completed, completed_count=len(completed), model_calls=len(invocations),
        usage=dict(usage), provider_cost_usd=None, openrouter_requests=0, retries=0)


async def run():
    frozen, rows = verify()
    provider = provider_module().CodexProvider(OUT, config=frozen['runtime'])
    for row in rows:
        target = OUT / 'calls' / row['case_token']
        if (target / 'SUCCESS.json').exists():
            continue
        if list(target.glob('attempt_*')):
            raise RuntimeError('Prior failed/interrupted call: no automatic or semantic retries')
        verify()
        if collect()['model_calls'] >= 12:
            raise RuntimeError('Twelve-call ceiling reached')
        print('START ' + row['key'], flush=True)
        try:
            raw = await provider.call(row['case_token'], (OUT / 'PROMPT.txt').read_text(),
                read(row['model_input']['path']), read(OUT / 'OUTPUT_SCHEMA.json'), read(row['semantic_packet']['path']))
        except Exception as exc:
            write(OUT / 'STOP_RECEIPT.json', dict(status='STOPPED_FAILURE', case=row['key'], error_type=type(exc).__name__, **collect()))
            print('STOPPED: ' + type(exc).__name__, flush=True)
            return
        attempt = next(target.glob('attempt_*'))
        if (attempt / 'prompt.txt').read_text() != exact_prompt(row):
            raise RuntimeError('Sent prompt differs from frozen prompt')
        if raw['case_token'] != row['case_token']:
            raise RuntimeError('Wrong response token')
        write(OUT / 'raw' / (row['key'] + '.json'), raw)
        write(OUT / 'raw' / (row['key'] + '.jsonl'), (attempt / 'raw.jsonl').read_text())
        write(OUT / 'receipts' / (row['key'] + '.json'), read(target / 'SUCCESS.json'))
        print('COMPLETE ' + row['key'] + '; ' + str(collect()['completed_count']) + '/12; no evaluation', flush=True)
    verify()
    write(OUT / 'INFERENCE_COMPLETE_RECEIPT.json', collect())
    print('ALL 12 COMPLETED; truth may now be opened', flush=True)


if __name__ == '__main__':
    if '--freeze' in sys.argv:
        freeze()
    elif '--preflight' in sys.argv:
        verify(); print('PREFLIGHT PASS; no inference')
    else:
        asyncio.run(run())
