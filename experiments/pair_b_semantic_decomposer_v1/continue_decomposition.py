"""Continue unfinished exact-input requests after transport timeouts only.

Completed responses are reused verbatim. The call implementation is copied from
recovery.py with only its outer wall-clock timeout changed from 900 to 3600 s.
Frozen prompt/schema/images and all semantic/comparison layers stay unchanged.
"""
import asyncio
from collections import Counter
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import shutil
import signal
import statistics
import subprocess
import sys
import time

from .decompose import OUT, REPO, verify_resume, diagnostics
from .preflight import read, sha, write
from .recovery import command_for
from experiments.project_change_semantic_codex_272.provider import safe_env, runtime_identity
from experiments.project_change_semantic_codex_272.schema import validate

RECOVERY = OUT / 'continuation_3'
CALL_ROOTS = [OUT / 'decomposition_raw', OUT / 'recovery_2/decomposition_raw', RECOVERY / 'decomposition_raw']

async def call(item, schema):
    key = item['bundle_id']
    source = OUT / 'decomposition_inputs' / key
    target = RECOVERY / 'decomposition_raw' / key
    if target.exists():
        raise FileExistsError('No automatic retry for ' + key)
    target.mkdir(parents=True)
    prompt = (source / 'EXACT_PROMPT.txt').read_bytes()
    if hashlib.sha256(prompt).hexdigest() != item['exact_prompt_sha256']:
        raise ValueError('Frozen prompt drift')
    shutil.copyfile(source / 'EXACT_PROMPT.txt', target / 'prompt.txt')
    shutil.copyfile(OUT / 'decomposition_inputs/OUTPUT_SCHEMA.json', target / 'schema.json')
    names = []
    for i, image in enumerate(read(source / 'IMAGES.json')):
        name = f'image_{i:02d}.png'
        shutil.copyfile(image['path'], target / name)
        if sha(target / name) != image['sha256']:
            raise ValueError('Raster drift')
        names.append(name)
    inputs = {p.name: sha(p) for p in target.iterdir() if p.is_file()}
    command = command_for(target, names)
    write(target / 'INVOCATION.json', dict(command=command, input_hashes=inputs,
          model='gpt-6-astra', reasoning='xhigh', fresh_context=True,
          exact_request_sha256=item['exact_prompt_sha256'], outer_retries=0,
          host_component_enabled=True, corpus_mounted=False))
    started = time.monotonic()
    proc = None
    error = None
    try:
        with (target / 'raw.jsonl').open('wb') as raw, (target / 'stderr.txt').open('wb') as err:
            proc = await asyncio.create_subprocess_exec(*command, stdin=asyncio.subprocess.PIPE,
                    stdout=raw, stderr=err, env=safe_env(), start_new_session=True)
            await asyncio.wait_for(proc.communicate(prompt), timeout=3600)
    except BaseException as exc:
        error = exc
        if proc is not None and proc.returncode is None:
            os.killpg(proc.pid, signal.SIGKILL)
            await proc.wait()
    records = []
    for line in (target / 'raw.jsonl').read_text().splitlines():
        try:
            records.append(json.loads(line))
        except ValueError:
            continue
    usage = [r['usage'] for r in records if r.get('type') == 'turn.completed' and r.get('usage')]
    tool_items = [r for r in records if r.get('type', '').startswith('item.')
                 and r.get('item', {}).get('type') not in {None, 'agent_message', 'reasoning', 'error'}]
    receipt = dict(seconds=time.monotonic() - started, usage=usage,
        input_integrity=all(sha(target / name) == digest for name, digest in inputs.items()),
        tool_items=len(tool_items), exit_code=proc.returncode if proc else None,
        exact_request_sha256=item['exact_prompt_sha256'], raw_sha256=sha(target / 'raw.jsonl'),
        diagnostics=[r['item'] for r in records if r.get('item', {}).get('type') == 'error'])
    try:
        if error:
            raise error
        if proc.returncode or not usage or tool_items or not receipt['input_integrity']:
            raise RuntimeError('Runtime failure or missing usage or unexpected tool execution')
        value = read(target / 'final.txt')
        validate(value, schema)
        if value['bundle_id'] != key:
            raise ValueError('Bundle ID mismatch')
        ids = [c['candidate_id'] for c in value['candidates']]
        if len(ids) != len(set(ids)):
            raise ValueError('Duplicate candidate IDs inside response')
        write(target / 'parsed.json', value)
        write(target / 'SUCCESS.json', dict(receipt, status='SUCCESS', output_sha256=sha(target / 'parsed.json')))
        print(f'COMPLETE {key} candidates={len(ids)} seconds={receipt["seconds"]:.1f}', flush=True)
        return value
    except BaseException as exc:
        write(target / 'FAILURE.json', dict(receipt, status='FAILED_CLOSED', error_type=type(exc).__name__, error=str(exc)))
        raise


def successes():
    found = {}
    for root in CALL_ROOTS:
        for path in root.glob('*/SUCCESS.json'):
            key = path.parent.name
            if key in found:
                raise ValueError('Multiple successful outputs; never choose by quality')
            if sha(path.parent / 'parsed.json') != read(path)['output_sha256']:
                raise ValueError('Successful output drift')
            found[key] = path.parent
    return found


def prepare():
    verify_resume()
    if RECOVERY.exists() or (OUT / 'DECOMPOSITION_FREEZE.json').exists():
        raise FileExistsError('Continuation or complete decomposition already exists')
    stop = read(OUT / 'recovery_2/STOP.json')
    if not stop['errors'] or any(e['type'] != 'TimeoutError' for e in stop['errors']):
        raise ValueError('Continuation admits transport timeouts only, no quality retries')
    selected = successes()
    plan = read(OUT / 'DECOMPOSITION_CALL_PLAN.json')
    if len(plan) != 18 or not set(selected) <= {i['bundle_id'] for i in plan}:
        raise ValueError('Unknown or changed context set')
    files = {str(p): sha(p) for p in [Path(__file__), Path(__file__).with_name('recovery.py'), Path(__file__).with_name('test_continuation.py')]}
    outputs = {str(p / name): sha(p / name) for p in selected.values() for name in ['parsed.json', 'SUCCESS.json']}
    write(RECOVERY / 'CONTINUATION_FREEZE.json', dict(status='READY',
        at=datetime.now(timezone.utc).isoformat(), reason='Previous absolute timeout interrupted active responses',
        code=files, previous_completed_outputs=outputs, previous_stop_sha256=sha(OUT / 'recovery_2/STOP.json'),
        runtime=runtime_identity(), outer_timeout_seconds=3600, previous_outer_timeout_seconds=900,
        input_bytes_changed=False, prompt_semantics_changed=False, context_membership_changed=False,
        model='gpt-6-astra', reasoning='xhigh', new_call_limit=18 - len(selected),
        completed_contexts=sorted(selected), plan=[i for i in plan if i['bundle_id'] not in selected],
        source_truth_opened=False, automatic_retries=0,
        authorization='User repeated instruction to complete the same 18 contexts; technical continuation without changing source or prompt',
        repo_head=subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=REPO, text=True).strip()))
    print(f'CONTINUATION READY: reuse={len(selected)} new={18-len(selected)} timeout=3600', flush=True)


def verify():
    verify_resume()
    f = read(RECOVERY / 'CONTINUATION_FREEZE.json')
    for path, digest in {**f['code'], **f['previous_completed_outputs']}.items():
        if sha(path) != digest:
            raise ValueError('Continuation code/result drift')
    if runtime_identity() != f['runtime']:
        raise ValueError('Runtime drift')
    return f


def finalize():
    verify()
    selected = successes()
    plan = read(OUT / 'DECOMPOSITION_CALL_PLAN.json')
    if set(selected) != {p['bundle_id'] for p in plan}:
        raise ValueError('Need exactly 18 completed contexts')
    results = [read(selected[p['bundle_id']] / 'parsed.json') for p in plan]
    usages = Counter(input_tokens=0, output_tokens=0, cached_input_tokens=0, reasoning_output_tokens=0)
    for path in selected.values():
        for turn in read(path / 'SUCCESS.json')['usage']:
            for key in usages:
                usages[key] += turn.get(key, 0)
    attempted = sum(len(list(root.glob('*/INVOCATION.json'))) for root in CALL_ROOTS)
    failures = sum(len(list(root.glob('*/FAILURE.json'))) for root in CALL_ROOTS)
    write(OUT / 'DECOMPOSITION_RESULTS.json', results)
    usage = dict(successful_calls=18, invocation_attempts=attempted, failed_attempts=failures,
        completed_usage=dict(usages), failed_usage='NOT_FULLY_REPORTED', cumulative_usage_complete=False,
        comparison_calls=0, OpenRouter=0, Claude=0)
    write(RECOVERY / 'MODEL_USAGE.json', usage)
    files = {str(p.relative_to(OUT)): sha(p) for folder in [OUT / 'decomposition_inputs', OUT / 'recovery_2', RECOVERY]
             for p in folder.rglob('*') if p.is_file()}
    files['DECOMPOSITION_RESULTS.json'] = sha(OUT / 'DECOMPOSITION_RESULTS.json')
    write(OUT / 'DECOMPOSITION_FREEZE.json', dict(status='FROZEN', at=datetime.now(timezone.utc).isoformat(),
        files=files, successful_model_calls=18, invocation_attempts=attempted, failed_attempts=failures,
        selected_raw_outputs={key: str(path.relative_to(OUT)) for key, path in selected.items()},
        source_inventory_sha256=sha(OUT / 'BROAD_CONTEXT_INDEX.json'),
        input_bytes_changed=False, context_content_changed=False, source_truth_opened=False,
        complete_result_frozen=False))
    candidates, audits, duplicates = diagnostics(results)
    write(OUT / 'LOCAL_CANDIDATE_INDEX.json', candidates)
    write(OUT / 'LOCAL_CANDIDATE_REFERENCE_AUDIT.json', audits)
    write(OUT / 'DUPLICATE_LOOKING_CANDIDATES.json', duplicates)
    counts = {r['bundle_id']: len(r['candidates']) for r in results}
    summary = dict(status='STOP_LOCAL_CANDIDATE_EXPLOSION' if len(candidates)>50 else 'DECOMPOSITION_COMPLETE_PACKAGE_GUARD_PASS',
        total_local_candidates=len(candidates), candidates_per_context=counts,
        distribution=dict(min=min(counts.values()), median=statistics.median(counts.values()), max=max(counts.values())),
        candidates_with_old_new_refs=sum(a['has_old_new_refs'] for a in audits),
        one_sided_candidates=sum(a['one_sided'] for a in audits), no_refs_candidates=sum(a['no_refs'] for a in audits),
        cross_modal_candidates=sum(a['cross_modal'] for a in audits),
        invalid_reference_candidates=sum(bool(a['invalid_refs'] or a['wrong_side_refs']) for a in audits),
        duplicate_looking_pairs=len(duplicates), duplicates_removed=0,
        local_package_guard='FAIL' if len(candidates)>50 else 'PASS', comparison_calls=0,
        broad_contexts=18, broad_context_limit=20, context_content_changed=False,
        successful_decomposition_calls=18, decomposition_attempts=attempted, failures=failures,
        no_truth_leakage='PASS')
    write(OUT / 'DECOMPOSITION_SUMMARY.json', summary)
    print(json.dumps(summary, ensure_ascii=False), flush=True)


async def run():
    f = verify()
    if (RECOVERY / 'decomposition_raw').exists():
        raise FileExistsError('No automatic resumption')
    schema = read(OUT / 'decomposition_inputs/OUTPUT_SCHEMA.json')
    plan = f['plan']
    for start in range(0, len(plan), 2):
        verify()
        batch = plan[start:start+2]
        for item in batch:
            print('START '+item['bundle_id'], flush=True)
        outcomes = await asyncio.gather(*(call(item, schema) for item in batch), return_exceptions=True)
        errors = [v for v in outcomes if isinstance(v, BaseException)]
        if errors:
            write(RECOVERY / 'STOP.json', dict(status='STOP_RUNTIME_FAILURE',
                errors=[dict(type=type(e).__name__, message=str(e)) for e in errors],
                completed_contexts=len(successes()), source_truth_opened=False))
            raise errors[0]
    finalize()


if __name__ == '__main__':
    if sys.argv[1] == 'prepare':
        prepare()
    elif sys.argv[1] == 'run':
        asyncio.run(run())
    elif sys.argv[1] == 'verify':
        verify()
        print('CONTINUATION VERIFIED')
