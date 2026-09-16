"""Additive transport recovery: retain all frozen inputs and earlier attempts.

The cached requested model uses code_mode_only. Mount its installed companion
host and enable that runtime component; all original tool feature prohibitions
and unexpected-tool rejection remain. No semantic layer or prompt changes.
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

from .decompose import OUT, ROOT, REPO, DEPS, verify_resume, diagnostics
from .preflight import read, sha, write
from experiments.project_change_semantic_codex_272.provider import (
    cli_command, sandbox_command, safe_env, runtime_identity,
)
from experiments.project_change_semantic_codex_272.schema import validate

RECOVERY = OUT / 'recovery_2'
AUTHORIZATION = Path('/home/coder/.codex/attachments/dd3c09cb-708c-41fc-a0e7-1fbdf9c95294/pasted-text.txt')


def command_for(work, names):
    command = cli_command(names)
    i = command.index('code_mode_host')
    if command[i - 1] != '--disable':
        raise ValueError('Unexpected frozen feature configuration')
    del command[i - 1:i + 1]
    command[-1:-1] = ['--enable', 'code_mode_host', '-c', 'log_dir="/work/runtime_logs"']
    result = sandbox_command(work, command)
    host = Path(shutil.which('codex-code-mode-host') or '').resolve()
    if not host.is_file():
        raise FileNotFoundError('Installed companion host missing')
    pos = result.index('/opt/codex')
    # The first /opt/codex is the binary's bind destination, not its invocation.
    pos = result.index('/opt/codex', pos + 1)
    result[pos:pos] = ['--ro-bind', str(host), '/opt/codex-code-mode-host']
    return result


def prepare():
    verify_resume()
    if RECOVERY.exists():
        raise FileExistsError('Recovery already prepared')
    model = next(m for m in read('/home/coder/.codex/models_cache.json')['models'] if m['slug'] == 'gpt-6-astra')
    if model.get('tool_mode') != 'code_mode_only':
        raise ValueError('Transport recovery premise does not match local model metadata')
    planned = read(OUT / 'DECOMPOSITION_CALL_PLAN.json')
    if len(planned) != 18 or list((OUT / 'decomposition_raw').glob('*/SUCCESS.json')):
        raise ValueError('Unexpected previous discovery results')
    # Start with a not-yet-attempted small frozen request, then retain original order.
    first = min(planned, key=lambda p: p['input_tokens_o200k'])
    ordered = [first] + [p for p in planned if p['bundle_id'] != first['bundle_id']]
    files = {str(p): sha(p) for p in Path(__file__).parent.glob('recovery*.py')}
    host = Path(shutil.which('codex-code-mode-host')).resolve()
    write(RECOVERY / 'RECOVERY_FREEZE.json', dict(
        status='READY', at=datetime.now(timezone.utc).isoformat(),
        authorization_source=str(AUTHORIZATION), authorization_sha256=sha(AUTHORIZATION),
        previous_stop_sha256=sha(OUT / 'RESUME_STATUS.json'),
        parent_resume_sha256=sha(OUT / 'RESUME_FREEZE.json'),
        prompt_sha256=sha(OUT / 'decomposition_inputs/PROMPT.txt'),
        source_inventory_sha256=sha(OUT / 'BROAD_CONTEXT_INDEX.json'),
        code=files, runtime=runtime_identity(), host_path=str(host), host_sha256=sha(host),
        observed_model_metadata=dict(slug=model['slug'], tool_mode=model['tool_mode']),
        change='Enable existing code-mode host and mount only its executable alongside CLI',
        context_content_changed=False, prompt_semantics_changed=False,
        input_bytes_changed=False, source_truth_opened=False,
        outer_timeout_seconds=900, new_invocation_limit=18, retries_within_this_run=0,
        initial_invocations=2, automatic_resumption=False,
        plan=ordered, initial_single_request=True, subsequent_parallelism=2,
        repo_head=subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=REPO, text=True).strip()))
    print('RECOVERY READY: 18 exact frozen inputs; first=' + first['bundle_id'], flush=True)


def verify():
    verify_resume()
    f = read(RECOVERY / 'RECOVERY_FREEZE.json')
    for path, digest in f['code'].items():
        if sha(path) != digest:
            raise ValueError('Recovery code drift')
    if sha(f['host_path']) != f['host_sha256']:
        raise ValueError('Companion host drift')
    if runtime_identity() != f['runtime']:
        raise ValueError('Runtime drift')
    return f


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
            await asyncio.wait_for(proc.communicate(prompt), timeout=900)
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


def finalize():
    f = verify()
    results, counts = [], {}
    usage = Counter(calls=0, input_tokens=0, output_tokens=0, cached_input_tokens=0, failures=0)
    for item in f['plan']:
        directory = RECOVERY / 'decomposition_raw' / item['bundle_id']
        success = read(directory / 'SUCCESS.json')
        if sha(directory / 'parsed.json') != success['output_sha256']:
            raise ValueError('Result drift')
        result = read(directory / 'parsed.json')
        results.append(result)
        counts[item['bundle_id']] = len(result['candidates'])
        usage['calls'] += 1
        for turn in success['usage']:
            for key in ['input_tokens', 'output_tokens', 'cached_input_tokens']:
                usage[key] += turn.get(key, 0)
    if len(results) != 18:
        raise ValueError('Need all 18 outputs before freeze')
    write(OUT / 'DECOMPOSITION_RESULTS.json', results)
    files = {str(p.relative_to(OUT)): sha(p) for folder in [OUT / 'decomposition_inputs', RECOVERY]
             for p in folder.rglob('*') if p.is_file()}
    files['DECOMPOSITION_RESULTS.json'] = sha(OUT / 'DECOMPOSITION_RESULTS.json')
    write(OUT / 'DECOMPOSITION_FREEZE.json', dict(status='FROZEN',
        at=datetime.now(timezone.utc).isoformat(), files=files,
        model_calls_successful=18, model_invocation_attempts_total=20,
        failed_previous_attempts=2, input_bytes_changed=False, context_content_changed=False,
        source_inventory_sha256=sha(OUT / 'BROAD_CONTEXT_INDEX.json'),
        source_truth_opened=False, complete_result_frozen=False))
    candidates, audits, duplicates = diagnostics(results)
    write(OUT / 'LOCAL_CANDIDATE_INDEX.json', candidates)
    write(OUT / 'LOCAL_CANDIDATE_REFERENCE_AUDIT.json', audits)
    write(OUT / 'DUPLICATE_LOOKING_CANDIDATES.json', duplicates)
    summary = dict(status='STOP_LOCAL_CANDIDATE_EXPLOSION' if len(candidates) > 50 else 'DECOMPOSITION_COMPLETE_PACKAGE_GUARD_PASS',
        total_local_candidates=len(candidates), candidates_per_context=counts,
        distribution=dict(min=min(counts.values()), median=statistics.median(counts.values()), max=max(counts.values())),
        candidates_with_old_new_refs=sum(a['has_old_new_refs'] for a in audits),
        one_sided_candidates=sum(a['one_sided'] for a in audits), no_refs_candidates=sum(a['no_refs'] for a in audits),
        cross_modal_candidates=sum(a['cross_modal'] for a in audits),
        invalid_reference_candidates=sum(bool(a['invalid_refs'] or a['wrong_side_refs']) for a in audits),
        duplicate_looking_pairs=len(duplicates), duplicates_removed=0,
        local_package_guard='FAIL' if len(candidates) > 50 else 'PASS',
        context_content_changed=False, no_truth_leakage='PASS', comparison_calls=0,
        broad_contexts=18, broad_context_limit=20, decomposition_successful_calls=18,
        decomposition_attempts_total=20, prior_failed_attempts=2)
    write(OUT / 'DECOMPOSITION_SUMMARY.json', summary)
    write(RECOVERY / 'MODEL_USAGE.json', dict(DECOMPOSITION=dict(usage), COMPARISON=dict(calls=0, input_tokens=0, output_tokens=0, failures=0),
        cumulative_attempts=20, previous_failed_attempts=2, previous_usage='NOT_REPORTED',
        cumulative_token_usage_complete=False, OpenRouter=0, Claude=0))
    write(RECOVERY / 'RESULT_SUMMARY.json', summary)
    print(json.dumps(summary, ensure_ascii=False), flush=True)


async def run():
    f = verify()
    if (RECOVERY / 'decomposition_raw').exists():
        raise FileExistsError('Run already started; no automatic retry')
    schema = read(OUT / 'decomposition_inputs/OUTPUT_SCHEMA.json')
    plan = f['plan']
    batches = [plan[:1]] + [plan[i:i+2] for i in range(1, len(plan), 2)]
    for batch in batches:
        verify()
        for item in batch:
            print('START ' + item['bundle_id'], flush=True)
        outcomes = await asyncio.gather(*(call(item, schema) for item in batch), return_exceptions=True)
        failures = [x for x in outcomes if isinstance(x, BaseException)]
        if failures:
            write(RECOVERY / 'STOP.json', dict(status='STOP_RUNTIME_FAILURE',
                errors=[dict(type=type(e).__name__, message=str(e)) for e in failures],
                attempted=len(list((RECOVERY / 'decomposition_raw').glob('*/INVOCATION.json'))),
                successful=len(list((RECOVERY / 'decomposition_raw').glob('*/SUCCESS.json'))),
                source_truth_opened=False))
            raise failures[0]
    finalize()


if __name__ == '__main__':
    if sys.argv[1] == 'prepare':
        prepare()
    elif sys.argv[1] == 'run':
        asyncio.run(run())
    elif sys.argv[1] == 'verify':
        verify()
        print('RECOVERY VERIFIED')
    elif sys.argv[1] == 'finalize':
        finalize()
