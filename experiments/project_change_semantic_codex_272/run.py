"""DEV-only Codex runner; immutable calls, explicit stop/resume, bounded queue."""
import argparse
import asyncio
from collections import Counter
import fcntl
import json
from pathlib import Path
import signal
import subprocess

from experiments.project_change_272.inventory import ROOT, REPO, read, sha, immutable, now
from experiments.project_change_semantic_272.access import prepared_pairs
from experiments.project_change_semantic_272.history import document_history
from experiments.project_change_semantic_272.packets import digest
from experiments.project_change_semantic_272.prompts import PROPOSE
from experiments.project_change_semantic_272.run import check_packet_scope
from .inference import infer_packet
from .provider import CodexProvider, AuthorizationBlocked, runtime_identity
from .schema import PROPOSAL, VERIFICATION

BASE = ROOT / 'semantic_codex_v1'
AUDIT = BASE / 'audit/PACKAGES.json'


def code_identity():
    return {str(p.relative_to(REPO)): sha(p)
            for package in ['project_change_272', 'project_change_semantic_272', 'project_change_semantic_codex_272']
            for p in sorted((REPO / 'experiments' / package).glob('*.py'))}


def verify_packet(path, expected, allowed, history):
    if sha(path) != expected:
        raise ValueError('Frozen packet file drift')
    p = read(path)
    if p['packet_id'] != digest({k: v for k, v in p.items() if k != 'packet_id'})[:24]:
        raise ValueError('Packet content ID drift')
    check_packet_scope(p, allowed, 'DEV')
    if any(e['page'] in history[p['pair_index']][side] for side in ['old', 'new'] for e in p['evidence'][side]):
        raise PermissionError('History source excluded')
    return p


def update_checkpoint(out, manifest, status, results, errors):
    calls = list((out / 'calls').glob('*/SUCCESS.json'))
    attempts = list((out / 'calls').glob('*/attempt_*/INVOCATION.json'))
    valid = [read(p) for p in (out / 'calls').glob('*/attempt_*/VALIDATION.json')]
    telemetry = dict(invocations=len(attempts), completed=len(calls),
        failed_or_interrupted=len(attempts) - len(calls), retries=len(attempts) - len({p.parent.parent for p in attempts}),
        input_tokens=sum(u.get('input_tokens', 0) for r in valid for u in r['usage']),
        cached_input_tokens=sum(u.get('cached_input_tokens', 0) for r in valid for u in r['usage']),
        output_tokens=sum(u.get('output_tokens', 0) for r in valid for u in r['usage']),
        seconds=sum(r['seconds'] for r in valid), cost_usd=None, unknown_cost_calls=len(calls))
    checkpoint = dict(at=now(), status=status, candidate=manifest['name'], partition='DEV',
        selected_packets=len(manifest['packets']), completed_packets=len(results),
        completed_by_route=dict(Counter(manifest['routes'][p] for p in results)),
        pending_packet_ids=sorted(set(manifest['routes']) - set(results)), errors=errors,
        coverage_complete=len(results) == len(manifest['packets']), telemetry=telemetry,
        config=manifest['config'], manifest_sha256=sha(out / 'MANIFEST.json'),
        project_changes=None, precision=None, coverage=None, high_value_recall=None,
        validation_opened=False, final_holdout_opened=False, mixed_openrouter_outputs=False)
    path = out / 'CHECKPOINT.json'
    tmp = out / 'CHECKPOINT.tmp'
    tmp.write_text(json.dumps(checkpoint, ensure_ascii=False, indent=2) + '\n')
    tmp.replace(path)
    return checkpoint


async def execute(name, smoke=False, parallel=2, closure_packets=None):
    if not name.startswith('codex_') or '/' in name or '..' in name:
        raise ValueError('Separate codex_ name required')
    if parallel not in [1, 2]:
        raise ValueError('Maximum two inference contexts')
    audit = read(AUDIT)
    if audit['status'] != 'PASS' or audit['split_sha256'] != sha(ROOT / 'SPLIT.json'):
        raise PermissionError('Completed unchanged-package audit required')
    rows = audit['packets']
    if smoke:
        rows = [next(r for r in rows if r['route'] == route)
                for route in ['dev_sheet_scopes_v1', 'dev_typed_semantic_v2']]
    elif len(rows) != 230 or Counter(r['route'] for r in rows) != {'dev_sheet_scopes_v1': 216, 'dev_typed_semantic_v2': 14}:
        raise ValueError('Full candidate requires all 216 + 14 frozen packets')
    primary_prompt = PROPOSE
    if closure_packets is not None:
        if smoke:
            raise ValueError('Closure cannot be a primary smoke test')
        from experiments.project_change_semantic_272.closure import CLOSURE_PROMPT
        directory = (BASE / closure_packets).resolve()
        if directory.parent != BASE.resolve():
            raise ValueError('Closure outside Codex artifact directory')
        closure = read(directory / 'MANIFEST.json')
        origin = BASE / 'runs' / closure['source_run']
        source = read(origin / 'RUN_RECEIPT.json')
        if (closure['partition'] != 'DEV' or source['completed_packets'] != 230
                or not source['coverage_complete'] or read(origin / 'MANIFEST.json')['smoke']
                or closure['source_run_receipt_sha256'] != sha(origin / 'RUN_RECEIPT.json')):
            raise PermissionError('Closure requires complete clean Codex-only primary inference')
        rows = [dict(packet_id=p.stem, path=str(p), sha256=sha(p), route='closure')
                for p in sorted((directory / 'packets').glob('*.json'))]
        if len(rows) != read(directory / 'COUNTS.json')['packets']:
            raise ValueError('Incomplete closure preparation')
        primary_prompt = CLOSURE_PROMPT
    allowed = {p['index']: p for p in prepared_pairs('DEV')}
    history = {i: {side: document_history(p[side], p['embargo_pages'][side])
                    for side in ['old', 'new']} for i, p in allowed.items()}
    packets = {r['packet_id']: verify_packet(r['path'], r['sha256'], allowed, history) for r in rows}
    config = runtime_identity()
    if not smoke:
        freeze_name = closure['source_run'] if closure_packets is not None else name
        frozen = read(BASE / 'candidates' / freeze_name / 'INFERENCE_FREEZE.json')
        if (frozen['code'] != code_identity() or frozen['config'] != config
                or frozen['audit_sha256'] != sha(AUDIT)):
            raise ValueError('DEV inference configuration drift; new smoke and candidate required')
    identity = dict(name=name, partition='DEV', smoke=smoke, config=config, parallelism=parallel,
        packets={r['path']: r['sha256'] for r in rows}, routes={r['packet_id']: r['route'] for r in rows},
        code=code_identity(), proposal_prompt_sha256=digest(primary_prompt),
        closure_packets=closure_packets,
        schema_hashes=dict(propose=digest(PROPOSAL), verify=digest(VERIFICATION)),
        audit_sha256=sha(AUDIT), split_sha256=sha(ROOT / 'SPLIT.json'))
    out = BASE / 'runs' / name
    out.mkdir(parents=True, exist_ok=True)
    lock = (out / '.lock').open('a')
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    if (out / 'MANIFEST.json').exists():
        manifest = read(out / 'MANIFEST.json')
        if manifest['identity_sha256'] != digest(identity):
            raise ValueError('Frozen configuration/code/input drift: new candidate required')
    else:
        manifest = dict(**identity, identity_sha256=digest(identity), created_at=now(),
            repo_commit=subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=REPO, text=True).strip(),
            historical_blind=False, output_contract='existing semantic proposal and audit',
            source_adjudication='NOT_SOURCE_ADJUDICATED', provider_cost_usd=None)
        immutable(out / 'MANIFEST.json', manifest)
    provider = CodexProvider(out, config)
    results = {}
    for key in packets:
        target = out / 'results' / (key + '.json')
        done = out / 'results' / (key + '.receipt.json')
        if target.exists() and done.exists():
            if read(done)['sha256'] != sha(target):
                raise ValueError('Completed packet result drift')
            results[key] = read(target)
    errors = []
    stopped = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in [signal.SIGINT, signal.SIGTERM]:
        loop.add_signal_handler(sig, stopped.set)
    sem = asyncio.Semaphore(parallel)

    async def call(packet, stage, system, data):
        return await provider.call(packet['packet_id'] + '_' + stage, system, data,
            VERIFICATION if stage.startswith('verify') else PROPOSAL, packet)

    async def one(row):
        async with sem:
            key = row['packet_id']
            if key in results or stopped.is_set() or provider.stopped:
                return
            target = out / 'results' / (key + '.json')
            done = out / 'results' / (key + '.receipt.json')
            try:
                if target.exists() and done.exists():
                    if read(done)['sha256'] != sha(target):
                        raise ValueError('Completed packet result drift')
                    result = read(target)
                else:
                    if target.exists():
                        raise ValueError('Orphan result requires explicit recovery')
                    result = await infer_packet(packets[key], call, primary_prompt)
                    immutable(target, result)
                    immutable(done, dict(sha256=sha(target)))
                results[key] = result
                print(key, 'completed', len(results), '/', len(rows), 'proposals', len(result['events']), flush=True)
            except Exception as exc:
                if isinstance(exc, AuthorizationBlocked):
                    provider.stopped = True
                errors.append(dict(packet_id=key, error_type=type(exc).__name__))
                stopped.set()
                print(key, type(exc).__name__, 'queue stopped', flush=True)
            update_checkpoint(out, manifest,
                'AUTHORIZATION_BLOCKED' if provider.stopped else ('RUNNING' if not stopped.is_set() else 'STOPPED'),
                results, errors)
    await asyncio.gather(*(one(row) for row in rows))
    status = 'AUTHORIZATION_BLOCKED' if provider.stopped else ('COMPLETE' if len(results) == len(rows) else 'INCOMPLETE')
    report = update_checkpoint(out, manifest, status, results, errors)
    if status == 'COMPLETE' and not (out / 'RUN_RECEIPT.json').exists():
        immutable(out / 'RUN_RECEIPT.json', dict(**report, results=list(results.values()),
            adjudication='NOT_SOURCE_ADJUDICATED'))
    print(json.dumps(dict(status=status, completed_packets=len(results), telemetry=report['telemetry']), ensure_ascii=False), flush=True)
    lock.close()
    return report


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--name', required=True)
    parser.add_argument('--smoke', action='store_true')
    parser.add_argument('--closure-packets')
    parser.add_argument('--parallel', type=int, choices=[1, 2], default=2)
    args = parser.parse_args()
    result = asyncio.run(execute(args.name, args.smoke, args.parallel, args.closure_packets))
    raise SystemExit(0 if result['status'] == 'COMPLETE' else 2)
