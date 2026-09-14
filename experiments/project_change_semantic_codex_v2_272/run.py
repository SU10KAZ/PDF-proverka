"""One frozen diagnostic only, fresh context per case; no full DEV entry point."""
import argparse
import asyncio
import fcntl
import json
from pathlib import Path

from experiments.project_change_272.inventory import REPO, now, read, sha
from experiments.project_change_semantic_272.access import prepared_pairs
from experiments.project_change_semantic_272.history import document_history
from experiments.project_change_semantic_272.packets import digest
from experiments.project_change_semantic_codex_272.provider import CodexProvider, runtime_identity, AuthorizationBlocked
from .packages import BASE, save, validate_package
from .contracts import evaluate
from .schema import PROMPT, OUTPUT


def identity():
    return {str(p.relative_to(REPO)): sha(p) for directory in
        ['project_change_semantic_codex_v2_272', 'project_change_semantic_codex_272',
         'project_change_semantic_272', 'project_change_272']
        for p in sorted((REPO/'experiments'/directory).glob('*.py'))}


def model_view(p):
    # No labels, audit decisions, expected outcomes or original validation enter inference.
    return {k: p[k] for k in ['case_id','object_id','old','new','logical_baseline',
        'engineering_subject','untrusted_candidate_summary','candidate_claims','sources',
        'allowed_page_scopes','coverage_complete','old_retrieval']}


def image_packet(p):
    return dict(evidence={side: [dict(e, source_kind='PDF_RASTER_CROP',
        raster={k:v for k,v in e['raster'].items() if k not in {'page','side','bbox','evidence_id'}})
        for e in p['sources'] if e['side'] == side and e.get('raster')] for side in ['old','new']})


def freeze():
    index = read(BASE/'PACKAGE_INDEX.json')
    config = runtime_identity()
    for r in index['packages']:
        if sha(r['path']) != r['sha256']:
            raise ValueError('Package drift')
        if len(PROMPT)+len(json.dumps(model_view(read(r['path'])),ensure_ascii=False)) > 60000:
            raise ValueError('Provider text budget exceeded')
    value = dict(algorithm_version='CODEX_V2_ARCHITECTURAL_DIAGNOSTIC_V1_R2',
        purpose='DEV_DIAGNOSTIC_ONLY_NO_RESERVE_NO_FULL_DEV', code=identity(), config=config,
        prompt_sha256=digest(PROMPT), schema_sha256=digest(OUTPUT),
        package_index_sha256=sha(BASE/'PACKAGE_INDEX.json'),
        diagnostic_manifest_sha256=sha(BASE/'DIAGNOSTIC_MANIFEST.json'),
        registry_sha256=sha(BASE/'SOURCE_SCOPE_REGISTRY.json'), max_calls=len(index['packages']),
        max_parallelism=2, retries=0, full_dev_authorized=False)
    save(BASE/'DIAGNOSTIC_FREEZE.json', value)
    print('Frozen diagnostic', len(index['packages']), flush=True)


async def execute(limit=None):
    frozen = read(BASE/'DIAGNOSTIC_FREEZE.json')
    if frozen['code'] != identity() or frozen['config'] != runtime_identity():
        raise ValueError('Frozen diagnostic implementation/config drift')
    for file,key in [('PACKAGE_INDEX.json','package_index_sha256'),
                     ('DIAGNOSTIC_MANIFEST.json','diagnostic_manifest_sha256'),
                     ('SOURCE_SCOPE_REGISTRY.json','registry_sha256')]:
        if sha(BASE/file) != frozen[key]:
            raise ValueError('Frozen diagnostic input drift')
    if (BASE/'REJECTION.json').exists():
        raise PermissionError('Rejected diagnostic cannot auto-resume')
    pairs = {p['index']: p for p in prepared_pairs('DEV')}
    exclusions = {(i,s): set(p['embargo_pages'][s]) | set(document_history(p[s],p['embargo_pages'][s]))
                  for i,p in pairs.items() for s in ['old','new']}
    provider = CodexProvider(BASE/'inference')
    items = read(BASE/'PACKAGE_INDEX.json')['packages']
    completed = 0
    queue = asyncio.Queue()
    for entry in items[:limit] if limit is not None else items:
        queue.put_nowait(entry)
    stopped = False
    async def one(entry):
        nonlocal completed, stopped
        if sha(entry['path']) != entry['sha256']:
            raise ValueError('Frozen package drift')
        p = read(entry['path']);validate_package(p,pairs,exclusions)
        cid = p['case_id'];call = BASE/'inference'/'calls'/cid
        if (call/'REQUEST.json').exists() and not (call/'SUCCESS.json').exists():
            raise PermissionError('Failed/interrupted call requires a new reviewed diagnostic, no automatic retry')
        try:
            result = await provider.call(cid, PROMPT, model_view(p), OUTPUT, image_packet(p))
        except Exception as exc:
            stopped = True
            save(BASE/'stops'/(cid+'.json'), dict(at=now(), case_id=cid,
                status='AUTHORIZATION_BLOCKED' if isinstance(exc,AuthorizationBlocked) else 'FAILED_CLOSED',
                error_type=type(exc).__name__, retry=False))
            raise
        local = evaluate(p,result)
        save(BASE/'results'/(cid+'.json'), dict(output=result, gate=local))
        completed += 1
        print(cid,local['status'],len(local['reasons']),'completed',completed,flush=True)
    async def worker():
        nonlocal stopped
        while not stopped and not queue.empty():
            entry = queue.get_nowait()
            try:
                await one(entry)
            except Exception:
                stopped = True
                raise
    # Let already-started calls retain their result if the other worker fails.
    results = await asyncio.gather(worker(),worker(),return_exceptions=True)
    for result in results:
        if isinstance(result,BaseException):
            raise result


if __name__ == '__main__':
    parser=argparse.ArgumentParser();parser.add_argument('action',choices=['freeze','run'])
    parser.add_argument('--limit',type=int);args=parser.parse_args()
    with (BASE/'EXECUTION.lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        if args.action=='freeze':freeze()
        else:asyncio.run(execute(args.limit))
