"""Explicit research-only continuation. Never imports the old executable harness."""
import argparse
import copy
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import traceback
from datetime import datetime, timezone

REPO = Path('/home/coder/projects/PDF-proverka')
ORIGINAL = Path('/home/coder/auditmanager/corpus-audits/20260921_dev5_astra_controlled_comparison_v3')
OUT = Path('/home/coder/auditmanager/corpus-audits/20260921_dev5_astra_controlled_resume')
RELEASE = Path('/home/coder/auditmanager/releases/ui-real-0e5d0837-v3opus/app')
sys.path.insert(0, str(REPO))
from experiments.project_change_272.astra_resume.metadata import MODEL, REASONING, normalize_checkpoint, region_id
from experiments.project_change_272.policy import admitted_pairs
from experiments.project_change_272.astra_resume.budget import reserve as budget_reserve
sys.path.insert(0, str(RELEASE))
from backend.app.services.project_change_v3 import engine, provider, contracts, transport


def read(p): return json.loads(Path(p).read_text())
def sha(p): return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def digest(v): return hashlib.sha256(json.dumps(v, ensure_ascii=False, sort_keys=True).encode()).hexdigest()
def now(): return datetime.now(timezone.utc).isoformat()
def write(name, value):
    p = OUT / name
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix('.tmp')
    tmp.write_text(json.dumps(value, ensure_ascii=False, indent=2, default=str) + '\n')
    tmp.replace(p)


def preflight():
    assert len(admitted_pairs(indices=[5])) == 1
    frozen = read(ORIGINAL / 'ASTRA_RESULT_FREEZE.json')
    for name, expected in frozen['files'].items():
        assert sha(ORIGINAL / name) == expected, name
    parity = read(ORIGINAL / 'SOURCE_PARITY.json')
    for name, expected in parity['engine_code_sha256'].items():
        assert sha(RELEASE / name) == expected, name
    for name, expected in parity['package_files'].items():
        assert sha(ORIGINAL / 'artifacts/source' / name) == expected, name
    for name, item in parity['sources'].items():
        assert sha(item['path']) == item['sha256'], name
    for name, expected in parity['prompt_sha256'].items():
        assert getattr(contracts, name.upper()) == expected
    for name, expected in read(ORIGINAL/'EXECUTION_CONTRACT.json')['files'].items():
        assert sha(ORIGINAL/name) == expected, name
    checkpoint_path = next((ORIGINAL / 'artifacts/miner_checkpoints').glob('*.json'))
    checkpoint = read(checkpoint_path)
    assert [r['region_id'] for r in checkpoint['regions']] == [f'A-R{i:03}' for i in range(1, 12)]
    receipts = {}
    for d in sorted((ORIGINAL/'calls').iterdir()):
        meta, receipt = read(d/'input.json'), read(d/'relay_receipt.json')
        assert receipt['received_generating_requests'] == receipt['forwarded_generating_requests'] == 1
        assert receipt['model'] == MODEL and receipt['reasoning']['effort'] == REASONING
        assert not receipt['blocked_additional_requests'] and not receipt['websocket_requests']
        assert sha(d/'provider_request.json') == receipt['request_body_sha256']
        receipts[meta['call_id']] = {**receipt, 'lineage': {'path': str(d/'relay_receipt.json'), 'sha256': sha(d/'relay_receipt.json')}}
    assert len(receipts) == 13
    mapping = read(ORIGINAL/'artifacts/project_change_v3_semantic_map.json')
    assert len(mapping['regions']) == 14
    for r in checkpoint['regions']:
        assert r['validation'] == 'ACCEPTED'
        d = next(d for d in (ORIGINAL/'calls').iterdir() if read(d/'input.json')['call_id'] == r['accepted_call_id'])
        assert r['result'] == read(d/'structured_response.json')
        assert receipts[r['accepted_call_id']]['response_completed']
    OUT.mkdir(exist_ok=True)
    normalized = normalize_checkpoint(checkpoint, receipts)
    normalized['original_checkpoint'] = {'path': str(checkpoint_path), 'sha256': sha(checkpoint_path)}
    write('NORMALIZED_ORIGINAL_CHECKPOINT.json', normalized)
    original_hashes = {str(p.relative_to(ORIGINAL)): sha(p) for p in ORIGINAL.rglob('*') if p.is_file()}
    write('ORIGINAL_INTEGRITY.json', original_hashes)
    work = OUT/'artifacts'
    work.mkdir(exist_ok=True)
    for name in ('source', 'DOCUMENT_STRUCTURE.json', 'SOURCE_MANIFEST.json'):
        if not (work/name).exists(): (work/name).symlink_to(ORIGINAL/'artifacts'/name)
    write('PREFLIGHT.json', {'status': 'PASS', 'mapper_reused': True, 'accepted_regions': 11,
                           'metadata': 'FIXED', 'model': MODEL, 'reasoning': REASONING,
                           'original_freeze_sha256': sha(ORIGINAL/'ASTRA_RESULT_FREEZE.json'),
                           'evaluation_control_opened': False})
    return checkpoint, mapping, parity


class ResumeProvider:
    model = MODEL
    reasoning = REASONING
    provider = 'codex_cli_subscription'
    last_transport = None
    last_response = None

    def __init__(self, checkpoint, mapping, live, continuation=False):
        self.checkpoint, self.mapping, self.live = checkpoint, mapping, live
        self.usage = []
        self.reused = []
        self.originals = {read(d/'input.json')['call_id']: d for d in (ORIGINAL/'calls').iterdir()}
        self.accepted = {r['accepted_call_id']: r for r in checkpoint['regions']}
        self.next_region = 12
        self.retry_seen = set()
        if continuation:
            prior = read(OUT/'RESUME_USAGE.json')
            assert len(prior) == 1 and prior[0]['region_id'] == 'A-R012' and prior[0]['provider_ok']
            d = OUT/'calls/014_p290a06df79_A-R012'
            receipt = read(d/'relay_receipt.json')
            assert receipt['response_completed'] and receipt['forwarded_generating_requests'] == 1
            saved = read(next((OUT/'artifacts/miner_checkpoints').glob('*.json')))
            assert len(saved['regions']) == 12
            r = saved['regions'][-1]
            assert r['region_id'] == 'A-R012' and r['validation'] == 'ACCEPTED'
            assert r['result'] == read(d/'structured_response.json')
            self.accepted[r['accepted_call_id']] = r
            self.originals[r['accepted_call_id']] = d
            self.usage = prior
            self.next_region = 13

    def complete(self, **kw):
        self.last_response = self.last_transport = None
        call_id = kw['call_id']
        payload, images, labels = provider.build_codex_payload(kw['prompt'], kw['data'], kw['images'])
        rid = region_id(kw['data'])
        d_old = self.originals.get(call_id)
        if d_old:
            old = read(d_old/'input.json')
            assert payload == (d_old/'payload.txt').read_text(), 'PAYLOAD_DRIFT'
            assert kw['schema'] == read(d_old/'schema.json'), 'SCHEMA_DRIFT'
            assert kw['prompt'] == old['prompt'] and kw['data'] == old['data']
            assert images == read(d_old/'images.json'), 'IMAGE_PATH_DRIFT'
            assert [sha(p) for p in images] == read(d_old/'WIRE_PARITY.json')['planned_image_sha256']
        if kw['stage'] == 'MAPPING' or call_id in self.accepted:
            assert d_old is not None
            self.last_transport = read(d_old/'transport.json')
            self.last_transport.update(model=MODEL, reasoning=REASONING, region_id=rid,
                                       reused_from=str(d_old), new_provider_post=False)
            result = self.mapping if kw['stage'] == 'MAPPING' else self.accepted[call_id]['result']
            assert result == read(d_old/'structured_response.json')
            self.reused.append(call_id)
            write('REUSED.json', self.reused)
            return copy.deepcopy(result)
        assert kw['stage'] in ('MINING', 'DEDUPE')
        if kw['stage'] == 'MINING':
            assert rid in ('A-R012', 'A-R013', 'A-R014')
            index = int(rid[-3:])
            if '_RETRY_' in call_id:
                assert call_id.endswith('_RETRY_1') and rid not in self.retry_seen
                assert index == self.next_region - 1
                self.retry_seen.add(rid)
            else:
                assert index == self.next_region
                self.next_region += 1
        else:
            assert self.next_region == 15
        spent = sum((r.get('usage') or {}).get('input_tokens', 0) + (r.get('usage') or {}).get('output_tokens', 0) for r in self.usage)
        estimate = budget_reserve(payload, json.dumps(kw['schema']), len(images))
        reserve = estimate['total']
        write('BUDGET_'+call_id+'.json', {**estimate, 'spent': spent, 'soft': 1000000, 'hard': 1500000})
        assert spent < 1000000 and spent + reserve <= 1500000, 'RESUME_BUDGET_REVIEW_REQUIRED'
        if any(not r.get('usage') for r in self.usage): raise RuntimeError('RESUME_MISSING_USAGE_STOP')
        meta = {'authorized_attempt_id': self.checkpoint['run_id'] + '_RESUME_' + f'{len(self.usage)+1:03}_{call_id}',
                'call_id': call_id, 'stage': kw['stage'], 'region_id': rid, 'model': MODEL, 'reasoning': REASONING,
                'payload_sha256': hashlib.sha256(payload.encode()).hexdigest(),
                'attempt': 2 if rid == 'A-R012' and '_RETRY_' not in call_id else (2 if '_RETRY_' in call_id else 1),
                'usage_category': 'TRANSPORT_FAILURE_RETRY_OVERHEAD' if rid == 'A-R012' else 'RESUME_PIPELINE',
                'new_provider_post': True}
        if rid == 'A-R012' and '_RETRY_' not in call_id:
            assert meta['payload_sha256'] == read(d_old/'input.json')['payload_sha256']
            write('A_R012_PARITY.json', {**meta, 'status': 'IDENTICAL', 'schema_identical': True,
                                       'images_identical': True, 'original_attempt': str(d_old),
                                       'original_receipt_sha256': sha(d_old/'relay_receipt.json')})
        if not self.live:
            write('DRY_RUN_READY.json', {**meta, 'projected_reserve': reserve, 'reused_calls': self.reused})
            raise provider.ProviderError('ZERO_MODEL_PREFLIGHT_COMPLETE')
        ordinal = f'{len(self.usage)+14:03}_{call_id}'
        d = OUT/'calls'/ordinal
        d.mkdir(parents=True, exist_ok=False)
        write('calls/'+ordinal+'/input.json', {**kw, **meta, 'labels': labels})
        (d/'payload.txt').write_text(payload)
        write('calls/'+ordinal+'/schema.json', kw['schema'])
        write('calls/'+ordinal+'/images.json', images)
        print(now(), 'DISPATCH', ordinal, 'spent', spent, 'reserve', reserve, flush=True)
        try:
            with (d/'supervisor.log').open('w') as log:
                subprocess.run([sys.executable, str(ORIGINAL/'controlled_attempt.py'), str(d)],
                               stdout=log, stderr=subprocess.STDOUT, timeout=3800, check=False)
            dispatch = read(d/'relay_receipt.json')
            raw_usage = (dispatch.get('usage_receipts') or [None])[-1]
            usage = provider.normalized_usage(raw_usage)
            if usage:
                usage['cached_input_tokens'] = raw_usage.get('input_tokens_details', {}).get('cached_tokens', 0)
                usage['reasoning_output_tokens'] = raw_usage.get('output_tokens_details', {}).get('reasoning_tokens', 0)
            self.last_transport = {**meta, 'usage': usage, 'dispatch_receipt': str(d/'relay_receipt.json'),
                                   'provider': self.provider, 'provider_ok': False}
            self.usage.append(self.last_transport)
            write('RESUME_USAGE.json', self.usage)
            assert dispatch['received_generating_requests'] <= 1 and dispatch['forwarded_generating_requests'] <= 1, 'UNAUTHORIZED_REDISPATCH'
            if dispatch.get('websocket_requests') or dispatch['forwarded_generating_requests'] != 1 or not dispatch.get('response_completed'):
                raise provider.ProviderError('ASTRA_TRANSPORT_UNSTABLE_AT_A_R012' if rid == 'A-R012' else 'CONTROLLED_TRANSPORT_FAILED')
            assert read(d/'CLI_EXIT.json')['exit_code'] == 0
            self.last_response = read(d/'last_message.txt')
            write('calls/'+ordinal+'/structured_response.json', self.last_response)
            import jsonschema
            jsonschema.validate(self.last_response, kw['schema'])
            self.last_transport['provider_ok'] = True
            write('RESUME_USAGE.json', self.usage)
            return self.last_response
        finally:
            if self.last_transport: write('calls/'+ordinal+'/transport.json', self.last_transport)


def main():
    args = argparse.ArgumentParser()
    args.add_argument('--live', action='store_true')
    args.add_argument('--continue-after-budget-review', action='store_true')
    parsed = args.parse_args()
    live, continuation = parsed.live, parsed.continue_after_budget_review
    if continuation:
        assert live and (OUT/'LIVE_STARTED.json').exists()
        assert not (OUT/'LIVE_CONTINUATION_STARTED.json').exists()
        assert 'RESUME_BUDGET_REVIEW_REQUIRED' in read(OUT/'STOP.json')['error']
        write('BUDGET_STOP_PRESERVED.json', read(OUT/'STOP.json'))
    elif (OUT/'LIVE_STARTED.json').exists():
        raise SystemExit('Refuse automatic restart of resume')
    checkpoint, mapping, parity = preflight()
    work = OUT/('continued_artifacts' if continuation else ('artifacts' if live else 'dry_run_artifacts'))
    work.mkdir(exist_ok=True)
    for name in ('source', 'DOCUMENT_STRUCTURE.json', 'SOURCE_MANIFEST.json'):
        if not (work/name).exists(): (work/name).symlink_to(ORIGINAL/'artifacts'/name)
    manifest = read(work/'SOURCE_MANIFEST.json')
    prepared = {'manifest': manifest, 'structure': read(work/'DOCUMENT_STRUCTURE.json'),
                'source_packaging_version': manifest['source_packaging_version'], 'structure_sha256': manifest['structure_sha256']}
    paths = {side: {key: Path(parity['sources'][side+'_'+key]['path']) for key in ('pdf', 'blocks', 'markdown')} for side in ('old', 'new')}
    rp = ResumeProvider(checkpoint, mapping, live, continuation)
    engine.get_provider = lambda: rp
    engine._work_dir = lambda *a: work
    engine._save_artifact = lambda session, pair, name, value: write(str(work.relative_to(OUT))+'/'+name+'.json', value)
    engine.prepare_comparison_sources = lambda **kw: prepared
    # Absolute original paths retain the exact accepted evidence/payload bytes.
    original_bundle = engine.optimized_region_bundle
    engine.optimized_region_bundle = lambda **kw: original_bundle(**{**kw, 'work_dir': ORIGINAL/'artifacts'})
    base_provenance = engine.build_provenance
    def provenance(**kw):
        p = base_provenance(**kw)
        p.update(model=MODEL, reasoning=REASONING, provider=rp.provider, thinking={'effort': REASONING},
                 engine_variant='ProjectChange V3 / Astra research', provider_transport_version=transport.CODEX_TRANSPORT_VERSION,
                 transport_version=transport.CODEX_TRANSPORT_VERSION)
        return p
    engine.build_provenance = provenance
    engine._publish_human_mapping = lambda **kw: {'object_id': None, 'review_region_count': 0, 'research_publication_suppressed': True}
    def state(status, message, reason, **kw):
        value = {'status': status, 'message': message, 'reason_code': reason, **kw, 'production_write': False}
        write('state.json', value)
        print(now(), status, reason, message, flush=True)
        return value
    if live: write('LIVE_CONTINUATION_STARTED.json' if continuation else 'LIVE_STARTED.json', {'at': now(), 'run_id': checkpoint['run_id'], 'additional_soft': 1000000, 'additional_hard': 1500000})
    try:
        result = engine._run_admitted(session_id=checkpoint['session_id'], pair_id=checkpoint['pair_id'],
                                      object_id=None, old_paths=paths['old'], new_paths=paths['new'],
                                      run_id=checkpoint['run_id'], using_test_provider=False, state=state)
        assert result['reason_code'] == 'v3_completed'
        final_path = work/'project_change_v3_result.json'
        assert final_path.exists()
        for name, expected in read(OUT/'ORIGINAL_INTEGRITY.json').items(): assert sha(ORIGINAL/name) == expected, name
        freeze = {'ASTRA_DEV5_RESULT_FROZEN': True, 'frozen_at': now(), 'run_id': checkpoint['run_id'],
                  'model': MODEL, 'reasoning': REASONING, 'final_result_sha256': sha(final_path),
                  'original_root': str(ORIGINAL), 'original_files': read(OUT/'ORIGINAL_INTEGRITY.json'),
                  'files': {str(p.relative_to(OUT)): sha(p) for folder in (work, OUT/'artifacts', OUT/'calls') for p in folder.rglob('*') if p.is_file()},
                  'prompt_sha256': parity['prompt_sha256'], 'sources': parity['sources'],
                  'known_missing_usage': 'Original interrupted A-R012: UNKNOWN / no final usage receipt',
                  'original_confirmed_input_plus_output': 1087479, 'resume_usage': rp.usage,
                  'quality_control_not_opened': True}
        write('ASTRA_RESULT_FREEZE.json', freeze)
        print('ASTRA_DEV5_RESULT_FROZEN=true', flush=True)
    except Exception as exc:
        write('STOP.json' if live else 'DRY_RUN_STOP.json', {'error': str(exc), 'traceback': traceback.format_exc(),
              'ASTRA_DEV5_RESULT_FROZEN': False, 'no_further_calls': True})
        print(traceback.format_exc(), flush=True)
        if live: raise


if __name__ == '__main__': main()
