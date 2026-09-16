"""Resume only the frozen 18 contexts; freeze all discovery before package guard."""
import asyncio
from collections import Counter
from datetime import datetime, timezone
import hashlib
import itertools
import json
from pathlib import Path
import re
import statistics
import subprocess
import sys

from .preflight import OUT, ROOT, BASE, REPO, read, sha, write, SourceGuard
from .transport import compact, encode, decode, request_bytes

HERE = Path(__file__).parent
DEPS = ROOT / 'controlled_inference_f1_f4_f2_v3/runtime_deps'
sys.path.insert(0, str(DEPS))


def object_schema(properties):
    return dict(type='object', properties=properties, required=list(properties), additionalProperties=False)


def output_schema():
    string = dict(type='string')
    strings = dict(type='array', items=string)
    candidate = object_schema(dict(
        candidate_id=string, engineering_subject=string, subject_identity=string,
        scope=string, location=string, system_or_subsystem=string, claim_type=string,
        possible_change_summary=string, old_evidence_refs=strings, new_evidence_refs=strings,
        supporting_evidence_refs=strings,
        required_modalities=dict(type='array', items=dict(type='string', enum=['TEXT', 'TABLE', 'GRAPHIC'])),
        identity_confidence=dict(type='string', enum=['HIGH', 'MEDIUM', 'LOW']),
        comparison_readiness=dict(type='string', enum=['READY', 'MISSING_OLD', 'MISSING_NEW', 'MISSING_BOTH', 'INSUFFICIENT_EVIDENCE']),
        candidate_kind=dict(type='string', enum=['POTENTIAL_CHANGE', 'POTENTIAL_NOT_CHANGE', 'INSUFFICIENT_FOR_COMPARISON']),
        reason=string))
    return object_schema(dict(bundle_id=string, candidates=dict(type='array', items=candidate), coverage_notes=strings))


def verify_original():
    frozen = read(OUT / 'EXPERIMENT_FREEZE.json')
    for name, digest in read(OUT / 'PREFLIGHT_ARTIFACT_MANIFEST.json').items():
        if sha(OUT / name) != digest:
            raise ValueError('Original frozen artifact drift: ' + name)
    for path, digest in frozen['input_hashes'].items():
        if sha(path) != digest:
            raise ValueError('Source drift: ' + path)
    for name, digest in {**frozen['code'], **frozen['frozen_v4_code_hashes']}.items():
        if sha(REPO / name) != digest:
            raise ValueError('Frozen code drift: ' + name)
    return frozen


def source_payload(bundle):
    data = dict(frozen_bundle=bundle, source_regions={}, source_evidence={},
                graphic_descriptions_authority='AUXILIARY_INDEX_ONLY',
                missing_raster_policy='ONLY_ATTACHED_SOURCE_RASTERS_ARE_VISIBLE')
    package = read(bundle['source_file'])
    images = []
    registry = {}
    for side in ('old', 'new'):
        inventory = read(BASE / f'pair_8/DOCUMENT_INVENTORY_{side.upper()}.json')
        regions = {r['region_id']: r for r in inventory['regions']}
        data['source_regions'][side] = [regions[r['region_id']] for r in bundle[side]['region_refs']]
        expected = bundle[side]['evidence_refs']
        evidence = {r['evidence_id']: r for r in package['evidence_packet']['evidence'][side]}
        data['source_evidence'][side] = [evidence[r['evidence_id']] for r in expected]
        for region in data['source_regions'][side]:
            registry[region['region_id']] = dict(side=side, page=region['page'], route=region['source_type'])
        for e in data['source_evidence'][side]:
            registry[e['evidence_id']] = dict(side=side, page=e['page'], route=e['route'])
            if e.get('raster'):
                raster = dict(e['raster'], path=str((BASE / e['raster']['path']).resolve()))
                if sha(raster['path']) != raster['sha256']:
                    raise ValueError('Frozen source raster drift')
                images.append(dict(evidence_id=e['evidence_id'], side=side, page=e['page'], bbox=e['bbox'], **raster))
        if len(data['source_regions'][side]) != len(bundle[side]['region_refs']):
            raise ValueError('Region membership drift')
    data['reference_registry'] = registry
    return data, images


def prepare():
    if (OUT / 'RESUME_FREEZE.json').exists():
        raise FileExistsError('Resume preparation is immutable')
    original = verify_original()
    contexts = read(OUT / 'BROAD_CONTEXT_INDEX.json')['contexts']
    active = [b for b in contexts if not b['empty']]
    if len(contexts) != 20 or len(active) != 18:
        raise ValueError('Authorized frozen context count mismatch')
    from experiments.project_change_semantic_codex_272.provider import runtime_identity
    runtime = runtime_identity()
    commit = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=REPO, text=True).strip()
    sys.path.insert(0, '/tmp/pair_b_decomposer_runtime')
    import tiktoken
    tokenizer = tiktoken.get_encoding('o200k_base')
    # Codec/model capacity metadata is local; no inference or account probe.
    prompt = (HERE / 'decomposition_prompt.txt').read_text()
    schema = output_schema()
    paths = [OUT / 'EXPERIMENT_FREEZE.json', OUT / 'PREFLIGHT_ARTIFACT_MANIFEST.json',
             OUT / 'BROAD_CONTEXT_INDEX.json', *map(Path, original['input_hashes'])]
    guard = SourceGuard(paths, OUT)
    sys.addaudithook(guard.check)
    write(OUT / 'decomposition_inputs/PROMPT.txt', prompt)
    write(OUT / 'decomposition_inputs/OUTPUT_SCHEMA.json', schema)
    plan = []
    frozen_inputs = {}
    for b in active:
        data, images = source_payload(b)
        encoded = encode(data)
        if decode(encoded) != data:
            raise ValueError('Lossless transport round-trip failed')
        payload = request_bytes(prompt, encoded, images)
        tokens = len(tokenizer.encode(payload.decode('utf-8'), disallowed_special=()))
        key = b['bundle_id']
        directory = OUT / 'decomposition_inputs' / key
        write(directory / 'SOURCE_PAYLOAD.json', data)
        write(directory / 'MODEL_INPUT.json', encoded)
        write(directory / 'IMAGES.json', images)
        write(directory / 'REFERENCE_REGISTRY.json', data['reference_registry'])
        write(directory / 'EXACT_PROMPT.txt', payload.decode('utf-8'))
        # Reserve 32k output/reasoning + a conservative 20k allowance for <=8 images.
        capacity_pass = tokens + 32000 + 20000 <= int(272000 * .95)
        item = dict(bundle_id=key, coarse_heading=b['coarse_heading'],
                    exact_prompt_sha256=sha(directory / 'EXACT_PROMPT.txt'),
                    source_payload_sha256=sha(directory / 'SOURCE_PAYLOAD.json'),
                    frozen_bundle_sha256=hashlib.sha256(compact(b).encode()).hexdigest(),
                    input_tokens_o200k=tokens, input_characters=len(payload.decode('utf-8')),
                    images=len(images), capacity_pass=capacity_pass, lossless_roundtrip=True,
                    old_pages=len(b['old']['pages']), new_pages=len(b['new']['pages']))
        plan.append(item)
        for path in directory.iterdir():
            frozen_inputs[str(path)] = sha(path)
        print(f'PREPARED {key} tokens={tokens} images={len(images)} capacity={capacity_pass}', flush=True)
    write(OUT / 'DECOMPOSITION_CALL_PLAN.json', plan)
    for path in [OUT / 'decomposition_inputs/PROMPT.txt', OUT / 'decomposition_inputs/OUTPUT_SCHEMA.json',
                 OUT / 'DECOMPOSITION_CALL_PLAN.json']:
        frozen_inputs[str(path)] = sha(path)
    code = {str(p.relative_to(REPO)): sha(p) for p in HERE.iterdir() if p.suffix in {'.py', '.txt'}}
    write(OUT / 'RESUME_NO_TRUTH_LEAKAGE_RECEIPT.json', dict(status='PASS',
          original_receipt_sha256=sha(OUT / 'NO_TRUTH_LEAKAGE_RECEIPT.json'),
          source_only_reads=sorted(guard.reads), denied_reads=guard.denied,
          context_content_changed=False, lossless_roundtrip_all=True,
          source_truth_opened=False, validation='NOT OPENED', final_holdout='NOT OPENED',
          prior_exposure='Historical DEV-known corpus; aggregate baseline in user task',
          scope='Local read allowlist; model processes mount only individual call input artifacts'))
    write(OUT / 'RESUME_FREEZE.json', dict(
        status='READY' if all(x['capacity_pass'] for x in plan) else 'STOP_INPUT_CAPACITY',
        at=datetime.now(timezone.utc).isoformat(), repo_head=commit, code=code, runtime=runtime,
        original_freeze_sha256=sha(OUT / 'EXPERIMENT_FREEZE.json'),
        original_index_sha256=sha(OUT / 'BROAD_CONTEXT_INDEX.json'),
        broad_context_limit=20, active_contexts=18, total_contexts=20,
        excluded_empty_contexts=[b['bundle_id'] for b in contexts if b['empty']],
        input_hashes=frozen_inputs, retries=0, model='gpt-6-astra', reasoning='xhigh',
        context_content_changed=False, comparison_serializer_changed=False,
        source_truth_opened=False, original_prompt_existed=False,
        prompt_semantics='Initial implementation of the original discovery task; no known findings',
        capacity=dict(context_tokens=272000, effective_percent=95, output_reserve=32000,
                      image_reserve=20000, tokenizer='o200k_base', text_truncation=False)))


def verify_resume():
    verify_original()
    f = read(OUT / 'RESUME_FREEZE.json')
    if f['status'] != 'READY':
        raise ValueError('Resume is not ready')
    for path, digest in f['input_hashes'].items():
        if sha(path) != digest:
            raise ValueError('Resume input drift: ' + path)
    for path, digest in f['code'].items():
        if sha(REPO / path) != digest:
            raise ValueError('Resume code drift: ' + path)
    return f


def normalized_words(value):
    return set(re.findall(r'\w+', value.casefold()))


def diagnostics(results):
    candidates, audits = [], []
    for result in results:
        bundle = result['bundle_id']
        registry = read(OUT / f'decomposition_inputs/{bundle}/REFERENCE_REGISTRY.json')
        for c in result['candidates']:
            key = bundle + '/' + c['candidate_id']
            candidates.append(dict(c, local_candidate_id=key, source_bundle=bundle))
            old, new = c['old_evidence_refs'], c['new_evidence_refs']
            invalid = [r for r in old + new + c['supporting_evidence_refs'] if r not in registry]
            wrong_side = [r for side, refs in [('old', old), ('new', new)] for r in refs
                          if r in registry and registry[r]['side'] != side]
            routes = {s: sorted({registry[r]['route'] for r in refs if r in registry and registry[r]['side'] == s})
                      for s, refs in [('old', old), ('new', new)]}
            cross = bool(routes['old'] and routes['new'] and
                         any(a != b for a in routes['old'] for b in routes['new']))
            audits.append(dict(local_candidate_id=key, invalid_refs=invalid, wrong_side_refs=wrong_side,
                               has_old_new_refs=bool(old and new), one_sided=bool(old) != bool(new),
                               no_refs=not old and not new, source_routes=routes, cross_modal=cross))
    duplicates = []
    for left, right in itertools.combinations(candidates, 2):
        if left['source_bundle'] == right['source_bundle']:
            continue
        words = [normalized_words(' '.join(c[k] for k in
                 ['engineering_subject', 'scope', 'location', 'possible_change_summary'])) for c in [left, right]]
        score = len(words[0] & words[1]) / max(1, len(words[0] | words[1]))
        refs = [set(c['old_evidence_refs'] + c['new_evidence_refs']) for c in [left, right]]
        same_subject = left['engineering_subject'].casefold() == right['engineering_subject'].casefold()
        if score >= .65 or (same_subject and bool(refs[0] & refs[1])):
            duplicates.append(dict(left=left['local_candidate_id'], right=right['local_candidate_id'],
                                   lexical_jaccard=round(score, 4), same_subject=same_subject,
                                   method='LEXICAL_DIAGNOSTIC_ONLY_NO_DELETION'))
    return candidates, audits, duplicates


def finalize():
    verify_resume()
    plan = read(OUT / 'DECOMPOSITION_CALL_PLAN.json')
    results = []
    usage = Counter(calls=0, input_tokens=0, output_tokens=0, cached_input_tokens=0, failures=0)
    for item in plan:
        path = OUT / 'decomposition_raw' / item['bundle_id']
        receipt = read(path / 'SUCCESS.json')
        if sha(path / 'parsed.json') != receipt['output_sha256']:
            raise ValueError('Output drift')
        results.append(read(path / 'parsed.json'))
        usage['calls'] += 1
        for turn in receipt['usage']:
            for key in ['input_tokens', 'output_tokens', 'cached_input_tokens']:
                usage[key] += turn.get(key, 0)
    if usage['calls'] != 18:
        raise ValueError('All 18 discovery calls must complete before freeze')
    write(OUT / 'DECOMPOSITION_RESULTS.json', results)
    files = {str(p.relative_to(OUT)): sha(p) for folder in ['decomposition_inputs', 'decomposition_raw']
             for p in (OUT / folder).rglob('*') if p.is_file()}
    files['DECOMPOSITION_RESULTS.json'] = sha(OUT / 'DECOMPOSITION_RESULTS.json')
    write(OUT / 'DECOMPOSITION_FREEZE.json', dict(status='FROZEN', at=datetime.now(timezone.utc).isoformat(),
          files=files, broad_context_index_sha256=sha(OUT / 'BROAD_CONTEXT_INDEX.json'),
          resume_freeze_sha256=sha(OUT / 'RESUME_FREEZE.json'), model_calls=18,
          source_truth_opened=False, result_freeze_created=False, context_content_changed=False))
    candidates, audits, duplicates = diagnostics(results)
    write(OUT / 'LOCAL_CANDIDATE_INDEX.json', candidates)
    write(OUT / 'LOCAL_CANDIDATE_REFERENCE_AUDIT.json', audits)
    write(OUT / 'DUPLICATE_LOOKING_CANDIDATES.json', duplicates)
    counts = {r['bundle_id']: len(r['candidates']) for r in results}
    guard = len(candidates) <= 50
    status = 'DECOMPOSITION_COMPLETE_PACKAGE_GUARD_PASS' if guard else 'STOP_LOCAL_CANDIDATE_EXPLOSION'
    summary = dict(status=status, broad_contexts=18, broad_context_limit=20, context_content_changed=False,
        decomposition_calls=18, local_candidates=len(candidates), candidates_per_context=counts,
        distribution=dict(min=min(counts.values()), median=statistics.median(counts.values()), max=max(counts.values())),
        duplicate_looking_pairs=len(duplicates), duplicates_removed=0,
        candidates_with_old_new_refs=sum(a['has_old_new_refs'] for a in audits),
        one_sided_candidates=sum(a['one_sided'] for a in audits),
        no_refs_candidates=sum(a['no_refs'] for a in audits),
        cross_modal_candidates=sum(a['cross_modal'] for a in audits),
        cross_modal_definition='At least one differing pair of referenced source modalities across OLD and NEW',
        invalid_reference_candidates=sum(bool(a['invalid_refs'] or a['wrong_side_refs']) for a in audits),
        local_package_guard='PASS' if guard else 'FAIL', comparison_calls=0,
        source_truth_opened=False, no_truth_leakage='PASS',
        explanation='Independent claims per broad context plus overlapping source pages across broad contexts; raw outputs retained without deduplication',
        production='UNCHANGED', validation='NOT OPENED', final_holdout='NOT OPENED')
    write(OUT / 'DECOMPOSITION_SUMMARY.json', summary)
    write(OUT / 'RESUME_MODEL_USAGE.json', dict(DECOMPOSITION=dict(usage),
          COMPARISON=dict(calls=0, input_tokens=0, output_tokens=0, failures=0), TOTAL=dict(usage), OpenRouter=0, Claude=0))
    lines = ['# Pair B / ИОС4.2 — resume', '', f'STATUS: {status}', '',
             'BROAD CONTEXTS: 18 non-empty; LIMIT: 20', 'CONTEXT CONTENT CHANGED: NO',
             f'DECOMPOSITION CALLS: 18; LOCAL CANDIDATES: {len(candidates)}',
             f'CANDIDATES PER CONTEXT min/median/max: {summary["distribution"]}',
             f'OLD+NEW refs: {summary["candidates_with_old_new_refs"]}; one-sided: {summary["one_sided_candidates"]}; no refs: {summary["no_refs_candidates"]}',
             f'CROSS-MODAL CANDIDATES: {summary["cross_modal_candidates"]}',
             f'DUPLICATE-LOOKING PAIRS: {len(duplicates)}; removed: 0',
             f'LOCAL PACKAGE GUARD: {summary["local_package_guard"]}; limit: 50',
             f'Invalid-reference candidates: {summary["invalid_reference_candidates"]}',
             'COMPARISON CALLS: 0', '', '## Per-context counts', '']
    headings = {p['bundle_id']: p['coarse_heading'] for p in plan}
    lines += [f'- {headings[k]} ({k}): {v}' for k, v in counts.items()]
    lines += ['', '## Interpretation', '',
              'Каждый broad context допускает несколько локальных claims. Контексты перекрываются по исходным страницам;',
              'один предмет может появиться в нескольких независимых ответах. Количество исходных candidates сохранено,',
              'лексическое сходство используется только для диагностики, не для удаления или оценки истинности.',
              'DECOMPOSITION_FREEZE создан после всех 18 успешных вызовов. Полный result freeze не создан.',
              'PROVEN findings denominator: 10 (из задания); discovery/subject/package/Astra/ACCEPT coverage: NOT EVALUATED.',
              'CORRECT ACCEPT / FALSE ACCEPT / F13: NOT EVALUATED.',
              'SUBJECT_TOO_BROAD before 4, after NOT EVALUATED; PACKAGE_SCOPE_WRONG before 4, after NOT EVALUATED.',
              'NO TRUTH LEAKAGE: PASS; source truth не открыта. Историческая известность DEV сохранена в receipts.',
              'PRODUCTION: UNCHANGED; Pair A / UI / baseline: UNCHANGED.',
              'VALIDATION: NOT OPENED; FINAL HOLDOUT: NOT OPENED.',
              'RECOMMENDATION: MORE_CONTROLLED_TESTING_REQUIRED.',
              'При STOP_LOCAL_CANDIDATE_EXPLOSION comparison и evaluation не запускались по пункту 7 resume.',
              'Разбиение контекста получено, но его точность и пригодность для final admission этим запуском не установлены.', '']
    write(OUT / 'RESUME_FINAL_REPORT.md', '\n'.join(lines))
    print(json.dumps(summary, ensure_ascii=False), flush=True)


async def run():
    from .broad_provider import call
    verify_resume()
    if (OUT / 'decomposition_raw').exists():
        raise FileExistsError('No automatic retries; inspect previous calls first')
    plan = read(OUT / 'DECOMPOSITION_CALL_PLAN.json')
    schema = read(OUT / 'decomposition_inputs/OUTPUT_SCHEMA.json')
    # Process two isolated requests at a time, as allowed by the existing provider.
    for start in range(0, len(plan), 2):
        verify_resume()
        batch = plan[start:start + 2]
        async def invoke(item):
            key = item['bundle_id']
            print('START ' + key, flush=True)
            value = await call(key, OUT / 'decomposition_inputs' / key, OUT / 'decomposition_raw',
                               schema, item['exact_prompt_sha256'])
            print(f'COMPLETE {key} candidates={len(value["candidates"])}', flush=True)
            return value
        outcomes = await asyncio.gather(*(invoke(p) for p in batch), return_exceptions=True)
        errors = [v for v in outcomes if isinstance(v, BaseException)]
        if errors:
            write(OUT / 'DECOMPOSITION_STOP.json', dict(status='STOP_MODEL_FAILURE_NO_RETRY',
                  errors=[str(e) for e in errors], source_truth_opened=False,
                  calls_started=len(list((OUT / 'decomposition_raw').glob('*/INVOCATION.json'))),
                  calls_successful=len(list((OUT / 'decomposition_raw').glob('*/SUCCESS.json')))))
            raise errors[0]
    finalize()


if __name__ == '__main__':
    action = sys.argv[1]
    if action == 'prepare':
        prepare()
    elif action == 'run':
        asyncio.run(run())
    elif action == 'verify':
        verify_resume()
        print('RESUME FREEZE VERIFIED')
    elif action == 'finalize':
        finalize()
    else:
        raise ValueError(action)
