"""Post-run assessment only after exactly 12 immutable successful model responses."""
from collections import Counter
from pathlib import Path

from .common import OUT, read, sha, ref, write, previous_hashes
from .normalization import normalize
from .run import verify, collect, exact_prompt, provider_module


def report():
    frozen, index = verify()
    progress = collect()
    if progress['completed_count'] != 12 or progress['model_calls'] != 12:
        raise PermissionError('Truth sealed until exactly 12 successful calls')
    provider_module()
    from experiments.project_change_semantic_codex_272.schema import validate
    responses, receipts = {}, []
    for row in index:
        path = OUT / 'calls' / row['case_token'] / 'SUCCESS.json'
        success = read(path)
        assert success['status'] == 'SUCCESS' and not success['tool_items']
        response_path = Path(success['normalized_path'])
        assert sha(response_path) == success['normalized_sha256']
        assert (response_path.parent / 'prompt.txt').read_text() == exact_prompt(row)
        response = read(response_path)
        validate(response, read(OUT / 'OUTPUT_SCHEMA.json'))
        assert response['case_token'] == row['case_token']
        responses[row['key']] = response
        receipts.append(ref(path))
    references = read(OUT / 'TRUTH_REFERENCES_SEALED.json')['references']
    truth = {}
    for path, digest in references.items():
        if sha(path) != digest:
            raise ValueError('Canonical truth drift')
        for case in read(path)['cases']:
            cid = case.get('case_id', case.get('id'))
            if cid in {r['case_id'] for r in index}:
                truth[cid] = dict(label=case.get('truth', case.get('source_truth')), reference=ref(path))
    write(OUT / 'TRUTH_OPEN_RECEIPT.json', dict(all_12_successes=receipts, references=references,
        opened_after_all_12=True, inference_inputs_unchanged=True))
    rows, normalization = [], {}
    for item in index:
        key, raw = item['key'], responses[item['key']]
        n = normalize(raw, read(item['semantic_packet']['path']), item['profile'])
        write(OUT / 'normalized' / (key + '.json'), n)
        normalization[key] = n
        rows.append(dict(case_id=item['case_id'], key=key, truth=truth[item['case_id']],
            raw_verdict=raw['verdict'], canonical_verdict=n['effective_verdict'],
            exists_change=n.get('exists_change', 'UNKNOWN'), f2=n['f2'],
            normalization_issues=n['issues'], warnings=n.get('warnings', []), sufficiency=n.get('sufficiency'),
            source_conflict=n.get('source_conflict'), negative_state=n['negative_state'],
            package_status=item['completeness'], raster_delivery=item['raster_allocation']['delivered_by_side'],
            model_reasoning=raw['reasoning_ru'], missing_evidence=raw['missing_evidence'],
            raw=ref(OUT / 'raw' / (key + '.json')), normalized=ref(OUT / 'normalized' / (key + '.json'))))
    real = [r for r in rows if r['truth']['label'].startswith('REAL')]
    controls = [r for r in rows if not r['truth']['label'].startswith('REAL')]
    def counts(group, field):
        c = Counter(r[field] for r in group)
        return dict(ACCEPT=c['ACCEPT'], REVIEW=c['REVIEW'], wrong_NOT_CHANGE=c['NOT_CHANGE'])
    canonical, raw_counts = counts(real, 'canonical_verdict'), counts(real, 'raw_verdict')
    false_accepts = [r['case_id'] for r in controls if r['canonical_verdict'] == 'ACCEPT']
    failures = [r['case_id'] for r in rows if r['normalization_issues'] or
        (r['raw_verdict'] == 'ACCEPT' and r['canonical_verdict'] != 'ACCEPT') or
        (r['exists_change'] != 'YES' and r['f2'] and r['f2']['materiality']['status'] == 'MATERIAL')]
    by_case = {r['case_id']: r for r in rows}
    a2 = read(OUT / 'A2_PACKAGE_REGRESSION.json')
    d2 = read(OUT / 'D2_MECHANICAL_REPLAY.json')
    f4_failures = [r['case_id'] for r in rows if any('RASTER' in issue or 'LOCATOR' in issue for issue in r['normalization_issues'])]
    success = (canonical['ACCEPT'] >= 6 and canonical['wrong_NOT_CHANGE'] == 0 and not false_accepts and not failures)
    status = 'CONTROLLED_V3_TARGET_MET' if success else 'COMPLETED_WITH_REMAINING_ERRORS'
    result = dict(schema='CONTROLLED_V3_RESULTS/3', status=status, completed=12,
        branch='research/projectchange-f1-f4-f2-v3', code_commit=frozen['code_commit'],
        model='gpt-6-astra', reasoning='xhigh', A2=a2['status'], D2=d2['status'], mechanical_replay=d2['status'],
        local_tests=read(OUT / 'LOCAL_TESTS.json'), raw_real_results=raw_counts, real_results=canonical,
        controls=dict(blocked=len(controls) - len(false_accepts), total=len(controls), false_ACCEPT=len(false_accepts), false_accept_cases=false_accepts),
        F2_POST_RUN_AUDIT=dict(status='PASS' if not failures else 'FAIL', cases=failures),
        F4=dict(status='PASS' if not f4_failures else 'FAIL', cases=f4_failures),
        R13=dict(mandatory_NEW_scheme_delivered=a2['checks']['NEW_mandatory_scheme_delivered'], semantic_result=by_case['R13']['raw_verdict']),
        usage=progress, validation_opened=False, final_holdout_opened=False, other_projects=False, full_dev=False,
        production='UNCHANGED', previous_runs_unchanged=read(OUT / 'PREVIOUS_RUN_HASHES.json') == previous_hashes(),
        recommendation='READY_FOR_FRESH_DEV_SAMPLE' if success else 'ARCHITECTURE_STILL_NEEDS_REPAIR',
        known_dev=True, blind=False, tuning_after_first_call=False, cases=rows)
    write(OUT / 'NORMALIZATION_AUDIT.json', normalization)
    write(OUT / 'CONTROLLED_V3_RESULTS.json', result)
    write(OUT / 'USAGE.json', progress)
    lines = ['# ProjectChange controlled V3', '', '**STATUS: ' + status + '**', '',
        'Branch: `' + result['branch'] + '`. Commit: `' + frozen['code_commit'] + '`.', '',
        f"LOCAL TESTS: {result['local_tests']['tests_run']} PASS. A2: {a2['status']}. D2: {d2['status']}. MECHANICAL REPLAY: {d2['status']}.", '',
        'MODEL CALLS: 12. CONTROLLED V3: 12/12 SUCCESS. gpt-6-astra / xhigh. Retries: 0.', '',
        f'RAW REAL: {raw_counts}. F2 FINAL REAL: {canonical}. CONTROLS: {result["controls"]}.', '',
        f'R13 mandatory NEW scheme delivered: {result["R13"]["mandatory_NEW_scheme_delivered"]}; PDF page 44. OLD primary and scheme delivered. F4: {result["F4"]["status"]}.', '',
        '| Case | Raw | F2 final | Exists change | Materiality |',
        '| --- | --- | --- | --- | --- |']
    for r in rows:
        lines.append(f'| {r["case_id"]} | {r["raw_verdict"]} | {r["canonical_verdict"]} | {r["exists_change"]} | {r["f2"]["materiality"]["status"] if r["f2"] else "ERROR"} |')
    lines += ['', 'F2 POST-RUN AUDIT: ' + result['F2_POST_RUN_AUDIT']['status'] + '; cases: ' + str(failures), '',
        '## Remaining findings', '']
    for r in rows:
        if r['canonical_verdict'] == 'ACCEPT' or (r not in real and r['case_id'] not in failures):
            continue
        lines += ['### ' + r['case_id'], '', r['model_reasoning'], '',
            'Missing evidence: ' + '; '.join(r['missing_evidence']), '',
            'Normalization: ' + str(r['normalization_issues']) + '; F2: ' + str(r['f2']), '']
    lines += ['## Usage and boundaries', '', str(progress), '',
        'Provider monetary cost is not exposed by the Codex ChatGPT allowance transport. OpenRouter requests: 0.', '',
        'VALIDATION: NOT OPENED. FINAL HOLDOUT: NOT OPENED. OTHER PROJECTS: NO. FULL DEV: NO. PRODUCTION: UNCHANGED.', '',
        'V1/V2 artifacts hash-verified unchanged. Only the same 12 known DEV cases; no generalization metrics. No post-call tuning or rerun.', '',
        '**RECOMMENDATION: ' + result['recommendation'] + '**', '']
    write(OUT / 'CONTROLLED_V3_REPORT.md', '\n'.join(lines))
    verify()
    write(OUT / 'FINAL_INTEGRITY_RECEIPT.json', dict(frozen_inputs_verified=True, previous_runs_unchanged=True,
        model_calls=12, successes=12, stopped_after_12=True))
    print(status, raw_counts, canonical, result['controls'], result['F2_POST_RUN_AUDIT'], flush=True)


if __name__ == '__main__':
    report()
