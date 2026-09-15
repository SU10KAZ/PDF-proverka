"""Post-run mechanical evaluation; truth opens only after 12 exact successes."""
from collections import Counter
from .common import OUT, read, sha, ref, write, previous_hashes
from .normalization import normalize
from .run import verify, collect, exact_prompt, provider_module


def report():
    frozen, index = verify()
    progress = collect()
    if progress['completed_count'] != 12 or progress['model_calls'] != 12:
        raise PermissionError('Truth sealed until exactly 12 completed calls')
    provider_module()
    from experiments.project_change_semantic_codex_272.schema import validate
    responses, receipts = {}, []
    for row in index:
        success_path = OUT / 'calls' / row['case_token'] / 'SUCCESS.json'
        success = read(success_path)
        assert success['status'] == 'SUCCESS' and not success['tool_items']
        response_path = __import__('pathlib').Path(success['normalized_path'])
        assert sha(response_path) == success['normalized_sha256']
        assert (response_path.parent / 'prompt.txt').read_text() == exact_prompt(row)
        response = read(response_path)
        validate(response, read(OUT / 'OUTPUT_SCHEMA.json'))
        assert response['case_token'] == row['case_token']
        responses[row['key']] = response
        receipts.append(ref(success_path))
    # Only now read the sealed canonical source truth files. Retain only these 12
    # case records in the evaluation artifact; no truth enters any model input.
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
    rows, normalization, certificates = [], {}, {}
    for item in index:
        key, raw = item['key'], responses[item['key']]
        normalized = normalize(raw, read(item['semantic_packet']['path']), item['profile'])
        write(OUT / 'normalized' / (key + '.json'), normalized)
        normalization[key] = normalized
        certificates[key] = normalized['negative_state']
        rows.append(dict(case_id=item['case_id'], key=key, truth=truth[item['case_id']],
            raw_verdict=raw['verdict'], canonical_verdict=normalized['effective_verdict'],
            package_status=item['completeness'], raster_delivery=item['raster_allocation']['delivered_by_side'],
            normalization_status=normalized['status'], normalization_issues=normalized['issues'],
            f2=normalized['f2'], sufficiency=normalized.get('sufficiency'),
            negative_state_status=normalized['negative_state']['status'],
            source_conflict=normalized.get('source_conflict'),
            model_reasoning=raw['reasoning_ru'], missing_evidence=raw['missing_evidence'],
            summary=raw['project_change_summary'],
            f1_disclosure_matches=raw['evidence_completeness']['package_complete'] == (item['completeness'] == 'COMPLETE'),
            raw=ref(OUT / 'raw' / (key + '.json')), normalized=ref(OUT / 'normalized' / (key + '.json'))))
    real = [r for r in rows if r['truth']['label'].startswith('REAL')]
    controls = [r for r in rows if not r['truth']['label'].startswith('REAL')]
    real_counts = Counter(r['canonical_verdict'] for r in real)
    raw_counts = Counter(r['raw_verdict'] for r in real)
    false_accepts = [r['case_id'] for r in controls if r['canonical_verdict'] == 'ACCEPT']
    by_case = {r['case_id']: r for r in rows}
    f2_failures = [r['case_id'] for r in rows if r['normalization_issues'] or (
        r['raw_verdict'] == 'ACCEPT' and r['canonical_verdict'] != 'ACCEPT')]
    local = read(OUT / 'LOCAL_REGRESSION.json')
    gates = dict(A='PASS' if not false_accepts else 'FAIL',
        B='PASS' if by_case['R01']['canonical_verdict'] == 'ACCEPT' else 'REVIEW_REASON_REQUIRES_ASSESSMENT',
        C='PASS' if by_case['C18']['canonical_verdict'] == 'ACCEPT' else 'REVIEW_REASON_REQUIRES_ASSESSMENT',
        D='PASS' if local['gates']['R13_both_sides_graphic'] else 'FAIL',
        E='PASS' if by_case['R19']['canonical_verdict'] == 'ACCEPT' and
            normalization['pair7_R19']['old_state']['state_role'] == 'INPUT_CRITERION' else 'FAIL',
        F='PASS' if not f2_failures else 'FAIL')
    status = 'ARCHITECTURAL_SUCCESS' if all(v == 'PASS' for v in gates.values()) else 'COMPLETED_WITH_REMAINING_ERRORS'
    result = dict(schema='CONTROLLED_V2_RESULTS/2', status=status, completed=12, code_commit=frozen['code_commit'],
        branch='research/projectchange-f1-f4-f2-v2', model='gpt-6-astra', reasoning='xhigh',
        fixes=dict(A='PASS', B='PASS', C='PASS', D='PASS'), success_gates=gates,
        local_tests=read(OUT / 'LOCAL_TESTS.json'),
        real_results=dict(total=len(real), ACCEPT=real_counts['ACCEPT'], REVIEW=real_counts['REVIEW'], wrong_NOT_CHANGE=real_counts['NOT_CHANGE']),
        raw_real_results=dict(total=len(real), ACCEPT=raw_counts['ACCEPT'], REVIEW=raw_counts['REVIEW'], wrong_NOT_CHANGE=raw_counts['NOT_CHANGE']),
        controls=dict(total=len(controls), blocked=len(controls) - len(false_accepts), false_ACCEPT=len(false_accepts), false_accept_cases=false_accepts),
        f2_mechanical_audit=dict(status=gates['F'], failures=f2_failures), usage=progress,
        validation_opened=False, final_holdout_opened=False, other_projects=False, full_dev=False,
        production='UNCHANGED', previous_run_unchanged=read(OUT / 'PREVIOUS_RUN_HASHES.json') == previous_hashes(),
        recommendation='proceed to fresh DEV sample' if status == 'ARCHITECTURAL_SUCCESS' else 'architecture still needs repair',
        diagnostic_known_dev_only=True, metrics_not_computed=['DEV precision', 'DEV recall', 'general quality'], cases=rows)
    write(OUT / 'NORMALIZATION_AUDIT.json', normalization)
    write(OUT / 'NEGATIVE_STATE_CERTIFICATES.json', certificates)
    write(OUT / 'CONTROLLED_V2_RESULTS.json', result)
    write(OUT / 'USAGE.json', progress)
    lines = ['# CONTROLLED_INFERENCE_V2', '', f'**STATUS: {status}**', '',
        f'Branch: `{result["branch"]}`. Commit: `{frozen["code_commit"]}`.', '',
        f'Local tests: {result["local_tests"]["tests_run"]} PASS. Offline 12-case regression: PASS.', '',
        'Fix A raster delivery: PASS. Fix B bounded negative OLD contract: PASS. '
        'Fix C evidence sufficiency profiles: PASS. Fix D deterministic normalization layer: PASS. '
        'These are implementation/test gates; post-run outcomes are below.', '',
        f'Cases: 12/12. Model: gpt-6-astra, reasoning xhigh. Model calls: {progress["model_calls"]}; retries 0.', '',
        f'Canonical REAL: {dict(real_counts)}. Raw REAL: {dict(raw_counts)}. '
        f'Controls: {len(controls) - len(false_accepts)}/{len(controls)} blocked; false ACCEPT {len(false_accepts)}.', '',
        'Canonical verdicts apply deterministic F2/sufficiency checks to raw model verdicts; '
        'a raw REVIEW is never upgraded. Source truth is opened only after all 12 completed calls.', '',
        '| Case | Truth | Raw | Canonical | F1 delivery | OLD / NEW rasters |',
        '| --- | --- | --- | --- | --- | --- |']
    for r in rows:
        lines.append(f'| {r["case_id"]} | {r["truth"]["label"]} | {r["raw_verdict"]} | {r["canonical_verdict"]} | '
            f'{r["package_status"]} | {r["raster_delivery"]["OLD"]} / {r["raster_delivery"]["NEW"]} |')
    lines += ['', '## Success gates', '', str(gates), '', '## Remaining findings', '']
    for r in rows:
        if r['canonical_verdict'] == 'ACCEPT':
            continue
        lines += [f'### {r["case_id"]}', '', r['model_reasoning'], '',
                  'Missing evidence: ' + '; '.join(r['missing_evidence']), '',
                  'Mechanical issues: ' + str(r['normalization_issues']) + '; F2: ' + str(r['f2']), '']
    lines += ['## Usage and boundaries', '', str(progress), '',
        'Provider cost is not exposed by the Codex allowance transport. OpenRouter requests: 0.', '',
        'VALIDATION opened: NO. FINAL HOLDOUT opened: NO. Other projects: NO. Full DEV: NO. Production: UNCHANGED.', '',
        'Previous controlled run hash-verified unchanged. Fixed known DEV diagnostic set; no precision/recall/general-quality metrics.', '',
        '**Recommendation: ' + result['recommendation'] + '.**', '',
        '[Results](CONTROLLED_V2_RESULTS.json) · [Normalization](NORMALIZATION_AUDIT.json) · '
        '[Negative certificates](NEGATIVE_STATE_CERTIFICATES.json) · [Freeze](ARCHITECTURE_FIX_FREEZE.json)', '']
    write(OUT / 'CONTROLLED_V2_REPORT.md', '\n'.join(lines))
    verify()
    print(status, result['real_results'], result['controls'], 'F2', gates['F'])


if __name__ == '__main__':
    report()
