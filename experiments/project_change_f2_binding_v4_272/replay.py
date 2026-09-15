"""One offline pass over saved V3 responses; no inference/provider dependencies."""
from collections import Counter
from pathlib import Path
import io
import json
import subprocess
import sys
import unittest

from experiments.project_change_contracts_v3_272.common import (
    CASES, CODE, OUT as V3, ROOT, read, sha, ref, write)
from .normalization import normalize

OUT = ROOT / 'mechanical_replay_f2_binding_v4'
HERE = Path(__file__).resolve().parent
EXPECTED = dict(C01='ACCEPT', C07='NOT_CHANGE', C15='ACCEPT', C18='ACCEPT',
    C22='NOT_CHANGE', C05='REVIEW', R01='ACCEPT', R13='ACCEPT', R19='ACCEPT',
    Q04='REVIEW', S_FP01='REVIEW', R21='ACCEPT')
REAL = ('C01', 'C15', 'C18', 'R01', 'R13', 'R19', 'R21')


def offline_guard():
    def guard(event, args):
        if (event.startswith('socket.') or event in {
                'subprocess.Popen', 'os.system', 'os.posix_spawn', 'os.exec', 'os.fork'}):
            raise PermissionError('Mechanical replay forbids network and child processes: ' + event)
    sys.addaudithook(guard)


def local_v3(path):
    path = Path(path).resolve()
    if not path.is_relative_to(V3.resolve()):
        raise PermissionError('Replay input outside frozen V3: ' + str(path))
    return path


def checked(receipt, *, v3_only=False):
    path = local_v3(receipt['path']) if v3_only else Path(receipt['path'])
    if sha(path) != receipt['sha256']:
        raise ValueError('Immutable input drift: ' + str(path))
    return path


def verify_previous():
    baseline = read(OUT / 'V3_BASELINE_HASHES.json')
    current = {str(p.relative_to(V3)): sha(p) for p in sorted(V3.rglob('*')) if p.is_file()}
    if baseline != current:
        raise ValueError('V3 directory changed since binding repair began')
    frozen = read(V3 / 'ARCHITECTURE_FIX_FREEZE.json')
    for receipt in frozen['code_files'] + frozen['artifact_files']:
        checked(receipt)
    for path, digest in read(V3 / 'TRUTH_REFERENCES_SEALED.json')['references'].items():
        # Hash-only integrity of the two established known-DEV truth files.
        checked(dict(path=path, sha256=digest))
    access = read(V3 / 'SOURCE_ACCESS.json')
    if access['partition'] != 'DEV' or access['pair_indices'] != [5, 7]:
        raise PermissionError('Only the established two known DEV pairs are admitted')
    checked(access['split'])
    return dict(v3_files=len(current), v3_unchanged=True, frozen_code_unchanged=True,
                known_dev_truth_unchanged=True, split_unchanged=True)


def test_receipt(module, prefix):
    suite = unittest.defaultTestLoader.loadTestsFromName(module)
    stream = io.StringIO()
    result = unittest.TextTestRunner(stream=stream, verbosity=2).run(suite)
    write(OUT / (prefix + '.txt'), stream.getvalue())
    receipt = dict(status='PASS' if result.wasSuccessful() and not result.skipped else 'FAIL',
        tests_run=result.testsRun, failures=len(result.failures), errors=len(result.errors), skipped=len(result.skipped))
    write(OUT / (prefix + '.json'), receipt)
    return receipt


def prepare():
    if (OUT / 'CANDIDATE_FREEZE.json').exists():
        raise ValueError('Candidate already frozen')
    commit = subprocess.check_output(['git', '-C', str(CODE), 'rev-parse', 'HEAD'], text=True).strip()
    offline_guard()
    integrity = verify_previous()
    tests = test_receipt('experiments.project_change_f2_binding_v4_272.test_binding', 'LOCAL_UNIT_TESTS')
    if tests['status'] != 'PASS':
        raise ValueError('Fix synthetic unit tests before the single replay')
    write(OUT / 'CANDIDATE_FREEZE.json', dict(schema='F2_BINDING_CANDIDATE/4',
        binding_commit=commit, code_files=[ref(p) for p in sorted(HERE.glob('*.py'))],
        readme=ref(HERE / 'README.md'), baseline=ref(OUT / 'V3_BASELINE_HASHES.json'),
        previous_integrity=integrity, unit_tests=tests, cases=CASES,
        expected=EXPECTED, model_calls=0, automatic_retries=0))
    print('CANDIDATE FROZEN;', tests['tests_run'], 'unit tests PASS; V3 unchanged')


def load_inputs():
    rows = read(V3 / 'PACKAGE_INDEX.json')['packages']
    if [(r['pair_index'], r['case_id']) for r in rows] != CASES:
        raise PermissionError('Replay must contain exactly the original 12 cases')
    access = read(V3 / 'SOURCE_ACCESS.json')
    documents = {(d['side'].lower(), d['document_version']): d for d in access['documents']}
    inputs = []
    for row in rows:
        packet = read(checked(row['semantic_packet'], v3_only=True))
        if (packet['partition'] != 'DEV' or packet['pair_index'] != row['pair_index']
                or packet['source_package_hash'] != row['package_hash']):
            raise PermissionError('Packet direction/scope drift')
        checked(row['package'], v3_only=True)
        for side in ('old', 'new'):
            for item in packet['evidence'][side]:
                doc = documents.get((side, item['document_version']))
                if (not doc or item['side'] != side or item['page'] in doc['excluded_pages']
                        or item['source_receipt'] != doc['pdf']
                        or packet['source_versions'][side] != item['document_version']):
                    raise PermissionError('Unadmitted evidence source or page')
                if item.get('raster'):
                    checked(item['raster'], v3_only=True)
        success_path = V3 / 'calls' / row['case_token'] / 'SUCCESS.json'
        success = read(success_path)
        if success['status'] != 'SUCCESS' or success.get('tool_items'):
            raise ValueError('Saved response is not an isolated successful V3 response')
        raw_ref = dict(path=success['normalized_path'], sha256=success['normalized_sha256'])
        raw = read(checked(raw_ref, v3_only=True))
        alias = V3 / 'raw' / (row['key'] + '.json')
        if sha(alias) != raw_ref['sha256'] or raw['case_token'] != row['case_token']:
            raise ValueError('Saved raw-response alias/token drift')
        before_ref = ref(V3 / 'normalized' / (row['key'] + '.json'))
        inputs.append((row, raw, packet, read(before_ref['path']), dict(
            raw=raw_ref, raw_alias=ref(alias), success=ref(success_path),
            packet=row['semantic_packet'], v3_normalized=before_ref)))
    return inputs


def replay():
    offline_guard()
    if (OUT / 'REPLAY_STARTED.json').exists():
        raise ValueError('Single replay already started; automatic replay/resumption is forbidden')
    freeze = read(OUT / 'CANDIDATE_FREEZE.json')
    for receipt in freeze['code_files'] + [freeze['baseline'], freeze['readme']]:
        checked(receipt)
    integrity_before = verify_previous()
    inputs = load_inputs()
    write(OUT / 'REPLAY_STARTED.json', dict(candidate=ref(OUT / 'CANDIDATE_FREEZE.json'),
        count=len(inputs), model_calls=0, network_and_process_audit_guard=True))
    rows = []
    for item, raw, packet, before, receipts in inputs:
        after = normalize(raw, packet, item['profile'])
        path = OUT / 'normalized' / (item['key'] + '.json')
        write(path, after)
        row = dict(case_id=item['case_id'], key=item['key'], profile=item['profile'],
            raw_verdict=raw['verdict'], before=before['effective_verdict'],
            after=after['effective_verdict'], issues=after['issues'],
            exists_change=after.get('exists_change'), f2=after['f2'],
            inputs=receipts, normalized=ref(path))
        rows.append(row)
        print(item['case_id'], raw['verdict'], '->', row['after'], flush=True)
    write(OUT / 'REPLAY_INDEX.json', dict(cases=rows, model_calls=0))
    regression_tests = test_receipt('experiments.project_change_f2_binding_v4_272.test_replay', 'REPLAY_TESTS')
    integrity_after = verify_previous()
    failures = [r['case_id'] for r in rows if r['after'] != EXPECTED[r['case_id']]]
    regressions = [r['case_id'] for r in rows if r['case_id'] != 'C01' and r['before'] != r['after']]
    def counts(field):
        count = Counter(r[field] for r in rows if r['case_id'] in REAL)
        return dict(ACCEPT=count['ACCEPT'], REVIEW=count['REVIEW'], wrong_NOT_CHANGE=count['NOT_CHANGE'])
    controls = [r for r in rows if r['case_id'] not in REAL]
    false_accepts = [r['case_id'] for r in controls if r['after'] == 'ACCEPT']
    passed = not failures and not regressions and not false_accepts and regression_tests['status'] == 'PASS'
    result = dict(schema='F2_BINDING_MECHANICAL_REPLAY/4', status='PASS' if passed else 'FAIL',
        branch='research/projectchange-f2-binding-v4', binding_commit=freeze['binding_commit'],
        candidate=ref(OUT / 'CANDIDATE_FREEZE.json'), cases=rows,
        local_tests=dict(unit=freeze['unit_tests'], replay=regression_tests,
            total=freeze['unit_tests']['tests_run'] + regression_tests['tests_run']),
        v3_raw_real=counts('raw_verdict'), v3_f2_real=counts('before'), new_f2_real=counts('after'),
        controls=dict(blocked=5-len(false_accepts), total=5, false_ACCEPT=len(false_accepts), cases=false_accepts),
        failures=failures, regressions=regressions, integrity_before=integrity_before, integrity_after=integrity_after,
        model_calls=0, codex_model_calls=0, openrouter_requests=0, claude_calls=0,
        validation_opened=False, final_holdout_opened=False, other_projects=False, full_dev=False,
        production='UNCHANGED', known_dev=True, blind=False,
        recommendation='READY_FOR_FRESH_DEV_SAMPLE' if passed else 'F2_BINDING_STILL_NEEDS_REPAIR')
    write(OUT / 'MECHANICAL_REPLAY.json', result)
    lines = ['# ProjectChange F2 evidence binding V4', '', '**STATUS: ' + result['status'] + '**', '',
        'Root cause: V3 required subject identity IDs to be a subset of value IDs. The separately cited,',
        'delivered NEW page 30 identity source was dropped from C01 normalized state. V4 preserves',
        'source-bound roles independently and validates existence against OLD/NEW value references.', '',
        'Branch: `' + result['branch'] + '`. Binding commit: `' + result['binding_commit'] + '`.', '',
        f"LOCAL TESTS: {result['local_tests']['total']} ({freeze['unit_tests']['tests_run']} unit + {regression_tests['tests_run']} replay assertions).", '',
        '| Case | V3 raw | V3 F2 | V4 F2 |', '| --- | --- | --- | --- |']
    lines += [f"| {r['case_id']} | {r['raw_verdict']} | {r['before']} | {r['after']} |" for r in rows]
    lines += ['', 'V3 RAW REAL: ' + str(result['v3_raw_real']),
        'NEW F2 REAL: ' + str(result['new_f2_real']), 'CONTROLS: ' + str(result['controls']), '',
        'MECHANICAL REPLAY: ' + result['status'], 'REGRESSIONS: ' + (', '.join(regressions) or 'NONE'),
        'FAILURES: ' + (', '.join(failures) or 'NONE'), '',
        'C01: OLD value 1 and NEW value 2 are retained with independent value references; SAME subject',
        'identity has its own IDs, including NEW page 30. One parent function remains 1→1; its declared',
        'zone count changes 1→2. The subsidiary label conflict remains nonblocking.', '',
        'V3 directory: all ' + str(integrity_after['v3_files']) + ' file hashes unchanged. Frozen V3 code,',
        'packages, model responses, manifest, reports and known-DEV source truth hashes verified.',
        'F1, raster allocation, F4, sufficiency profiles, bounded negative OLD, materiality and prompts unchanged.', '',
        'MODEL CALLS: 0 (Codex 0 / OpenRouter 0 / Claude 0).',
        'VALIDATION: NOT OPENED. FINAL HOLDOUT: NOT OPENED. OTHER PROJECTS: NO.',
        'FULL DEV: NO. PRODUCTION: UNCHANGED. No fresh DEV sample started.', '',
        '**RECOMMENDATION: ' + result['recommendation'] + '**', '']
    if failures:
        lines += ['## Failure details', ''] + [json.dumps(r, ensure_ascii=False) for r in rows if r['case_id'] in failures]
    write(OUT / 'REPORT.md', '\n'.join(lines))
    print(result['status'], result['new_f2_real'], result['controls'], result['recommendation'])
    return 0 if passed else 1


if __name__ == '__main__':
    if sys.argv[1:] == ['--prepare']:
        prepare()
    elif sys.argv[1:] == ['--replay']:
        raise SystemExit(replay())
    else:
        raise SystemExit('Use --prepare, then --replay. No inference mode exists.')
