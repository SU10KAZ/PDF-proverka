"""Verify Foundation and freeze the sole authorized TABLE DEV truth before tuning."""
from collections import Counter
from pathlib import Path
import subprocess
import sys

from experiments.semantic_foundation_v3 import frozen_v1 as v1
from experiments.semantic_foundation_v3.run import read, write, receipt, audit, replay
from experiments.semantic_foundation_v3.scorer import AnchorResolver, score

ROOT = Path('/home/coder/auditmanager/corpus-audits/20260913_table_materialization_v3')
FOUNDATION = ROOT.parent / '20260911_semantic_foundation_v3'
FINAL = ROOT.parent / '20260913_human_dev_truth_qa/DEV_HUMAN_TRUTH_WAVE1_FINAL.json'
FINAL_SHA = '71ed12f693442665a64c073a56f3d13df321bfdd0303f4a378286437616906d7'


def immutable(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    data = v1.canonical_bytes(value)
    with path.open('xb') as f:
        f.write(data)
    path.chmod(0o444)
    return receipt(path)


def run_foundation(documents, output, producer='foundation'):
    subprocess.run([sys.executable, '-m', 'experiments.semantic_foundation_v3.run',
                    '--documents', str(documents), '--output', str(output),
                    '--producer', producer], check=True)


def load_result(directory, version):
    return {name: read(Path(directory) / version / (name + '.json'))
            for name in ('ledger', 'semantics', 'decisions')}


def predictions(cases, directory, resolver=AnchorResolver):
    out = {}
    for version in sorted({c['document_version'] for c in cases}):
        r = resolver(load_result(directory, version))
        for case in cases:
            if case['document_version'] == version:
                assert all(r.resolve(a) is not None for a in case['anchors'])
                out[case['case_id']] = r.predict(case)
    return out


def main():
    reports = ROOT / 'reports'
    reports.mkdir(parents=True, exist_ok=True)
    assert v1.file_sha(FINAL) == FINAL_SHA
    freeze = read(FOUNDATION / 'reports/FOUNDATION_FREEZE.json')
    assert all(v1.file_sha(Path(x['path'])) == x['sha256'] for x in freeze['code'])
    state = {'main_head': subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip(),
             'branch': subprocess.check_output(['git', 'branch', '--show-current'], text=True).strip(),
             'initial_status': subprocess.check_output(['git', 'status', '--short'], text=True),
             'production_release': str(Path('/home/coder/auditmanager/current').resolve()),
             'v2_preserved': subprocess.check_output(['git', 'rev-parse', 'research/section-table-v2-frozen-0f4ea8a9'], text=True).strip(),
             'foundation_candidate': subprocess.check_output(['git', 'rev-parse', '5bd396fc'], text=True).strip()}
    assert state['branch'] == 'main'
    immutable(reports / 'INITIAL_STATE.json', state)
    test = subprocess.run([sys.executable, '-m', 'unittest',
                           'experiments.semantic_foundation_v3.test_foundation',
                           'experiments.semantic_foundation_v3.test_dev_packet'], capture_output=True, text=True)
    write(reports / 'FOUNDATION_TESTS.json', {'returncode': test.returncode, 'output': test.stdout + test.stderr})
    assert test.returncode == 0
    docs = Path(freeze['documents']['path'])  # Source manifest only. No EVAL answers.
    assert v1.file_sha(docs) == freeze['documents']['sha256']
    for n in (1, 2):
        run_foundation(docs, ROOT / f'foundation_controls_run{n}')
    conservation = audit(read(docs), ROOT / 'foundation_controls_run1', FOUNDATION / 'v1_run1')
    replay_result = replay(ROOT / 'foundation_controls_run1', ROOT / 'foundation_controls_run2')
    assert conservation['pass'] and replay_result['pass']
    immutable(reports / 'FOUNDATION_VERIFICATION.json', {'conservation': conservation, 'replay': replay_result})
    dev_docs = FOUNDATION / 'dev_documents.json'
    inputs = {'code': freeze['code'], 'controls_documents': receipt(docs), 'dev_documents': receipt(dev_docs)}
    immutable(reports / 'FOUNDATION_INPUT_FREEZE.json', {**inputs, 'input_sha256': v1.digest(inputs)})
    final, packet = read(FINAL), read(FOUNDATION / 'reports/FIRST_WAVE_DEV_PACKET.json')
    assert len(final['cases']) == 104
    assert Counter(c['kind'] for c in final['cases']) == {'SECTION': 52, 'TABLE': 44, 'OWNER': 8}
    assert len({c['case_id'] for c in final['cases']}) == 104
    assert all(c['final_answer'] in {'YES', 'NO'} for c in final['cases'])
    by_id = {c['case_id']: c for c in packet['cases']}
    cases = []
    for c in final['cases']:
        if c['kind'] != 'TABLE':
            continue
        source = by_id[c['case_id']]
        assert c['source_anchors'] == source['anchors']
        assert c['document_version'] == source['document_version']
        cases.append({k: source[k] for k in ('case_id', 'kind', 'document_version', 'anchors', 'stratum',
                                             'variant', 'page_pair', 'page_content_hashes', 'structural_evidence')})
        cases[-1].update(human_final_answer=c['final_answer'], answer={'YES': 'SAME', 'NO': 'NEW'}[c['final_answer']])
    truth = {'schema': 'table-v3-dev-truth.v1', 'parent_final_truth': receipt(FINAL), 'cases': cases}
    truth_receipt = immutable(ROOT / 'TABLE_V3_DEV_TRUTH.json', truth)
    immutable(ROOT / 'TABLE_V3_DEV_CONTROLS.json', {'cases': [c for c in packet['cases']
               if c['kind'] == 'TABLE' and c['annotation_mode'] == 'AUTOMATIC_CONTROL']})
    immutable(reports / 'TABLE_DEV_TRUTH_FREEZE.json', {'truth': truth_receipt,
               'parent_final_sha256': FINAL_SHA, 'cases': len(cases),
               'answers': dict(Counter(c['answer'] for c in cases)),
               'strata': dict(Counter(c['stratum'] for c in cases)),
               'document_distribution': dict(Counter(c['document_version'] for c in cases)),
               'duplicates': 0, 'missing_anchors': 0, 'unsure': 0, 'broken': 0})
    run_foundation(dev_docs, ROOT / 'foundation_dev_run1')
    run_foundation(dev_docs, ROOT / 'foundation_dev_run2')
    pred = predictions(cases, ROOT / 'foundation_dev_run1')
    immutable(ROOT / 'baseline_predictions.json', pred)
    metrics = score(pred, {c['case_id']: c['answer'] for c in cases})
    immutable(reports / 'TABLE_V3_BASELINE.json', metrics)
    (reports / 'TABLE_V3_BASELINE.md').write_text('# TABLE DEV baseline\n\n' +
        'Fresh Foundation runs before any Table V3 rules. REVIEW is incorrect in the legacy metric.\n\n' +
        '\n'.join(f'- {k}: {v}' for k, v in metrics.items()) + '\n')
    # Declared before inspecting per-case DEV outcomes or implementing rule predicates.
    immutable(reports / 'PROMOTION_POLICY.json', {
        'minimum_precision': 0.90, 'minimum_human_cases_per_rule': 5,
        'minimum_documents_per_rule': 2,
        'contract': 'Each evidence rule, and each emitted decision stratum, must meet these gates. '
                    'Automatic controls are reported separately and do not enlarge human denominators. '
                    'Rejected evidence stays neutral with its source predicate visible. '
                    'No numerical feature thresholds are fitted. Minimum sample size is a governance gate, not a classifier threshold.',
        'predeclared_before_rule_implementation': True})
    print(metrics, flush=True)


if __name__ == '__main__':
    main()
