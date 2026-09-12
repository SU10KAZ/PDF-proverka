"""Independent persisted-row checks, replay and alternating fresh-process benchmarks."""
from collections import Counter
from pathlib import Path
import statistics
import subprocess
import sys

from experiments.semantic_foundation_v3 import frozen_v1 as v1
from experiments.semantic_foundation_v3.run import read
from .prepare import ROOT, FOUNDATION, FINAL, FINAL_SHA, immutable, load_result
from .run import ARTIFACTS
from .evaluate import evaluate


def invoke(module, documents, directory):
    subprocess.run([sys.executable, '-m', module, '--documents', str(documents), '--output', str(directory)], check=True)


def compare(left, right, names=ARTIFACTS):
    a, b = read(left / 'index.json'), read(right / 'index.json')
    assert [r['document_version'] for r in a] == [r['document_version'] for r in b]
    differences = []
    for x, y in zip(a, b):
        for name in names:
            # Compare bytes directly, not just the recorded receipt.
            if Path(x['artifacts'][name]['path']).read_bytes() != Path(y['artifacts'][name]['path']).read_bytes():
                differences.append([x['document_version'], name])
    return {'pass': not differences, 'files': len(a) * len(names), 'differences': differences}


def conservation(directory):
    rows, pages, multi, tables, max_span, fragments = 0, 0, 0, 0, 0, 0
    for record in read(directory / 'index.json'):
        result = load_result(directory, record['document_version'])
        data = read(directory / record['document_version'] / 'tables.json')
        col, units = result['ledger']['columns'], result['semantics']['units']
        expected = [i for i, kind in enumerate(col['kind']) if kind == 'TABLE_ROW']
        represented = [i for t in data['tables'] for i in t['rows']['line']]
        assert represented == expected and len(represented) == len(set(represented))
        assert len({t['table_key'] for t in data['tables']}) == len(data['tables'])
        represented_segments = [s for t in data['tables'] for s in t['segments']]
        assert represented_segments == [i for i, u in enumerate(units) if u['kind'] == 'TABLE_SEGMENT']
        for t in data['tables']:
            assert len(set(t['rows']['content_key'])) == len(t['rows']['line'])
            assert {len(v) for v in t['rows'].values()} == {len(t['rows']['line'])}
            for s in t['segments']:
                u = units[s]
                if u['first_row']['kind'] == 'DATA_LIKE':
                    i = t['rows']['line'].index(u['first_content_line'])
                    assert t['rows']['role'][i] == 'DATA'
        for relation in data['candidate_relations']:
            edge = data['boundaries'][relation['boundary_ref']]
            assert edge['decision'] == 'REVIEW'
            assert relation['left_table'] != relation['right_table']
        rows += len(represented)
        pages += len(result['semantics']['pages'])
        tables += len(data['tables'])
        fragments += len(represented_segments)
        multi += sum(len(t['pages']) > 1 for t in data['tables'])
        max_span = max([max_span] + [len(t['pages']) for t in data['tables']])
    return {'pass': True, 'rows_preserved': rows, 'pages': pages, 'tables': tables,
            'source_segments': fragments, 'multi_page_tables': multi, 'maximum_table_pages': max_span,
            'duplicate_rows': 0, 'lost_rows': 0, 'duplicate_table_keys': 0, 'lost_segments': 0}


def main():
    dev = FOUNDATION / 'dev_documents.json'
    controls = Path(read(ROOT / 'reports/FOUNDATION_INPUT_FREEZE.json')['controls_documents']['path'])
    # Benchmark first, while this orchestrator is small. Fresh children share the same parent
    # memory floor; no Foundation audit RSS is inherited by just one producer.
    perf = {'foundation': [], 'candidate': []}
    for n in range(5):
        for producer in (('foundation', 'candidate') if n % 2 == 0 else ('candidate', 'foundation')):
            module = 'experiments.semantic_foundation_v3.run' if producer == 'foundation' else 'experiments.table_materialization_v3.run'
            output = ROOT / f'benchmark_{producer}_{n + 1}'
            invoke(module, dev, output)
            perf[producer].append(read(output / 'performance.json'))
    for label, values in perf.items():
        perf[label] = {'runs': values, **{k: statistics.median(v[k] for v in values)
                                        for k in ('seconds', 'peak_rss_kib', 'artifact_bytes')}}
    ratios = {k: perf['candidate'][k] / perf['foundation'][k] for k in ('seconds', 'peak_rss_kib', 'artifact_bytes')}
    perf.update(ratios=ratios, targets={'seconds': 1.2, 'peak_rss_kib': 1.15, 'artifact_bytes': 1.25},
                method='Five fresh processes each, alternating order, identical 21 DEV documents, pinned input hashing and persisted artifact writes included; median; raw trials retained.')
    immutable(ROOT / 'reports/PERFORMANCE_MEASUREMENTS.json', perf)
    invoke('experiments.table_materialization_v3.run', dev, ROOT / 'candidate_dev_run2')
    for n in (1, 2):
        invoke('experiments.table_materialization_v3.run', controls, ROOT / f'candidate_controls_run{n}')
    checks = {
        'dev_foundation_unchanged': compare(ROOT / 'foundation_dev_run1', ROOT / 'candidate_dev_run1', ARTIFACTS[:3]),
        'controls_foundation_unchanged': compare(ROOT / 'foundation_controls_run1', ROOT / 'candidate_controls_run1', ARTIFACTS[:3]),
        'dev_conservation': conservation(ROOT / 'candidate_dev_run1'),
        'controls_conservation': conservation(ROOT / 'candidate_controls_run1'),
    }
    assert all(v['pass'] for v in checks.values())
    immutable(ROOT / 'reports/TABLE_V3_INVARIANTS.json', checks)
    replay = {'dev': compare(ROOT / 'candidate_dev_run1', ROOT / 'candidate_dev_run2'),
              'controls': compare(ROOT / 'candidate_controls_run1', ROOT / 'candidate_controls_run2'),
              'excluded_nonsemantic_files': ['index.json: timings and paths', 'performance.json', 'producer.json: paths']}
    replay['pass'] = replay['dev']['pass'] and replay['controls']['pass']
    assert replay['pass']
    immutable(ROOT / 'reports/TABLE_V3_REPLAY.json', replay)
    controls_cases = read(ROOT / 'TABLE_V3_DEV_CONTROLS.json')['cases']
    pred, _ = evaluate(ROOT / 'candidate_dev_run1', controls_cases)
    immutable(ROOT / 'reports/AUTOMATIC_CONTROL_RESULTS.json', {'cases': [{
        'case_id': c['case_id'], 'stratum': c['variant'], 'expected': c['control_expectation']['decision'],
        'prediction': pred[c['case_id']]['decision']} for c in controls_cases],
        'separate_from_human_denominator': True})
    tests = subprocess.run([sys.executable, '-m', 'unittest', 'experiments.semantic_foundation_v3.test_foundation',
                            'experiments.semantic_foundation_v3.test_dev_packet',
                            'experiments.table_materialization_v3.test_tables'], capture_output=True, text=True)
    assert tests.returncode == 0, tests.stdout + tests.stderr
    immutable(ROOT / 'reports/TABLE_V3_TESTS.json', {'returncode': tests.returncode, 'output': tests.stdout + tests.stderr})
    assert v1.file_sha(FINAL) == FINAL_SHA
    assert all(v1.file_sha(Path(x['path'])) == x['sha256'] for x in read(ROOT / 'reports/FOUNDATION_INPUT_FREEZE.json')['code'])
    print('PASS', checks, 'performance', ratios)


if __name__ == '__main__':
    main()
