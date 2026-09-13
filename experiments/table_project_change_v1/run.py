"""Reproducible offline run. DEV inputs are explicitly allowlisted."""
import argparse
from collections import defaultdict
import platform
from pathlib import Path
import resource
import subprocess
import sys
import time

from experiments.project_change_text_v1.contract import SCHEMA
from .approaches import run as run_approaches
from .common import code_key, digest, file_hash, read, write
from .controls import cases
from .engine import compare
from .source import load_document, project_scope, verify_evidence

AUDITS = Path('/home/coder/auditmanager/corpus-audits')
DEFAULT_ROOT = AUDITS / '20260913_table_project_change_v1'
DEV_MANIFEST = AUDITS / '20260911_semantic_foundation_v3/dev_documents.json'
SOURCE_MANIFEST = AUDITS / '20260910_section_table_materialization_v2/baseline_documents.json'
V3 = AUDITS / '20260913_table_materialization_v3'
SHARED_SCHEMA = AUDITS / '20260913_project_change_text_v1/reports/PROJECT_CHANGE_SCHEMA.json'


def select_pairs(documents):
    by_scope = defaultdict(lambda: defaultdict(list))
    for doc in documents:
        by_scope[project_scope(doc), code_key(doc['document_code'])][doc['stage']].append(doc)
    pairs = []
    for (project, code), sides in sorted(by_scope.items()):
        if len(sides.get('stage_1', [])) == len(sides.get('stage_2', [])) == 1:
            old, new = sides['stage_1'][0], sides['stage_2'][0]
            if old['document_version'] != new['document_version']:
                pairs.append(dict(project_scope=project, code_key=code, old=old, new=new,
                                  basis='Unique project + document code after syntax-only punctuation normalization; stage_1 -> stage_2'))
    return pairs


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--output', type=Path, default=DEFAULT_ROOT)
    args = parser.parse_args()
    root = args.output.resolve()
    start = time.perf_counter()
    root.mkdir(parents=True, exist_ok=True)
    if SCHEMA != read(SHARED_SCHEMA):
        raise ValueError('Established shared schema mismatch')
    # Freeze the adapter before loading any source-only comparison content.
    code = {str(p): file_hash(p) for p in sorted(Path('experiments/table_project_change_v1').glob('*.py'))}
    frozen = dict(code_files=code, candidate_content_hash=digest(code), chosen='hybrid_entity_event_collapse',
                  dev_manifest=dict(path=str(DEV_MANIFEST), sha256=file_hash(DEV_MANIFEST)),
                  source_only_manifest=dict(path=str(SOURCE_MANIFEST), sha256=file_hash(SOURCE_MANIFEST)),
                  shared_schema=dict(path=str(SHARED_SCHEMA), sha256=file_hash(SHARED_SCHEMA)),
                  algorithm_changes_after_source_only_replay=0)
    write(root / 'CANDIDATE_FREEZE.json', frozen)
    experiment = run_approaches()
    write(root / 'reports/APPROACH_EXPERIMENT.json', experiment)
    constructed = []
    for case in cases():
        r = compare(case['pair'])
        constructed.append(dict(case=case['name'], **r))
    write(root / 'reports/CONSTRUCTED_DEV_CHANGES.json', constructed)
    dev = []
    for doc in read(DEV_MANIFEST):
        d = load_document(doc, V3 / 'optimized_benchmark_candidate_1')
        same = compare(dict(comparison_scope='same-version/' + d['document_version'],
                            old_records=d['records'], new_records=d['records']))
        if same['facts'] or same['project_changes']:
            raise ValueError('Same-version no-change conservation failed')
        dev.append({k: v for k, v in d.items() if k != 'records'})
        dev[-1]['records'] = len(d['records'])
        dev[-1]['same_version_entity_matches'] = len(same['entity_matches'])
        dev[-1]['same_version_unresolved'] = len(same['unresolved'])
    write(root / 'reports/DEV_SOURCE_INVENTORY.json', dev)
    source_start = time.perf_counter()
    pairs = select_pairs(read(SOURCE_MANIFEST))
    write(root / 'SOURCE_ONLY_PAIRS.json', pairs)
    outputs, input_docs = [], []
    for n, pair in enumerate(pairs):
        sides = {side: load_document(pair[side], V3 / 'candidate_controls_optimized_run1') for side in ('old', 'new')}
        scope = 'table/' + digest([pair['project_scope'], pair['code_key'], pair['old']['document_version'], pair['new']['document_version']])[:24]
        p = dict(comparison_scope=scope, project_scope=pair['project_scope'],
                 old_records=sides['old']['records'], new_records=sides['new']['records'])
        a = compare(p)
        b = compare(p)
        if digest(a) != digest(b):
            raise ValueError('Deterministic replay mismatch')
        output = dict(comparison_scope=scope, old_document=pair['old']['document_code'],
                      new_document=pair['new']['document_code'], **a)
        outputs.append(output)
        write(root / 'pairs' / (scope.split('/')[-1] + '.json'), output)
        input_docs.extend(sides.values())
    source_elapsed = time.perf_counter() - source_start
    changes = [c for o in outputs for c in o['project_changes']]
    evidence = [e for c in changes for side in ('old', 'new') for e in c['evidence_' + side]]
    checked = verify_evidence(evidence)
    write(root / 'reports/TABLE_PROJECT_CHANGES.json', changes)
    write(root / 'reports/SOURCE_ONLY_INPUT_VIEWS.json', input_docs)
    write(root / 'reports/TABLE_DIFFS.json', outputs)
    score = dict(dataset='Existing source-only conservation controls; not human ProjectChange truth',
                 document_pairs=len(pairs), documents=len(input_docs), raw_table_differences=sum(len(o['facts']) for o in outputs),
                 paired_cell_difference_occurrences=sum(f['changed_cell_occurrences'] for o in outputs for f in o['facts']),
                 unmatched_entity_observations=sum(f['property'] in {'added', 'removed'} for o in outputs for f in o['facts']),
                 entity_matches=sum(len(o['entity_matches']) for o in outputs),
                 entity_matches_proven=sum(m['status'] == 'PROVEN' for o in outputs for m in o['entity_matches']),
                 project_changes=len(changes), proven=sum(c['status'] == 'PROVEN' for c in changes),
                 review=sum(c['status'] == 'REVIEW' for c in changes),
                 false_project_changes=None, duplicate_project_changes=len(changes) - len({c['project_change_id'] for c in changes}),
                 over_grouped=None, under_grouped=None,
                 unresolved_observations=sum(len(o['unresolved']) for o in outputs),
                 table_components=sum(d['table_count'] for d in input_docs), table_rows=sum(d['table_rows'] for d in input_docs),
                 table_boundary_proven=sum(d['boundary_proven'] for d in input_docs),
                 table_boundary_review=sum(d['boundary_review'] for d in input_docs), verified_evidence_cells=checked,
                 parameter_compression_ratio=sum(len(o['facts']) for o in outputs) / len(changes) if changes else None)
    write(root / 'reports/SCORECARD.json', score)
    timings = []
    for _ in range(5):
        t = time.perf_counter()
        for p in input_docs:
            compare(dict(comparison_scope='benchmark/' + p['document_version'], old_records=p['records'], new_records=p['records']))
        timings.append(time.perf_counter() - t)
    for path, sha in frozen['code_files'].items():
        if file_hash(path) != sha:
            raise ValueError('Candidate changed after freeze')
    performance = dict(wall_seconds=time.perf_counter() - start, source_load_compare_verify_seconds=source_elapsed,
                       warm_same_version_replay_seconds=timings, max_rss_kib=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
                       python=platform.python_version(), llm_calls=0, network_calls=0, new_ocr_calls=0,
                       source_replay_equal=True, scope='Local artifacts, hashes, parsing, diff, grouping; excludes source OCR and V3 generation')
    write(root / 'reports/PERFORMANCE.json', performance)
    from .reports import render
    render(root, score, experiment, dev, input_docs, outputs, performance, frozen)
    print(score)


if __name__ == '__main__':
    main()
