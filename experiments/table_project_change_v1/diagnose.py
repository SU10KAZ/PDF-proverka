"""Post-freeze coverage/accounting audit; does not change extraction or grouping.

Run after `run`. Distinguishes no compared facts from no raw table differences.
"""
import argparse
from collections import Counter
from pathlib import Path
import time

from experiments.table_materialization_v3.model import row_values
from .common import canonical, digest, file_hash, norm, read, write
from .controls import cases
from .engine import compare
from .run import DEFAULT_ROOT, DEV_MANIFEST, V3
from .source import load_document, verify_evidence


def row_inventory(document_view):
    receipts = document_view['artifact_receipts']
    ledger, tables = read(receipts['ledger']['path']), read(receipts['tables']['path'])
    raw = Path(ledger['sources']['work_md']['path']).read_text().splitlines()
    rows = []
    for table in tables['tables']:
        for i in range(len(table['rows']['line'])):
            row = row_values(table, i, ledger, raw)
            if any(row['cells']):
                rows.append(dict(key=digest([norm(c) for c in row['cells']]), cells=row['cells'],
                                 table_key=table['table_key'], row_key=row['row_key'],
                                 role=table['rows']['role'][i], page=row['page'],
                                 markdown_line=ledger['columns']['markdown_line'][row['line_refs'][0]],
                                 work_md=ledger['sources']['work_md']['path']))
    return rows


def diagnose(root):
    start = time.perf_counter()
    frozen = read(root / 'CANDIDATE_FREEZE.json')
    for name in ('engine.py', 'source.py', 'controls.py', 'approaches.py'):
        path = 'experiments/table_project_change_v1/' + name
        if file_hash(path) != frozen['code_files'][path]:
            raise ValueError('Extraction/grouping changed after replay')
    views = read(root / 'reports/SOURCE_ONLY_INPUT_VIEWS.json')
    by_version = {d['document_version']: d for d in views}
    diagnostics = []
    persisted = read(root / 'reports/TABLE_DIFFS.json')
    for pair in read(root / 'SOURCE_ONLY_PAIRS.json'):
        rows = {side: row_inventory(by_version[pair[side]['document_version']]) for side in ('old', 'new')}
        counts = {side: Counter(r['key'] for r in rows[side]) for side in rows}
        residual = {}
        for side, other in [('old', 'new'), ('new', 'old')]:
            remaining = counts[side] - counts[other]
            selected = []
            for row in rows[side]:
                if remaining[row['key']]:
                    selected.append(row)
                    remaining[row['key']] -= 1
            residual[side] = selected
        diagnostics.append(dict(old_document=pair['old']['document_code'], new_document=pair['new']['document_code'],
                                old_version=pair['old']['document_version'], new_version=pair['new']['document_version'],
                                old_only_rows=len(residual['old']), new_only_rows=len(residual['new']),
                                unchanged_row_occurrences=sum((counts['old'] & counts['new']).values()),
                                raw_nonempty_cell_observations=sum(bool(c) for side in residual for r in residual[side] for c in r['cells']),
                                residual_rows=residual))
    for pair, output in zip(read(root / 'SOURCE_ONLY_PAIRS.json'), persisted):
        replay = compare(dict(comparison_scope=output['comparison_scope'], project_scope=pair['project_scope'],
                              old_records=by_version[pair['old']['document_version']]['records'],
                              new_records=by_version[pair['new']['document_version']]['records']))
        if digest(replay) != digest({k: output[k] for k in replay}):
            raise ValueError('Persisted input cross-process replay mismatch')
    # Verify every extracted value/identity even when ProjectChange output is empty.
    source_evidence = [e for d in views for r in d['records'] for e in
                       r['evidence'] + [e for v in r['values'] for e in v['evidence']]]
    source_checked = verify_evidence(source_evidence)
    dev_evidence = []
    for doc in read(DEV_MANIFEST):
        d = load_document(doc, V3 / 'optimized_benchmark_candidate_1')
        dev_evidence.extend(e for r in d['records'] for e in
                            r['evidence'] + [e for v in r['values'] for e in v['evidence']])
    dev_checked = verify_evidence(dev_evidence)
    totals = dict(raw_unmatched_row_observations=sum(d['old_only_rows'] + d['new_only_rows'] for d in diagnostics),
                  old_only_rows=sum(d['old_only_rows'] for d in diagnostics),
                  new_only_rows=sum(d['new_only_rows'] for d in diagnostics),
                  raw_nonempty_cell_observations=sum(d['raw_nonempty_cell_observations'] for d in diagnostics),
                  source_input_evidence_cells_verified=source_checked, dev_input_evidence_cells_verified=dev_checked,
                  cross_process_replayed_pairs=len(persisted),
                  audit_wall_seconds=time.perf_counter() - start,
                  interpretation='Lexical multiset residuals, no row pairing/engineering truth; includes headers and formatting. Not comparable fact differences.')
    write(root / 'reports/RAW_TABLE_DIAGNOSTICS.json', dict(totals=totals, pairs=diagnostics))
    score = read(root / 'reports/SCORECARD.json')
    score.update(raw_unmatched_row_observations=totals['raw_unmatched_row_observations'],
                 raw_nonempty_cell_observations=totals['raw_nonempty_cell_observations'],
                 comparable_fact_differences=score['raw_table_differences'],
                 source_input_evidence_cells_verified=source_checked, dev_input_evidence_cells_verified=dev_checked,
                 real_parameter_compression_evaluable=bool(score['entity_matches']))
    write(root / 'reports/SCORECARD.json', score)
    manifest = read(root / 'reports/CANDIDATE_MANIFEST.json')
    manifest['source_only_score'] = score
    manifest['reporting_audit'] = dict(module='experiments/table_project_change_v1/diagnose.py',
                                       sha256=file_hash('experiments/table_project_change_v1/diagnose.py'),
                                       changes_to_extraction_grouping_after_replay=0)
    write(root / 'reports/CANDIDATE_MANIFEST.json', manifest)
    n, cells = totals['raw_unmatched_row_observations'], totals['raw_nonempty_cell_observations']
    warning = f'''\n## Coverage audit: zero output is not zero source change

Raw TABLE lexical residuals: **{n} unmatched row observations** ({totals['old_only_rows']} OLD-only + {totals['new_only_rows']} NEW-only), containing {cells} nonempty source cell observations. These are multiset differences of source TABLE rows, including headers/formatting. They are not paired changed-cell facts and do not imply added/removed equipment.

Compared typed facts: {score['comparable_fact_differences']}; entity matches: {score['entity_matches']}; ProjectChanges: {score['project_changes']}. Real parameter compression is **NOT EVALUABLE**, not an apparent {n} → 0 improvement. The adapter has insufficient semantic coverage to connect these raw observations. In particular, no complete TableIdentity or entity identity is invented for the {score['unresolved_observations']} unanchored observations.

Both V3 boundary uncertainty and the narrow TABLE semantic extractor limit this candidate. The result does not establish that the source PDFs lack useful table evidence. Source-only data were not used to tune a broader extractor after seeing this result. Next development needs permitted paired TABLE evidence and event truth, plus DEV-based header/entity extraction work.

Independent persisted input verification: {source_checked} source-only and {dev_checked} DEV cells, including headers, source receipts and TABLE ownership. This verification is non-vacuous even with an empty ProjectChange array. Detailed raw locators and cell values: RAW_TABLE_DIAGNOSTICS.json. No algorithm changed during this accounting audit.
'''
    for name in ('TABLE_PROJECTCHANGE_SCORECARD.md', 'PARAMETER_COLLAPSE_REPORT.md', 'QUALITY_AUDIT.md',
                 'TABLE_ENTITY_RESOLUTION_REPORT.md', 'TABLE_DIFF_CONTRACT.md', 'ENGINEER_FACING_REPORT.md',
                 'TABLE_PROJECTCHANGE_ARCHITECTURE.md', 'CHECKPOINT.md', 'NEXT_ACTION.md'):
        path = root / 'reports' / name
        original = path.read_text().split('\n## Coverage audit:')[0]
        path.write_text(original.rstrip() + '\n' + warning)
    p = root / 'reports/PERFORMANCE_REPORT.md'
    control_timings = []
    for _ in range(5):
        tick = time.perf_counter()
        for case in cases():
            compare(case['pair'])
        control_timings.append(time.perf_counter() - tick)
    write(root / 'reports/POSTFREEZE_PERFORMANCE.json', dict(constructed_18_case_replay_seconds=control_timings,
          audit_wall_seconds=totals['audit_wall_seconds'], cross_process_replayed_pairs=len(persisted)))
    p.write_text(p.read_text().split('\nPost-freeze')[0].rstrip() +
                 f"\n\nPost-freeze raw-accounting and persisted input verification audit: {totals['audit_wall_seconds']:.3f} s, reported separately from the adapter run.\n" +
                 f"\nFive warm 18-case constructed grouping passes (40 facts → 17 events): {control_timings} s. " +
                 "The source-only warm benchmark has zero matched entities and does not characterize positive grouping throughput. " +
                 f"All {len(persisted)} pairs replay identically in this separate process from persisted input views.\n")
    constructed = read(root / 'reports/CONSTRUCTED_DEV_CHANGES.json')
    lines = ['# Constructed DEV engineer report', '',
             'All examples below are explicitly constructed tests, not changes found in real project PDFs.', '']
    for case in constructed:
        for change in case['project_changes']:
            lines += [f"## {case['case']} — {change['short_summary_ru']}", '',
                      f"- Что изменилось: {change['change_type']}; {change['status']}.",
                      f"- Где: {change['comparison_scope']}.",
                      f"- Было: {change['old_state']}.", f"- Стало: {change['new_state']}.",
                      f"- Основная сущность: {change['engineering_subject']['semantic_subject']}.",
                      '- Почему одно событие: одна явно заданная сущность и один ожидаемый тип события; независимое количество остаётся отдельным.', '',
                      '<details><summary>Все параметры и ссылки на тестовые ячейки</summary>', '',
                      '| Параметр | Было | Стало | OLD / NEW evidence |', '|---|---|---|---|']
            for f in change['supporting_fact_changes']:
                lines.append(f"| {f['property']} | {f['old']['value']} | {f['new']['value']} | {', '.join(f['evidence_old'])} / {', '.join(f['evidence_new'])} |")
            lines += ['', '</details>', '']
    (root / 'reports/ENGINEER_FACING_CONSTRUCTED_DEV_REPORT.md').write_text('\n'.join(lines))
    print(totals)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--output', type=Path, default=DEFAULT_ROOT)
    diagnose(parser.parse_args().output.resolve())
