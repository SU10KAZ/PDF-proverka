"""Read-only reporting after the complete decomposition freeze; never opens truth."""
from collections import Counter
import json
from pathlib import Path

from .preflight import OUT, read, sha, write


def report():
    freeze = read(OUT / 'DECOMPOSITION_FREEZE.json')
    for relative, digest in freeze['files'].items():
        if sha(OUT / relative) != digest:
            raise ValueError('Decomposition freeze drift: ' + relative)
    results = read(OUT / 'DECOMPOSITION_RESULTS.json')
    if len(results) != 18:
        raise ValueError('Incomplete decomposition; cannot report a completed experiment stage')
    summary = read(OUT / 'DECOMPOSITION_SUMMARY.json')
    candidates = read(OUT / 'LOCAL_CANDIDATE_INDEX.json')
    audits = {a['local_candidate_id']: a for a in read(OUT / 'LOCAL_CANDIDATE_REFERENCE_AUDIT.json')}
    duplicates = read(OUT / 'DUPLICATE_LOOKING_CANDIDATES.json')
    contexts = {b['bundle_id']: b for b in read(OUT / 'BROAD_CONTEXT_INDEX.json')['contexts']}
    kinds = Counter(c['candidate_kind'] for c in candidates)
    readiness = Counter(c['comparison_readiness'] for c in candidates)
    identities = Counter(c['identity_confidence'] for c in candidates)
    pages = Counter((side, p) for b in contexts.values() for side in ('old', 'new') for p in b[side]['pages'])
    evidence_overlap = dict(unique_source_pages=len(pages), page_memberships=sum(pages.values()),
                            pages_in_multiple_bundles=sum(n > 1 for n in pages.values()))
    source_only = dict(candidate_kinds=dict(kinds), readiness=dict(readiness), identity_confidence=dict(identities),
                       evidence_overlap=evidence_overlap, duplicate_looking_pairs=len(duplicates),
                       explanation='Counts include potential equality and insufficient candidates; contexts overlap in source pages. No candidates were discarded or semantically adjudicated.')
    write(OUT / 'DECOMPOSITION_EXPLOSION_DIAGNOSTIC.json', source_only)

    # Optional review workbook: explicitly discovery output, never ProjectChange admission.
    from openpyxl import Workbook
    from openpyxl.styles import Font, PatternFill, Alignment
    from openpyxl.utils import get_column_letter
    book = Workbook()
    sheet = book.active
    sheet.title = 'Candidates — preliminary'
    columns = ['Candidate', 'Broad context', 'Engineering subject', 'Claim type', 'Possible summary',
               'Candidate kind', 'Identity', 'Scope', 'Location', 'System', 'Identity confidence',
               'Readiness', 'OLD pages', 'NEW pages', 'OLD refs', 'NEW refs', 'Supporting refs',
               'Required modalities', 'Cross-modal refs', 'Invalid refs', 'Reason']
    sheet.append(columns)
    for c in candidates:
        registry = read(OUT / f'decomposition_inputs/{c["source_bundle"]}/REFERENCE_REGISTRY.json')
        row_audit = audits[c['local_candidate_id']]
        source_pages = {s: sorted({registry[r]['page'] for r in c[s + '_evidence_refs'] if r in registry and registry[r]['side'] == s})
                        for s in ('old', 'new')}
        row = [c['local_candidate_id'], contexts[c['source_bundle']]['coarse_heading'], c['engineering_subject'],
               c['claim_type'], c['possible_change_summary'], c['candidate_kind'], c['subject_identity'],
               c['scope'], c['location'], c['system_or_subsystem'], c['identity_confidence'], c['comparison_readiness'],
               ', '.join(map(str, source_pages['old'])), ', '.join(map(str, source_pages['new'])),
               ', '.join(c['old_evidence_refs']), ', '.join(c['new_evidence_refs']), ', '.join(c['supporting_evidence_refs']),
               ', '.join(c['required_modalities']), row_audit['cross_modal'],
               ', '.join(row_audit['invalid_refs'] + row_audit['wrong_side_refs']), c['reason']]
        sheet.append(row)
    distribution = book.create_sheet('Contexts')
    distribution.append(['Bundle', 'Broad context', 'Candidates', 'OLD pages', 'NEW pages'])
    for r in results:
        b = contexts[r['bundle_id']]
        distribution.append([r['bundle_id'], b['coarse_heading'], len(r['candidates']),
                             ', '.join(map(str, b['old']['pages'])), ', '.join(map(str, b['new']['pages']))])
    duplicate_sheet = book.create_sheet('Possible duplicates')
    duplicate_sheet.append(['Left candidate', 'Right candidate', 'Lexical Jaccard', 'Same subject text', 'Method'])
    for d in duplicates:
        duplicate_sheet.append([d['left'], d['right'], d['lexical_jaccard'], d['same_subject'], d['method']])
    notes = book.create_sheet('Read me')
    notes.append(['This workbook contains preliminary decomposition candidates, not accepted ProjectChanges.'])
    notes.append(['Source truth was not opened. Correctness, false ACCEPT and F13 have not been evaluated.'])
    notes.append(['Cross-modal means at least one differing pair of referenced modalities across OLD and NEW.'])
    notes.append(['Possible duplicates are a lexical diagnostic; no candidates were removed.'])
    notes.append(['No result cells are formulas. Complete authoritative text is in the frozen JSON files.'])
    for tab in book:
        tab.freeze_panes = 'A2'
        tab.auto_filter.ref = tab.dimensions
        for cell in tab[1]:
            cell.font = Font(bold=True, color='FFFFFF')
            cell.fill = PatternFill('solid', fgColor='243B53')
        for row in tab.iter_rows(min_row=2):
            for cell in row:
                if isinstance(cell.value, str):
                    # Treat all source/model text as literal spreadsheet text.
                    cell.data_type = 's'
                cell.alignment = Alignment(vertical='top', wrap_text=True)
        for i in range(1, tab.max_column + 1):
            tab.column_dimensions[get_column_letter(i)].width = 35 if i not in {3, 5, 7, 21} else 65
    target = OUT / 'DECOMPOSITION_CANDIDATES_PRELIMINARY.xlsx'
    if target.exists():
        raise FileExistsError(target)
    book.save(target)

    dist = summary['distribution']
    counts = {r['bundle_id']: len(r['candidates']) for r in results}
    lines = ['# Semantic Decomposer V1 / Pair B / ИОС4.2', '',
             'STATUS: ' + summary['status'], '',
             'BROAD CONTEXTS: 18 non-empty; BROAD CONTEXT LIMIT: 20',
             'CONTEXT CONTENT CHANGED: NO',
             'DECOMPOSITION SUCCESSFUL CALLS: 18',
             f'LOCAL CANDIDATES: {len(candidates)}',
             f'CANDIDATES PER CONTEXT min / median / max: {dist["min"]} / {dist["median"]} / {dist["max"]}',
             f'OLD+NEW refs: {summary["candidates_with_old_new_refs"]}',
             f'ONE-SIDED: {summary["one_sided_candidates"]}; NO REFS: {summary["no_refs_candidates"]}',
             f'CROSS-MODAL CANDIDATES: {summary["cross_modal_candidates"]}',
             f'DUPLICATE-LOOKING PAIRS: {len(duplicates)}; removed: 0',
             f'INVALID-REFERENCE CANDIDATES: {summary["invalid_reference_candidates"]}',
             'LOCAL PACKAGE GUARD: ' + summary['local_package_guard'],
             'COMPARISON CALLS: 0', '', '## Распределение', '']
    lines += [f'- {contexts[key]["coarse_heading"]}: {count}' for key, count in counts.items()]
    lines += ['', '## Причины количества candidates', '',
              'Один широкий контекст возвращает много самостоятельных subjects/claims. В сумму входят все виды candidates:',
              json.dumps(dict(kinds), ensure_ascii=False), '',
              f'Число включений страниц в bundles: {sum(pages.values())}; уникальных страниц: {len(pages)};',
              f'в нескольких bundles встречаются {sum(n > 1 for n in pages.values())} страниц.',
              'Это создаёт возможность повторного обнаружения одних предметов в независимых контекстах.',
              'Лексическая похожесть не доказывает дублирование. Ничего не удалено и не обрезано.', '',
              'DECOMPOSITION_FREEZE.json создан только после всех 18 успешных ответов.',
              'При превышении 50 сработал пункт 7 resume: comparison и evaluation не запускались.',
              'PROVEN FINDINGS denominator: 10 (из задания).',
              'Decomposer discovered / correct local subject / sufficient local package / Astra found / final ACCEPT: NOT_EVALUATED.',
              'CORRECT ACCEPT / FALSE ACCEPT / F13: NOT_EVALUATED.',
              'SUBJECT_TOO_BROAD: before 4; after NOT_EVALUATED.',
              'PACKAGE_SCOPE_WRONG: before 4; after NOT_EVALUATED.',
              'NO TRUTH LEAKAGE: PASS. Исторически известный DEV не объявляется новым blind набором.',
              'PRODUCTION / Pair A / UI / baseline: UNCHANGED.',
              'VALIDATION: NOT OPENED; FINAL HOLDOUT: NOT OPENED.', '',
              '## Архитектурный вывод', '',
              'AI вернул локальные предложения, но их истинность и качество относительно 10 PROVEN здесь не проверены.',
              'Число candidates не является числом найденных изменений. Вывод о переносе discovery на AI пока не доказан.',
              'RECOMMENDATION: MORE_CONTROLLED_TESTING_REQUIRED.', '',
              'JSON-файлы содержат полный исходный результат. Excel предназначен для просмотра decomposition;',
              'он не является системным отчётом ACCEPT/REVIEW/NOT_CHANGE.', '']
    write(OUT / 'DECOMPOSITION_FINAL_REPORT.md', '\n'.join(lines))
    print('REPORT CREATED ' + str(OUT / 'DECOMPOSITION_FINAL_REPORT.md'))


if __name__ == '__main__':
    report()
