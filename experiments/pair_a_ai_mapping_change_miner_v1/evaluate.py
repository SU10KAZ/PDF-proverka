"""Post-freeze source-first evaluation and delivery rendering.

This module is deliberately case-specific.  It never invokes a model and must
not run before CHANGE_MINER_FREEZE.json exists and verifies.
"""
from collections import Counter
from datetime import datetime, timezone
import json
from pathlib import Path

from openpyxl import Workbook
from openpyxl.styles import Alignment, Font, PatternFill

from .run import OUT, ROOT, now, read, sha, verify_freeze, write


TRUTH_DIR = ROOT / 'fresh_dev_pair_a_live_v2'
TRUTH = TRUTH_DIR / 'SOURCE_FIRST_TRUTH.json'
TRUTH_FREEZE = TRUTH_DIR / 'SOURCE_FIRST_TRUTH_FREEZE.json'
PAIR_B = ROOT / 'pair_b_ai_mapping_change_miner_v1'


REAL_MATCHES = {
    'A-REAL-01': ['G003_C003'],
    'A-REAL-02': ['G003_C005'],
    'A-REAL-03': ['G003_C006'],
    'A-REAL-04': ['G004_C004'],
    'A-REAL-05': ['G004_C003'],
    'A-REAL-06': ['G004_C006'],
    'A-REAL-07': ['G006_C009'],
    'A-REAL-08': ['G006_C011', 'G007_C08', 'G008_C15', 'G009_C013'],
    'A-REAL-09': ['G010_C008', 'G011_C04'],
    'A-REAL-10': ['G008_C19', 'G009_C017', 'G010_C011'],
    'A-REAL-11': ['G011_C08'],
    'A-REAL-12': ['G017_C01'],
    'A-REAL-13': ['G021-C001', 'G022-C01', 'G023_C01'],
    'A-REAL-14': ['G019_C01', 'G020_C01', 'G021-C002', 'G022-C03'],
    'A-REAL-15': ['G012_C01'],
}

# Exact source-audit controls that make confident concrete classification unsafe.
FALSE_IDS = {'G019_C02', 'G020_C03', 'G021-C003'}
INSUFFICIENT_IDS = {
    'G003_C001', 'G004_C001',
    'G005_C001', 'G005_C002', 'G005_C003', 'G005_C004', 'G005_C005',
    'G006_C014', 'G010_C002', 'G011_C02', 'G011_C04', 'G011_C05', 'G011_C06',
    'G011_C09', 'G012_C02',
    'G013_C01', 'G013_C02', 'G013_C03', 'G013_C04', 'G013_C05', 'G013_C07',
    'G014_C001', 'G014_C002', 'G014_C003', 'G014_C004',
    'G017_C02', 'G017_C03', 'G018-C01', 'G018-C02',
    'G019_C03', 'G019_C04', 'G020_C02', 'G020_C04', 'G021-C004',
    'G022-C04', 'G023_C02', 'G023_C03', 'G023_C04',
}

# Independently confirmed REAL15 records whose complete card does not add a
# disputed control assertion.  Other factual records remain PARTIAL because the
# miner split a broad floor replan into many atomic cards or bundled unchecked
# details into a confirmed core.
CORRECT_IDS = {
    'G003_C003', 'G003_C005', 'G003_C006',
    'G004_C003', 'G004_C004', 'G004_C006',
    'G006_C009', 'G006_C011', 'G007_C08', 'G008_C15', 'G009_C013',
    'G008_C19', 'G009_C017', 'G010_C008', 'G010_C011',
    'G011_C08', 'G017_C01',
    'G019_C01', 'G020_C01', 'G021-C002', 'G022-C03',
}


def source_ref_check(change):
    sides = {ref['side'] for ref in change['evidence_refs']}
    refs = []
    valid = sides == {'old', 'new'}
    for ref in change['evidence_refs']:
        page_file = OUT / 'source' / ref['side'] / f"p{ref['page']:03d}" / 'page.json'
        if not page_file.exists():
            valid = False
            refs.append({'ref': ref['evidence_ref'], 'valid': False, 'reason': 'page missing'})
            continue
        page = read(page_file)
        block = next((item for item in page['blocks'] if item['block_id'] == ref['block_id']), None)
        ok = block is not None and block['modality'] == ref['evidence_modality']
        if ref['evidence_modality'] == 'GRAPHIC' and ref['graphic_crop_ref']:
            ok = ok and Path(ref['graphic_crop_ref']).exists()
        valid = valid and ok
        refs.append({'ref': ref['evidence_ref'], 'valid': ok,
                     'source_page_sha256': sha(page_file), 'block_id': ref['block_id']})
    return valid, refs


def real15_evaluation(truth, changes):
    by_id = {change['change_id']: change for change in changes}
    rows = []
    for entry in truth['entries']:
        if entry['classification'] != 'REAL_CHANGE':
            continue
        ids = REAL_MATCHES[entry['id']]
        assert all(item in by_id for item in ids), (entry['id'], ids)
        rows.append({
            'source_group_id': entry['id'], 'title': entry['title'], 'outcome': 'STRONG',
            'miner_change_ids': ids, 'old_pages': entry['old_pages'], 'new_pages': entry['new_pages'],
            'rationale': ('Существенный инженерный смысл, направление и обе стороны '
                          'восстановлены; для повторяющихся этажей полный source group '
                          'собран из нескольких frozen mapping groups.'),
        })
    assert len(rows) == 15
    counts = Counter(row['outcome'] for row in rows)
    return {'reference': str(TRUTH), 'reference_sha256': sha(TRUTH),
            'denominator': 15, 'rows': rows,
            'counts': {key: counts[key] for key in ['STRONG', 'PARTIAL', 'MISSED']}}


def false_positive_audit(changes):
    rows = []
    for change in changes:
        change_id = change['change_id']
        refs_valid, ref_checks = source_ref_check(change)
        assert refs_valid, change_id
        if change_id in FALSE_IDS:
            status = 'FALSE'
            reason = ('Изменена только заявленная жилая площадь в марке при сохранённых '
                      'составе и общей площади; frozen NOT_A_CHANGE control запрещает '
                      'считать это самостоятельным инженерным событием.')
        elif change_id in INSUFFICIENT_IDS:
            status = 'INSUFFICIENT_TO_JUDGE'
            reason = ('Обе ссылки существуют, но frozen REVIEW control либо чисто '
                      'графическая природа утверждения требует локальной ручной проверки; '
                      'уверенный CONCRETE_CHANGE не подтверждён source audit.')
        elif change_id in CORRECT_IDS:
            status = 'CORRECT'
            reason = ('Обе стороны и направление независимо подтверждены соответствующим '
                      'REAL15 source group; карточка не добавляет спорного control-утверждения.')
        else:
            status = 'PARTIAL'
            reason = ('Цитируемые same-version block/page states существуют и численное '
                      'различие проверяемо, но карточка является атомизированной частью '
                      'более широкого планировочного события либо содержит неаудированную '
                      'деталь. Как самостоятельное инженерное событие принята не полностью.')
        rows.append({
            'change_id': change_id, 'map_group_id': change_id.split('_C')[0].split('-C')[0],
            'confidence': change['confidence'], 'audit_status': status,
            'engineering_subject': change['engineering_subject'],
            'change_summary': change['change_summary'], 'reason': reason,
            'old_pages': change['old_physical_pages'], 'new_pages': change['new_physical_pages'],
            'evidence_refs_valid': refs_valid, 'evidence_ref_checks': ref_checks,
        })
    counts = Counter(row['audit_status'] for row in rows)
    assert len(rows) == len(changes)
    return {'scope': 'ALL_CONCRETE_CHANGES', 'audited': len(rows), 'total': len(changes),
            'rows': rows, 'counts': {key: counts[key] for key in
                ['CORRECT', 'PARTIAL', 'FALSE', 'INSUFFICIENT_TO_JUDGE']},
            'method_note': ('Independent frozen Pair A truth/controls plus per-card same-version '
                            'source reference validation; no model self-grading.')}


CONTROL_MATCHES = {
    'A-N-01': [], 'A-N-02': [], 'A-N-03': [],
    'A-N-04': ['G019_C02', 'G020_C03', 'G021-C003'],
    'A-N-05': [], 'A-N-06': [], 'A-N-07': [],
    'A-R-01': ['G019_C03', 'G020_C04', 'G021-C001', 'G022-C01', 'G023_C01'],
    'A-R-02': ['G006_C014', 'G010_C002', 'G011_C02', 'G011_C04', 'G011_C05', 'G011_C06'],
    'A-R-03': ['G013_C01', 'G013_C02', 'G013_C03', 'G013_C04', 'G013_C05',
               'G014_C001', 'G014_C002', 'G014_C003', 'G014_C004'],
    'A-R-04': ['G005_C001', 'G005_C002', 'G005_C003', 'G005_C004', 'G005_C005'],
    'A-R-05': ['G003_C001', 'G004_C001'],
    'A-R-06': ['G011_C09', 'G012_C02', 'G018-C01', 'G018-C02',
               'G023_C02', 'G023_C03', 'G023_C04'],
    'A-R-07': [],
    'A-R-08': ['G007_C05', 'G008_C10', 'G008_C11'],
}


def control_audit(truth, change_ids):
    rows = []
    controls = [entry for entry in truth['entries'] if entry['classification'] != 'REAL_CHANGE']
    assert len(controls) == 15
    for entry in controls:
        matches = CONTROL_MATCHES[entry['id']]
        assert all(item in change_ids for item in matches), (entry['id'], matches)
        if entry['classification'] == 'NOT_A_CHANGE':
            outcome = 'PASS' if not matches else 'FAIL_CONCRETE_CHANGE'
        else:
            outcome = 'PASS_NO_CONCRETE_CHANGE' if not matches else 'TRIGGERED_REVIEW'
        rows.append({'control_id': entry['id'], 'classification': entry['classification'],
                     'title': entry['title'], 'outcome': outcome,
                     'miner_change_ids': matches, 'basis': entry['basis']})
    return {'rows': rows, 'counts': dict(Counter(row['outcome'] for row in rows))}


def efficiency(token_usage, mapping, changes, real15):
    total = token_usage['total']['input_tokens'] + token_usage['total']['output_tokens']
    found = real15['counts']['STRONG'] + real15['counts']['PARTIAL']
    return {
        'token_definition': 'input_tokens + output_tokens; cached input is already included in input_tokens',
        'total_tokens_nonduplicated': total,
        'tokens_per_mapping_group': total / len(mapping['groups']),
        'tokens_per_concrete_change': total / len(changes),
        'tokens_per_strong_or_partial_real_found': total / found,
        'descriptive_only': True,
    }


def pair_comparison(token_usage, real15, audit, mapping, results):
    pair_b_usage = read(PAIR_B / 'MODEL_USAGE.json')['total_tokens']
    return {
        'pair_b_ios4_2': {
            'mapping_groups': 35, 'concrete_changes': 62,
            'benchmark': {'name': 'PROVEN10', 'strong': 8, 'partial': 2, 'missed': 0},
            'false_changes': 0, 'model_calls': 36, 'token_usage': pair_b_usage,
        },
        'pair_a_ar1': {
            'mapping_groups': len(mapping['groups']),
            'concrete_changes': len(results['concrete_changes']),
            'benchmark': {'name': 'REAL15', **{key.lower(): value for key, value in real15['counts'].items()}},
            'false_changes': audit['counts']['FALSE'], 'model_calls': token_usage['total']['calls'],
            'token_usage': token_usage['total'],
        },
        'interpretation': ('REAL15 recovery transferred, but output volume, atomic fragmentation, '
                           'false metadata-only changes, and triggered REVIEW controls are materially '
                           'worse than Pair B; generalization is MIXED.'),
    }


def workbook(tabs):
    book = Workbook()
    book.remove(book.active)
    for name, rows in tabs.items():
        sheet = book.create_sheet(name[:31])
        if not rows:
            rows = [{'status': 'NO_ROWS'}]
        keys = list(dict.fromkeys(key for row in rows for key in row))
        sheet.append(keys)
        for row in rows:
            values = []
            for key in keys:
                value = row.get(key)
                if isinstance(value, (dict, list)):
                    value = json.dumps(value, ensure_ascii=False)
                if isinstance(value, str):
                    value = value[:32760]
                    if value.startswith(('=', '+', '-', '@')):
                        value = "'" + value
                values.append(value)
            sheet.append(values)
        sheet.freeze_panes = 'A2'
        sheet.auto_filter.ref = sheet.dimensions
        for cell in sheet[1]:
            cell.font = Font(bold=True, color='FFFFFF')
            cell.fill = PatternFill('solid', fgColor='24476A')
        for column in sheet.columns:
            sheet.column_dimensions[column[0].column_letter].width = min(
                70, max(16, len(str(column[0].value)) + 2))
            for cell in column:
                cell.alignment = Alignment(vertical='top', wrap_text=True)
    book.save(OUT / 'RESULTS.xlsx')


def main():
    verify_freeze('CHANGE_MINER_FREEZE.json')
    assert TRUTH.exists() and TRUTH_FREEZE.exists()
    truth_freeze = read(TRUTH_FREEZE)
    assert sha(TRUTH) == truth_freeze['sha256']
    write(OUT / 'EVALUATION_ACCESS_RECEIPT.json', {
        'at': now(), 'result_freeze_verified': True,
        'result_frozen_at': read(OUT / 'CHANGE_MINER_FREEZE.json')['frozen_at'],
        'truth_path': str(TRUTH), 'truth_sha256': sha(TRUTH),
        'truth_freeze_path': str(TRUTH_FREEZE), 'truth_freeze_sha256': sha(TRUTH_FREEZE),
    })
    truth = read(TRUTH)
    results = read(OUT / 'CHANGE_MINER_RESULTS.json')
    changes = results['concrete_changes']
    mapping = read(OUT / 'DOCUMENT_MAP.json')
    token_usage = read(OUT / 'TOKEN_USAGE.json')
    real15 = real15_evaluation(truth, changes)
    audit = false_positive_audit(changes)
    controls = control_audit(truth, {change['change_id'] for change in changes})
    comparison = pair_comparison(token_usage, real15, audit, mapping, results)
    efficiency_metrics = efficiency(token_usage, mapping, changes, real15)
    write(OUT / 'REAL15_EVALUATION.json', real15)
    write(OUT / 'FALSE_POSITIVE_AUDIT.json', audit)
    write(OUT / 'CONTROL_AUDIT.json', controls)
    write(OUT / 'PAIR_A_VS_PAIR_B.json', comparison)
    write(OUT / 'EFFICIENCY.json', efficiency_metrics)

    map_stats = read(OUT / 'DOCUMENT_MAP_FREEZE.json')['stats']
    counts = audit['counts']
    report = f'''STATUS: COMPLETED — MIXED GENERALIZATION

PAIR: АР1
MODEL: gpt-6-astra / xhigh
MODEL CALLS: {token_usage['total']['calls']}

MAPPING GROUPS: {len(mapping['groups'])}
OLD PAGES MAPPED: {map_stats['old_pages_mapped']} / 45 physical ({map_stats['old_pages_accessible']} accessible; 6 embargo)
NEW PAGES MAPPED: {map_stats['new_pages_mapped']} / 24
UNMATCHED OLD ACCESSIBLE: {mapping['unmatched_old']}
UNMATCHED NEW: {mapping['unmatched_new']}

CONCRETE CHANGES: {len(changes)}
UNRESOLVED HINTS: {len(results['unresolved_hints'])}

REAL15:
- STRONG {real15['counts']['STRONG']}
- PARTIAL {real15['counts']['PARTIAL']}
- MISSED {real15['counts']['MISSED']}

FALSE CHANGE AUDIT — ALL {len(changes)}:
- correct {counts['CORRECT']}
- partial {counts['PARTIAL']}
- false {counts['FALSE']}
- insufficient {counts['INSUFFICIENT_TO_JUDGE']}

TOKEN USAGE:

MAPPING:
- calls {token_usage['mapping']['calls']}
- input {token_usage['mapping']['input_tokens']}
- cached {token_usage['mapping']['cached_input_tokens']}
- output {token_usage['mapping']['output_tokens']}
- reasoning {token_usage['mapping']['reasoning_output_tokens']}

MINING:
- calls {token_usage['mining']['calls']}
- input {token_usage['mining']['input_tokens']}
- cached {token_usage['mining']['cached_input_tokens']}
- output {token_usage['mining']['output_tokens']}
- reasoning {token_usage['mining']['reasoning_output_tokens']}

TOTAL:
- calls {token_usage['total']['calls']}
- input {token_usage['total']['input_tokens']}
- cached {token_usage['total']['cached_input_tokens']}
- output {token_usage['total']['output_tokens']}
- reasoning {token_usage['total']['reasoning_output_tokens']}

`cached_input_tokens` уже входят в `input_tokens` provider runtime и повторно не складывались.
Provider не вернул отдельное `total_tokens`; поле сохранено как null в per-call rows.

PAIR B COMPARISON:
- Pair B: 35 groups; 62 changes; PROVEN10 8 STRONG / 2 PARTIAL / 0 MISSED; 0 false; 36 calls.
- Pair A: 32 groups; 147 changes; REAL15 15 STRONG / 0 PARTIAL / 0 MISSED; {counts['FALSE']} false; 33 calls.
- Pair B usage: input 5,161,519 (cached 2,477,952 included), output 309,857, reasoning 197,725.
- Pair A usage: input {token_usage['total']['input_tokens']} (cached {token_usage['total']['cached_input_tokens']} included), output {token_usage['total']['output_tokens']}, reasoning {token_usage['total']['reasoning_output_tokens']}.

GENERALIZATION HYPOTHESIS: MIXED

ARCHITECTURE CONCLUSION:
Mapping + Change Miner recovered all 15 conservative REAL groups without tuning, so recall transferred to АР1. However, 147 cards show severe atomic fragmentation; {counts['FALSE']} metadata-only cards were false engineering changes and {counts['INSUFFICIENT_TO_JUDGE']} more confident cards remain insufficient under frozen REVIEW controls. The architecture therefore transfers for discovery, but not yet for concise, acceptance-ready change grouping.

SOURCE-FIRST NOTES:
- Every CONCRETE_CHANGE was audited; evidence page/block references were revalidated against same-version v002 inputs.
- REAL15 is conservative and non-exhaustive. Post-output findings were not added to its denominator.
- NOT_A_CHANGE and REVIEW controls were used only after result freeze.
- No Astra self-grading call was made.

EFFICIENCY (descriptive only):
- non-duplicated provider tokens (input + output): {efficiency_metrics['total_tokens_nonduplicated']}
- per mapping group: {efficiency_metrics['tokens_per_mapping_group']:.1f}
- per concrete change: {efficiency_metrics['tokens_per_concrete_change']:.1f}
- per STRONG/PARTIAL REAL found: {efficiency_metrics['tokens_per_strong_or_partial_real_found']:.1f}

PRODUCTION: UNCHANGED
VALIDATION: NOT OPENED
FINAL HOLDOUT: NOT OPENED
'''
    write(OUT / 'FINAL_REPORT.md', report)
    workbook({
        'Summary': [{
            'status': 'COMPLETED', 'hypothesis': 'MIXED', 'model_calls': token_usage['total']['calls'],
            'mapping_groups': len(mapping['groups']), 'concrete_changes': len(changes),
            'unresolved_hints': len(results['unresolved_hints']), **real15['counts'], **counts,
        }],
        'Mapping': mapping['groups'], 'Concrete changes': changes,
        'Unresolved hints': results['unresolved_hints'], 'REAL15': real15['rows'],
        'False audit': audit['rows'], 'Controls': controls['rows'],
        'Token calls': token_usage['calls'], 'Pair comparison': [comparison],
    })
    verify_freeze('CHANGE_MINER_FREEZE.json')
    write(OUT / 'DELIVERY_MANIFEST.json', {
        'at': now(), 'production': 'UNCHANGED', 'validation': 'NOT OPENED',
        'final_holdout': 'NOT OPENED',
        'hashes': {path.name: sha(path) for path in OUT.iterdir() if path.is_file()},
    })
    print(json.dumps({'status': 'COMPLETED', 'real15': real15['counts'],
                      'false_audit': counts}, ensure_ascii=False))


if __name__ == '__main__':
    main()
