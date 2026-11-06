"""Prepare full, lossless human comparison; shared candidate IDs are navigation only."""
import argparse
from collections import Counter
import html
import json
from pathlib import Path
from source_contract import read, write, sha


def compare(baseline, candidate):
    rows = []
    for old in baseline:
        source_ids = set(old.get('source_finding_ids') or [])
        matches = [new for new in candidate if source_ids & set(new.get('source_finding_ids') or [])]
        exact = [new for new in candidate if
                 {k: v for k, v in old.items() if k != 'id'} ==
                 {k: v for k, v in new.items() if k != 'id'}]
        # Final F-IDs are renumbered per run and cannot establish correspondence.
        if exact:
            status, matches = 'exact_record_except_output_id', exact
        elif matches:
            status = 'shared_input_ids_requires_atom_review'
        else:
            status = 'no_input_link_requires_search'
        rows.append({'baseline': old, 'candidate_matches': matches, 'status': status,
                     'semantic_preservation_verified': bool(exact),
                     'technical_validity_verified': False})
    covered = {id(new) for row in rows for new in row['candidate_matches']}
    return {'rows': rows, 'candidate_rows_without_baseline_link':
            [new for new in candidate if id(new) not in covered],
            'counts': dict(Counter(row['status'] for row in rows)),
            'limits': 'Full records retained. Exact content preservation does not prove validity. '
                      'Shared source IDs do not prove retention of all atoms; missing IDs do not '
                      'prove loss. Output IDs never establish a match. Unlinked candidate rows '
                      'are not automatically new findings. Baseline useful atoms need primary adjudication.'}


def render(result):
    def record(value):
        return '<pre>' + html.escape(json.dumps(value, ensure_ascii=False, indent=2)) + '</pre>'
    parts = ['<!doctype html><meta charset="utf-8"><title>Сохранение замечаний</title>',
             '<style>body{font:16px/1.4 sans-serif;margin:25px}table{width:100%;table-layout:fixed;border-collapse:collapse}td{border:1px solid #ccc;vertical-align:top;padding:10px}pre{white-space:pre-wrap;overflow-wrap:anywhere}h2{margin-top:45px}</style>',
             '<h1>Сопоставление результатов без сокращения исходных записей</h1>',
             '<p>' + html.escape(result['limits']) + '</p>', record(result['counts'])]
    for row in result['rows']:
        parts.extend(['<h2>' + html.escape(str(row['baseline'].get('id'))) + ': ' + row['status'] + '</h2>',
                      '<table><tr><td>' + record(row['baseline']) + '</td><td>' +
                      record(row['candidate_matches']) + '</td></tr></table>'])
    parts.extend(['<h2>Строки варианта без установленной связи с контролем</h2>',
                  record(result['candidate_rows_without_baseline_link'])])
    return ''.join(parts)


def run(baseline, candidate, output):
    result = compare(read(baseline)['findings'], read(candidate)['findings'])
    result['inputs'] = {str(p): sha(p) for p in [baseline, candidate]}
    write(output, result)
    output.with_suffix('.html').write_text(render(result), encoding='utf-8')
    print(result['counts'])


if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('--baseline', type=Path, required=True)
    p.add_argument('--candidate', type=Path, required=True)
    p.add_argument('--output', type=Path, required=True)
    a = p.parse_args()
    run(a.baseline, a.candidate, a.output)
