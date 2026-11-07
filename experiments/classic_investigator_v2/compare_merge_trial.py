"""Render paired merge outputs without treating pending/error as empty success."""
import argparse
import html
from pathlib import Path
from compare_outputs import compare, render
from source_contract import read, write, sha


def response_state(path):
    if not path.exists():
        return {'status': 'pending', 'path': str(path)}
    result = read(path)
    base = {'path': str(path), 'sha256': sha(path), 'usage': result.get('usage')}
    if result.get('error'):
        return {**base, 'status': 'failed', 'error': result['error']}
    data = result.get('json')
    if not isinstance(data, dict) or not isinstance(data.get('findings'), list):
        return {**base, 'status': 'invalid', 'error': 'Missing findings array'}
    findings = data['findings']
    ids = [f.get('id') if isinstance(f, dict) else None for f in findings]
    if any(not isinstance(i, str) or not i for i in ids) or len(set(ids)) != len(ids):
        return {**base, 'status': 'invalid', 'error': 'Ambiguous finding identities'}
    return {**base, 'status': 'completed_unadjudicated', 'findings': findings}


def run(trial, output):
    protocol = read(trial/'protocol.json')
    groups = {}
    for index, item in enumerate(protocol['requests']):
        if sha(item['path']) != item['sha256']:
            raise ValueError('Frozen request changed')
        disc, arm = item['discipline'], item['arm']
        if arm not in {'control', 'candidate'} or arm in groups.setdefault(disc, {}):
            raise ValueError('Ambiguous trial arm')
        groups[disc][arm] = response_state(trial/f'{index:02d}_response.json')
    output.mkdir(parents=True, exist_ok=True)
    rows = []
    for disc, pair in groups.items():
        if set(pair) != {'control', 'candidate'}:
            raise ValueError('Incomplete protocol pair')
        ready = all(x['status'] == 'completed_unadjudicated' for x in pair.values())
        row = {'discipline': disc, 'paired_outputs_ready': ready,
               'arms': {arm: {k: v for k, v in data.items() if k != 'findings'}
                        for arm, data in pair.items()},
               'technical_acceptance': 'not_established'}
        if ready:
            result = compare(pair['control']['findings'], pair['candidate']['findings'])
            result['response_receipts'] = row['arms']
            write(output/f'{disc}.json', result)
            (output/f'{disc}.html').write_text(render(result))
            row['counts'] = {arm: len(data['findings']) for arm, data in pair.items()}
            row['comparison'] = f'{disc}.html'
        rows.append(row)
    summary = {'protocol_sha256': sha(trial/'protocol.json'), 'rows': rows,
               'all_pairs_ready': all(r['paired_outputs_ready'] for r in rows),
               'limits': 'Prepared responses only. Pending is not clean/failed; empty successful output is not '
                         'proof of no defects. Row counts and shared source IDs do not measure useful recall '
                         'or technical truth. Full records are retained for semantic review.'}
    write(output/'summary.json', summary)
    parts = ['<!doctype html><meta charset="utf-8"><title>Парное сравнение объединения</title>',
             '<h1>Парное сравнение объединения: шесть дисциплин</h1>',
             '<p>Готовность ответов не означает техническую приёмку. Полные записи доступны после завершения пары.</p><ul>']
    for r in rows:
        label = html.escape(r['discipline'])
        if r['paired_outputs_ready']:
            parts.append(f'<li><a href="{label}.html">{label}</a>: '
                         f'{r["counts"]["control"]} / {r["counts"]["candidate"]} строк; требуется проверка смысла.</li>')
        else:
            states = ', '.join(f'{k}: {v["status"]}' for k, v in r['arms'].items())
            parts.append(f'<li>{label}: {html.escape(states)}</li>')
    (output/'index.html').write_text(''.join(parts)+'</ul>')
    print('Pairs ready:', sum(r['paired_outputs_ready'] for r in rows), '/', len(rows))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--trial', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    run(args.trial, args.output)
