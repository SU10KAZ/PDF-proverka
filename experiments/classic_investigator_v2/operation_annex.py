"""Lossless experimental appendix; preserves questions without endorsing model claims."""
import argparse
import copy
import hashlib
import html
import json
from pathlib import Path


def digest(value):
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True).encode()).hexdigest()


def build(text, final):
    findings = final['findings']
    output_ids = [f['id'] for f in findings]
    if any(not x for x in output_ids) or len(set(output_ids)) != len(output_ids):
        raise ValueError('Ambiguous output IDs')
    groups = []
    seen_candidates, seen_operations = set(), set()
    for candidate in text['text_findings']:
        cid = candidate['id']
        if not cid or cid in seen_candidates:
            raise ValueError('Ambiguous input IDs')
        seen_candidates.add(cid)
        if 'operation_receipts' not in candidate:
            continue
        receipts = candidate['operation_receipts']
        ids = [r['operation_id'] for r in receipts]
        if not ids or ids != candidate.get('operation_ids'):
            raise ValueError('Operation list does not match receipts')
        if candidate.get('candidate_status') != 'question':
            raise ValueError('Unsupported operation candidate status')
        for receipt in receipts:
            oid = receipt['operation_id']
            if not oid or oid in seen_operations:
                raise ValueError('Duplicate operation receipt')
            if receipt.get('status') not in {'arithmetic_question', 'quantity_question'}:
                raise ValueError('Unperformed operation cannot become a question')
            if any(not receipt.get(key) for key in ['source_quote', 'block_id', 'finding']):
                raise ValueError('Missing source evidence')
            seen_operations.add(oid)
        links = [f['id'] for f in findings if cid in f.get('source_finding_ids', [])]
        groups.append({
            'candidate_id': cid, 'original_candidate': copy.deepcopy(candidate),
            'original_candidate_sha256': digest(candidate), 'status': 'question',
            'linked_output_ids': links,
            'link_state': 'linked_requires_semantic_review' if links else 'unlinked_requires_disposition',
            'atoms': [{'operation_id': r['operation_id'], 'receipt': copy.deepcopy(r),
                      'receipt_sha256': digest(r), 'status': 'question',
                      'disposition': 'pending', 'technical_defect_confirmed': False}
                     for r in receipts],
        })
    return {'schema_version': 1, 'input_sha256': digest(text), 'final_sha256': digest(final),
            'groups': groups, 'counts': {'questions': len(groups), 'receipts': len(seen_operations)},
            'production_ready': False,
            'limits': 'Lossless storage only. Links do not verify semantic retention, technical truth, '
                      'object identity, operating mode or formula applicability. Pending is not rejection. '
                      'The original model result is unchanged.'}


def render(annex):
    esc = html.escape
    chunks = ['<!doctype html><html lang="ru"><meta charset="utf-8">',
              '<title>Приложение расчётных вопросов</title>',
              '<style>body{font:16px system-ui;max-width:1100px;margin:30px auto}pre{white-space:pre-wrap}',
              'article{border:1px solid #aaa;padding:16px;margin:16px 0}</style>',
              '<h1>Расчётные вопросы: исходные примеры сохранены</h1>',
              '<p>Экспериментальное приложение. Это вопросы, а не подтверждённые дефекты. '
              'Связь с итоговой строкой не доказывает сохранение её смысла. '
              'Каждый расчёт ожидает решения; исходная выдача модели не изменена.</p>']
    for group in annex['groups']:
        links = ', '.join(group['linked_output_ids']) or 'нет ссылки — требуется разбор'
        chunks.append(f'<h2>{esc(group["candidate_id"])} → {esc(links)}</h2>')
        for atom in group['atoms']:
            r = atom['receipt']
            chunks.append(f'<article><b>{esc(atom["operation_id"])}</b><p>PDF {esc(str(r.get("page", "?")))}; '
                          f'{esc(r["block_id"])}</p><pre>{esc(r["source_quote"])}</pre>'
                          f'<p>{esc(r["finding"])}</p><p>Статус: вопрос. Решение: ожидается.</p></article>')
    return ''.join(chunks) + '</html>'


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--text', type=Path, required=True)
    parser.add_argument('--final', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    result = build(json.loads(args.text.read_text()), json.loads(args.final.read_text()))
    if args.output.exists() or args.output.with_suffix('.html').exists():
        raise ValueError('Existing appendix requires an explicit new revision')
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, ensure_ascii=False, indent=2))
    args.output.with_suffix('.html').write_text(render(result))
    print(result['counts'])
