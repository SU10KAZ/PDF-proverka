"""Evaluator-only replay of a sealed baseline arithmetic claim; no model calls."""
import hashlib
import json
from decimal import Decimal
from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[2]
HERE = Path(__file__).resolve().parent
BASE = Path('/tmp/auditmanager_comparative_trial_context_v3_20261111/ak_r2_classic/classic_control')
RUN = BASE / 'inputs/AK/versions/v001/03_analysis/runs/comparison'
BLOCK = 'blk_6ba4df04a74d4e8d8db22cb4db7aff4f'


def binding(path):
    return {'path': str(path), 'sha256': hashlib.sha256(path.read_bytes()).hexdigest()}


def main():
    md_path = BASE / 'inputs/AK/versions/v001/02_work/document.md'
    text = md_path.read_text()
    start = re.search(r'^### BLOCK[^\n]*' + BLOCK + r'[^\n]*', text, re.M)
    assert start
    end = re.search(r'^### BLOCK', text[start.end():], re.M)
    block = text[start.start():start.end() + end.start()] if end else text[start.start():]
    decision_path = ROOT / 'experiments/auditmanager_stage1_20261109/decisions/AK_F025_area_arithmetic.json'
    decision = json.loads(decision_path.read_text())
    assert block.strip() == decision['evidence'][0]['authoritative_TEXT'].strip()
    request_path = BASE / 'requests/AK/1794392797287819608/request.json'
    request = json.loads(request_path.read_text())
    messages = '\n'.join(m['content'] for m in request['messages'])
    table_lines = [line for line in block.splitlines() if line.startswith('|')]
    assert all(line in messages for line in table_lines)
    receipts_path = request_path.parent / 'attempts.json'
    receipts = json.loads(receipts_path.read_text())
    assert receipts and all(not row['is_error'] for row in receipts)
    # Respect table order and explicit subtotal boundaries, not room-name prefixes.
    # The repeated header is not a new group; room5.К.18 belongs to technical rooms.
    groups, pending = [], []
    grand = None
    for line in table_lines:
        cells = [cell.strip() for cell in line.strip('|').split('|')]
        if len(cells) != 5 or not re.fullmatch(r'\d+,\d+', cells[3]):
            continue
        label, area = cells[0], Decimal(cells[3].replace(',', '.'))
        if label.startswith('**'):
            if label == '**Общий итог**':
                grand = area
                assert not pending
            else:
                assert pending
                groups.append({'label': label, 'operands': pending,
                               'count': len(pending),
                               'calculated_m2': str(sum((Decimal(row['area_m2']) for row in pending), Decimal(0))),
                               'published_m2': str(area)})
                pending = []
        else:
            pending.append({'room': label, 'area_m2': str(area)})
    assert not pending and grand is not None
    assert len(groups) == 9 and sum(g['count'] for g in groups) == 191
    assert all(Decimal(g['calculated_m2']) == Decimal(g['published_m2']) for g in groups)
    total = sum((Decimal(g['calculated_m2']) for g in groups), Decimal(0))
    assert total == grand
    chain = []
    for name, key, fid in [('02_text_analysis.json', 'text_findings', 'T-032'),
                           ('03_findings_codex_base.json', 'findings', 'F-032'),
                           ('03_findings_pre_merge.json', 'findings', 'F-032'),
                           ('03_findings.json', 'findings', 'F-034')]:
        path = RUN / name
        row = next(r for r in json.loads(path.read_text())[key] if r['id'] == fid)
        claim = row.get('finding', row.get('description'))
        assert '453,2' in claim and '19058,9' in claim
        chain.append({'source': binding(path), 'id': fid, 'claim': claim,
                      'source_finding_ids': row.get('source_finding_ids', []),
                      'related_block_ids': row.get('related_block_ids', [])})
    seal_path = BASE / 'status/AK.json'
    assert json.loads(seal_path.read_text())['final_sha256'] == binding(RUN / '03_findings.json')['sha256']
    result = {
        'status': 'sealed_baseline_primary_false_arithmetic_traced',
        'scope': 'AK repeat2, T-032 to final F-034; evaluator-only, frozen runtime unchanged',
        'sources': {'document': binding(md_path), 'decision': binding(decision_path),
                    'request': binding(request_path), 'receipts': binding(receipts_path), 'seal': binding(seal_path)},
        'source_check': {'block_id': BLOCK, 'same_authoritative_block_as_benchmark': True,
                         'table_line_count': len(table_lines), 'every_table_line_present_in_request': True},
        'request_model': request['options']['model'],
        'calculations': {'method': 'Decimal sum of individual rows through explicit subtotal boundaries',
                         'groups': groups, 'room_rows': 191, 'calculated_grand_m2': str(total), 'published_grand_m2': str(grand)},
        'chain': chain,
        'conclusion': 'Неверный расчёт уже сформулирован первичным текстовым анализом при наличии всей таблицы в запросе. Объединение сохраняет ошибку; итоговая обработка её не снимает. Арифметика исходных строк совпадает с опубликованными итогами.',
        'limits': ['Наличие таблицы в запросе не доказывает фактическое внимание модели к каждой строке.',
                   'Внутренний способ получения моделью453,2/19058,9 неизвестен; не утверждается конкретный пропущенный операнд.',
                   'Рекомендация проверить округление не опровергает ошибочный пересчёт и создаёт лишнюю задачу инженеру.',
                   'Это проверка арифметики TEXT, не геометрии помещений и не исходных неокруглённых площадей.',
                   'Результат не доказывает эффективность будущего фильтра; полезные арифметические находки должны сохраняться.'],
        'acceptance_implication': 'Проверять исполняемый расчёт по полному списку операндов и границам групп; не вводить универсальный допуск0,1м² и не снимать все малые расхождения.'}
    output = HERE / 'AK_r2_area_arithmetic_primary_trace.json'
    output.write_text(json.dumps(result, ensure_ascii=False, indent=2) + '\n')
    print(json.dumps({'groups': len(groups), 'rows': 191, 'total': str(total), 'output': str(output)}, ensure_ascii=False))


if __name__ == '__main__':
    main()
