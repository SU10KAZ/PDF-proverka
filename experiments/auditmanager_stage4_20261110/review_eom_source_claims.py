"""Reproducible source receipts for five known EOM semantic controls, no inference."""
import hashlib
import json
import re
from decimal import Decimal
from pathlib import Path

from backend.app.pipeline.stages.text_analysis.requirement_checks import authoritative_text_blocks


def main():
    root = Path(__file__).parent
    source = Path('/home/coder/auditmanager/corpus-audits/20261106_classic_investigation/classic_control/inputs/EOM/versions/v002/02_work/document.md')
    md = source.read_text()
    queue = json.loads((root/'EOM_source_review_queue.json').read_text())
    digest = hashlib.sha256(md.encode()).hexdigest()
    assert digest == queue['source_md_sha256']
    blocks = authoritative_text_blocks(md)
    packets = {p['finding_id']: p for p in queue['packets']}
    reviews = []

    def evidence(bid, quote):
        assert quote in blocks[bid], (bid, quote)
        return {'block_id': bid, 'quote': quote}

    def record(fid, claims, recommendation, **extra):
        packet = packets[fid]
        reviews.append({'finding_id': fid, 'finding_sha256': packet['finding_sha256'],
                        'original_requests': packet['finding_snapshot']['source_review_requests'],
                        'final_finding': packet['finding_snapshot'], 'claims': claims,
                        'recommendation': recommendation,
                        'engineer_decision': 'Отдельное решение инженера по этой классической записи не установлено.',
                        **extra})

    general = 'blk_d90c772851514a0a9c65cdd019e56957'
    spec = 'blk_67c8cc82d7e24f738797d9b75ce14e97'
    record('F-001', [{'claim': 'Описания одного кабеля в общих указаниях и спецификации различаются.',
                     'status': 'supported', 'evidence': [
                         evidence(general, 'Все сети выполнены кабелями с медными жилами с ПВХ изоляцией и не поддерживающие горение ПВХ оболочкой, с низким дымовыделением типа ППГнг(A)-HF.'),
                         evidence(spec, 'Кабель силовой с медными жилами, с изоляцией и оболочкой из полимерных композиций не распространяющие горение при групповой прокладке и не выделяющие коррозионно-активных газообразных продуктов при горении и тлении, сечением: | ППГнг(A)-HF')],
                     'limit': 'Проверено расхождение описаний. Материал фактически поставленного кабеля, паспорт и соответствие нормам не проверены.'}],
           'Сохранить требование согласовать описание; не утверждать фактическую поставку кабеля с неподходящими свойствами.')

    bid = 'blk_c5f0556f94c14503bfe178ef5941c077'
    rows = []
    for line in blocks[bid].splitlines():
        cells = [s.strip() for s in line.strip('|').split('|')]
        if cells and re.fullmatch(r'C25\d+', cells[0]):
            rows.append({'id': cells[0], 'py': cells[2], 'pp': cells[3], 'quote': line})
    assert {r['id'] for r in rows} == {f'C25{i}' for i in [*range(1, 15), *range(18, 28)]}
    assert len(rows) == 24
    totals = {key: sum(Decimal(r[key].replace(',', '.')) for r in rows) for key in ('py', 'pp')}
    assert totals == {'py': Decimal('94.5'), 'pp': Decimal('89.9')}
    record('F-003', [
        {'claim': 'Суммы опубликованных строк РП5 равны 94,5 и 89,9 кВт.', 'status': 'supported',
         'evidence': [evidence(bid, r['quote']) for r in rows], 'row_inventory': [r['id'] for r in rows],
         'operation': {'py_sum': str(totals['py']), 'pp_sum': str(totals['pp']),
                       'published_totals': [evidence(bid, '| РП5 | Pp | 94,10 | 20,60'), evidence(bid, '|  | Pp.лож |  | 89,50')],
                       'py_difference': '0.40', 'pp_difference': '0.40'}},
        {'claim': 'Причина расхождения — пропуск клапанов, а все нагрузки должны суммироваться в пожарном режиме.',
         'status': 'needs_data', 'evidence': [evidence(bid, '| C2518 | Клапана ОЗК и ДУ | 0,4 | 0,4')],
         'reason': 'Совпадение разницы с мощностью клапанов не устанавливает причину. Нужны алгоритм режимов и одновременность работы.'}],
        'Сохранить проверку расхождения; не заменять условную причину на доказанный пропуск клапанов.')

    other = 'blk_76d1760dd5b34882a0df18538f78e39b'
    refs = 'blk_b6605a23b38f4e00b2f8076b62b21532'
    for fid, actual, term in [('F-044', other, 'правила технической эксплуатации электроустановок потребителей'),
                              ('F-045', other, 'МПБЭЭ'),
                              ('F-046', refs, 'ГОСТ 21.101-2026')]:
        packet = packets[fid]
        assert all(term not in s['authoritative_text'] for s in packet['sources'])
        quotes = [line for line in blocks[actual].splitlines() if term in line]
        if fid == 'F-046':
            quotes += [line for line in blocks[actual].splitlines() if '50571.5.54-2024' in line or '6.13130.2025' in line]
        record(fid, [
            {'claim': 'Исходный T-021 об идентификации проводников подтверждает этот итоговый тезис.',
             'status': 'refuted', 'reason': 'T-021 относится к ГОСТ Р 50462-2009. Итоговый тезис имеет другой предмет; привязанный блок не содержит проверяемого обозначения.',
             'evidence': [evidence(general, 'ГОСТ Р 50462-2009')]},
            {'claim': 'Упоминание документа присутствует в авторитетном MD.', 'status': 'supported',
             'evidence': [evidence(actual, quote) for quote in quotes]},
            {'claim': 'Ссылка недействительна либо нарушение технических требований доказано.',
             'status': 'needs_data', 'reason': 'Упоминание или неполнота обозначения не доказывают недействительность. Внешняя проверка статуса и применимости здесь не выполнена.'}],
            'Перепривязать доказательства; рассматривать как вопрос/уточнение. Не удалять только из-за неправильной родословной и не выдавать как подтверждённое техническое нарушение.',
            failure_stage='merge_source_link_semantic_mismatch', recovered_block_id=actual)

    result = {'source': {'path': str(source), 'sha256': digest},
              'scope': 'Known-case source review by the current assistant; assertions reproduce text and arithmetic, not independent blind discovery.',
              'reviews': reviews, 'reviewed_findings': len(reviews),
              'remaining_queue_findings': [fid for fid in packets if fid not in {r['finding_id'] for r in reviews}],
              'automatic_application': False, 'new_model_calls': 0,
              'runtime_pending_flags_unchanged': True,
              'arithmetic_control_pair': {'retain': 'F-003: source row sums really differ; operating-mode cause remains unresolved.',
                                          'reject': 'F-040: EOM_F040_semantic_refutation.json reproduces 1636.5, not claimed 1622.7.',
                                          'limit': 'Source arithmetic pair only; end-to-end model detection and engineering-mode adjudication remain unverified.'}}
    (root/'EOM_source_claim_review_first5.json').write_text(json.dumps(result, ensure_ascii=False, indent=2)+'\n')
    print('Reviewed:', len(reviews), 'remaining:', len(result['remaining_queue_findings']))


if __name__ == '__main__':
    main()
