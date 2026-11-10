"""Reproduce known EOM calculations without inferring equipment compliance."""
import hashlib
import json
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path

from backend.app.pipeline.stages.text_analysis.requirement_checks import authoritative_text_blocks


def main():
    root = Path(__file__).parent
    source = Path('/home/coder/auditmanager/corpus-audits/20261106_classic_investigation/classic_control/inputs/EOM/versions/v002/02_work/document.md')
    md = source.read_text()
    queue = json.loads((root/'EOM_source_review_queue.json').read_text())
    assert hashlib.sha256(md.encode()).hexdigest() == queue['source_md_sha256']
    blocks = authoritative_text_blocks(md)
    packets = {p['finding_id']: p for p in queue['packets']}
    reviews = []

    def lines(bid, term):
        selected = [line for line in blocks[bid].splitlines() if term in line]
        assert selected, (bid, term)
        return [{'block_id': bid, 'quote': line} for line in selected]

    def record(fid, claims, recommendation):
        packet = packets[fid]
        reviews.append({'finding_id': fid, 'finding_sha256': packet['finding_sha256'],
                        'final_finding': packet['finding_snapshot'], 'claims': claims,
                        'recommendation': recommendation,
                        'engineer_decision': 'Отдельное решение инженера по этой классической записи не установлено.'})

    tt2 = 'blk_7d22e811017e4e969b2d732e9e30c50d'
    tt3 = 'blk_7ca68a936c85428dbd4b60b2893398a2'
    operations = []
    for bid, sections in [(tt2, [('23.9', '70'), ('30.6', '80')]),
                           (tt3, [('39.4', '100'), ('28.6', '80')])]:
        for numerator, denominator in sections:
            assert numerator in blocks[bid]
            value = Decimal(numerator)/Decimal(denominator)
            operations.append({'block_id': bid, 'operation': f'{numerator}/{denominator}',
                               'secondary_current_A': str(value),
                               'rounded_0_001_A': str(value.quantize(Decimal('.001'), rounding=ROUND_HALF_UP)),
                               'above_finding_stated_0_25_A': value > Decimal('.25'),
                               'above_source_stated_0_1_A': value > Decimal('.1')})
    assert [v['rounded_0_001_A'] for v in operations] == ['0.341', '0.383', '0.394', '0.358']
    record('F-030', [
        {'claim': 'Строка минимального тока использует 77 и 74 А вместо рассчитанных выше токов при 15% нагрузке.',
         'status': 'supported', 'evidence': sum((lines(b, '| 2 |') + lines(b, '| 4 |') for b in (tt2, tt3)), []),
         'operations': operations},
        {'claim': 'Порог 0,25 А (5% от 5 А) является применимым критерием для этого исполнения.', 'status': 'needs_data',
         'reason': 'Численное неравенство с 0,25 А верно. Но в строке проверки и пояснении проекта указан порог 0,1 А; происхождение и применимость 0,25 А в замечании не обоснованы. Это не опровергает найденную ошибку подстановки.',
         'evidence': lines(tt2, 'Iмин -') + lines(tt3, 'Iмин –')},
        {'claim': 'Выбранные ТТ и счётчики соответствуют всем применимым требованиям.', 'status': 'needs_data',
         'reason': 'Арифметика не заменяет паспорт, класс точности, нагрузку вторичной цепи и проверку применимости норм.'}],
        'Сохранить ошибку подстановки; отделить её от неподтверждённого порога 0,25 А и общего вывода о пригодности ТТ.')

    max_checks = []
    for calc, summary, a, b in [(tt2, 'blk_c5f0556f94c14503bfe178ef5941c077', '384.1', '390.3'),
                                 (tt3, 'blk_5b30188592764230a7cd894838a60430', '465.1', '471.4')]:
        max_checks.append({'evidence': lines(calc, a.replace('.', ',')) + lines(summary, b.replace('.', ',')),
                           'difference_A': str(Decimal(b)-Decimal(a))})
    record('F-031', [
        {'claim': 'Максимальные токи, принятые в проверках ТТ, отличаются от сводных значений аварийно-пожарного режима с компенсацией.',
         'status': 'supported', 'comparisons': max_checks},
        {'claim': 'В расчётах явно принято допущение 20% перегрузки.', 'status': 'supported',
         'evidence': lines(tt2, '20%') + lines(tt3, '20%')},
        {'claim': 'Допустимость перегрузки конкретного исполнения ТТ опровергнута либо отсутствие паспортов во всём комплекте доказано.',
         'status': 'needs_data', 'reason': 'Проверенные таблицы не содержат паспортного подтверждения; его отсутствие во всём проекте не установлено. Нужны точное исполнение, паспорт и сопоставимый режим.'}],
        'Сохранить согласование исходных токов. Паспортную допустимость проверять отдельно; не назначать замену ТТ только по этим таблицам.')

    calc = 'blk_fb5f780dae7a475ca6c249f62b64771e'
    explications = ['blk_f2b273baee994702906a6e5d94a43063', 'blk_29be4ff018f040daaa2000c0a3ebde08']
    area_checks = []
    for room, expected in [('7Н', Decimal('108.1')), ('8Н', Decimal('63.0'))]:
        for bid in explications:
            evidence = lines(bid, f'| {room}.')
            assert len(evidence) == 3
            values = [Decimal(e['quote'].strip('|').split('|')[2].strip().replace(',', '.')) for e in evidence]
            assert sum(values) == expected
            area_checks.append({'room': room, 'evidence': evidence, 'area_sum_m2': str(sum(values))})
    assert Decimal('108.1') * Decimal('.22') == Decimal('23.782')
    record('F-032', [
        {'claim': 'Для одинаковых обозначений 7Н и 8Н площади в расчёте отличаются от обеих экспликаций.',
         'status': 'supported', 'calculation_rows': lines(calc, '| 7Н |') + lines(calc, '| 8Н |'),
         'explication_checks': area_checks},
        {'claim': 'При сохранении коэффициента 0,22 произведение площади 7Н по экспликации равно 23,782 кВт.',
         'status': 'supported', 'operation': '108.1 * 0.22 = 23.782', 'evidence': lines(calc, '| 7Н |')},
        {'claim': 'Следует автоматически заменить расчётную нагрузку и считать минимум 20 кВт универсальным правилом.',
         'status': 'needs_data', 'reason': 'Нужно согласовать актуальную площадь, метод назначения мощности и проектный минимум. Частное значение 20 кВт не является универсальным допуском.'}],
        'Сохранить междокументное расхождение площадей; согласовать базу расчёта до изменения нагрузки.')

    result = {'source': {'path': str(source), 'sha256': queue['source_md_sha256']},
              'scope': 'Known-case assistant source review; no external normative or equipment-compliance adjudication.',
              'reviews': reviews, 'reviewed_findings': len(reviews), 'automatic_application': False,
              'runtime_pending_flags_unchanged': True, 'new_model_calls': 0}
    (root/'EOM_calculation_claim_reviews.json').write_text(json.dumps(result, ensure_ascii=False, indent=2)+'\n')
    print('Reviewed calculation cases:', len(reviews))


if __name__ == '__main__':
    main()
