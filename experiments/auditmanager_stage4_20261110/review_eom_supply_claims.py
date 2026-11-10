"""Known EOM specification controls: row facts do not establish supply excess."""
import hashlib
import json
from decimal import Decimal
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
    records = []

    def rows(bid, term):
        return [{'block_id': bid, 'line_in_block': n, 'quote': line,
                 'cells': [s.strip() for s in line.strip('|').split('|')]}
                for n, line in enumerate(blocks[bid].splitlines(), 1)
                if line.startswith('|') and term in line]

    def qty(row):
        return Decimal(row['cells'][6].replace(',', '.'))

    def record(fid, fact, evidence, operation, unresolved, recommendation):
        packet = packets[fid]
        records.append({'finding_id': fid, 'finding_sha256': packet['finding_sha256'],
                        'final_finding': packet['finding_snapshot'],
                        'claims': [{'claim': fact, 'status': 'supported', 'evidence': evidence, 'operation': operation},
                                   {'claim': unresolved, 'status': 'needs_data'}],
                        'recommendation': recommendation,
                        'engineer_decision': 'Отдельное решение инженера по классической записи не установлено.',
                        'recommended_section': 'questions_and_clarifications',
                        'automatic_application': False})

    tube = 'blk_1da24da5ad594599aa59defe9fde265b'
    repeated = rows(tube, 'Труба ПЛЛ гибкая гофр. не содержит галогенов Ø32мм')
    assert len(repeated) == 2 and repeated[0]['cells'] == repeated[1]['cells']
    assert [qty(r) for r in repeated] == [Decimal(190)] * 2
    record('F-034', 'Две одинаковые строки трубы Ø32мм по 190 м дают 380 м.', repeated,
           {'sum_m': str(sum(qty(r) for r in repeated))},
           'Доказан ли двойной учёт одного участка, либо строки относятся к разным участкам?',
           'Сохранить вопрос о назначении строк; не удалять 190 м автоматически без ведомости участков.')

    hardware = 'blk_0779457166304b91a87d7b7902f8359f'
    connectors, screws = rows(hardware, 'LG1000'), rows(hardware, 'СМ010620')
    assert len(connectors) == len(screws) == 2
    assert [qty(r) for r in connectors] == [Decimal(96)] * 2
    assert [qty(r) for r in screws] == [Decimal(768)] * 2
    assert max(r['line_in_block'] for r in connectors + screws) < rows(hardware, 'В пом. ВРУ-К')[0]['line_in_block']
    record('F-035', 'В одном фрагменте до следующего заголовка дважды повторены соединители и винты.',
           connectors + screws, {'connectors_total': '192', 'screws_total': '1536'},
           'Относятся ли повторы к самостоятельным участкам или к одному и тому же объёму?',
           'Сохранить условную проверку повторов; одинаковые артикулы и количества сами по себе не доказывают лишнюю поставку.')

    cover_rows = []
    differences = []
    for bid, expected in [('blk_79370c7a344c44559b2d828291b6a1b6', [(130, 126), (57, 55)]),
                          ('blk_f3f718605a024394b9026b08fa4c9b20', [(41, 40)])]:
        trays, covers = rows(bid, '| 3530410 |'), rows(bid, '| 3552410 |')
        assert [(qty(t), qty(c)) for t, c in zip(trays, covers)] == expected
        assert len(trays) == len(covers) == len(expected)
        headings = rows(bid, 'Техпространство на отм.')
        assert len(headings) == len(expected)
        for i, (tray, cover) in enumerate(zip(trays, covers)):
            assert headings[i]['line_in_block'] < tray['line_in_block'] < cover['line_in_block']
            if i+1 < len(headings):
                assert cover['line_in_block'] < headings[i+1]['line_in_block']
            differences.append(qty(tray)-qty(cover))
        cover_rows += headings + trays + covers
    assert sum(differences) == 7
    record('F-037', 'В трёх указанных технических пространствах разность метража лотков и крышек равна 4+2+1=7 м.',
           cover_rows, {'differences_m': [str(v) for v in differences], 'total_m': '7'},
           'Требуются ли крышки по всей длине этих лотков и не предусмотрены ли открытые участки?',
           'Сохранить вопрос о покрытии участков; не назначать дополнительную поставку 7 м без требования и трассировки.')

    clips = rows(tube, 'Клипса с замком')
    assert len(clips) == 2 and [r['cells'][5] for r in clips] == ['м', 'м']
    assert [qty(r) for r in clips] == [Decimal(2345), Decimal(190)]
    record('F-038', 'В спецификации обе позиции клипс выражены в метрах: 2345 и 190.', clips,
           {'unit': 'м', 'quantities': ['2345', '190']},
           'Какова принятая база учёта креплений и требуемое количество штук с учётом шага?',
           'Запросить единицу поставки и расчёт креплений. Не заменять метры на штуки с сохранением чисел.')

    frames = 'blk_99cc8bf95861437186be76b36e9d446b'
    sockets, single, double = (rows(frames, term) for term in ['| ATN001043 |', '| ATN001001 |', '| ATN440102 |'])
    assert len(sockets) == len(single) == len(double) == 1
    assert [qty(r[0]) for r in (sockets, single, double)] == [56, 51, 5]
    seats = qty(single[0]) + 2 * qty(double[0])
    record('F-039', 'Для указанных 56 розеток перечислены рамки на 61 посадочное место.',
           sockets + single + double, {'seats': str(seats), 'difference': str(seats-qty(sockets[0])),
                                       'conditional_single_frames_if_only_these_sockets_and_no_spares': '46'},
           'Предусмотрены ли другие механизмы, запас или иная комплектация по АИ и MAF?',
           'Сохранить вопрос о комплектации с учётом прямой ссылки в строках на АИ/MAF; не объявлять пять рамок лишними.')

    result = {'source': {'path': str(source), 'sha256': queue['source_md_sha256']},
              'scope': 'Known-case assistant review with reproducible source-row checks, not independent discovery.',
              'reviews': records, 'reviewed_findings': len(records),
              'automatic_application': False, 'new_model_calls': 0,
              'filter_control_pair_status': 'No confirmed same-scope technical excess/shortage pair established; do not deploy blanket suppression of repeated rows or count mismatches.',
              'runtime_pending_flags_unchanged': True}
    (root/'EOM_supply_claim_reviews.json').write_text(json.dumps(result, ensure_ascii=False, indent=2)+'\n')
    print('Reviewed supply cases:', len(records), '; proven supply excess/shortage: not established')


if __name__ == '__main__':
    main()
