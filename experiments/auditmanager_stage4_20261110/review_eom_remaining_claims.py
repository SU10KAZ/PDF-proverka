"""Source review of remaining EOM packets; no catalogue/normative assertions."""
import hashlib
import json
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

    def evidence(bid, term):
        found = [{'block_id': bid, 'quote': s} for s in blocks[bid].splitlines() if term in s]
        assert found, (bid, term)
        return found

    def record(fid, supported, receipts, unresolved, action, **extra):
        packet = packets[fid]
        reviews.append({'finding_id': fid, 'finding_sha256': packet['finding_sha256'],
                        'final_finding': packet['finding_snapshot'],
                        'claims': [{'claim': supported, 'status': 'supported', 'evidence': receipts},
                                   {'claim': unresolved, 'status': 'needs_data'}],
                        'recommendation': action,
                        'engineer_decision': 'Отдельное решение инженера по классической записи не установлено.', **extra})

    general = 'blk_d90c772851514a0a9c65cdd019e56957'
    cabinets = 'blk_7170586af51c43f79c96734a40269d7d'
    cabinet_rows = evidence(cabinets, 'Щит квартирный учета')
    quantities = [int(x['quote'].strip('|').split('|')[6].strip()) for x in cabinet_rows]
    assert quantities == [63, 27]
    record('F-033', 'В спецификации 63+27=90 квартирных щитов, общие указания относят внутриквартирный щит к собственнику, примечания предусматривают временный щит.',
           cabinet_rows + evidence(general, 'Внутриквартирный щит') +
           sum((evidence(b, 'На период отделки вместо квартирного щита') for b in [
               'blk_16237a8ebd10423194e4a2b0f4b82c26', 'blk_73536f4c222b401e8da714ed88d23fce', 'blk_17ebeac60c9b497cb8c124a8e3b66068']), []),
           'Отождествлены ли позиции 3ЩКВ/2ЩКВ с временными или постоянными щитами; какова граница поставки?',
           'Сохранить вопрос о назначении позиций; не объявлять 90 щитов лишними. Полное соответствие числа щитов числу квартир здесь не проверено.')

    tray_evidence = sum((evidence(s['block_id'], '| 3530410 |') for s in packets['F-036']['sources']), [])
    assert any('Лоток перфорированный' in e['quote'] for e in tray_evidence)
    assert any('Лоток неперфорированный' in e['quote'] for e in tray_evidence)
    record('F-036', 'Один артикул 3530410 указан в строках с двумя различными описаниями перфорации.', tray_evidence,
           'Какое исполнение требуется на каждом участке и каковы корректные артикулы по каталогу?',
           'Сохранить внутреннее противоречие спецификации; не подставлять исправленный артикул без каталога и проектного назначения.',
           external_catalogue_checked=False)

    record('F-041', 'В общих указаниях действительно приведена ссылка ГОСТ Р 50462-2009.',
           evidence(general, 'ГОСТ Р 50462-2009'),
           'Официальный статус, дата замены и применимость к этому проекту отдельно не проверены.',
           'Не подтверждать устаревание только по пересказу модели. До официальной проверки оставить вопрос о ссылке.',
           external_normative_check_status='not_checked',
           supplied_norm_list_verification_status='not_checked_in_this_review')

    record('F-042', 'В общих указаниях запрещена совместная прокладка СПЗ, а примечания и легенда используют объединённую формулировку.',
           evidence(general, 'Не допускается совместная прокладка') +
           evidence('blk_b3482bfd0cc04cb8b9f47c726e9147f3', '8. Кабельные линии') +
           evidence('blk_397b5080da9f400ea3bd03c1c1bfea5e', 'Кабель проложенный в лотке СПЗ') +
           evidence('blk_628818db493e46e7bdccc75b01afbd16', '8. Кабельные линии'),
           'Проходят ли на одном физическом лотке СПЗ и прочие сети; как распределены потребители и трассы?',
           'Сохранить уточнение легенды и указаний. Фактическую совместную прокладку по тексту не объявлять установленной.',
           graphic_route_review_status='not_checked_in_this_review')

    local = 'blk_c995ba0cdd7f4f6fa9cffb30c05b8c27'
    specification = ['blk_82bd0b6201d04037beca3195d87933ce', 'blk_8a5db6616dac4b8bbe73c13b7c54a140']
    assert all('МБОР' not in blocks[b] for b in specification)
    occurrences = [{'block_id': bid, 'quote': line} for bid, body in blocks.items()
                   for line in body.splitlines() if 'мбор' in line.lower()]
    record('F-043', 'Локальная спецификация требует МБОР 10 мм для подвесов; в двух приведённых блоках общей спецификации отдельной строки с этим названием нет.',
           evidence(local, 'МБОР') + evidence(specification[1], 'ФК-10') + evidence(specification[1], 'ТЕХНОВЕНТ') + evidence(specification[1], 'Герметик'),
           'Не учтён ли материал в комплекте или под другим обозначением; каков подтверждённый состав системы и объём защиты подвесов?',
           'Сохранить вопрос о комплектности. Отсутствие отдельного названия не доказывает отсутствие защиты или непригодность системы.',
           literal_md_search={'term': 'мбор (case insensitive)', 'scope': 'all unique authoritative TEXT/TABLE blocks',
                              'matches': occurrences, 'limit': 'Literal search cannot rule out synonyms, kit inclusion or linked documents.'},
           system_identity_and_certified_execution='not_verified_in_this_review')

    result = {'source': {'path': str(source), 'sha256': queue['source_md_sha256']},
              'scope': 'Known-case assistant review of textual facts; unresolved catalogue, normative and graphic questions remain explicit.',
              'reviews': reviews, 'reviewed_findings': len(reviews),
              'automatic_application': False, 'runtime_pending_flags_unchanged': True, 'new_model_calls': 0}
    (root/'EOM_remaining_claim_reviews.json').write_text(json.dumps(result, ensure_ascii=False, indent=2)+'\n')
    print('Remaining packet reviews recorded:', len(reviews))


if __name__ == '__main__':
    main()
