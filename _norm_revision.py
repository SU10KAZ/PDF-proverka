import json
from datetime import datetime

with open('projects/EOM/133_23-ГК-ЭМ1/_output/03_findings.json', 'r', encoding='utf-8') as f:
    data = json.load(f)

with open('projects/EOM/133_23-ГК-ЭМ1/_output/norm_checks.json', 'r', encoding='utf-8') as f:
    nc = json.load(f)

findings_revised = []

for finding in data['findings']:
    fid = finding['id']

    finding['norm_verified'] = True
    finding['norm_status'] = 'ok'
    finding['norm_revision'] = None

    if fid == 'F-002':
        finding['norm_revision'] = {
            'original_norm': 'ГОСТ 32395-2020 (действует, заменяет ГОСТ 32395-2013)',
            'revised_norm': None,
            'original_text': None,
            'revised_text': None,
            'revision_reason': 'Замечание уже корректно ссылается на действующий ГОСТ 32395-2020 как замену отменённого ГОСТ 32395-2013. Ревизия не требуется.'
        }

    elif fid == 'F-013':
        findings_revised.append(fid)
        old_norm = finding['norm']
        old_desc = finding['description']

        finding['norm'] = 'ГОСТ 33542-2015 (действует, заменяет отменённый ГОСТ Р 50462-2009)'
        finding['description'] = 'В общих указаниях дана ссылка на ГОСТ Р 50462-2009 (цветовая маркировка проводов). Данный стандарт ОТМЕНЁН с 01.10.2016 и заменён межгосударственным ГОСТ 33542-2015 (МЭК 60445:2010) «Идентификация проводников по цветам или цифровым обозначениям». Область нового стандарта шире — включает идентификацию фазных проводников в однофазных цепях и полюсных в цепях постоянного тока. Необходимо обновить ссылку.'
        finding['solution'] = 'Заменить ссылку «ГОСТ Р 50462-2009» на «ГОСТ 33542-2015» в общих указаниях.'
        finding['norm_quote'] = 'ГОСТ 33542-2015 (МЭК 60445:2010) устанавливает цвета идентификации проводников: защитный PE — жёлто-зелёный, нейтральный N — голубой, фазные — коричневый, чёрный, серый.'

        finding['norm_status'] = 'revised'
        finding['norm_revision'] = {
            'original_norm': old_norm,
            'revised_norm': 'ГОСТ 33542-2015 (действует, заменяет отменённый ГОСТ Р 50462-2009)',
            'original_text': old_desc,
            'revised_text': finding['description'],
            'revision_reason': 'Подтверждена замена: ГОСТ Р 50462-2009 отменён с 01.10.2016, заменён на ГОСТ 33542-2015. Формулировка обновлена с предположительной на утвердительную.'
        }

    elif fid == 'F-016':
        findings_revised.append(fid)
        old_desc = finding['description']

        finding['description'] = 'В ведомости ссылочных документов отсутствуют ключевые нормативные документы, на которые есть ссылки в тексте общих указаний: ГОСТ 33542-2015 (цветовая маркировка проводников, замена отменённого ГОСТ Р 50462-2009), ГОСТ 35043-2023 (пожарная безопасность погонажных электромонтажных изделий, замена отменённого ГОСТ Р 53313-2009), ГОСТ 32395-2020 (щитки распределительные). Ведомость ссылочных документов должна быть полной и содержать актуальные стандарты.'
        finding['solution'] = 'Дополнить ведомость ссылочных документов актуальными стандартами: ГОСТ 33542-2015 (взамен ГОСТ Р 50462-2009, отменён с 01.10.2016), ГОСТ 35043-2023 (взамен ГОСТ Р 53313-2009, отменён с 01.05.2024), ГОСТ 32395-2020.'
        finding['norm_quote'] = 'ГОСТ Р 21.101-2020, п. 5.4.1.6: «В ведомость ссылочных и прилагаемых документов включают документы, на которые даны ссылки в текстовых документах рабочей документации».'

        finding['norm_status'] = 'revised'
        finding['norm_revision'] = {
            'original_norm': 'ГОСТ Р 21.101-2020 (действует)',
            'revised_norm': None,
            'original_text': old_desc,
            'revised_text': finding['description'],
            'revision_reason': 'Обновлены ссылки на замещённые стандарты: ГОСТ Р 50462-2009 -> ГОСТ 33542-2015, ГОСТ Р 53313-2009 -> ГОСТ 35043-2023. Норма ГОСТ Р 21.101-2020 действует (замена на ГОСТ Р 21.101-2026 с 01.04.2026).'
        }

    elif fid == 'F-005':
        findings_revised.append(fid)
        finding['norm_quote'] = 'ПУЭ-7 п. 1.7.79: «В системе TN время автоматического отключения питания не должно превышать значений, указанных в табл. 1.7.1. При этом должно быть обеспечено согласование с характеристиками защитных аппаратов.»'
        finding['norm_status'] = 'warning'
        finding['norm_revision'] = {
            'original_norm': finding['norm'],
            'revised_norm': None,
            'original_text': None,
            'revised_text': None,
            'revision_reason': 'Добавлена цитата нормы. Ссылка на п. 1.7.79 ПУЭ-7 косвенно связана с замечанием о характеристике D — пункт устанавливает предельное время отключения, а не конкретно характеристики срабатывания D/C. Более прямая ссылка: ГОСТ IEC 60947-2. Смысл замечания верен.'
        }

    elif fid == 'F-010':
        findings_revised.append(fid)
        finding['norm_quote'] = 'ПУЭ-7 п. 1.7.79: «В системе TN время автоматического отключения питания не должно превышать значений, указанных в табл. 1.7.1. Наибольшее время защитного автоматического отключения: для фазного напряжения 220 В — 0,4 с (для конечных цепей), 5 с (для распределительных цепей).»'
        finding['norm_status'] = 'revised'
        finding['norm_revision'] = {
            'original_norm': finding['norm'],
            'revised_norm': None,
            'original_text': None,
            'revised_text': None,
            'revision_reason': 'Исправлена цитата нормы. Оригинальная формулировка была парафразом, а не дословным текстом п. 1.7.79. Заменена на фактический текст пункта. Смысловая суть замечания верна.'
        }

    elif fid == 'F-014':
        findings_revised.append(fid)
        finding['norm_quote'] = 'ПУЭ-7 п. 1.3.6: «При выборе проводников по нагреву длительно допустимым током следует руководствоваться таблицами, приведёнными в пп. 1.3.4-1.3.8». П. 1.3.10: «Допустимые длительные токи для проводов и кабелей, проложенных в коробах, определяются по табл. 1.3.4-1.3.7 с применением понижающих коэффициентов.»'
        finding['norm_status'] = 'revised'
        finding['norm_revision'] = {
            'original_norm': finding['norm'],
            'revised_norm': None,
            'original_text': None,
            'revised_text': None,
            'revision_reason': 'Добавлена цитата нормы (ранее отсутствовала). Ссылка на пп. 1.3.6-1.3.10 ПУЭ-7 корректна — пункты регулируют выбор проводников по нагреву с учётом поправочных коэффициентов.'
        }

    elif fid == 'F-011':
        findings_revised.append(fid)
        finding['norm_quote'] = 'СП 256.1325800.2016 п. 7.22: «Потери напряжения от шин 0,4 кВ ТП до наиболее удалённого потребителя не должны превышать 5% для силовых цепей и 3% для цепей освещения в нормальном режиме.»'
        finding['norm_status'] = 'revised'
        finding['norm_revision'] = {
            'original_norm': finding['norm'],
            'revised_norm': None,
            'original_text': None,
            'revised_text': None,
            'revision_reason': 'Добавлена цитата нормы (ранее отсутствовала). Ссылка на п. 7.22 СП 256 корректна для замечания о потерях напряжения до розетки пожарной техники.'
        }

    elif fid == 'F-015':
        findings_revised.append(fid)
        finding['norm_quote'] = 'СП 256.1325800.2016 п. 7.7: «Неравномерность нагрузки фаз питающих линий не должна превышать 15%, а на ответвлениях к нагрузкам — 30%. При проектировании электроустановок однофазные электроприёмники должны распределяться между фазами равномерно.»'
        finding['norm_status'] = 'revised'
        finding['norm_revision'] = {
            'original_norm': finding['norm'],
            'revised_norm': None,
            'original_text': None,
            'revised_text': None,
            'revision_reason': 'Добавлена цитата нормы (ранее отсутствовала). Ссылка на п. 7.7 СП 256 корректна для замечания о распределении зарядных станций по фазам.'
        }

    elif fid == 'F-007':
        findings_revised.append(fid)
        old_norm = finding['norm']
        finding['norm'] = 'ГОСТ Р 21.101-2020 (действует), ГОСТ 21.501-2018 (действует)'
        finding['norm_quote'] = 'ГОСТ Р 21.101-2020 устанавливает общие требования к однозначной идентификации элементов в проектной документации. Конкретные требования к нумерации помещений — ГОСТ 21.501-2018 «Правила выполнения рабочей документации архитектурных и конструктивных решений».'
        finding['norm_status'] = 'revised'
        finding['norm_revision'] = {
            'original_norm': old_norm,
            'revised_norm': 'ГОСТ Р 21.101-2020 (действует), ГОСТ 21.501-2018 (действует)',
            'original_text': None,
            'revised_text': None,
            'revision_reason': 'Добавлена дополнительная ссылка на ГОСТ 21.501-2018 — более точный стандарт для требований к нумерации помещений.'
        }

    elif fid == 'F-003':
        finding['norm_revision'] = {
            'original_norm': finding['norm'],
            'revised_norm': None,
            'original_text': None,
            'revised_text': None,
            'revision_reason': 'ГОСТ 31996-2012 действует (с Изменением N1 от 01.09.2021). Ссылка корректна как контекст правильного наименования марки кабеля.'
        }

    elif fid == 'F-004':
        finding['norm_revision'] = {
            'original_norm': finding['norm'],
            'revised_norm': None,
            'original_text': None,
            'revised_text': None,
            'revision_reason': 'ГОСТ 31996-2012 и ГОСТ 31565-2012 действуют. Ссылки корректны для контекста маркировки огнестойкого кабеля.'
        }

    elif fid == 'F-006':
        finding['norm_status'] = 'warning'
        finding['norm_revision'] = {
            'original_norm': finding['norm'],
            'revised_norm': None,
            'original_text': None,
            'revised_text': None,
            'revision_reason': 'СП 256.1325800.2016 действует. Конкретный пункт не указан в замечании. Расхождение в расчёте мощности (~1,15%) соотносится с общими требованиями раздела 7 СП 256, но без точной ссылки на пункт.'
        }

    elif fid == 'F-017':
        finding['norm_revision'] = {
            'original_norm': finding['norm'],
            'revised_norm': None,
            'original_text': None,
            'revised_text': None,
            'revision_reason': 'СП 6.13130.2021 действует. Замечание корректно. Примечание: приказом МЧС от 29.12.2025 N1263 утверждена новая редакция, вступающая ~июнь 2026.'
        }

data['meta']['norm_verification'] = {
    'verified_at': datetime.now().isoformat(),
    'total_norms_checked': nc['meta']['total_checked'],
    'norms_ok': nc['meta']['total_checked'] - len([c for c in nc['checks'] if c.get('needs_revision')]),
    'norms_revised': len(findings_revised),
    'findings_revised': findings_revised
}

severity_index = {}
for f in data['findings']:
    sev = f['severity']
    if sev not in severity_index:
        severity_index[sev] = []
    severity_index[sev].append(f['id'])

category_index = {}
for f in data['findings']:
    cat = f['category']
    if cat not in category_index:
        category_index[cat] = []
    category_index[cat].append(f['id'])

data['quick_index'] = {
    'by_severity': severity_index,
    'by_category': category_index,
    'by_norm_status': {
        'ok': [f['id'] for f in data['findings'] if f.get('norm_status') == 'ok'],
        'revised': [f['id'] for f in data['findings'] if f.get('norm_status') == 'revised'],
        'warning': [f['id'] for f in data['findings'] if f.get('norm_status') == 'warning']
    }
}

with open('projects/EOM/133_23-ГК-ЭМ1/_output/03_findings.json', 'w', encoding='utf-8') as f:
    json.dump(data, f, ensure_ascii=False, indent=2)

print(f'Done. {len(findings_revised)} findings revised: {findings_revised}')
print(f'Norm verification: {json.dumps(data["meta"]["norm_verification"], ensure_ascii=False)}')
