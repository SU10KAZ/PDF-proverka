"""Local evaluator-only consolidation; never calls a model or changes audit inputs."""
import hashlib
import html
import json
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
WORK = Path('/home/coder/auditmanager/corpus-audits/20261106_classic_investigation')
EV = WORK / 'evaluation'
OUT = Path(__file__).resolve().parent

def read(p):
    return json.loads(Path(p).read_text())

def digest(p):
    return hashlib.sha256(Path(p).read_bytes()).hexdigest()

def receipt(p):
    return {'path': str(Path(p).resolve()), 'sha256': digest(p)}

def write(name, data):
    (OUT / name).write_text(json.dumps(data, ensure_ascii=False, indent=2) + '\n')

history_path = ROOT / 'experiments/auditmanager_v4_transfer_20261105/control_cases.json'
history = {x['control_id']: x for x in read(history_path)}
contract = read(EV / 'scoring_contract_v1/contract.json')
readiness = read(EV / 'target_readiness.json')['controls']
atoms = json.loads(json.dumps(contract['atoms']))
# Explicit revisions following addressed primary reviews. Original contract retained by hash.
revisions = {
 'KM:OGM4_length': ('confirmed_document_dimension_difference', '400+2730+110=3240 мм против 3640 мм в позиции 1 ОГМ4 на PDF8; сохранить документное расхождение. Длину заготовки и припуски не назначать.', 'KM5_source_check.json'),
}
for a in atoms:
    if a['atom_id'] in revisions:
        status, wording, filename = revisions[a['atom_id']]
        a['previous_contract_assertion'] = json.loads(json.dumps(a))
        a.update(technical_status=status, required_semantics=wording,
                 evidence=[receipt(EV / filename)])

def add(cid, suffix, status, wording, boundary, filename, utility='requires_engineer_review'):
    atoms.append({'atom_id': cid + ':' + suffix, 'historical_control_ids': [cid],
                  'technical_status': status, 'required_semantics': wording,
                  'unsupported_or_refuted_extension': boundary,
                  'engineering_utility': utility, 'evidence': [receipt(EV / filename)],
                  'result': None, 'scoring': 'Ручная оценка полного тезиса и доказательств; совпадение ID/чисел не является зачётом.'})

add('AK:100','motor_segments','confirmed_bounded_document_omission',
    'Два сегмента к M2/M3 4×2,5 длиной 15 м без индивидуальных обозначений; среди 30 строк КЖ соответствующего шкафа нет адресных строк этих моторных соединений.',
    'Не считать физическим дефицитом закупки; строки 10 м между шкафами не подменяют моторные сегменты 15 м.', 'AK100_journal_primary_check.json', 'historical_parent_accepted_atom_usefulness_not_separately_adjudicated')
add('AK:137','local20m','confirmed_document_difference',
    'Локальная ведомость содержит 20 м 3×2,5, в общей кабельной СО такой позиции нет; в КЖ П4.1.1-4 указано 20 м с буквальным суффиксом НФ.',
    'НФ не исправлять на HF. Граница поставки не установлена; ещё 10 м не прибавлять как доказанный дефицит.', 'AK137_primary_atom_split.json')
add('AK:137','additional10m','unresolved_object_identity',
    'Разрешить связь 10 м 3×2,5 локальной ведомости с По5.1-9, где КЖ задаёт 10 м 4×4.',
    'Общий дефицит 30 м не доказан.', 'AK137_primary_atom_split.json')
add('ITP:63','head_roles','confirmed_document_difference_utility_rejected',
    'Один артикул 2786092: выбранная характеристика 47,08 м³/ч и 21,29 м в ПЗ/СО против 21,91 м в листе характеристик. Требуемый напор отдельно: 15,4 м.',
    'Не называть 21,29 требуемым напором. Непригодность насоса, ошибка настройки и универсальный допуск 0,62 м не доказаны.', 'ITP63_primary_semantic_check.json', 'historically_rejected_for_low_impact')
add('ITP:64','work_code','confirmed_omission_outside_historical_technical_profile',
    'Требуемая ТЗ графа «Код работ» отсутствует во всех 15 шапках СО; «Код изделия» её не заменяет.',
    'Технический профиль и договорную полноту оценивать отдельно; отклонение не означает наличия графы.', 'ITP64_primary_profile_check.json', 'historically_out_of_technical_profile')
add('ITP:68','scope','bounded_reconciliation_question',
    '33+137+214+435=819 муфт в четырёх строках; требование ТЗ относится к оцинкованным трубам водоснабжения жилой части. Требуется адресная проверка применимости.',
    'Не утверждать 819 недопустимых соединений без привязки к системе, материалу и диаметру.', 'ITP68_primary_scope_check.json')
add('AR:14','support_chain','conditional_geometry_question',
    'При условии единой базы (20,990−20,875)×1000−40=75 мм против указанного диапазона опор 25–50 мм.',
    'Уточнить поверхность +20,875 и слои; 75 мм не объявлять доказанной фактической высотой опоры.', 'AR14_source_check.json')
add('EOM:11','FRHF_position','confirmed_bounded_document_omission',
    'С2513 65 м и С3510 95 м заданы FRHF 5×35; в выданном подразделе кабельной СО нет FRHF 5×35.',
    'HF 5×35 есть 172 м; не объявлять отсутствие всех 5×35 и физический дефицит 160 м. Проверить смежную поставку.', 'EOM11_both_lines_source_check.json', 'historical_parent_accepted_atom_usefulness_not_separately_adjudicated')
add('EOM:27','active_circuits','confirmed_bounded_document_omission',
    'Активные группы С2326 и С2327 по 2 кВт есть на схеме и отсутствуют отдельными строками таблицы РП3.',
    'С2325 и С2328 — резервы без заданной мощности. Не выводить автоматически дефицит 4 кВт и непригодность защиты.', 'EOM27_source_check.json', 'historical_parent_accepted_atom_usefulness_not_separately_adjudicated')

add('ITP:72b','velocity','confirmed_arithmetic_conflict',
    'По указанным в ПЗ 61,12 м³/ч и трубе 219×6 мм: Dвн=207 мм, v=0,504487 м/с против напечатанных 0,66 м/с.',
    'Только внутренняя арифметическая согласованность; инженерная полезность производного тезиса оценивается отдельно.', str(OUT/'ITP72b_primary_TEXT.json'))
add('VK:14','specification_size','confirmed_ambiguous_product_designation',
    'В позиции СО буквально д50x0, 5,2 м, Pro Aqua, без кода изделия; требуется уточнение типоразмера.',
    'Не утверждать физическую трубу с нулевой стенкой или отсутствие труб. Связь с обозначениями плана не проверена.', str(OUT/'VK14_primary_TEXT.json'))

add('EOM:76','panel_sum','confirmed_arithmetic_conflict',
    'В 2ЩСС1 восемь групп по 0,40 кВт и одна 0,20 кВт: сумма 3,40 кВт против итога 5,4 кВт.',
    'Резервы 10–13 имеют пустые ячейки мощности; не дописывать им 2 кВт. Непригодность оборудования не установлена.', str(OUT/'EOM76_primary_GRAPHIC.json'))
add('KM:3','plan_mark','bounded_reconciliation_question',
    'На плане ЛК6 на отметке −0.700 присутствует выноска ОГМ26; требуется связать её со спецификацией и деталировкой.',
    'Поиск в TEXT не доказывает отсутствие деталировки в графике. Не исправлять на ОГМ25 без подтверждения.', str(OUT/'KM3_primary_GRAPHIC.json'))

# Evidence integrity is distinct from engineering truth. No labels inferred from hashes.
checks = []
for a in atoms:
    for e in a['evidence']:
        p = Path(e['path'])
        checks.append({'atom_id': a['atom_id'], **e, 'exists': p.exists(),
                       'hash_matches': p.exists() and digest(p) == e['sha256']})
for e in contract['primary_receipts_revalidated']:
    p = Path(e['path'])
    checks.append({'kind': 'previous_primary_image', **e, 'exists': p.exists(),
                   'hash_matches': p.exists() and digest(p) == e['sha256']})
assert all(c['hash_matches'] for c in checks), 'Changed/missing evidence: do not publish consolidation'

controls = []
for r in readiness:
    cid = r['control_id']
    aa = [a for a in atoms if cid in a['historical_control_ids']]
    row = {'control_id': cid, 'discipline': cid.split(':')[0],
           'historical_record': history.get(cid),
           'historical_readiness_snapshot': r,
           'current_assertion_ids': [a['atom_id'] for a in aa],
           'current_wording': [a['required_semantics'] for a in aa],
           'accounting_status': 'explicit_assertions_recorded' if aa else 'primary_adjudication_pending',
           'whole_historical_row_confirmed': False,
           'open_work': [], 'result': None}
    if not aa:
        row['open_work'].append({'kind': 'source_review',
          'target': r['target_wording'], 'boundary': r['evidence_boundary'],
          'required_action': 'Проверить указанный в исторической записи объект и место по той же версии MD/GRAPHIC; разделить составную претензию, сохранить цитаты/фрагменты и отдельно оценить полезность.',
          'status': 'not_checked'})
    else:
        row['open_work'].append({'kind': 'whole_row_remainder',
           'required_action': 'Проверить, покрывают ли выделенные тезисы весь исходный состав; остаток исторической строки не считать подтверждённым или снятым автоматически.',
           'status': 'not_checked'})
    controls.append(row)
assert len(controls) == len({r['control_id'] for r in controls}) == 47
assert set(history) == {r['control_id'] for r in controls} - {'ITP:72b'} or set(history) == {r['control_id'] for r in controls}

# Retain full classic records. Useful subset is NOT inferred from row count or model severity.
baseline = []
baseline_sources = []
for disc in ['AK','ITP','AR','KM','VK','EOM']:
    p = WORK / 'classic_control/snapshots' / disc / 'findings_verify.json'
    baseline_sources.append({'discipline': disc, **receipt(p)})
    for row in read(p)['findings']:
        baseline.append({'record_key': disc + ':' + row['id'], 'discipline': disc,
                         'source': receipt(p), 'original_record': row,
                         'atomic_claims': None, 'technical_status': 'not_adjudicated_in_this_register',
                         'engineering_utility': 'not_adjudicated_in_this_register',
                         'preservation_result': None})
assert len(baseline) == 244 and len({x['record_key'] for x in baseline}) == 244

rules = [
 'Эталон только для оценщика. Не передавать ответы, решения инженеров и историю независимым исполнителям.',
 'Шесть известных проектов — DEV; не объявлять их новыми слепыми испытаниями.',
 'Подтверждённый документный факт, инженерная полезность и итоговая выдача оцениваются отдельно.',
 'Не включать вопросы, внепрофильные случаи и непроверенные тезисы в знаменатель подтверждённых полезных дефектов.',
 'Не исключать нерешённые случаи молча: показывать отдельный счётчик и остаток по каждому проекту.',
 'Повтор одного тезиса в нескольких строках — одна находка и дополнительная нагрузка дубля.',
 'Новый фильтр требует пары: доказанно ошибочный тезис снят, похожий полезный сохранён. Логический контрпример не заменяет инженерно проверенную пару.',
 'Проверка хэша подтверждает неизменность файла, но не техническую истинность его вывода.',
 'Полезный классический набор ещё не определён из всех 244 строк. Не объявлять сохранность классики доказанной.',
 'Для заморозки нужны адресные тезисы, доказательства, статусы, правила зачёта, полный учёт полезных классических находок и явные нерешённые вопросы.',
]
summary = {'date': '2026-11-09', 'stage': 1, 'status': 'in_progress_not_frozen',
 'registry_coverage': '47/47 tracked records accounted for; not 47 verified defects',
 'controls_with_explicit_assertions': sum(bool(r['current_assertion_ids']) for r in controls),
 'controls_awaiting_primary_adjudication': sum(not r['current_assertion_ids'] for r in controls),
 'explicit_assertions': len(atoms), 'baseline_records_preserved': len(baseline),
 'baseline_useful_atom_set_frozen': False, 'evidence_integrity_checks': len(checks),
 'stage_complete': False, 'production_promotion_allowed': False,
 'next_stage_allowed': False,
 'blocking_work': ['Завершить первичную оценку контролей без конкретных тезисов.',
                   'Разобрать остаток составных исторических строк.',
                   'Определить полезные тезисы классического результата, не потеряв ни одну из 244 строк.',
                   'Зафиксировать правила профиля и инженерной полезности спорных вопросов.']}
package = {'summary': summary, 'inputs': [receipt(history_path),receipt(EV/'target_readiness.json'),receipt(EV/'scoring_contract_v1/contract.json')],
           'rules': rules, 'controls': controls, 'assertions': atoms, 'baseline_sources': baseline_sources}
write('register.json', package)
write('baseline_preservation_queue.json', baseline)
write('evidence_integrity.json', checks)
write('remaining_work.json', [{'control_id':r['control_id'],'work':r['open_work']} for r in controls])
write('summary.json', summary)

esc = lambda s: html.escape(str(s))
parts = ['<!doctype html><html lang="ru"><meta charset="utf-8"><title>Этап 1 — эталон оценки</title><style>body{font:16px/1.5 system-ui;max-width:1250px;margin:32px auto;padding:0 18px}table{border-collapse:collapse;width:100%}td,th{border:1px solid #ccc;padding:9px;text-align:left;vertical-align:top}details{margin:14px 0}pre{white-space:pre-wrap} .note{background:#fff0d7;padding:16px}</style><h1>Этап 1: единый реестр оценки</h1>',
 '<p class="note">Рабочая версия от 9 ноября 2026. Этап не закрыт, эталон не заморожен. Это материалы оценщика; независимым исполнителям их не передавать.</p>',
 '<p>Учтены все 47 контрольных записей; сформулировано '+str(len(atoms))+' проверочных тезисов. Сохранены все 244 строки классических отчётов. Эти числа не являются количеством полезных дефектов.</p>',
 '<h2>Правила оценки</h2><ul>'+''.join('<li>'+esc(x)+'</li>' for x in rules)+'</ul>',
 '<h2>Контрольные записи</h2>']
for r in controls:
    h = r['historical_record'] or {}
    parts.append('<details><summary>'+esc(r['control_id']+' — '+r['historical_readiness_snapshot']['target_wording'])+'</summary>')
    for label, val in [('Проект',h.get('project')),('ID замечания',h.get('finding_id')),('Исходный тезис',h.get('original_claim')),('Решение инженера',h.get('engineer_decision')),('Основание инженера',h.get('engineer_reason')),('Комментарий инженера',h.get('engineer_comment')),('Поздний исторический вывод аудитора',h.get('later_learning'))]:
        parts.append('<p><b>'+label+':</b> '+esc(val if val is not None else 'Не зафиксировано / производный контроль')+'</p>')
    aa=[a for a in atoms if a['atom_id'] in r['current_assertion_ids']]
    if not aa:parts.append('<p><b>Первичная оценка ещё не завершена. В подтверждённую полноту не включать.</b></p>')
    for a in aa:
        parts.append('<p><b>'+esc(a['atom_id'])+' — '+esc(a['technical_status'])+'</b><br>'+esc(a['required_semantics'])+'<br><b>Граница:</b> '+esc(a['unsupported_or_refuted_extension'])+'<br><b>Полезность:</b> '+esc(a['engineering_utility'])+'</p>')
        for e in a['evidence']:parts.append('<p>Доказательство: '+esc(e['path'])+'<br>SHA256: '+esc(e['sha256'])+'</p>')
    parts.append('<p><b>Остаток работы:</b> '+esc(r['open_work'])+'</p></details>')
parts.append('<h2>Условия закрытия этапа</h2><ul>'+''.join('<li>'+esc(x)+'</li>' for x in summary['blocking_work'])+'</ul><p><a href="register.json">Полный реестр JSON</a> · <a href="baseline_preservation_queue.json">244 классические строки</a> · <a href="evidence_integrity.json">Проверка неизменности доказательств</a></p></html>')
(OUT/'Этап_1_эталон.html').write_text(''.join(parts))
print(json.dumps(summary,ensure_ascii=False,indent=2))
