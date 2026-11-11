"""Build a local evidence-linked progress snapshot; no inferred acceptance."""
import json,html
from datetime import datetime,timezone
from pathlib import Path
from .score_atomic_review import sha

HERE=Path(__file__).resolve().parent
ROOT=Path('/tmp/auditmanager_comparative_trial_context_v3_20261111')

def read(p):return json.loads(Path(p).read_text())
def esc(x):return html.escape(str(x))
def table(headers,rows):
    return '<table><thead><tr>'+''.join('<th>'+esc(x)+'</th>' for x in headers)+'</tr></thead><tbody>'+''.join('<tr>'+''.join('<td>'+esc(x)+'</td>' for x in row)+'</tr>' for row in rows)+'</tbody></table>'

def build():
    sources=['baseline_three_repeat_coverage_summary.json','baseline_target_stability.json','baseline_runtime_summary.json','baseline_question_section_score.json','baseline_r3_claim_review_completion.json','AK_profile_fallback_investigation.json','EOM11_r3_text_handoff_trace.json','user_KM_214_readiness_revalidation.json']
    sources += ['AK_r1_experimental_zero_merge_observation.json',
                'AK_r1_merge_source_handoff_trace.json',
                'AK_r1_all_text_recovery_admission_replay.json',
                'recovery_format_pilot_preparation.json']
    docs={name:read(HERE/name) for name in sources}
    coverage=docs[sources[0]];stability=docs[sources[1]];runtime=docs[sources[2]];sections=docs[sources[3]]
    plan=read(HERE.parent/'auditmanager_stage1_20261109/sequential_plan.json')
    state=read(ROOT/'experimental_collection_status.json');trial=read(ROOT/'experimental_collection_trial.json')
    assert sha(ROOT/'experimental_collection_trial.json')==state['trial_sha256']
    complete=[r for r in state['runs'] if r.get('exit_code')==0 and r.get('execution_status')=='completed_unadjudicated']
    running=[r for r in state['runs'] if r.get('status')=='running']
    statuses={'complete':'Завершён','in_progress':'Выполняется','pending_completion':'Не завершён','not_started':'Не начат'}
    body='<h1>AuditManager: проверенная сводка эксперимента</h1><p>Снимок на '+esc(datetime.now(timezone.utc).isoformat(timespec='seconds'))+'. Состояние не обновляется автоматически.</p>'
    body+='<p class="notice"><strong>Закрыты 3 из 8 этапов. Превосходство экспериментальных методов и готовность к production ещё не доказаны.</strong> Завершены 18 базовых прогонов; поштучно оценены все 232 строки третьего повтора. Оценка включает ошибки и неопределённости, а не означает принятие каждой строки.</p>'
    body+='<h2>Общий план</h2>'+table(['Этап','Работа','Статус'],[(x['id'],x['name'],statuses.get(x['status'],x['status'])) for x in plan['stages']])
    body+='<h2>Экспериментальная серия</h2><p>Зафиксированных успешных завершений: '+str(len(complete))+' из '+str(len(trial['runs']))+'. Записей со статусом running: '+str(len(running))+'. Статус-файл сам по себе не доказывает живой процесс.</p>'
    body+='<p>Основная модель: gpt-6-astra, high; два графических детектора и судья. Текущая серия не использует Claude или OpenRouter. Критерии приёмки сохраняются из исходного протокола.</p>'
    recovery=docs['AK_r1_all_text_recovery_admission_replay.json']['summary']
    body+='<h2>Обнаруженные потери в первом экспериментальном АК</h2>'
    body+='<p>Промежуточное объединение отложило все 146 кандидатов с решением «нужны данные» и выпустило ноль замечаний. Промпт требует проверки первичных источников, но запрос содержит два результата предыдущих анализов, без изображений и инструментов чтения исходников. Затем выполняется восстановление по отдельным пакетам с контекстом. Это промежуточное наблюдение, а не итоговый результат прогона.</p>'
    body+=table(['Проверка текстового восстановления','Количество'],[
        ('Завершённые первичные запросы',recovery['requests']),
        ('Предложенные моделью замечания',recovery['raw_findings']),
        ('Проходят структурный контроль доказательств',recovery['admitted_findings']),
        ('Отложены: нет допустимой ссылки внутри evidence замечания',recovery['pending_reasons']['missing_inspected_source_reference'])])
    body+='<p>Прохождение структурного контроля не подтверждает техническую верность. Ссылки в решении по кандидату не доказывают каждый тезис разделённого замечания. Поздние проходы ещё могут изменить итоговую полноту.</p>'
    body+='<p>В следующей версии уточнён формат доказательств и добавлен один запрос исправления формата с повторным контролем. 21 локальный тест прошёл; улучшение на реальном модельном ответе ещё не доказано. Подготовлен отдельный пилот АК T-002, пока без отправки. Зафиксированная версия текущей серии не изменена.</p>'
    body+='<h2>Покрытие эталона базовым методом</h2>'
    rows=[]
    for r in coverage['repeats']:
        t=r['totals']['technical'];q=r['totals']['question'];n=r['totals']['negative']
        rows.append((r['repeat'],f"{t.get('full',0)}/{t['denominator']}",t.get('partial',0),t.get('absent',0),f"{q.get('full',0)}/{q['denominator']}",n.get('full',0),n.get('partial',0)))
    body+=table(['Повтор','Технические полностью','Частично','Пропущены','Вопросы полностью','Отрицательные повторены полностью','Частично'],rows)
    body+='<p>84 технических тезиса, 198 вопросов, 76 отрицательных контролей; один спорный тезис исключён из оценки истинности. Полное покрытие тезиса не подтверждает все дополнения модели. Отсутствие ошибочной строки не доказывает работу фильтра.</p>'
    body+='<h2>Устойчивость технических находок</h2>'+table(['Исход трёх повторов','Количество'],[(label,stability['counts_by_route']['technical'][key]) for key,label in [('always_full','Полностью во всех трёх'),('always_partial','Частично во всех трёх'),('variable','Результат меняется'),('always_absent','Пропущено во всех трёх')]])
    missed=[t for t in stability['targets'] if t['route']=='technical' and t['stability']=='always_absent']
    body+=table(['Дисциплина','Контрольный тезис'],[(t['discipline'],t['claim'] or t['target_id']) for t in missed])
    body+='<h2>Время и расход</h2>'+table(['Дисциплина','Минимум, мин','Медиана, мин','Максимум, мин'],[(d,round(v['min_seconds']/60,1),round(v['median_seconds']/60,1),round(v['max_seconds']/60,1)) for d,v in runtime['by_discipline'].items()])
    body+='<p>Всего '+str(runtime['request_totals']['requests'])+' запросов; зарегистрированных неудачных попыток '+str(runtime['request_totals']['failed_attempts'])+'. Денежная стоимость и процент квоты не измерены: ноль в поле стоимости подписки не означает бесплатный расход. Время инженера также не измерено.</p>'
    body+='<h2>Выдача вопросов</h2>'+table(['Повтор','Полностью найденные вопросы без отдельного раздела'],[(rep,c['incorrect_missing_questions_section']) for rep,c in sections['counts_by_repeat'].items()])
    body+='<p>Проверен реальный Excel-экспорт 18 базовых прогонов: 730 строк без отдельного раздела вопросов. Спорные маршруты исключены. Экспериментальный экспорт требует самостоятельной проверки.</p>'
    body+='<h2>Что уже объяснено доказательствами</h2><ul><li>АК не зарегистрирован как профиль: базовые и текущий контрольный прогоны используют роль ЭОМ в текстовом анализе. Это общая ограниченность конфигурации; её влияние ещё не измерено.</li><li>FRHF 5×35, ЭОМ: спецификация передана текстовому этапу, но свойства двух линий со схем не сохранены как связь со спецификацией. В четырёх исходных графических ответах и 29 текстовых кандидатах соответствующей находки нет.</li><li>Новый комплект КМ 214 подготовлен, но стройка уже знакома исследованию. Это дополнительная калибровка, а не независимая стройка для проверки переносимости.</li></ul>'
    body+='<h2>Что остаётся до внедрения</h2><ol><li>Завершить экспериментальные прогоны и поштучно оценить их на том же эталоне.</li><li>Сравнить полноту, сохранение полезных результатов, новые ошибки, дубли и раздел вопросов; проследить потери до конкретных этапов.</li><li>Оценить добавочную пользу моделей, время, расход и нагрузку на инженера.</li><li>Проверить переносимость на допустимой новой выборке, сохраняя резерв и независимость поиска.</li><li>Пройти критерии приёмки и релиза; проверить работу после выпуска и возможность отката.</li></ol>'
    body+='<h2>Доказательства</h2><ul>'+''.join('<li><a href="'+esc(n)+'">'+esc(n)+'</a></li>' for n in sources)+'</ul>'
    return '<!doctype html><html lang="ru"><meta charset="utf-8"><title>AuditManager — проверенная сводка</title><style>body{font:16px/1.5 system-ui;max-width:1150px;margin:32px auto;padding:0 20px;color:#182332}table{border-collapse:collapse;width:100%;margin:16px 0}td,th{border:1px solid #cad3df;padding:9px;text-align:left}th{background:#eef3f8}.notice{background:#fff4d8;padding:16px}h2{margin-top:30px}a{overflow-wrap:anywhere}</style>'+body+'</html>'

if __name__=='__main__':
    output=HERE/'Проверенная_сводка_прогресса.html'
    output.write_text(build())
    print(output)
