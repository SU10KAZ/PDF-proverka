"""Human source-check overlay. Counts are review coverage, not model quality."""
import csv,html,json,hashlib
from pathlib import Path
HERE=Path(__file__).resolve().parent
OUT=HERE/'consolidated'
def read(p):return json.loads(p.read_text())
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def main():
    records=read(OUT/'records.json');known={r['candidate_id'] for r in records};decisions=[];covered={}
    for p in sorted((HERE/'decisions').glob('*.json')):
        d=read(p);d['decision_file']=str(p)
        assert not d['production_changed']
        for cid in d['candidate_ids']:
            assert cid in known and cid not in covered
            covered[cid]=d
        for image in d.get('graphic_evidence', []):
            assert image.get('directly_viewed') and sha(Path(image['path']))==image['sha256']
        ref=d.get('primary_adjudication')
        if ref:assert sha(Path(ref['path']))==ref['sha256']
        source=d.get('source_md') or d.get('project_TEXT')
        if source:
            pth=Path(source['path']);assert sha(pth)==source['sha256'];text=pth.read_text()
            for q in source.get('quotes',[]):assert q in text
            for e in d.get('evidence',[]):
                if 'authoritative_TEXT' in e:assert text[e['start']:e['end']]==e['authoritative_TEXT']
        if 'followup_dispositions' in d:
            assert [x['requirement'] for x in d['followup_dispositions']]==d.get('followup_requirements',[])
            assert all(x['unscored_claims'] and x['retained_evidence_boundary'] for x in d['followup_dispositions'])
        decisions.append(d)
    summary={'candidate_records':len(records),'records_with_narrow_primary_decision':len(covered),'remaining_records':len(records)-len(covered),
             'decision_themes':len(decisions),'technical_themes':sum(d['output_section']=='Технические замечания' for d in decisions),
             'question_themes':sum(d['output_section']=='Вопросы и уточнения' for d in decisions),
             'omitted_themes':sum(d['output_section'] is None for d in decisions),
             'scope':'Only listed narrow claims; not blanket acceptance of entire source rows.','stage_complete':False,'production_promotion_allowed':False}
    (OUT/'primary_decisions.json').write_text(json.dumps({'summary':summary,'decisions':decisions},ensure_ascii=False,indent=2)+'\n')
    pending=[{'candidate_id':r['candidate_id'],'discipline':r['discipline'],'phase':r['phase'],'flags':r['current_flags'],
              'original_claim':r['original_candidate']['claim'],
              'model_atoms_to_verify':r['effective_provisional_assessment']['atoms'],
              'remaining_claims':[a for a in r['effective_provisional_assessment']['atoms'] if a['verdict']=='needs_data'],
              'coverage_warning':'Все исходные тезисы требуют сверки; список needs_data не отражает пропущенные моделью тезисы и не подтверждает остальные.',
              'next_action':'Проверить первичные факты и контрпример; неопределённые последствия не превращать в обязательный вопрос инженеру.'}
             for r in records if r['candidate_id'] not in covered]
    (OUT/'remaining_primary_queue.json').write_text(json.dumps(pending,ensure_ascii=False,indent=2)+'\n')
    with (OUT/'Реестр_291.csv').open('w',encoding='utf-8-sig',newline='') as f:
        w=csv.writer(f);w.writerow(['ID','Дисциплина','Происхождение','Статус узкого тезиса','Раздел','Формулировка','Доказательства'])
        for r in records:
            d=covered.get(r['candidate_id'],{});w.writerow([r['candidate_id'],r['discipline'],r['phase'],d.get('decision','Первичная проверка не завершена'),d.get('output_section',''),d.get('wording',d.get('required_behavior','')),d.get('decision_file','')])
    followups=[{'candidate_ids':d['candidate_ids'],'decision_file':d['decision_file'],
                'output_section':d['output_section'],'required_checks':d['followup_requirements'],
                'dispositions':d.get('followup_dispositions',[])}
               for d in decisions if d.get('followup_requirements')]
    (OUT/'explicit_followups.json').write_text(json.dumps({
        'scope':'Explicit followup_requirements only; not an exhaustive inventory of uncertainties in limits or atomic_dispositions.',
        'completion_rule':'A narrow decision and removal from remaining_primary_queue do not close these checks. Resolve them or explicitly freeze the affected claims as unscored before stage 1 closure.',
        'items':followups},ensure_ascii=False,indent=2)+'\n')
    esc=lambda x:html.escape(str(x))
    body=['<!doctype html><html lang="ru"><meta charset="utf-8"><title>Проверенные выводы и остаток работ</title><style>body{font:16px sans-serif;max-width:1200px;margin:32px auto}td,th{border:1px solid #ccc;padding:9px;text-align:left;vertical-align:top}table{border-collapse:collapse;width:100%}pre{white-space:pre-wrap}</style><h1>Проверенные выводы после двух прогонов</h1>',f'<p>Сохранены291 исходная запись и история25 повторов. Предметные решения вынесены по {len(covered)} записям, объединённым в {len(decisions)} тем. Ещё {len(pending)} записей требуют первичной проверки. Этап1 не закрыт.</p><p>Решение относится только к сформулированному узкому тезису. Это не автоматическое принятие всех рисков и рекомендаций исходной строки.</p>']
    for section in ['Технические замечания','Вопросы и уточнения',None]:
        body.append('<h2>'+esc(section or 'Не выпускать как замечание (основание указано отдельно)')+'</h2><table><tr><th>Записи</th><th>Решение</th><th>Границы доказательств</th></tr>')
        for d in decisions:
            if d['output_section']!=section:continue
            body.append('<tr><td>'+esc(', '.join(d['candidate_ids']))+'</td><td>'+esc(d.get('adjudicator_disposition',d.get('wording',d.get('required_behavior',''))) )+'</td><td>'+esc(d.get('limits',d.get('unsupported_extensions',d.get('claim_refuted',''))))+'</td></tr>')
        body.append('</table>')
    body.append('<h2>Явно отложенные проверки по разобранным записям</h2><p>Эти задачи сохраняются сверх очереди первичного разбора. Список включает только явно записанные followup_requirements; остальные ограничения доказательств остаются в решениях выше. Наличие узкого решения не означает завершения проверки всей записи.</p><ul>')
    for f in followups:
        body.append('<li>'+esc(', '.join(f['candidate_ids']))+': '+esc(' '.join(f['required_checks']))+'</li>')
    body.append('</ul>')
    body.append('<h2>Остаток работы</h2><p>Сверить оставшиеся записи по источникам, сохранить полезные классические находки и проверить контрольные пары фильтров. Независимые испытания новой версии ещё не проведены.</p>')
    (OUT/'Проверенные_выводы.html').write_text('\n'.join(body)+'</html>')
    print(json.dumps(summary,ensure_ascii=False,indent=2))
if __name__=='__main__':main()
