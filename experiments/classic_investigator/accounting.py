"""Loss accounting, not automatic truth adjudication or semantic deduplication."""
import argparse
from collections import Counter
import hashlib
import html
import json
from pathlib import Path
import time
from investigate import read, write, sha


def claim_key(finding):
    # Deliberately exact: a rewritten engineering claim needs semantic review.
    return str(finding.get('id','')),str(finding.get('claim',''))


def reconcile(run, final, proposals):
    final_by_key={claim_key(f):f for f in final}
    final_ids={str(f.get('id')) for f in final}
    variants={}
    for proposal in proposals:
        finding=proposal['finding'];key=claim_key(finding)
        row=variants.setdefault(key,{'registry_id':run+':'+key[0]+':'+hashlib.sha256(key[1].encode()).hexdigest()[:12],
            'run':run,'candidate_id':key[0],'claim':key[1],'versions':[]})
        row['versions'].append(proposal)
    # Also preserve final-only data if historical responses are missing.
    for key,finding in final_by_key.items():
        if key not in variants:
            variants[key]={'registry_id':run+':'+key[0]+':'+hashlib.sha256(key[1].encode()).hexdigest()[:12],
                'run':run,'candidate_id':key[0],'claim':key[1],'versions':[], 'missing_proposal_history':True}
    for key,row in variants.items():
        row['retention_status']='retained_final' if key in final_by_key else (
            'same_id_different_wording_needs_review' if key[0] in final_ids else 'not_in_final_needs_disposition')
        row['final_finding']=final_by_key.get(key)
        row['technical_status']='not_inferred_from_retention'
        row['disposition']=None
    return list(variants.values())


def live_controller(pid,work):
    if pid is None:return None
    try:cmd=Path(f'/proc/{pid}/cmdline').read_bytes().replace(b'\x00',b' ').decode()
    except (FileNotFoundError,ProcessLookupError):return False
    return 'classic_investigator/review.py' in cmd and str(work) in cmd


def build(work,pid=None):
    candidates=[];initial=[]
    for status in sorted((work/'runs').glob('*/status.json')):
        state=read(status);proposals=[]
        for path in sorted(status.parent.glob('*_response.json')):
            data=read(path).get('json')
            if not isinstance(data,dict):continue
            evidence=path.with_name(path.name.replace('_response','_evidence'))
            feedback=read(evidence) if evidence.exists() else []
            for finding in data.get('findings',[]):
                errors=[e['error'] for e in feedback if e.get('finding')==finding and e.get('error')]
                proposals.append({'step':int(path.name[:2])+1,'finding':finding,'structural_errors':errors,'source_file':str(path),'source_sha256':sha(path)})
        candidates.extend(reconcile(status.parent.name,state['findings'],proposals))
        initial.append({'run':status.parent.name,'status':state['status'],'model':state['model'],'steps':state['steps'],'candidates':len(state['findings'])})
    manifests=[work/'review/jobs.json',work/'review/codex_continuation_jobs.json']
    jobs={j['id']:j for path in manifests if path.exists() for j in read(path)}
    superseded={j['previous_job'] for j in jobs.values() if j.get('previous_job')}
    reviews=[];assessments={}
    for jid,job in jobs.items():
        path=work/'review/jobs'/jid/'status.json'
        state=read(path) if path.exists() else {'status':'not_started','steps':0,'assessments':[]}
        effective=jid not in superseded
        reviews.append({'job':jid,'effective':effective,'status':state['status'],'model':state.get('model'),
            'candidate_count':len(job['candidates']),'assessed_count':len(state['assessments']),
            'steps':state['steps'],'error':state.get('error'),
            'independence_limit':job.get('independence_limit','Different model family in separate session')})
        for item in state['assessments']:
            assessments.setdefault((job['origin_run'],item['candidate_id']),[]).append({
                'job':jid,'effective':effective,'job_status':state['status'],
                'review':item,'trust':'model_provisional_not_final_truth','source_sha256':sha(path)})
    checks=read(work/'evaluation/primary_checks.json')
    disposition_path=work/'evaluation/orphan_dispositions.json'
    dispositions={d['registry_id']:d for d in read(disposition_path)} if disposition_path.exists() else {}
    for row in candidates:
        if row['registry_id'] in dispositions:
            decision=dispositions[row['registry_id']]
            if any(sha(Path(path))!=digest for path,digest in decision.get('evidence_files',{}).items()):
                raise ValueError('Primary evidence changed; recheck disposition')
            row['disposition']=decision
            row['technical_status']=decision['technical_status']
        row['model_reviews']=assessments.get((row['run'],row['candidate_id']),[])
        row['primary_checks']=[c for c in checks if f"{row['run']}:{row['candidate_id']}" in c.get('runs',[])] if row['retention_status']=='retained_final' else []
        if row['retention_status']!='retained_final' and row['model_reviews']:
            row['review_link_limit']='Same ID only; reviewers received final wording. Do not apply verdict automatically to historical variant.'
    effective=[r for r in reviews if r['effective']]
    summary={'generated_at':time.time(),'controller_live':live_controller(pid,work),
        'initial_runs':initial,'final_candidates':sum(r['candidates'] for r in initial),
        'distinct_proposed_wordings':len(candidates),'retention':dict(Counter(r['retention_status'] for r in candidates)),
        'review_jobs':len(effective),'review_statuses':dict(Counter(r['status'] for r in effective)),
        'fully_reviewed_candidates':sum(r['candidate_count'] for r in effective if r['status']=='reviewed_provisional'),
        'scope':'Development pilot only. No overall quality score or production approval.',
        'limits':['Wordings are not independent defects. ID link is not semantic equivalence.',
                  'Supported model atom can be counterevidence/context rather than a defect.',
                  'Initial model completion does not prove full source/operation coverage.',
                  'Same-family Codex continuation is not independent cross-model replication.']}
    write(work/'evaluation/candidate_accounting.json',{'summary':summary,'candidates':candidates,'reviews':reviews})
    write(work/'evaluation/current_progress.json',summary)
    coverage=read(work/'evaluation/coverage.json')
    esc=lambda x:html.escape(str(x))
    rows=''.join('<tr>'+''.join('<td>'+esc(v)+'</td>' for v in (r['run'],r['findings'],f"{len(r['text_fully_read'])}/{r['text_total']}",f"{len(r['graphic_requested'])}/{r['graphic_total']}",r['calculator_calls']))+'</tr>' for r in coverage)
    reviewrows=''.join('<tr>'+''.join('<td>'+esc(v)+'</td>' for v in (r['job'],r['model'] or 'ожидает запуска',r['status'],f"{r['assessed_count']}/{r['candidate_count']}"))+'</tr>' for r in effective)
    page='''<!doctype html><html lang="ru"><meta charset="utf-8"><meta http-equiv="refresh" content="30"><title>Проверка пилота AuditManager</title><style>body{font:16px system-ui;max-width:1200px;margin:32px auto}table{border-collapse:collapse}td,th{border:1px solid #ccc;padding:7px}p{max-width:1000px}</style><h1>Пилот: находки, охват и перепроверка</h1>'''
    page+=f'<p>Зафиксировано {summary["final_candidates"]} итоговых кандидата. Полностью обработаны второй модельной проверкой {summary["fully_reviewed_candidates"]}; это предварительные оценки, не число доказанных дефектов. Процесс перепроверки: {esc(summary["controller_live"])}.</p>'
    page+='<p>Claude остановлен из-за ограничения пользователя 30% и отсутствия свежего счётчика. Продолжение — GPT-6 Astra. Исходные попытки Sonnet сохранены. Никакого выпуска в production.</p>'
    page+='<table><tr><th>Пилот</th><th>Кандидаты</th><th>TEXT полностью доставлено</th><th>GRAPHIC запрошено</th><th>Расчёты инструментом</th></tr>'+rows+'</table>'
    page+='<p>Доставка текста и изображения не доказывает их понимание. Графические описания не заменяют рисунки. Проверено по исходникам: частично возвращён прежний пропуск ЭОМ11 — линия С2513 FRHF5×35, 65м отсутствует в предоставленной спецификации. Вторая линия С3510 и полный итог160м этим кандидатом не восстановлены.</p>'
    page+='<p>Варианты формулировок: '+esc(summary['retention'])+'. Структурная ошибка не означает техническую ложность; исчезнувшие варианты требуют явного решения.</p>'
    page+='<table><tr><th>Задание перепроверки</th><th>Модель</th><th>Статус</th><th>Кандидаты с оценкой</th></tr>'+reviewrows+'</table>'
    page+='<p>Остаются: техническая проверка различий и сохранения полезных находок, обязательный охват объектов и операций, свежий классический контроль, выбор варианта, все шесть регрессионных проектов и новые проекты. Цель не достигнута.</p></html>'
    target=work/'Проверка_пилота.html';temp=target.with_suffix('.tmp');temp.write_text(page);temp.replace(target)
    return summary

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--work',type=Path,required=True);p.add_argument('--watch-pid',type=int)
    args=p.parse_args()
    while True:
        summary=build(args.work,args.watch_pid)
        print(json.dumps(summary,ensure_ascii=False),flush=True)
        if not args.watch_pid or not summary['controller_live']:break
        time.sleep(30)
