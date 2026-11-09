"""Stage-one evaluator queue: Astra only, source tools, no automatic acceptance."""
import argparse
import asyncio
import fcntl
import hashlib
import json
from pathlib import Path
import sys
import time

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
sys.path[:0] = [str(ROOT/'experiments/classic_investigator'),str(ROOT)]
from investigate import read, write, sha, configure
import review

MODEL = 'gpt-6-astra'
PROMPT = review.REVIEW_PROMPT + '''\nЭто адресная оценка кандидата, не независимый поиск новых дефектов.
Тебе не переданы решения инженера, история проверок и ожидаемые ответы.
Не считай предварительное мнение предыдущего аудитора доказательством.
Раздели исходный текст, риск и рекомендацию на отдельные проверяемые тезисы.
Если один тезис верен, это не подтверждает все последствия и все части строки.
Для поддержанного или опровергнутого тезиса добавь evidence_refs:
[{"block_id":"id","kind":"TEXT","quote":"дословный фрагмент прочитанного TEXT"}]
или {"block_id":"id","kind":"GRAPHIC","observation":"точная видимая подпись/связь"}.
Графика должна присутствовать среди attached_images текущего запроса.
Отдельно укажи practical_impact: installation|procurement|identification|documentation|unknown.
Это предварительная оценка влияния, она не заменяет решения инженера.
Нельзя завершать проверку отсутствия после одного неудачного поиска: укажи область,
действительно просмотренные разделы и непроверенные смежные документы.
'''
BASE_VALIDATE = review.validate_assessments

def validate(items, job, corpus, current_images):
    valid, errors = BASE_VALIDATE(items, job, corpus, current_images)
    accepted=[]
    for item in valid:
        try:
            for atom in item['atoms']:
                if atom['verdict'] not in ('supported','refuted'):continue
                refs=atom.get('evidence_refs')
                if not isinstance(refs,list) or not refs:raise ValueError('Exact evidence_refs required')
                if {r.get('block_id') for r in refs} != set(atom['source_ids']):
                    raise ValueError('Evidence refs must cover cited source IDs exactly')
                for ref in refs:
                    bid=ref['block_id']; block=corpus.blocks[bid]
                    if block['kind']=='TEXT':
                        q=ref.get('quote')
                        if ref.get('kind')!='TEXT' or not isinstance(q,str) or not q or q not in block['text']:
                            raise ValueError('TEXT quote not in authoritative block')
                    elif ref.get('kind')!='GRAPHIC' or bid not in current_images or not ref.get('observation'):
                        raise ValueError('Graphic observation needs current attached image')
            accepted.append(item)
        except (ValueError,TypeError,KeyError) as exc:
            errors.append({'assessment':item,'error':str(exc)})
    return accepted,errors


def prepare(work, source_work):
    if work.exists():raise ValueError('New evaluator directory required')
    register=read(HERE/'register.json'); baseline=read(HERE/'baseline_preservation_queue.json')
    work.mkdir(parents=True)
    (work/'sources').symlink_to(source_work/'sources',target_is_directory=True)
    write(work/'manifest.json',read(source_work/'manifest.json'))
    write(work/'provider_policy.json',{'claude_mode':'disabled','allowed_models':[MODEL],'openrouter':False})
    candidates={d:[] for d in ['AK','ITP','AR','KM','VK','EOM']}; jobs=[]
    # Previously unadjudicated controls first, then remainder of historical composites.
    controls=sorted(register['controls'],key=lambda r:bool(r['current_assertion_ids']))
    for row in controls:
        d=row['discipline']; h=row['historical_record'] or {}
        cid='H-'+row['control_id'].replace(':','-')
        c={'id':cid,'claim':h.get('original_claim',row['historical_readiness_snapshot']['target_wording']),
           'title':h.get('original_title'),'location':h.get('location'),'source':'historical_candidate_unverified'}
        candidates[d].append(c)
        jobs.append({'id':cid,'discipline':d,'reviewer':'codex','origin_run':d,'candidates':[c],
                     'phase':'historical_control','tracking_control_id':row['control_id']})
    for d in candidates:
        rows=[r for r in baseline if r['discipline']==d]
        for i in range(0,len(rows),4):
            cc=[{'id':'B-'+r['record_key'].replace(':','-'),'claim':r['original_record'].get('description',''),
                 'full_finding':r['original_record']} for r in rows[i:i+4]]
            candidates[d].extend(cc)
            jobs.append({'id':f'B-{d}-{i//4:03d}','discipline':d,'reviewer':'codex','origin_run':d,
                         'candidates':cc,'phase':'classic_usefulness'})
    for d,cc in candidates.items():write(work/'runs'/d/'status.json',{'status':'sealed_for_evaluation','findings':cc})
    for job in jobs:job['origin_sha256']=sha(work/'runs'/job['origin_run']/'status.json')
    write(work/'review/jobs.json',jobs)
    write(work/'protocol.json',{'model':MODEL,'reasoning_effort':'medium','jobs':len(jobs),
          'historical_candidates':47,'classic_candidates':len(baseline),'concurrency':1,
          'max_steps_per_job':12,'timeout_per_request_seconds':600,'transient_retry_delay_seconds':10,
          'prompt_sha256':hashlib.sha256((PROMPT+review.TOOLS).encode()).hexdigest(),
          'code_sha256':sha(__file__),'review_code_sha256':sha(review.__file__),
          'register':{'path':str(HERE/'register.json'),'sha256':sha(HERE/'register.json')},
          'jobs_sha256':sha(work/'review/jobs.json'),
          'phase':'stage_one_evaluator_only','source_work':str(source_work),
          'no_engineer_answers_sent':True,'all_verdicts_provisional':True,
          'later_stages_auto_start':False,'production_promotion_allowed':False})
    print('Prepared',len(jobs),'jobs /',sum(len(j['candidates']) for j in jobs),'candidates; no model calls')


def summary(work,jobs):
    rows=[]
    for j in jobs:
        p=work/'review/jobs'/j['id']/'status.json'
        s=read(p) if p.exists() else {}
        rows.append({'job':j['id'],'phase':j['phase'],'status':s.get('status','pending'),
                     'assessments':len(s.get('assessments',[])),'candidates':len(j['candidates']),
                     'steps':s.get('steps',0),'error':s.get('error')})
    return rows


async def run(work):
    lock=(work/'.lock').open('a');fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
    protocol=read(work/'protocol.json');jobs=read(work/'review/jobs.json')
    assert protocol['model']==MODEL and review.MODELS['codex']==MODEL
    assert sha(__file__)==protocol['code_sha256'] and sha(review.__file__)==protocol['review_code_sha256']
    assert sha(work/'review/jobs.json')==protocol['jobs_sha256']
    assert sha(protocol['register']['path'])==protocol['register']['sha256']
    assert all(j['reviewer']=='codex' for j in jobs)
    assert hashlib.sha256((PROMPT+review.TOOLS).encode()).hexdigest()==protocol['prompt_sha256']
    configure(work)
    from backend.app.services.llm import codex_runner, claude_json_runner
    original=codex_runner.run_codex_json_messages
    async def only_astra(*args,**kwargs):
        from backend.app.services.llm.codex_model_substitution import substitute_codex_model
        if kwargs.get('model')!=MODEL or substitute_codex_model(MODEL)!=MODEL:
            raise RuntimeError('Astra-only policy; model substitution forbidden')
        if (Path(protocol['source_work'])/'STOP_REQUESTED.json').exists():raise RuntimeError('Parent stop requested')
        return await original(*args,**kwargs)
    async def deny(*args,**kwargs):raise RuntimeError('Claude disabled by explicit user instruction')
    codex_runner.run_codex_json_messages=only_astra
    claude_json_runner.run_claude_json_messages=deny
    review.REVIEW_PROMPT=PROMPT
    review.validate_assessments=validate
    started=time.time()
    state={'status':'running','started':started,'model':MODEL,'stage':1,'stage_complete':False}
    try:
        for job in jobs:
            if (work/'STOP_REQUESTED.json').exists() or (Path(protocol['source_work'])/'STOP_REQUESTED.json').exists():
                state['status']='stopped';break
            out=work/'review/jobs'/job['id']
            if out.exists():continue  # retain incomplete/failed jobs in explicit follow-up queue
            state.update(current_job=job['id'],jobs=summary(work,jobs),updated_at=time.time())
            write(work/'controller.json',state)
            await review.review_job(work,job)
        else:state['status']='queue_finished_requires_adjudication'
    except BaseException as exc:
        state.update(status='interrupted_or_failed',error=f'{type(exc).__name__}: {exc}');raise
    finally:
        state.update(jobs=summary(work,jobs),updated_at=time.time())
        write(work/'controller.json',state)
        lock.close()

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('command',choices=['prepare','run']);p.add_argument('--work',type=Path,required=True)
    p.add_argument('--source-work',type=Path,default=Path('/home/coder/auditmanager/corpus-audits/20261106_classic_investigation'))
    a=p.parse_args()
    if a.command=='prepare':prepare(a.work,a.source_work)
    else:asyncio.run(run(a.work))
