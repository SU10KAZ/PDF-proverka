"""Independent source review of sealed candidates; does not publish model verdicts."""
import argparse
import asyncio
import json
from pathlib import Path
import sys
import time

from investigate import Corpus, MODELS, PROMPT, configure, read, sha, write
from subscription_retry import retry_subscription_call
from quota_policy import assert_allowed

REVIEW_PROMPT = '''Ты выполняешь независимую проверку кандидатов инженерного аудита.
Полученные кандидаты — гипотезы, а не факты. Другой аудитор может ошибаться.
Для КАЖДОГО исходного кандидата отдели атомарные утверждения. Найди точные
источники, контекст, режимы, единицы, границы поставки, продолжения таблиц и
возможное опровержение. TEXT из MD авторитетен; GRAPHIC требует просмотра
изображения. Описание графики никогда не является визуальным доказательством.
Не исправляй TEXT по растру. Не делай нормативных выводов по памяти.
Поиск, чтение, изображения и calculator доступны через actions по протоколу ниже.
Ответ JSON: {"actions": [...], "assessments": [...], "done": false}.
assessment: {"candidate_id":"исходный ID", "atoms":[{
"claim":"точный отдельный тезис", "verdict":"supported|refuted|needs_data|out_of_scope",
"source_ids":["..."], "evidence":"значения, листы и операции",
"counterevidence":"что проверено против тезиса, включая альтернативное объяснение",
"limits":"что не установлено", "recommended_output":"finding|question|omit|hold"}]}
Supported означает только подтверждение именно этого тезиса источниками, не
доказанный отказ оборудования. Refuted требует конкретного опровержения того же
тезиса, отсутствие данных = needs_data. Составные кандидаты могут иметь разные
вердикты по атомам. Сохраняй полезное узкое замечание даже если часть преувеличена.
На каждом шаге присылай уже проверенные оценки. done=true только когда оценены
ВСЕ кандидаты. Для графического вывода заново запроси соответствующее изображение:
в запросе присутствуют лишь перечисленные attached_images; старый хеш не картинка.
Не считай source_id доказательством: проверь значение и смысл.
История решений инженеров и ожидаемые ответы не предоставлены.
'''
TOOLS = PROMPT[PROMPT.index('Доступные actions'):PROMPT.index('Finding:')]


def prepare(work):
    target=work/'review';target.mkdir(exist_ok=False)
    jobs=[]
    for p in sorted((work/'runs').glob('*/status.json')):
        state=read(p)
        if state['status'] not in ('completed_unadjudicated','budget_exhausted'):raise ValueError('Initial outputs must be sealed')
        disc,origin=p.parent.name.split('_',1)
        for i in range(0,len(state['findings']),4):
            jobs.append({'id':f'{disc}_{origin}_{i//4:02d}','discipline':disc,
                'origin_run':p.parent.name,'origin_sha256':sha(p),
                'reviewer':'sonnet' if origin=='codex' else 'codex',
                'candidates':state['findings'][i:i+4]})
    write(target/'jobs.json',jobs)
    write(target/'protocol.json',{'prompt_sha256':__import__('hashlib').sha256((REVIEW_PROMPT+TOOLS).encode()).hexdigest(),
        'steps':12,'parallelism':2,'reviewer':'other model family, not original author',
        'limits':'Independent model review is provisional, requires primary checks before quality scoring; historical answers withheld',
        'promotion':False})
    print(f'{len(jobs)} jobs; {sum(len(j["candidates"]) for j in jobs)} candidates')


def validate_assessments(items,job,corpus,current_images):
    valid=[];errors=[]
    known={c['id'] for c in job['candidates']}
    for item in items:
        try:
            if item.get('candidate_id') not in known or not item.get('atoms'):raise ValueError('Unknown candidate or no atoms')
            for atom in item['atoms']:
                verdict=atom.get('verdict');refs=atom.get('source_ids') or []
                if verdict not in ('supported','refuted','needs_data','out_of_scope'):raise ValueError('Invalid verdict')
                if atom.get('recommended_output') not in ('finding','question','omit','hold'):raise ValueError('Invalid output')
                if any(r not in corpus.blocks for r in refs):raise ValueError('Unknown source')
                if verdict in ('supported','refuted'):
                    if not refs or not atom.get('evidence') or not atom.get('counterevidence'):raise ValueError('Evidence and counterevidence review required')
                    if any(r not in corpus.seen|corpus.viewed for r in refs):raise ValueError('Read cited source first')
                    if any(corpus.blocks[r]['kind']!='TEXT' and r not in current_images for r in refs):raise ValueError('Reopen graphic before deciding; prior hash is not pixels')
            valid.append(item)
        except (TypeError,ValueError,KeyError) as e:errors.append({'assessment':item,'error':str(e)})
    return valid,errors


def update_assessment_cache(assessments, valid, errors):
    """A rejected revision invalidates an earlier provisional candidate result.

    Raw responses retain the older result for inspection; it cannot satisfy the
    completion gate until a later revision passes validation.
    """
    for item in valid:
        assessments[item['candidate_id']] = item
    for error in errors:
        item = error.get('assessment')
        if isinstance(item, dict):
            assessments.pop(item.get('candidate_id'), None)


async def review_job(work,job):
    from backend.app.services.llm import codex_runner,claude_json_runner
    out=work/'review/jobs'/job['id'];out.mkdir(parents=True,exist_ok=False)
    corpus=Corpus(work/'sources'/job['discipline'],out/'images')
    source=next(p for p in read(work/'manifest.json') if p['discipline']==job['discipline'])
    for name,digest in source['files'].items():
        if sha(corpus.source/name)!=digest:raise ValueError('Frozen source changed')
    if sha(work/'runs'/job['origin_run']/'status.json')!=job['origin_sha256']:raise ValueError('Sealed candidates changed')
    messages=[{'role':'system','content':REVIEW_PROMPT+TOOLS},
        {'role':'user','content':json.dumps({'candidates':job['candidates'],'catalogue':corpus.catalogue()},ensure_ascii=False)}]
    state={'status':'running','job':job['id'],'model':MODELS[job['reviewer']],'steps':0,'started':time.time(),'assessments':[],'usage':[],
           'review_assignment':{k:job.get(k) for k in ('origin_run','reviewer','previous_job','original_reviewer','independence_limit')}}
    write(out/'status.json',state)
    attached=[];assessments={}
    def record(attempt):
        with (out/'attempts.jsonl').open('a') as f:f.write(json.dumps(attempt)+'\n')
    try:
        for step in range(12):
            if (work/'STOP_REQUESTED.json').exists():raise RuntimeError('User stop requested')
            current_ids={x['id'] for x in attached}
            context={'step':step+1,'limit':12,'attached_images':[{'id':x['id'],'region':x['region']} for x in attached],
                     'pending_candidate_ids':[c['id'] for c in job['candidates'] if c['id'] not in assessments]}
            messages.append({'role':'user','content':json.dumps(context,ensure_ascii=False)})
            write(out/f'{step:02d}_request.json',{'messages':messages,'attached_images':attached})
            fn=codex_runner.run_codex_json_messages if job['reviewer']=='codex' else claude_json_runner.run_claude_json_messages
            async def guarded(*args, **kwargs):
                assert_allowed(work, kwargs.get('model'))
                return await fn(*args, **kwargs)
            result=await retry_subscription_call(guarded,messages,model=MODELS[job['reviewer']],reasoning_effort='medium',
                timeout=600,stage='classic_candidate_review',project_id=job['id'],allowed_tools='',
                image_paths=[x['path'] for x in attached],stop_requested=lambda:(work/'STOP_REQUESTED.json').exists() or time.time()-state['started']>7200,on_attempt=record)
            write(out/f'{step:02d}_response.json',{'text':result.text,'json':result.json_data,'error':result.error_message})
            state['usage'].append({k:getattr(result,k,None) for k in ('model','input_tokens','output_tokens','duration_ms','cost_usd')})
            if result.is_error:raise RuntimeError(result.error_message)
            data=result.json_data
            if not isinstance(data,dict):raise ValueError('Expected object')
            messages.append({'role':'assistant','content':json.dumps(data,ensure_ascii=False)})
            valid,feedback=validate_assessments(data.get('assessments',[]),job,corpus,current_ids)
            update_assessment_cache(assessments, valid, feedback)
            actions=data.get('actions',[])
            if not isinstance(actions,list) or len(actions)>6:raise ValueError('At most six actions')
            attached=[]
            for action in actions:
                try:
                    evidence,images=corpus.execute(action)
                    for image in images:attached.append({'id':action['id'],'region':action.get('region',[0,0,1,1]),'path':image})
                except (ValueError,KeyError,TypeError,ArithmeticError) as e:evidence={'error':str(e)}
                feedback.append({'action':action,'result':evidence})
            write(out/f'{step:02d}_evidence.json',feedback)
            messages.append({'role':'user','content':json.dumps(feedback,ensure_ascii=False)})
            state.update(steps=step+1,assessments=list(assessments.values()),elapsed_s=time.time()-state['started'])
            write(out/'status.json',state)
            if data.get('done') is True and not actions and len(assessments)==len(job['candidates']):
                state['status']='reviewed_provisional';break
        else:state['status']='incomplete_budget'
    except Exception as e:state.update(status='failed',error=f'{type(e).__name__}: {e}')
    state.update(finished=time.time(),elapsed_s=time.time()-state['started'],pending=[c['id'] for c in job['candidates'] if c['id'] not in assessments])
    write(out/'status.json',state)
    print(job['id'],state['status'],len(assessments),'/',len(job['candidates']),flush=True)


async def run_all(work, jobs_manifest=None):
    configure(work)
    jobs=read(jobs_manifest or work/'review/jobs.json')
    # Existing jobs are never silently overwritten; failures remain visible.
    pending=[j for j in jobs if not (work/'review/jobs'/j['id']).exists()]
    semaphore=asyncio.Semaphore(2)
    async def launch(job):
        async with semaphore:
            if not (work/'STOP_REQUESTED.json').exists():await review_job(work,job)
    await asyncio.gather(*(launch(job) for job in pending))

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('command',choices=['prepare','run']);p.add_argument('--work',type=Path,required=True);p.add_argument('--jobs-manifest',type=Path)
    a=p.parse_args();prepare(a.work) if a.command=='prepare' else asyncio.run(run_all(a.work,a.jobs_manifest))
