"""Bounded local proposal calls with repository paid-API guard and disk replay.

No production entry points/flags are changed. Only local packets are transmitted;
API keys and request headers are never written to audit artifacts.
"""
import asyncio
import json
import time
from dataclasses import asdict
from experiments.text_comparison_v1.common import read,write,digest
from .run import ROOT,verify
from .decisions import SYSTEM_PROMPT,exact_decision,validate_decision,fallback

MODEL='openai/gpt-5.4'
MAX_OUTPUT_TOKENS=2400
CONCURRENCY=3


def payload(packet):
    def view(x):
        return {k:x[k] for k in ('unit_id','text','headings','context','engineering_marks','equipment_classes')}
    return dict(new=view(packet['new']),old_candidates=[view(x) for x in packet['old_candidates']],
                full_old_scope_coverage_proven=False)


async def run(root=ROOT):
    verify(root)
    from openai import AsyncOpenAI
    from backend.app.core.config import OPENROUTER_API_KEY,OPENROUTER_BASE_URL
    from backend.app.services.llm.paid_api_guard import PaidApiContext,reserve_paid_api,release_reservation
    client=AsyncOpenAI(api_key=OPENROUTER_API_KEY,base_url=OPENROUTER_BASE_URL,max_retries=0,timeout=90)
    semaphore=asyncio.Semaphore(CONCURRENCY);reservations=[]
    async def one(path):
        p=read(path);out=root/'decisions'/(path.stem+'.json');rawpath=root/'model_calls'/(path.stem+'.json')
        if out.exists():
            d=read(out);assert d['packet_hash']==p['packet_hash'];return
        exact=exact_decision(p)
        if exact:
            write(out,exact);return
        if not p['old_candidates']:
            write(out,fallback(p,'OLD retrieval returned no source context'));return
        async with semaphore:
            # Keep reservations until the batch ends so cumulative offline cost
            # cannot evade the existing daily ceiling when calls finish quickly.
            ctx=PaidApiContext(source='offline_research.old_scope_recovery',model=MODEL,
                project_id='corpus-audits/20260913_text_old_scope_recovery_v1',stage='old_scope_recovery',
                job_id=p['project_change_id'],estimated_cost_usd=.15)
            try:
                reservation=reserve_paid_api(ctx);reservations.append(reservation)
            except Exception as e:
                write(rawpath,dict(model=MODEL,packet_hash=p['packet_hash'],guard_blocked=True,error_type=type(e).__name__,reason=getattr(e,'reason','guard_error')))
                write(out,fallback(p,'Repository paid API guard blocked request: '+getattr(e,'reason','guard_error')))
                print(path.stem,'GUARD_BLOCKED',getattr(e,'reason','guard_error'),flush=True)
                return
            messages=[dict(role='system',content=SYSTEM_PROMPT),dict(role='user',content=json.dumps(payload(p),ensure_ascii=False))]
            started=time.perf_counter()
            try:
                response=await client.chat.completions.create(model=MODEL,messages=messages,
                    response_format={'type':'json_object'},max_tokens=MAX_OUTPUT_TOKENS,temperature=0,
                    extra_body={'reasoning':{'effort':'low'},'provider':{'data_collection':'deny'}})
                body=response.choices[0].message.content or ''
                usage=response.usage.model_dump() if response.usage else {}
                record=dict(model=MODEL,packet_hash=p['packet_hash'],request_hash=digest(messages),
                            local_input_characters=sum(len(m['content']) for m in messages),response=body,
                            usage=usage,finish_reason=response.choices[0].finish_reason,seconds=time.perf_counter()-started)
                write(rawpath,record)
                try:proposal=json.loads(body)
                except (ValueError,TypeError):proposal={}
                d=validate_decision(p,proposal);d['method']='LOCAL_AI_VALIDATED'
                write(out,d)
                print(path.stem,d['decision'],d['confidence'],usage.get('prompt_tokens'),usage.get('completion_tokens'),flush=True)
            except Exception as e:
                # Error class only: exception text may contain provider request data.
                write(rawpath,dict(model=MODEL,packet_hash=p['packet_hash'],error_type=type(e).__name__,seconds=time.perf_counter()-started))
                write(out,fallback(p,'Local provider call failed: '+type(e).__name__))
                print(path.stem,'PROVIDER_ERROR',type(e).__name__,flush=True)
    try:await asyncio.gather(*(one(p) for p in sorted((root/'packets').glob('*.json'))))
    finally:
        for reservation in reservations:release_reservation(reservation)
        await client.close()


if __name__=='__main__':asyncio.run(run())
