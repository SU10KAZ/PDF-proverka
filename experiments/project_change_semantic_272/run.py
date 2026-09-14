"""Two bounded model passes with exact source witnesses and paid-call receipts."""
import argparse
import asyncio
from collections import defaultdict
import json
from pathlib import Path
import statistics
import time

from experiments.project_change_272.inventory import ROOT, REPO, read, immutable, now, sha
from experiments.project_change_272.policy import admitted_pairs
from .packets import BASE, digest, normalize
from .prompts import PROPOSE, VERIFY, REPAIR
from .vision import image_messages
from .access import authorize
from .history import document_history

MODEL = 'openai/gpt-5.4'
ESTIMATED_CALL_CEILING_USD = .30


def view(packet):
    return {k:packet[k] for k in ['packet_id','proposal_kind','proposal_query','coverage_complete']} | {
        'authoritative_object':'Садовническая 76 / Балчуг Эстейт, object 272',
        'comparison_direction':'OLD stage_1 -> NEW stage_2, logical v002 baseline',
        'pair_key':packet['pair_key'],
        'sheet_scopes':packet.get('sheet_scopes',{}),
        'audit_candidate':packet.get('audit_candidate'),
        'evidence': {s:[{k:e[k] for k in ['evidence_id','side','source_kind','route','page','bbox','quote']} |
                        {'requires_visual_scope':e.get('requires_visual_scope',False)}
                        for e in packet['evidence'][s]] for s in ['old','new']}}


def check_event(packet, event, require_primary_confidence=True):
    """Mechanical support gate, deliberately not a semantic truth oracle."""
    required = ['event_id','engineering_subject','identity_basis','old_state','new_state',
                'summary_ru','change_type','confidence','importance','facts']
    if not isinstance(event,dict) or any(k not in event for k in required):
        return ['MALFORMED_EVENT']
    errors=[]
    counter_seed=packet.get('audit_candidate')
    if isinstance(counter_seed,dict) and event['event_id']!=counter_seed.get('event_id'):
        errors.append('COUNTER_AUDIT_CANNOT_INVENT_EVENT')
    if event['change_type'] not in {'SYSTEM_CONFIGURATION_CHANGED','SYSTEM_MODE_CHANGED',
            'CAPACITY_CHANGED','EQUIPMENT_REPLACED','REQUIREMENT_CHANGED','ENGINEERING_SOLUTION_CHANGED'}:
        errors.append('UNSUPPORTED_CHANGE_TYPE')
    if require_primary_confidence and (event['confidence']!='HIGH' or event['importance']!='HIGH'):
        errors.append('UNCERTAIN_OR_LOW_VALUE')
    if event['confidence'] not in {'HIGH','MEDIUM','LOW'} or event['importance'] not in {'HIGH','LOW'}:
        errors.append('INVALID_CONFIDENCE_OR_IMPORTANCE')
    if not all(isinstance(event[k],str) and event[k].strip() for k in required if k!='facts'):
        errors.append('EMPTY_OR_INVALID_FIELD')
    if not isinstance(event['facts'],list) or not event['facts']:
        return errors+['NO_FACTS']
    for f in event['facts']:
        if not isinstance(f,dict) or not all(k in f for k in ['property','old_value','new_value','old_witnesses','new_witnesses']):
            errors.append('MALFORMED_FACT');continue
        if not all(isinstance(f[k],str) and f[k].strip() for k in ['property','old_value','new_value']):
            errors.append('EMPTY_FACT_STATE');continue
        if normalize(f['old_value'])==normalize(f['new_value']):
            errors.append('UNCHANGED_FACT')
        for side in ['old','new']:
            sources={e['evidence_id']:e for e in packet['evidence'][side]}
            witnesses=f[side+'_witnesses']
            if not isinstance(witnesses,list) or not witnesses:
                errors.append('MISSING_'+side.upper()+'_WITNESS');continue
            for w in witnesses:
                if not isinstance(w,dict) or w.get('evidence_id') not in sources:
                    errors.append('WRONG_VERSION_OR_SOURCE');continue
                e=sources[w['evidence_id']]
                if e.get('requires_visual_scope'):
                    errors.append('MIXED_TABLE_CONTEXT_NEEDS_VISUAL_SCOPE')
                if e['document_version']!=packet['source_versions'][side]:
                    errors.append('WRONG_DOCUMENT_VERSION')
                q=w.get('quote')
                if not isinstance(q,str) or len(normalize(q))<12:
                    errors.append('QUOTE_NOT_IN_SOURCE')
                elif e.get('source_kind')=='PDF_RASTER_CROP':
                    if not e.get('visual_audit_required') or not e.get('raster'):
                        errors.append('MISSING_VISUAL_SOURCE_RECEIPT')
                    if w.get('route') not in {'TEXT','TABLE','GRAPHIC'}:
                        errors.append('MISSING_VISUAL_WITNESS_ROUTE')
                elif not isinstance(e['quote'],str) or normalize(q) not in normalize(e['quote']):
                    errors.append('QUOTE_NOT_IN_SOURCE')
    return sorted(set(errors))


def choose(directory, limit):
    """Round robin across ciphers, independent of any model outcome."""
    groups=defaultdict(list)
    for p in sorted((directory/'packets').glob('*.json')):
        groups[read(p)['pair_index']].append(p)
    chosen=[]
    while groups and len(chosen)<limit:
        for index in sorted(list(groups)):
            if len(chosen)<limit:
                chosen.append(groups[index].pop(0))
            if not groups[index]:del groups[index]
    return chosen


def check_packet_scope(packet, allowed, partition):
    pair=allowed.get(packet.get('pair_index'))
    if pair is None or packet.get('partition')!=partition or packet.get('pair_key')!=pair['pair_key']:
        raise PermissionError('Packet outside admitted cipher partition')
    for side in ['old','new']:
        doc=pair[side]
        if packet['source_versions'][side]!=doc['document_version']:
            raise PermissionError('Packet from a different document version')
        for e in packet['evidence'][side]:
            receipt=e['source_receipt'];source=doc['artifacts']['pdf']
            if e['side']!=side or e['document_version']!=doc['document_version'] or e['page'] in pair['embargo_pages'][side]:
                raise PermissionError('Wrong-side or embargoed packet evidence')
            if receipt['sha256']!=source['sha256'] or Path(receipt['path']).resolve()!=Path(source['path']).resolve():
                raise PermissionError('Foreign or unpinned packet PDF')


async def run(name, directory, limit=13, partition='DEV', candidate=None, proposal_prompt=None):
    allowed={p['index']:p for p in authorize(partition,candidate)}
    history={i:{s:document_history(p[s],p['embargo_pages'][s]) for s in ['old','new']} for i,p in allowed.items()}
    if read(directory/'MANIFEST.json')['partition']!=partition:
        raise PermissionError('Wrong packet partition')
    if not (directory/'COUNTS.json').exists():
        raise PermissionError('Packet preparation did not complete')
    counts=read(directory/'COUNTS.json')
    expected=counts['packets'] if 'packets' in counts else sum(counts.values())
    if len(list((directory/'packets').glob('*.json')))!=expected:
        raise ValueError('Prepared packet count drift')
    out=BASE/'runs'/name
    selected=choose(directory,limit)
    manifest=dict(started_at=now(),model=MODEL,partition=partition,
        candidate_manifest=str(candidate) if candidate else None,
        split_sha256=sha(ROOT/'SPLIT.json'),packets={str(p):sha(p) for p in selected},
        code={str(p.relative_to(REPO)):sha(p) for p in Path(__file__).parent.glob('*.py')},
        max_calls=4*len(selected),estimated_max_cost_usd=4*len(selected)*ESTIMATED_CALL_CEILING_USD,
        authorization_basis='Source request permits local semantic packets, model calls and cost tracking; active repository paid API guard; no whole projects sent',
        semantic_review='Separate model call, same model; not independent human adjudication')
    manifest.update(max_request_text_characters=60000,max_source_images_per_call=8)
    primary_prompt=proposal_prompt or PROPOSE
    manifest['proposal_prompt_sha256']=digest(primary_prompt)
    immutable(out/'MANIFEST.json',manifest)
    for p in Path(__file__).parent.glob('*.py'):
        t=out/'code'/p.name;t.parent.mkdir(parents=True,exist_ok=True);t.write_bytes(p.read_bytes())
    from openai import AsyncOpenAI
    from backend.app.core.config import OPENROUTER_API_KEY, OPENROUTER_BASE_URL
    from backend.app.services.llm.paid_api_guard import PaidApiContext,reserve_paid_api,release_reservation
    from backend.app.services.common.usage_service import paid_cost_tracker
    client=AsyncOpenAI(api_key=OPENROUTER_API_KEY,base_url=OPENROUTER_BASE_URL,max_retries=0,timeout=120)
    calls=[]

    async def call(packet,stage,system,data):
        data_text=json.dumps(data,ensure_ascii=False)
        if len(system)+len(data_text)>60000:
            raise ValueError('Bounded local request text exceeded 60000 characters')
        text_messages=[dict(role='system',content=system),dict(role='user',content=data_text)]
        images,image_receipts=image_messages(packet)
        messages=[text_messages[0],dict(role='user',content=[dict(type='text',text=data_text)]+images)] if images else text_messages
        request_hash=digest(dict(model=MODEL,messages=text_messages,images=image_receipts))
        target=out/'calls'/(packet['packet_id']+'_'+stage+'.json')
        immutable(out/'requests'/(packet['packet_id']+'_'+stage+'.json'),dict(request_hash=request_hash,messages=text_messages,images=image_receipts))
        reservation=reserve_paid_api(PaidApiContext(source='offline_research.project_change_272',model=MODEL,
            project_id='272_Sadovnicheskaya_76_Balchug_Esteyt',stage='semantic_'+stage,
            job_id=packet['packet_id'],estimated_cost_usd=ESTIMATED_CALL_CEILING_USD))
        tick=time.monotonic()
        try:
            response=await client.chat.completions.create(model=MODEL,messages=messages,
                max_tokens=6500 if stage in {'propose','repair'} else 2500,temperature=0,
                response_format={'type':'json_object'},
                extra_body={'reasoning':{'effort':'low'},'provider':{'data_collection':'deny'}})
            usage=response.usage.model_dump() if response.usage else {}
            cost=usage.get('cost')
            receipt=dict(packet_id=packet['packet_id'],stage=stage,request_hash=request_hash,model=MODEL,
                response_id=response.id,response=response.choices[0].message.content or '',usage=usage,
                finish_reason=response.choices[0].finish_reason,network_call=True,
                seconds=time.monotonic()-tick,input_characters=len(system)+len(data_text),
                source_images=len(image_receipts),provider_cost_usd=cost)
            immutable(target,receipt);calls.append(receipt)
            # Record known provider cost; never invent actual cost from a bound.
            if isinstance(cost,(float,int)) and cost>0:
                paid_cost_tracker.record_paid(cost,model=MODEL,project_id='272_Sadovnicheskaya_76_Balchug_Esteyt',
                    stage='semantic_'+stage,source='offline_research.project_change_272',job_id=packet['packet_id'],
                    input_tokens=usage.get('prompt_tokens',0),output_tokens=usage.get('completion_tokens',0),
                    response_id=response.id,extra={'research_receipt':str(target)})
            if receipt['finish_reason']!='stop':return {}
            try:return json.loads(receipt['response'])
            except (ValueError,TypeError):return {}
        except Exception as exc:
            # Do not log provider exception strings, headers or credentials.
            error=dict(packet_id=packet['packet_id'],stage=stage,error_type=type(exc).__name__,
                       reason=getattr(exc,'reason','provider_call_failed'),seconds=time.monotonic()-tick)
            if not target.exists():immutable(target,error)
            raise
        finally:release_reservation(reservation)

    results=[]
    sem=asyncio.Semaphore(3)

    async def one(path):
        if sha(path)!=manifest['packets'][str(path)]:raise ValueError('Packet drift')
        p=read(path)
        check_packet_scope(p,allowed,partition)
        if any(e['page'] in history[p['pair_index']][s] for s in ['old','new'] for e in p['evidence'][s]):
            raise PermissionError('Revision-history source cannot witness a current OLD/NEW state')
        if digest({k:v for k,v in p.items() if k!='packet_id'})[:24]!=p['packet_id']:
            raise ValueError('Packet hash mismatch')
        proposed=await call(p,'propose',primary_prompt,view(p))
        events=proposed.get('events',[]) if isinstance(proposed,dict) else []
        if not isinstance(events,list):events=[]
        mechanical=[dict(event=e,errors=check_event(p,e,False)) for e in events]
        event_ids=[e.get('event_id') for e in events if isinstance(e,dict) and isinstance(e.get('event_id'),str)]
        for row in mechanical:
            if isinstance(row['event'],dict) and event_ids.count(row['event'].get('event_id'))>1:
                row['errors'].append('DUPLICATE_EVENT_ID')
        clean=[r['event'] for r in mechanical if not r['errors']]
        reviewed=await call(p,'verify',VERIFY,dict(packet=view(p),proposals=clean)) if clean else {}
        decisions=reviewed.get('decisions',[]) if isinstance(reviewed,dict) else []
        if not isinstance(decisions,list):decisions=[]
        # One bounded repair may remove unsupported detail. It never
        # bypasses a fresh exact-witness and semantic audit.
        repairable=[e for e in clean if any(isinstance(d,dict) and d.get('event_id')==e['event_id'] and d.get('verdict')=='REVIEW' and d.get('scope_correct') is True and d.get('material_change') is True for d in decisions)]
        if repairable:
            repair=await call(p,'repair',REPAIR,dict(packet=view(p),proposals=repairable,audit=decisions))
            repaired=repair.get('events',[]) if isinstance(repair,dict) else []
            if not isinstance(repaired,list):repaired=[]
            repair_ids={e['event_id'] for e in repairable}
            repaired=[e for e in repaired if isinstance(e,dict) and e.get('event_id') in repair_ids and not check_event(p,e,False)]
            if repaired:
                second=await call(p,'verify_repair',VERIFY,dict(packet=view(p),proposals=repaired))
                ds=second.get('decisions',[]) if isinstance(second,dict) else []
                if isinstance(ds,list):
                    replacements={e['event_id']:e for e in repaired}
                    for row in mechanical:
                        if isinstance(row['event'],dict) and row['event'].get('event_id') in replacements:
                            row['original_event']=row['event'];row['event']=replacements[row['event']['event_id']]
                            row['errors']=[]
                    decisions=[d for d in decisions if isinstance(d,dict) and d.get('event_id') not in replacements]+ds
        for row in mechanical:
            event=row['event'];matches=[d for d in decisions if isinstance(event,dict) and isinstance(d,dict) and d.get('event_id')==event.get('event_id')]
            d=matches[0] if len(matches)==1 else {}
            accepted=not row['errors'] and d.get('verdict')=='ACCEPT' and all(d.get(k) is True for k in ['scope_correct','states_entailed','material_change','grouping_correct'])
            row.update(status='ACCEPTED_CANDIDATE' if accepted else 'REVIEW',semantic_audit=d,
                evidence_scope=dict(packet_id=p['packet_id'],pair_key=p['pair_key'],coverage_complete=False))
        result=dict(packet_id=p['packet_id'],pair_index=p['pair_index'],events=mechanical,
                    unknowns=proposed.get('unknowns',[]) if isinstance(proposed,dict) else [])
        immutable(out/'results'/(p['packet_id']+'.json'),result);results.append(result)
        print(p['pair_index'],p['packet_id'],'proposed',len(mechanical),'accepted',sum(r['status']=='ACCEPTED_CANDIDATE' for r in mechanical),flush=True)

    async def bounded_one(path):
        async with sem:
            await one(path)

    try:
        outcomes=await asyncio.gather(*(bounded_one(path) for path in selected),return_exceptions=True)
        errors=[e for e in outcomes if isinstance(e,BaseException)]
        if errors:raise errors[0]
    finally:
        await client.close()
        sizes=[c['input_characters'] for c in calls]
        costs=[c['provider_cost_usd'] for c in calls if isinstance(c['provider_cost_usd'],(int,float))]
        immutable(out/'RUN_RECEIPT.json',dict(completed_packets=len(results),selected_packets=len(selected),
            calls=len(calls),input_tokens=sum(c['usage'].get('prompt_tokens',0) for c in calls),
            output_tokens=sum(c['usage'].get('completion_tokens',0) for c in calls),
            median_input_characters=statistics.median(sizes) if sizes else None,
            p95_input_characters=sorted(sizes)[max(0,int(len(sizes)*.95)-1)] if sizes else None,
            known_provider_cost_usd=sum(costs),calls_with_unknown_cost=len(calls)-len(costs),
            results=results,adjudication='NOT_SOURCE_ADJUDICATED'))


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--name',required=True)
    p.add_argument('--packets',type=Path,default=BASE/'dev_visual_packets_v3');p.add_argument('--limit',type=int,default=13)
    p.add_argument('--partition',choices=['DEV','VALIDATION','FINAL_HOLDOUT'],default='DEV')
    p.add_argument('--candidate',type=Path)
    a=p.parse_args();asyncio.run(run(a.name,a.packets,a.limit,a.partition,a.candidate))
