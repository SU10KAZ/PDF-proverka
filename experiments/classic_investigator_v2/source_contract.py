"""Host-owned source-delivery contract. Access is never proof of engineering review."""
import argparse
import hashlib
import json
from pathlib import Path
import sys
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'classic_investigator'))
from investigate import Corpus,read,write,sha


def uncovered(length,ranges):
    end=0;missing=[]
    for start,stop in sorted(ranges):
        start=max(0,min(length,start));stop=max(start,min(length,stop))
        if start>end:missing.append((end,start))
        end=max(end,stop)
    if end<length:missing.append((end,length))
    return missing


def delivered_entries(run):
    """Only real successful request/response pairs, not actions after the last turn.

    Graphic evidence requires bytes on the same request as its receipt. Merely
    retaining an old image hash in conversation does not count as attachment.
    """
    entries=[];seen=set()
    for path in sorted(run.glob('*_request.json')):
        response_path=path.with_name(path.name.replace('_request','_response'))
        if not response_path.exists():continue
        response=read(response_path)
        if response.get('error') or not isinstance(response.get('json'),dict):continue
        request=read(path);images={sha(p) for p in request.get('images',[]) if Path(p).is_file()}
        for message in request.get('messages',[]):
            if message.get('role')!='user':continue
            try:data=json.loads(message.get('content',''))
            except (ValueError,TypeError):continue
            if not isinstance(data,list):continue
            for entry in data:
                if not isinstance(entry,dict):continue
                action=entry.get('action',{});result=entry.get('result',{})
                if not isinstance(action,dict) or not isinstance(result,dict) or result.get('error'):continue
                if action.get('tool')=='image' and result.get('image_sha256') not in images:continue
                key=json.dumps(entry,sort_keys=True,ensure_ascii=False)
                if key in seen:continue
                seen.add(key);entries.append({'entry':entry,'request':path.name,'request_sha256':sha(path)})
    return entries


def assess(corpus,entries):
    ranges={};full_images=set();partial_images=set();receipts=[]
    for record in entries:
        entry=record['entry'];a=entry.get('action',{});r=entry.get('result',{});bid=a.get('id')
        if bid not in corpus.blocks:continue
        block=corpus.blocks[bid]
        if a.get('tool')=='read' and block['kind']=='TEXT' and not r.get('navigation_only'):
            start=int(a.get('offset',0));text=r.get('text','')
            if start<0 or not text or text!=block['text'][start:start+len(text)]:continue
            ranges.setdefault(bid,[]).append((start,start+len(text)));receipts.append(record)
        if a.get('tool')=='image' and block['kind']!='TEXT':
            region=r.get('region')
            if region==[0,0,1,1]:full_images.add(bid)
            else:partial_images.add(bid)
            receipts.append(record)
    tasks=[];text_full=0
    for bid,b in corpus.blocks.items():
        if b['kind']=='TEXT':
            missing=uncovered(len(b['text']),ranges.get(bid,[]))
            if not missing:text_full+=1
            for start,stop in missing:
                for offset in range(start,stop,12000):
                    tasks.append({'kind':'text_delivery','block_id':bid,'offset':offset,'end':min(stop,offset+12000),
                                  'action':{'tool':'read','id':bid,'offset':offset}})
        elif bid not in full_images:
            tasks.append({'kind':'graphic_delivery','block_id':bid,'prior_partial_crop':bid in partial_images,
                          'action':{'tool':'image','id':bid,'region':[0,0,1,1]}})
    for task in tasks:
        task['task_id']=hashlib.sha256(json.dumps(task,sort_keys=True).encode()).hexdigest()[:20]
    return {'source_delivery_complete':not tasks,'text_fully_delivered':text_full,'graphics_full_image_delivered':len(full_images),
            'pending_tasks':tasks,'successful_delivery_receipts':receipts,
            'engineering_review_complete':False,
            'limits':'Successful response and matching source delivery only. Not proof of correct reading, all objects discovered, executed operations, valid findings or release readiness.'}


def completion(source_report,operations):
    # A declaration or a cited source is not an execution receipt.
    unresolved=[op.get('id') for op in operations if op.get('status')!='performed' or not op.get('execution_receipt')]
    return {'can_claim_complete':source_report['source_delivery_complete'] and not unresolved and bool(operations),
            'unresolved_operation_ids':unresolved,'operation_inventory_exists':bool(operations),
            'limits':'Procedural gate only. Receipt correctness and inventory completeness require separate checks. needs_data/out_of_scope remain explicit, not checked.'}


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--work',type=Path,required=True);a=p.parse_args();rows=[]
    for run in sorted((a.work/'runs').iterdir()):
        if not (run/'status.json').exists():continue
        disc=run.name.split('_')[0];c=Corpus(a.work/'sources'/disc,a.work/'evaluation/source_contract_scratch')
        result=assess(c,delivered_entries(run));result.update(run=run.name,status_sha256=sha(run/'status.json'),source_md_sha256=sha(c.source/'document.md'))
        write(a.work/'evaluation/source_contract'/f'{run.name}.json',result)
        rows.append({k:result[k] for k in ['run','source_delivery_complete','text_fully_delivered','graphics_full_image_delivered'] }|{'pending_delivery_tasks':len(result['pending_tasks'])})
    write(a.work/'evaluation/source_contract/summary.json',rows)
    print(json.dumps(rows,ensure_ascii=False,indent=2))
