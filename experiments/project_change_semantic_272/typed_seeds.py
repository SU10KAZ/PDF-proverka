"""Typed states supply retrieval seeds, never bypass semantic source checks."""
import argparse
from collections import Counter
from pathlib import Path

from experiments.project_change_272.inventory import ROOT,read,immutable,sha,now
from .access import prepared_pairs
from .packets import BASE,sources,packet,evidence,bounded,digest,add_sheet_scopes
from .history import document_history


def source_pages(change,side):
    pages=Counter()
    for e in change['evidence_'+side]:
        locator=e.get('locator') or {}
        if locator.get('page'):pages[locator['page']]+=1
        for ref in e.get('source_refs',[]):
            if ref.get('page'):pages[ref['page']]+=1
    return [p for p,n in sorted(pages.items(),key=lambda r:(-r[1],r[0]))]


def prepare(name,typed_run,partition='DEV',candidate=None):
    pairs={p['index']:p for p in prepared_pairs(partition,candidate)}
    origin=ROOT/'runs'/typed_run
    if read(origin/'MANIFEST.json')['partition']!=partition:raise PermissionError('Wrong typed source partition')
    result=read(origin/'RESULTS.json')
    if {p['index'] for p in result['pairs']}!=set(pairs):raise PermissionError('Typed run lacks complete cipher allocation')
    out=BASE/name
    immutable(out/'MANIFEST.json',dict(created_at=now(),partition=partition,split_sha256=sha(ROOT/'SPLIT.json'),
        code_sha256=sha(__file__),typed_source_sha256=sha(origin/'RESULTS.json'),
        candidate_manifest=str(candidate) if candidate else None,
        policy='Typed outputs are untrusted retrieval seeds; no typed acceptance bypass; original same-version source validation required'))
    pools={};history={};count=0
    for r in result['project_changes']:
        c=r['change'];index=r['pair_index']
        if c['status']!='PROVEN':continue
        pair=pairs[index]
        if c['comparison_scope']!=pair['pair_key']:raise PermissionError('Typed state belongs to a different cipher')
        if index not in pools:
            history[index]={s:document_history(pair[s],pair['embargo_pages'][s]) for s in ['old','new']}
            pools[index]={s:[e for e in sources(pair[s],pair['embargo_pages'][s]) if e['page'] not in history[index][s]] for s in ['old','new']}
            immutable(out/'history_quarantine'/(str(index)+'.json'),history[index])
        query='\n'.join([str(c['engineering_subject']),c['short_summary_ru'],'OLD: '+c['old_state'],'NEW: '+c['new_state']])
        new_pages=source_pages(c,'new')
        p=packet(pair,query,pools[index],'TYPED_STATE_SEED',dict(typed_event=c['project_change_id'],
            new_anchor_page=new_pages[0] if new_pages else None))
        p.pop('packet_id')
        for side in ['old','new']:
            for e in c['evidence_'+side]:
                if e['document_version']!=pair[side]['document_version']:raise PermissionError('Typed witness from wrong version')
            anchors=source_pages(c,side)
            if any(page in pair['embargo_pages'][side] or page in history[index][side] for page in anchors):
                raise PermissionError('Typed seed points into embargo/history')
            # Prioritize the complete source scope of the deterministic witness;
            # its value and status do not enter the evidence as trusted facts.
            scoped=[evidence(e,side) for page in anchors[:2] for e in pools[index][side] if e['page']==page]
            seen={e['evidence_id'] for e in scoped}
            p['evidence'][side]=bounded(scoped+[e for e in p['evidence'][side] if e['evidence_id'] not in seen],11000)
        p['typed_seed_receipt']=dict(path=str(origin/'RESULTS.json'),sha256=sha(origin/'RESULTS.json'),event_id=c['project_change_id'],is_truth=False)
        add_sheet_scopes(pair,p)
        p['packet_id']=digest(p)[:24]
        immutable(out/'packets'/(p['packet_id']+'.json'),p);count+=1
    immutable(out/'COUNTS.json',dict(packets=count))
    print(out,count,flush=True)
    return out


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--name',required=True);p.add_argument('--typed-run',required=True)
    p.add_argument('--partition',default='DEV',choices=['DEV','VALIDATION','FINAL_HOLDOUT']);p.add_argument('--candidate',type=Path)
    a=p.parse_args();prepare(a.name,a.typed_run,a.partition,a.candidate)
