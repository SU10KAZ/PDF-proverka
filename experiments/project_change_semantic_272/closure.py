"""Counter-search all admitted native blocks before accepting a local change."""
import argparse
import asyncio
from copy import deepcopy
from pathlib import Path

from experiments.project_change_272.inventory import ROOT,read,immutable,sha,now
from .access import prepared_pairs
from .packets import BASE,sources,packet,digest
from .history import document_history
from .admission import decide
from .prompts import PROPOSE
from .run import run

CLOSURE_PROMPT = PROPOSE + '''
This is a COUNTER-EVIDENCE pass. Audit ONLY audit_candidate, which is an untrusted
proposal from an earlier local search. Preserve its event_id and original subject.
Do not generate unrelated changes. The source pools were searched across the same
complete document pair using the proposed OLD and NEW states as retrieval queries.
Explicitly check whether OLD ALREADY contains the purported NEW state elsewhere,
whether NEW retains the OLD state, and whether calculations refer to different
conditions, aggregate/component scopes, consumer groups or functions. A short
legend description is NOT an exhaustive description of a system's OLD coverage.
The main body can contradict an inference from a drawing caption. Compare only
commensurate engineering states; a total is not the state of one branch.
Changing a survey edition date, an agreement reference, moving a description to
another volume, or deleting a statement from text does not by itself establish a
changed engineering design. State the actual changed design requirement/solution.
If the proposed change is contradicted, unchanged, only administrative, or lacks
same-subject positive states, return events=[] and explain in unknowns/unchanged.
Do not promote missing evidence to absence. If a narrower useful change survives,
remove unsupported detail and retain only the independently supported facts.
Every retained fact still needs exact native or visually verified witnesses.'''


def prepare(name,source_run,source_packets,partition='DEV',candidate=None):
    pairs={p['index']:p for p in prepared_pairs(partition,candidate)}
    original=BASE/'runs'/source_run
    receipt=read(original/'RUN_RECEIPT.json')
    if receipt['completed_packets']!=receipt['selected_packets']:
        raise ValueError('Cannot evaluate an incomplete source run as a full candidate')
    if (original/'QUALITY_STATUS.json').exists():
        raise ValueError('Quarantined source run cannot feed acceptance')
    out=BASE/name
    immutable(out/'MANIFEST.json',dict(created_at=now(),partition=partition,
        split_sha256=sha(ROOT/'SPLIT.json'),code_sha256=sha(__file__),
        source_run_receipt_sha256=sha(original/'RUN_RECEIPT.json'),
        source_run=source_run,candidate_manifest=str(candidate) if candidate else None,
        purpose='Search all admitted native blocks for counter-state evidence; original raster witnesses retained',
        coverage='Full native pools searched; top local scopes selected; unrecognized raster-only statements remain an explicit residual risk'))
    pools={};ledger=[]
    for r in sorted(receipt['results'],key=lambda r:(r['pair_index'],r['packet_id'])):
        index=r['pair_index'];pair=pairs[index]
        if index not in pools:
            history={s:document_history(pair[s],pair['embargo_pages'][s]) for s in ['old','new']}
            pools[index]={s:[e for e in sources(pair[s],pair['embargo_pages'][s])
                if e['page'] not in history[s]] for s in ['old','new']}
        origin=read(BASE/source_packets/'packets'/(r['packet_id']+'.json'))
        for row in r['events']:
            if row['status']!='ACCEPTED_CANDIDATE':continue
            event=row['event'];policy=decide(event)
            trace=dict(pair_index=index,source_packet=r['packet_id'],source_event=event['event_id'],admission=policy)
            if policy['status']!='ACCEPTED_FOR_SOURCE_AUDIT':
                ledger.append(trace);continue
            query=' '.join(str(event.get(k,'')) for k in ['engineering_subject','old_state','new_state','summary_ru'])
            p=packet(pair,query,pools[index],'COUNTER_STATE_SEARCH',dict(source_packet=r['packet_id']))
            p.pop('packet_id')
            p['audit_candidate']=deepcopy(event)
            p['origin_candidate']=dict(packet_id=r['packet_id'],event_id=event['event_id'])
            p['native_search_pool_counts']={s:len(pools[index][s]) for s in ['old','new']}
            for side in ['old','new']:
                witnesses={w['evidence_id'] for f in event['facts'] for w in f[side+'_witnesses']}
                seen={e['evidence_id'] for e in p['evidence'][side]}
                for e in origin['evidence'][side]:
                    if e['evidence_id'] not in seen and (e['evidence_id'] in witnesses or e.get('source_kind')=='PDF_RASTER_CROP'):
                        p['evidence'][side].append(e);seen.add(e['evidence_id'])
            p['packet_id']=digest(p)[:24]
            immutable(out/'packets'/(p['packet_id']+'.json'),p)
            trace['closure_packet_id']=p['packet_id'];ledger.append(trace)
    immutable(out/'ADMISSION_LEDGER.json',ledger)
    print(out,'closure packets',sum('closure_packet_id' in r for r in ledger),flush=True)
    return out


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('action',choices=['prepare','run'])
    p.add_argument('--name',default='dev_closure_packets_v1')
    p.add_argument('--source-run',default='dev_multimodal_v1')
    p.add_argument('--source-packets',default='dev_visual_packets_v3')
    p.add_argument('--output-run',default='dev_closure_v1')
    p.add_argument('--partition',default='DEV',choices=['DEV','VALIDATION','FINAL_HOLDOUT'])
    p.add_argument('--candidate',type=Path)
    a=p.parse_args()
    if a.action=='prepare':prepare(a.name,a.source_run,a.source_packets,a.partition,a.candidate)
    else:asyncio.run(run(a.output_run,BASE/a.name,1000,a.partition,a.candidate,proposal_prompt=CLOSURE_PROMPT))
