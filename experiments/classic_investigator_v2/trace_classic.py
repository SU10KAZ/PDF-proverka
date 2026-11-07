"""Trace classic input candidates into each output snapshot, without guessing semantics."""
import argparse
from collections import Counter
from pathlib import Path
from source_contract import read,write,sha


def source_candidates(graph,text):
    rows=[]
    for block in graph.get('block_analyses',[]):
        for finding in block.get('findings',[]):
            rows.append({'origin':'graphic','block_id':block.get('block_id'),'candidate':finding})
    rows.extend({'origin':'text','candidate':f} for f in text.get('text_findings',[]))
    return rows


def trace(candidates,final):
    counts=Counter(row['candidate'].get('id') for row in candidates)
    ambiguities=[key for key,count in counts.items() if not key or count>1]
    rows=[]
    for row in candidates:
        candidate=row['candidate'];cid=candidate.get('id')
        linked=[] if final is None else [f for f in final if cid and cid in (f.get('source_finding_ids') or [])]
        status='awaiting_stage_output' if final is None else 'id_link_requires_semantic_review' if linked else 'unaccounted_requires_disposition'
        if cid in ambiguities:status='ambiguous_source_id'
        rows.append({**row,'status':status,'linked_outputs':linked,'semantic_preservation_verified':False,
                     'technical_status':'not_inferred_from_trace'})
    return {'rows':rows,'counts':dict(Counter(r['status'] for r in rows)),
            'ambiguous_source_ids':ambiguities,
            'reused_source_candidates':[
                {'source_id':r['candidate']['id'],
                 'output_ids':[f.get('id') for f in r['linked_outputs']],
                 'review_status':'check_duplicate_or_valid_atom_split',
                 'automatic_removal_allowed':False}
                for r in rows if r['candidate'].get('id') not in ambiguities
                and len(r['linked_outputs'])>1],
            'limits':'ID linkage is navigation, not proof that every atom survived. Unlinked is not proof of technical falsity or actual loss. Split, merged, reworded and removed candidates require explicit semantic disposition.'}


def run(work,disc):
    root=work/'classic_control';project=next(p for p in read(root/'input_manifest.json') if p['discipline']==disc);out=Path(project['output_dir'])
    graph=read(out/'01_blocks_analysis.json');text=read(out/'02_text_analysis.json') if (out/'02_text_analysis.json').exists() else {}
    candidates=source_candidates(graph,text)
    artifacts={'01_blocks_analysis.json':sha(out/'01_blocks_analysis.json')}
    if text:artifacts['02_text_analysis.json']=sha(out/'02_text_analysis.json')
    compact=read(out/'01_blocks_for_text.json');compact_ids={f.get('id') for b in compact.get('blocks',[]) for f in b.get('findings',[])}
    stages={}
    for name in ['findings_merge','findings_verify']:
        path=root/'snapshots'/disc/f'{name}.json'
        if path.exists():
            data=read(path)
            if not isinstance(data.get('findings'),list):raise ValueError('Unknown final findings schema')
            stages[name]=trace(candidates,data['findings']);artifacts[str(path)]=sha(path)
        else:stages[name]=trace(candidates,None)
    result={'discipline':disc,'input_counts':dict(Counter(r['origin'] for r in candidates)),
        'graphic_ids_absent_from_compact':[r['candidate'].get('id') for r in candidates if r['origin']=='graphic' and r['candidate'].get('id') not in compact_ids],
        'source_artifact_hashes':artifacts,'stages':stages,'scope':'Classic premerge candidates; raw detector proposals suppressed before block output are outside this trace and remain in request/response logs.'}
    write(work/'evaluation/classic_trace'/f'{disc}.json',result)
    print(disc,result['input_counts'],{k:v['counts'] for k,v in stages.items()})

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--work',type=Path,required=True);p.add_argument('--discipline',required=True);a=p.parse_args();run(a.work,a.discipline)
