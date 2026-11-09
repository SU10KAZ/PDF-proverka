"""Build a candidate-to-final lineage inventory; no unlinked candidate is presumed lost."""
import hashlib
import json
from collections import Counter, defaultdict
from pathlib import Path
HERE = Path(__file__).resolve().parent
BASE = Path('/home/coder/auditmanager/corpus-audits/20261106_classic_investigation/classic_control/inputs')

def read(p): return json.loads(p.read_text())
def ref(p): return {'path':str(p),'sha256':hashlib.sha256(p.read_bytes()).hexdigest()}

def main():
    projects=[]
    for disc in ('AK','ITP','AR','KM','VK','EOM'):
        finals=list((BASE/disc/'versions').glob('*/03_analysis/runs/control/03_findings.json'))
        assert len(finals)==1
        fp=finals[0];out=fp.parent;tp=out/'02_text_analysis.json';bp=out/'01_blocks_analysis.json'
        final=read(fp)['findings'];text=read(tp)['text_findings'];blocks=read(bp)['block_analyses']
        candidates=[];deferred=[]
        for i,item in enumerate(text):
            candidates.append({'source_id':item.get('id'),'origin':'text','pointer':f'/text_findings/{i}','finding':item})
        for i,block in enumerate(blocks):
            for j,item in enumerate(block.get('findings',[])):
                candidates.append({'source_id':item.get('id'),'origin':'graphic','block_id':block['block_id'],
                                   'pointer':f'/block_analyses/{i}/findings/{j}','finding':item})
            for j,item in enumerate(block.get('deferred_findings',[])):
                deferred.append({'block_id':block['block_id'],'pointer':f'/block_analyses/{i}/deferred_findings/{j}','finding':item})
        linked=defaultdict(list)
        for f in final:
            for sid in f.get('source_finding_ids',[]): linked[sid].append(f['id'])
        ids=Counter(c['source_id'] for c in candidates if c['source_id'])
        for c in candidates:
            cid=c['source_id'];c['linked_final_ids']=linked.get(cid,[]) if cid else []
            c['identity_ambiguous']=not cid or ids[cid]>1
            c['status']='ambiguous_source_id' if c['identity_ambiguous'] else ('explicitly_linked_not_semantically_verified' if c['linked_final_ids'] else 'unlinked_requires_semantic_trace')
        reasons=Counter(r for c in deferred for r in c['finding'].get('_evidence_gate',{}).get('reasons',[]))
        summary={'text_candidates':len(text),'graphic_candidates':sum(c['origin']=='graphic' for c in candidates),
                 'deferred_candidates':len(deferred),'final_findings':len(final),
                 'lineage_status':dict(Counter(c['status'] for c in candidates)),
                 'deferred_reason_counts':dict(reasons)}
        projects.append({'discipline':disc,'sources':[ref(tp),ref(bp),ref(fp)],'summary':summary,
                         'candidates':candidates,'deferred':deferred,
                         'final_refs_not_in_candidate_index':{k:v for k,v in linked.items() if k not in ids},
                         'finals_without_source_ids':[f['id'] for f in final if not f.get('source_finding_ids')]})
    result={'purpose':'Stage2 lineage inventory over saved runs; distinguishes observed linkage from semantic preservation.',
            'rules':['No source_finding_ids link does not prove a candidate was lost; match meaning and inspect merge decisions.',
                     'A link does not prove all useful atoms or locations survived.',
                     'Deferred candidates are not automatically valid; retain original gate reason and check against frozen evidence.',
                     'Targeted findings can legitimately originate outside the text/graphic index; unresolved source IDs are not automatically fabricated.',
                     'Repeated IDs are ambiguous and must be resolved by block/source context.'],
            'projects':projects,'stage_2_complete':False,'production_changed':False}
    (HERE/'merge_coverage_inventory.json').write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n')
    print(json.dumps([{**{'discipline':p['discipline']},**p['summary']} for p in projects],ensure_ascii=False))

if __name__=='__main__':main()
