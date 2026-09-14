"""Traceable union of completed disjoint proposal routes, without new inference."""
import argparse
from pathlib import Path

from experiments.project_change_272.inventory import ROOT,read,immutable,sha,now
from .access import authorize
from .packets import BASE
from .run import check_packet_scope
from .history import document_history


def merge(name,inputs,partition='DEV',candidate=None):
    allowed={p['index']:p for p in authorize(partition,candidate)}
    history={i:{s:document_history(p[s],p['embargo_pages'][s]) for s in ['old','new']} for i,p in allowed.items()}
    output=BASE/'runs'/name;packets=BASE/(name+'_packets');results=[];seen=set();origins=[];hashes={}
    for source_name in inputs:
        src=BASE/'runs'/source_name
        if (src/'QUALITY_STATUS.json').exists():raise ValueError('Quarantined route')
        m=read(src/'MANIFEST.json');r=read(src/'RUN_RECEIPT.json')
        if m['partition']!=partition or r['completed_packets']!=r['selected_packets'] or len(r['results'])!=r['completed_packets']:
            raise ValueError('Incomplete or foreign source route')
        origins.append(dict(run=source_name,manifest_sha256=sha(src/'MANIFEST.json'),receipt_sha256=sha(src/'RUN_RECEIPT.json')))
        paths={Path(k).stem:Path(k) for k in m['packets']}
        for result in r['results']:
            key=result['packet_id']
            if key in seen:raise ValueError('Duplicate source packet; refuse double counting')
            seen.add(key);path=paths[key]
            if sha(path)!=m['packets'][str(path)]:raise ValueError('Source packet drift')
            p=read(path);check_packet_scope(p,allowed,partition)
            if any(e['page'] in history[p['pair_index']][s] for s in ['old','new'] for e in p['evidence'][s]):
                raise PermissionError('Historical before-column cannot become state evidence through union')
            target=packets/'packets'/(key+'.json');immutable(target,p);hashes[str(target)]=sha(target)
            immutable(output/'results'/(key+'.json'),result);results.append(result)
    common=dict(created_at=now(),partition=partition,split_sha256=sha(ROOT/'SPLIT.json'),
        candidate_manifest=str(candidate) if candidate else None,code_sha256=sha(__file__),origins=origins)
    immutable(packets/'MANIFEST.json',common)
    immutable(packets/'COUNTS.json',dict(packets=len(results)))
    immutable(output/'MANIFEST.json',dict(**common,packets=hashes,kind='DERIVED_COMPLETED_ROUTE_UNION'))
    immutable(output/'RUN_RECEIPT.json',dict(completed_packets=len(results),selected_packets=len(results),
        results=results,calls=0,new_provider_cost_usd=0,source_costs='See original immutable run receipts; do not double count',
        adjudication='NOT_SOURCE_ADJUDICATED'))
    print(output,len(results),flush=True)
    return output,packets


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--name',required=True);p.add_argument('--inputs',nargs='+',required=True)
    p.add_argument('--partition',default='DEV',choices=['DEV','VALIDATION','FINAL_HOLDOUT']);p.add_argument('--candidate',type=Path)
    a=p.parse_args();merge(a.name,a.inputs,a.partition,a.candidate)
