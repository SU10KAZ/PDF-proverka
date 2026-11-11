"""Execution accounting only: completion is never a semantic quality verdict."""
import hashlib
import json
from pathlib import Path


def summarize_methods(output_dir, config, stage_results):
    root=Path(output_dir);issues=[];artifacts={};details={};ready_ids=None
    def read(name):
        path=root/name
        if not path.is_file():
            issues.append({'artifact':name,'reason':'required_execution_receipt_missing'});return None
        raw=path.read_bytes();artifacts[name]=hashlib.sha256(raw).hexdigest()
        return json.loads(raw)
    for stage,result in stage_results.items():
        if result.get('error'):issues.append({'stage':stage,'reason':'reported_error','detail':result['error']})
    if config.get('inventory_max_packets',0):
        analysis=read('02_text_analysis.json')
        inventory=(analysis or {}).get('inventory_enrichment')
        if not isinstance(inventory,dict):issues.append({'reason':'inventory_receipt_missing'})
        else:
            details['inventory']={k:inventory.get(k) for k in ('status','completed_packets','total_packets')}
            if (inventory.get('status')!='finished' or any(type(inventory.get(k)) is not int or inventory[k]<0 for k in ('completed_packets','total_packets'))
                    or inventory.get('completed_packets')!=inventory.get('total_packets')):
                issues.append({'reason':'inventory_incomplete',**details['inventory']})
        plan=read('03_object_operation_plan.json')
        if plan is not None:
            details['operation_backlog']=plan.get('backlog',[])
            details['invalid_objects']=plan.get('invalid_objects',[])
            if details['operation_backlog'] or details['invalid_objects']:
                issues.append({'reason':'object_operations_unresolved','backlog':len(details['operation_backlog']),
                               'invalid_objects':len(details['invalid_objects'])})
    if config.get('source_review_max_findings',0):
        reviews=read('03_source_review_candidates.json')
        if reviews is not None:
            statuses={}
            for r in reviews.get('reports',[]):statuses[r['status']]=statuses.get(r['status'],0)+1
            ready_ids=[r.get('finding_id') for r in reviews.get('reports',[]) if r['status']=='candidate_ready']
            details['source_reviews']={'statuses':statuses,'not_attempted':reviews.get('not_attempted'),
                                     'status':reviews.get('status')}
            if reviews.get('status')!='finished' or reviews.get('not_attempted')!=0 or any(k!='candidate_ready' for k in statuses):
                issues.append({'reason':'source_reviews_unresolved',**details['source_reviews']})
            if statuses.get('candidate_ready',0) and not config.get('source_final_check_max_findings',0):
                issues.append({'reason':'review_candidates_not_final_checked','count':statuses['candidate_ready']})
    if config.get('source_final_check_max_findings',0):
        release=read('03_final_release_results.json')
        if release is not None:
            statuses={}
            for r in release.get('records',[]):statuses[r['status']]=statuses.get(r['status'],0)+1
            if ready_ids is not None:
                checked_ids=[r.get('finding_id') for r in release.get('records',[])]
                if (any(not isinstance(i,str) or not i for i in ready_ids+checked_ids)
                        or len(set(ready_ids))!=len(ready_ids) or len(set(checked_ids))!=len(checked_ids)
                        or set(ready_ids)!=set(checked_ids)):
                    issues.append({'reason':'final_check_candidate_coverage_mismatch'})
            details['final_checks']={'statuses':statuses,'not_attempted':release.get('not_attempted'),'status':release.get('status')}
            if release.get('status')!='finished' or release.get('not_attempted')!=0 or any(k not in ('applied','already_applied') for k in statuses):
                issues.append({'reason':'final_edits_unresolved',**details['final_checks']})
    return {'declared_methods_execution_complete':not issues,'issues':issues,'details':details,'artifact_sha256':artifacts,
            'semantic_acceptance':False,'boundary':'Execution receipts only. Planned operations and successful calls do not establish engineering truth, candidate retention, or whole-project coverage.'}
