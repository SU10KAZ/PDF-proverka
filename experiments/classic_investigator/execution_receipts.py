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


def summarize_request_usage(request_root):
    """Sum transport attempt receipts exactly once, including failures/retries.

    Stage counters may omit nested callbacks or already include retry totals;
    never add them here. Token cache fields are subsets, not extra input tokens.
    Summed call duration is not wall time when requests run concurrently.
    """
    import math
    root=Path(request_root)
    fields=('input_tokens','output_tokens','cached_tokens','cache_write_tokens',
            'reasoning_tokens','cost_usd','duration_ms')
    total={k:0 for k in fields};groups={};issues=[];files={};requests=attempts=failures=0
    def read(path):
        try:
            raw=path.read_bytes();files[str(path.relative_to(root))]=hashlib.sha256(raw).hexdigest()
            return json.loads(raw)
        except (OSError,ValueError) as exc:
            issues.append({'file':str(path.relative_to(root)),'reason':'unreadable_receipt','detail':type(exc).__name__})
            return None
    for folder in sorted(p for p in root.iterdir() if p.is_dir()) if root.exists() else []:
        requests+=1
        request=read(folder/'request.json');rows=read(folder/'attempts.json')
        if not isinstance(request,dict) or not isinstance(rows,list) or not rows:
            issues.append({'request':folder.name,'reason':'missing_or_invalid_request_attempts'});continue
        indexes=[]
        for row in rows:
            if not isinstance(row,dict):
                issues.append({'request':folder.name,'reason':'invalid_attempt'});continue
            index=row.get('attempt');indexes.append(index)
            if type(index) is not int or index<1 or indexes.count(index)>1:
                issues.append({'request':folder.name,'reason':'duplicate_or_invalid_attempt_number'});continue
            attempts+=1;failures+=row.get('is_error') is True
            response=read(folder/f'response_{index}.json')
            if not isinstance(response,dict):issues.append({'request':folder.name,'attempt':index,'reason':'response_missing_or_invalid'})
            model=row.get('reported_model') or (response or {}).get('model') or row.get('model') or 'unknown'
            cost_source=row.get('cost_source') or 'unknown'
            if cost_source=='unknown':issues.append({'request':folder.name,'attempt':index,'reason':'cost_provenance_unknown'})
            key=(model,cost_source)
            group=groups.setdefault(key,{'model':model,'cost_source':cost_source,'attempts':0,**{k:0 for k in fields}})
            group['attempts']+=1
            usage=row.get('usage')
            for field in fields:
                value=usage.get(field) if isinstance(usage,dict) else None
                if type(value) not in (int,float) or not math.isfinite(value) or value<0:
                    issues.append({'request':folder.name,'attempt':index,'field':field,'reason':'usage_missing_or_invalid'});continue
                total[field]+=value;group[field]+=value
        if indexes!=list(range(1,len(rows)+1)):
            issues.append({'request':folder.name,'reason':'attempt_sequence_incomplete'})
        expected={f'response_{i}.json' for i in indexes if type(i) is int and i>0}
        if {p.name for p in folder.glob('response_*.json')}!=expected:
            issues.append({'request':folder.name,'reason':'response_attempt_set_mismatch'})
    if not requests:issues.append({'reason':'no_transport_requests_recorded'})
    money_measured = bool(groups) and not issues and all(source == 'actual' for _, source in groups)
    return {'accounting_complete':not issues,'request_count':requests,'attempt_count':attempts,
            'failed_attempt_count':failures,'totals':total,'by_model_and_cost_source':list(groups.values()),
            'actual_monetary_cost_usd':total['cost_usd'] if money_measured else None,
            'monetary_cost_fully_measured':money_measured,
            'issues':issues,'artifact_sha256':files,
            'boundary':'Transport receipts only. Summed duration is not wall time. Cache/reasoning tokens are not added again. Subscription cost_usd=0 is not proof of zero resource cost. Missing receipts make totals lower bounds.'}
