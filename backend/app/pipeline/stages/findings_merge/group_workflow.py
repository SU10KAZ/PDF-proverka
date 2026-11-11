"""Explicit bounded group discovery and independent source review; no default provider."""
from copy import deepcopy
import json
from pathlib import Path
from .group_proposal import prepare_group_proposal,validate_group_proposal
from .group_source_review import prepare_group_review,validate_group_review
from .reviewed_grouping import apply_reviewed_group
from backend.app.pipeline.stages.findings_verify.source_review_evidence import evidence_digest


def _write(path,value):
    temp=path.with_suffix('.tmp');temp.write_text(json.dumps(value,ensure_ascii=False,indent=2)+'\n');temp.replace(path)


async def run_grouping_workflow(document,md,graphic_dir,work_dir,*,invoke,executor_id,max_groups,max_findings=256,max_chars=250000,is_cancelled=None):
    if not callable(invoke) or not isinstance(executor_id,str) or not executor_id:raise ValueError('explicit_grouping_executor_required')
    if type(max_groups) is not int or max_groups<1:raise ValueError('positive_group_review_budget_required')
    root=Path(work_dir);root.mkdir(parents=True,exist_ok=True)
    async def call(request,phase):
        if is_cancelled and is_cancelled():raise ValueError('grouping_cancelled')
        folder=root/phase;folder.mkdir(exist_ok=True)
        binding={'request_sha256':evidence_digest(request),'executor_id':executor_id}
        path=folder/'binding.json'
        if path.exists():
            if json.loads(path.read_text())!=binding:raise ValueError('grouping_request_changed')
        else:
            with path.open('x') as f:json.dump(binding,f)
        _write(folder/'request.json',request)
        result_path=folder/'result.json';bound=folder/'result_binding.json'
        if result_path.exists():
            result=json.loads(result_path.read_text())
            if not bound.exists() or json.loads(bound.read_text())!={'request':binding['request_sha256'],'result':evidence_digest(result)}:
                raise ValueError('grouping_result_changed')
        else:
            with (folder/'attempt.json').open('x') as f:json.dump({'status':'started',**binding},f)
            try:
                result=await invoke(deepcopy(request),phase)
                _write(result_path,result);_write(bound,{'request':binding['request_sha256'],'result':evidence_digest(result)})
                _write(folder/'attempt.json',{'status':'finished',**binding})
            except BaseException as exc:
                _write(folder/'attempt.json',{'status':'interrupted_or_failed','error_type':type(exc).__name__,**binding});raise
        if not isinstance(result,dict) or result.get('is_error') or not isinstance(result.get('json_data'),dict):raise ValueError('grouping_provider_result_unusable')
        return result['json_data']
    request=prepare_group_proposal(document,max_findings=max_findings,max_chars=max_chars)
    proposal=validate_group_proposal(document,await call(request,'group_proposal'))
    current=deepcopy(document);records=[];attempted=0
    for index,group in enumerate(proposal['groups']):
        if attempted>=max_groups or (is_cancelled and is_cancelled()):
            records.append({'member_ids':group['member_ids'],'status':'not_attempted'});continue
        try:prepared=prepare_group_review(current,group,md,graphic_dir)
        except ValueError as exc:
            records.append({'member_ids':group['member_ids'],'status':'not_eligible','reason':str(exc)});continue
        attempted+=1
        try:
            response=await call(prepared['request'],f'group_review_{index:03d}')
            if evidence_digest(prepare_group_review(current,group,md,graphic_dir)['request'])!=evidence_digest(prepared['request']):
                raise ValueError('group_sources_changed_during_dispatch')
            decision=validate_group_review(current,group,md,graphic_dir,response)
            if decision['action']=='group':
                after,receipt=apply_reviewed_group(current,group,decision,md)
                if receipt['status']!='applied':raise ValueError('group_admission_refused: '+receipt['reason'])
                current=after
                records.append({'member_ids':group['member_ids'],'status':'applied','proposal':group,'decision':decision,'receipt':receipt})
            else:
                records.append({'member_ids':group['member_ids'],'status':decision['action'],'decision':decision})
        except (ValueError,OSError,TypeError,KeyError) as exc:
            records.append({'member_ids':group['member_ids'],'status':'not_completed','reason':str(exc)})
        _write(root/'progress.json',{'status':'running','records':records})
    report={'status':'finished','records':records,'proposed_groups':len(proposal['groups']),
        'keep_separate_ids':proposal['keep_separate_ids'],'groups_reviewed':attempted,
        **{k:sum(r['status']==k for r in records) for k in ('applied','keep_separate','needs_context','not_eligible','not_completed','not_attempted')},
        'original_findings':len(document['findings']),'final_findings':len(current['findings']),
        'semantic_certified':False,'original_document_mutated':False}
    _write(root/'report.json',report)
    return current,report
