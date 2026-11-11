"""Explicit experimental final checks and sequential source-bound report edits."""
from copy import deepcopy
import json
import hashlib
from pathlib import Path
from .source_review_evidence import evidence_digest
from .source_review_final_check import prepare_final_check, validate_final_check
from .source_review_admission import apply_review_admissions


def _write(path,value):
    temp=path.with_suffix('.tmp');temp.write_text(json.dumps(value,ensure_ascii=False,indent=2)+'\n');temp.replace(path)


async def run_final_release_check(entry,document,md,work_dir,*,invoke,executor_id):
    if not callable(invoke) or not isinstance(executor_id,str) or not executor_id:
        raise ValueError('explicit_final_check_executor_required')
    prepared=prepare_final_check(entry,document,md)
    request=prepared['request'];root=Path(work_dir);root.mkdir(parents=True,exist_ok=True)
    binding={'request_sha256':evidence_digest(request),'executor_id':executor_id,'version':1}
    path=root/'binding.json'
    if path.exists():
        if json.loads(path.read_text())!=binding:raise ValueError('final_check_inputs_changed')
    else:
        with path.open('x') as f:json.dump(binding,f)
    _write(root/'request.json',request)
    result_path=root/'result.json';result_binding=root/'result_binding.json'
    if result_path.exists():
        result=json.loads(result_path.read_text())
        if not result_binding.exists() or json.loads(result_binding.read_text())!={'request':binding['request_sha256'],'result':evidence_digest(result)}:
            raise ValueError('final_check_result_changed_or_unbound')
    else:
        attempt=root/'attempt.json'
        with attempt.open('x') as f:json.dump({'status':'started',**binding},f)
        try:
            result=await invoke(deepcopy(request),'final_release_check')
            _write(result_path,result)
            _write(result_binding,{'request':binding['request_sha256'],'result':evidence_digest(result)})
            _write(attempt,{'status':'finished',**binding})
        except BaseException as exc:
            _write(attempt,{'status':'interrupted_or_failed','error_type':type(exc).__name__,**binding})
            raise
    if not isinstance(result,dict) or result.get('is_error') or not isinstance(result.get('json_data'),dict):
        raise ValueError('final_check_provider_result_unusable')
    if evidence_digest(prepare_final_check(entry,document,md)['request'])!=binding['request_sha256']:
        raise ValueError('final_check_inputs_changed_during_dispatch')
    receipt=validate_final_check(entry,document,md,result['json_data'])
    receipt.update(executor_id=executor_id,request_sha256=binding['request_sha256'],result_sha256=evidence_digest(result))
    after=deepcopy(document)
    if receipt['status']=='release_candidate':
        after,admissions=apply_review_admissions(document,[entry],md)
        if admissions[0]['status']!='applied':raise ValueError('final_edit_no_longer_applicable')
        receipt['status']='applied'
        receipt['applied_finding_sha256']=evidence_digest(next((f for f in after['findings'] if f.get('id')==entry['response']['finding_id']),None))
        after['source_review_history'][-1]['final_release_check']=deepcopy(receipt)
    _write(root/'receipt.json',receipt)
    return after,receipt


async def run_final_release_phase(output_dir,md_path,*,invoke,executor_id,max_findings,is_cancelled=None):
    """No callback is installed by default. Holds preserve current report text."""
    import fcntl
    if type(max_findings) is not int or max_findings<1:raise ValueError('positive_final_check_budget_required')
    root=Path(output_dir);candidate_path=root/'03_source_review_candidates.json'
    if not candidate_path.exists():return {'skipped':True,'applied':0,'held':0,'not_completed':0,'not_attempted':0}
    lock=(root/'03_final_release.lock').open('a')
    try:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        candidates=json.loads(candidate_path.read_text())
        reports=candidates.get('reports')
        if not isinstance(reports,list):raise ValueError('invalid_source_candidates')
        entries=[r['entry'] for r in reports if r.get('status')=='candidate_ready']
        ids=[e['response']['finding_id'] for e in entries]
        if len(set(ids))!=len(ids):raise ValueError('duplicate_final_check_candidates')
        path=root/'03_findings.json';records=[];processed=0
        for entry in entries:
            if is_cancelled and is_cancelled():break
            document_bytes=path.read_bytes();document=json.loads(document_bytes)
            digest=evidence_digest(entry)
            prior=[h['final_release_check'] for h in document.get('source_review_history',[])
                   if h.get('final_release_check',{}).get('binding',{}).get('entry_sha256')==digest]
            if prior:
                current=next((f for f in document['findings'] if f.get('id')==entry['response']['finding_id']),None)
                intact=(len(prior)==1 and prior[0].get('applied_finding_sha256')==evidence_digest(current)
                        and prior[0]['binding']['source_md_sha256']==hashlib.sha256(Path(md_path).read_bytes()).hexdigest())
                records.append({'finding_id':entry['response']['finding_id'],
                                'status':'already_applied' if intact else 'not_completed',
                                **({} if intact else {'reason':'previous_release_state_changed'})});continue
            if processed>=max_findings:break
            processed+=1
            md_bytes=Path(md_path).read_bytes();md=md_bytes.decode('utf-8')
            key=evidence_digest({'entry':entry,'document':document,'md':md,'executor_id':executor_id})
            try:
                after,receipt=await run_final_release_check(entry,document,md,root/'final_release_checks'/key,invoke=invoke,executor_id=executor_id)
                if path.read_bytes()!=document_bytes or Path(md_path).read_bytes()!=md_bytes:
                    raise ValueError('report_or_source_changed_during_final_check')
                if after!=document:_write(path,after)
                records.append({'finding_id':entry['response']['finding_id'],**receipt})
            except (OSError,ValueError,TypeError,KeyError) as exc:
                records.append({'finding_id':entry['response']['finding_id'],'status':'not_completed','reason':str(exc)})
            _write(root/'03_final_release_results.json',{'status':'running','records':records})
        summary={'skipped':False,'status':'finished','records':records,
            'applied':sum(r['status']=='applied' for r in records),'held':sum(r['status']=='held' for r in records),
            'not_completed':sum(r['status']=='not_completed' for r in records),
            'already_applied':sum(r['status']=='already_applied' for r in records),
            'not_attempted':len(entries)-len(records),'semantic_certified':False}
        _write(root/'03_final_release_results.json',summary)
        return summary
    finally:lock.close()
