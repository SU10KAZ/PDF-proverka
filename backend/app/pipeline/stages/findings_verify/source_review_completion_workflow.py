"""At most one citation-only completion, followed by a fresh independent check."""
from copy import deepcopy
import json
from pathlib import Path
from .source_review_citation_completion import prepare,apply_candidate
from .source_review_evidence import evidence_digest


def _write(path,value):
    temporary=path.with_suffix('.tmp')
    temporary.write_text(json.dumps(value,ensure_ascii=False,indent=2)+'\n');temporary.replace(path)


async def run_with_citation_completion(entry,document,md,work_dir,*,invoke,executor_id,is_cancelled=None):
    from .source_review_release_workflow import run_final_release_check
    root=Path(work_dir)
    after,initial=await run_final_release_check(entry,document,md,root/'initial',invoke=invoke,executor_id=executor_id)
    failed=[c for c in initial['review']['checks'] if c['status']!='pass']
    if initial['status']!='held' or len(failed)!=1 or failed[0]['check_id']!='final_citations':return after,initial
    if is_cancelled and is_cancelled():raise ValueError('citation_completion_cancelled')
    request,binding=prepare(entry,document,md,initial['review'])
    repair=root/'citation_completion';repair.mkdir(parents=True,exist_ok=True)
    contract={'binding':binding,'request_sha256':evidence_digest(request),'executor_id':executor_id,'version':1}
    path=repair/'binding.json'
    if path.exists():
        if json.loads(path.read_text())!=contract:raise ValueError('citation_completion_inputs_changed')
    else:
        with path.open('x') as f:json.dump(contract,f)
    _write(repair/'request.json',request)
    result_path=repair/'result.json';result_binding=repair/'result_binding.json'
    if result_path.exists():
        result=json.loads(result_path.read_text())
        if not result_binding.exists() or json.loads(result_binding.read_text())!={'request':contract['request_sha256'],'result':evidence_digest(result)}:
            raise ValueError('citation_completion_result_changed')
    else:
        with (repair/'attempt.json').open('x') as f:json.dump({'status':'started',**contract},f)
        try:
            result=await invoke(deepcopy(request),'citation_completion')
            _write(result_path,result)
            _write(result_binding,{'request':contract['request_sha256'],'result':evidence_digest(result)})
            _write(repair/'attempt.json',{'status':'finished',**contract})
        except BaseException as exc:
            _write(repair/'attempt.json',{'status':'interrupted_or_failed','error_type':type(exc).__name__,**contract});raise
    if not isinstance(result,dict) or result.get('is_error') or not isinstance(result.get('json_data'),dict):
        raise ValueError('citation_completion_provider_unusable')
    if evidence_digest(prepare(entry,document,md,initial['review'])[0])!=contract['request_sha256']:
        raise ValueError('citation_completion_sources_changed')
    if result['json_data'].get('status')=='cannot_complete':
        initial['citation_completion']={'status':'cannot_complete','reason':result['json_data'].get('reason'),**contract}
        _write(root/'receipt.json',initial);return after,initial
    revised=apply_candidate(entry,document,md,initial['review'],result['json_data'])
    _write(repair/'candidate.json',{'entry':revised,'semantic_acceptance':False})
    if is_cancelled and is_cancelled():raise ValueError('citation_completion_cancelled')
    # This call gets the revised edit and sources, never the earlier verdict.
    after,receipt=await run_final_release_check(revised,document,md,root/'after_completion',invoke=invoke,executor_id=executor_id)
    receipt['requested_entry_sha256']=evidence_digest(entry)
    receipt['citation_completion']={'status':'candidate_rechecked','initial_check_status':initial['status'],
        'result_sha256':evidence_digest(result),**contract}
    if receipt['status']=='applied':after['source_review_history'][-1]['final_release_check']=deepcopy(receipt)
    _write(root/'receipt.json',receipt)
    return after,receipt
