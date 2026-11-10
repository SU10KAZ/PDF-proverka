"""Bounded, resumable object checks with an injected, caller-authorized provider.

No implicit transport or retries. A preparation-only run can import one sealed
response and stop before the next dispatch. This does not certify semantics.
"""
from copy import deepcopy
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

from ..findings_verify.source_review_evidence import evidence_digest
from .object_operation_continuation import prepare_object_operation_continuation
from .object_operation_passes import validate_object_operation_payload


def _write(path, value):
    temporary=path.with_suffix('.tmp')
    temporary.write_text(json.dumps(value,ensure_ascii=False,indent=2))
    temporary.replace(path)


async def run_object_operation_workflow(request, sources, images, md, work_dir, *,
        executor_id, expected_request_sha256, expected_md_sha256,
        invoke=None, initial_result=None, graph=None, max_revisions=3,
        is_cancelled=None):
    """invoke(request, phase) returns a provider dict with json_data/is_error.

    The caller owns authorization for EACH dispatch. None means prepare only.
    An imported initial_result is bound to this workflow, never sent again.
    Findings and operation receipts from every revision remain separate. A later
    partial response cannot silently erase an earlier claim or close an operation.
    """
    if not isinstance(executor_id,str) or not executor_id:
        raise ValueError('executor_identity_required')
    if type(max_revisions) is not int or not 0<=max_revisions<=3:
        raise ValueError('invalid_revision_budget')
    if evidence_digest(request)!=expected_request_sha256:
        raise ValueError('request_changed')
    if hashlib.sha256(md.encode()).hexdigest()!=expected_md_sha256:
        raise ValueError('md_changed')
    if graph is not None and graph.get('source_md_sha256')!=expected_md_sha256:
        raise ValueError('graph_version_changed')
    root=Path(work_dir);root.mkdir(parents=True,exist_ok=True)
    binding={'request':expected_request_sha256,'md':expected_md_sha256,
        'sources':evidence_digest(sources),'images':evidence_digest(images),
        'graph':evidence_digest(graph),'initial_result':evidence_digest(initial_result),
        'executor_id':executor_id,'max_revisions':max_revisions,'workflow_version':1}
    if (root/'binding.json').exists():
        if json.loads((root/'binding.json').read_text())!=binding:
            raise ValueError('workflow_inputs_changed')
    else:
        with (root/'binding.json').open('x') as handle:
            json.dump(binding,handle)
    current=deepcopy(request)
    report={'status':'running','steps':[],'semantic_verified':False,
        'automatic_report_generation_verified':False}
    for step in range(max_revisions+1):
        directory=root/f'{step:02d}';directory.mkdir(exist_ok=True)
        digest=evidence_digest(current)
        packet=json.loads(current['messages'][-1]['content'])
        hashes={}
        for bid,path in zip(packet['attached_image_ids_in_order'],current['image_paths'],strict=True):
            image=images.get(bid)
            if not image or Path(image['path']).resolve()!=Path(path).resolve():
                raise ValueError('image_binding_changed')
            hashes[bid]=hashlib.sha256(Path(path).read_bytes()).hexdigest()
            if hashes[bid]!=image['sha256']:
                raise ValueError('image_bytes_changed')
        request_path=directory/'request.json'
        if request_path.exists():
            if evidence_digest(json.loads(request_path.read_text()))!=digest:
                raise ValueError('saved_request_changed')
        else:
            _write(request_path,current)
        if is_cancelled and is_cancelled():
            report['status']='cancelled';break
        output=directory/'result.json'
        result_binding=directory/'result_binding.json'
        if output.exists():
            result=json.loads(output.read_text())
            if not result_binding.exists() or json.loads(result_binding.read_text())!={'request':digest,'result':evidence_digest(result)}:
                raise ValueError('saved_result_changed_or_unbound')
            origin='reused'
        elif step==0 and initial_result is not None:
            result=deepcopy(initial_result);origin='imported'
            _write(output,result)
            _write(result_binding,{'request':digest,'result':evidence_digest(result)})
        else:
            attempt=directory/'attempt.json'
            if attempt.exists():
                raise ValueError('previous_attempt_has_no_result_do_not_repeat')
            if invoke is None:
                report.update(status='awaiting_dispatch',next_request=str(request_path),next_request_sha256=digest)
                break
            with attempt.open('x') as handle:
                json.dump({'status':'started','request':digest},handle)
            try:
                result=await invoke(deepcopy(current),f'object_operations_revision_{step}')
                _write(output,result)
                _write(result_binding,{'request':digest,'result':evidence_digest(result)})
                _write(attempt,{'status':'finished','request':digest})
            except BaseException as exc:
                _write(attempt,{'status':'interrupted_or_failed','request':digest,'error_type':type(exc).__name__})
                raise
            origin='performed'
        if not isinstance(result,dict) or result.get('is_error') or not isinstance(result.get('json_data'),dict):
            raise ValueError('provider_result_not_usable')
        validated=validate_object_operation_payload(SimpleNamespace(messages=current['messages']),result['json_data'])
        _write(directory/'validation.json',validated)
        report['steps'].append({'revision':step,'origin':origin,'request_sha256':digest,
            'result_sha256':evidence_digest(result),'payload':validated})
        if not any(r.get('status')=='needs_data' for r in validated['object_operation_results']):
            report['status']='no_context_request';break
        if step==max_revisions:
            report['status']='revision_limit';break
        try:
            # Supply the model response with scoped receipts, not the executor's
            # expanded validation ledger (which would duplicate source context).
            scoped_response={**result['json_data'],
                'object_operation_results':validated['object_operation_results']}
            prepared=prepare_object_operation_continuation(current,scoped_response,sources,images,md,
                expected_request_sha256=digest,expected_md_sha256=expected_md_sha256,
                original_image_hashes=hashes,graph=graph,max_revisions=max_revisions)
        except ValueError as exc:
            report.update(status='continuation_not_prepared',reason=str(exc));break
        _write(directory/'continuation.json',prepared)
        if prepared['status']!='prepared':
            report['status']=prepared['status'];break
        current=prepared['request']
    _write(root/'report.json',report)
    return report
