"""Atomic, opt-in grouping phase; resumable without reissuing completed requests."""
import fcntl
import hashlib
import json
from pathlib import Path
from .group_workflow import run_grouping_workflow,_write
from backend.app.pipeline.stages.findings_verify.source_review_evidence import evidence_digest


def _sha(raw):return hashlib.sha256(raw).hexdigest()


async def run_grouping_phase(output_dir,md_path,graphic_dir,*,invoke,executor_id,max_groups,max_findings=256,max_chars=250000,is_cancelled=None):
    root=Path(output_dir);path=root/'03_findings.json';source=Path(md_path)
    lock=(root/'03_grouping_workflow.lock').open('a')
    try:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        raw=path.read_bytes();md_bytes=source.read_bytes()
        contract={'source_md_sha256':_sha(md_bytes),'executor_id':executor_id,'max_groups':max_groups,
                  'max_findings':max_findings,'max_chars':max_chars,'version':1}
        state_path=root/'03_grouping_workflow_state.json';summary_path=root/'03_grouping_workflow_results.json'
        if state_path.exists():
            state=json.loads(state_path.read_text())
            if state['contract']!=contract:raise ValueError('grouping_phase_sources_or_config_changed')
            if state['status'] not in ('prepared','finished'):raise ValueError('invalid_grouping_commit_state')
            if _sha(raw) not in (state['input_sha256'],state['output_sha256']):raise ValueError('grouping_phase_report_changed')
            if state['status']=='finished' and _sha(raw)!=state['output_sha256']:raise ValueError('previous_grouping_output_changed')
            key=evidence_digest({'input_sha256':state['input_sha256'],'contract':contract})
            saved=root/'grouping_workflows'/key/'output.json'
            content=saved.read_bytes()
            if _sha(content)!=state['output_sha256']:raise ValueError('grouping_saved_output_changed')
            if source.read_bytes()!=md_bytes or path.read_bytes()!=raw:raise ValueError('grouping_inputs_changed_before_recovery')
            if state['status']=='prepared' and _sha(raw)!=state['output_sha256']:
                temporary=path.with_suffix('.grouping.tmp');temporary.write_bytes(content);temporary.replace(path)
            state['status']='finished';_write(state_path,state);_write(summary_path,state['report'])
            return {**state['report'],'skipped':True,'already_applied':state['report']['applied'],'applied':0}
        key=evidence_digest({'input_sha256':_sha(raw),'contract':contract})
        work=root/'grouping_workflows'/key
        after,report=await run_grouping_workflow(json.loads(raw),md_bytes.decode('utf-8'),graphic_dir,work,
            invoke=invoke,executor_id=executor_id,max_groups=max_groups,max_findings=max_findings,max_chars=max_chars,is_cancelled=is_cancelled)
        if source.read_bytes()!=md_bytes or path.read_bytes()!=raw:raise ValueError('grouping_inputs_changed_during_dispatch')
        if is_cancelled and is_cancelled():raise ValueError('grouping_cancelled_before_commit')
        # Preserve report bytes too when no group was admitted.
        content=raw if after==json.loads(raw) else (json.dumps(after,ensure_ascii=False,indent=2)+'\n').encode()
        (work/'output.json').write_bytes(content)
        state={'status':'prepared','contract':contract,'input_sha256':_sha(raw),'output_sha256':_sha(content),'report':report}
        _write(state_path,state)
        if source.read_bytes()!=md_bytes or path.read_bytes()!=raw:raise ValueError('grouping_inputs_changed_before_commit')
        if content!=raw:
            temporary=path.with_suffix('.grouping.tmp');temporary.write_bytes(content);temporary.replace(path)
        state['status']='finished';_write(state_path,state);_write(summary_path,report)
        return {**report,'skipped':False,'already_applied':0}
    finally:lock.close()
