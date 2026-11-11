"""Re-adjudicate two old cable reviews under the current explicit report contract."""
import argparse
import asyncio
import json
import os
from pathlib import Path
from .run_itp_pipe_operations import immutable
from .run_eom_cable_final_release import BASE, BEFORE
REVIEW=Path('/tmp/auditmanager_eom_cable_detail_source_workflow_20261111')
from backend.app.pipeline.stages.findings_verify.source_review_request import prepare_source_review_request
from backend.app.pipeline.stages.findings_verify.source_review_workflow import prepare_adjudication_request,direct_cli_executor
from backend.app.pipeline.stages.findings_verify.source_review_admission import apply_review_admissions
from backend.app.pipeline.stages.findings_verify.source_review_evidence import evidence_digest
ROOT=Path('/tmp/auditmanager_eom_cable_current_adjudication_20261111')

async def main(execute):
    os.environ['CODEX_TRANSIENT_RETRIES']='0'
    md=(BASE/'02_work/document.md').read_text();document=json.loads(BEFORE.read_text())
    for fid in ('F-050','F-051'):
        root=ROOT/fid;root.mkdir(parents=True,exist_ok=True)
        old=json.loads((REVIEW/fid/'candidate.json').read_text())['entry']
        original,=[f for f in document['findings'] if f['id']==fid]
        source=prepare_source_review_request(old['queue'],original,md,max_images=16)
        wire=prepare_adjudication_request(source,old['response'])
        manifest={'request_sha256':evidence_digest(wire),'old_entry_sha256':evidence_digest(old),
            'model':'codex/gpt-6-astra','effort':'high','message_chars':sum(len(m['content']) for m in wire['messages']),
            'image_count':len(wire['image_paths']),'image_bytes':sum(Path(p).stat().st_size for p in wire['image_paths']),
            'maximum_new_calls':1,'automatic_retries':0,'history_and_engineer_answers_supplied':False,
            'previous_source_review_supplied_as_untrusted':True,'previous_adjudication_not_supplied':True}
        immutable(root/'request.json',wire);immutable(root/'manifest.json',manifest)
        print('PREPARED',fid,json.dumps(manifest),flush=True)
        if not execute:continue
        if (root/'result.json').exists():
            result=json.loads((root/'result.json').read_text())
            assert json.loads((root/'result_binding.json').read_text())=={'request':manifest['request_sha256'],'result':evidence_digest(result)}
        else:
            with (root/'attempt.json').open('x') as f:json.dump({'status':'started','request_sha256':manifest['request_sha256']},f)
            result=await direct_cli_executor('codex/gpt-6-astra',project_id=fid+'-current-adjudication')(wire,'adjudication')
            immutable(root/'result.json',result);immutable(root/'result_binding.json',{'request':manifest['request_sha256'],'result':evidence_digest(result)})
        assert not result.get('is_error') and isinstance(result.get('json_data'),dict)
        assert (BASE/'02_work/document.md').read_text()==md
        current=prepare_adjudication_request(prepare_source_review_request(old['queue'],original,md,max_images=16),old['response'])
        assert evidence_digest(current)==manifest['request_sha256']
        entry={**old,'decision':result['json_data']}
        _,receipts=apply_review_admissions(document,[entry],md)
        status='candidate_ready' if receipts[0]['status']=='applied' else 'admission_rejected'
        immutable(root/'candidate.json',{'status':status,'entry':entry,'admission_receipts':receipts,'semantic_acceptance':False})
        print('COMPLETE',fid,status,flush=True)

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--execute',action='store_true');asyncio.run(main(p.parse_args().execute))
