"""Replay both sealed OGM5 responses through continuation and final merge locally."""
import asyncio
from copy import deepcopy
import hashlib
import json
from pathlib import Path
from tempfile import TemporaryDirectory

from backend.app.pipeline.stages.findings_verify.source_review_evidence import evidence_digest
from backend.app.pipeline.stages.findings_verify.source_review_queue import attach_pending_source_reviews
from backend.app.pipeline.stages.prepare.codex_targeted_findings import combine_findings_with_targeted
from backend.app.pipeline.stages.prepare.object_operation_workflow import run_object_operation_workflow
from .prepare_ogm5_continuation import ROOT, INVENTORY, DEST, MD


async def main():
    first=json.loads((ROOT/'request.json').read_text())
    first_result=json.loads((ROOT/'result.json').read_text())
    second=json.loads((DEST/'request.json').read_text())
    second_result=json.loads((DEST/'result.json').read_text())
    manifest=json.loads((INVENTORY/'manifest.json').read_text())
    sources,images={},{}
    for row,digest in zip(manifest['packets'],manifest['request_digests'],strict=True):
        request=json.loads((INVENTORY/(row['stage']+'.json')).read_text())
        assert evidence_digest(request)==digest
        packet=json.loads(request['messages'][-1]['content'])
        sources.update({b['block_id']:b for b in packet['source_blocks']})
        for binding,path in zip(packet['images_in_attachment_order'],request['image_paths'],strict=True):
            images[binding['block_id']]={**binding,'path':path}
    graph=json.loads(Path('/tmp/auditmanager_six_candidate_inputs_20261110/KM/03_analysis/runs/candidate/document_graph.json').read_text())
    baseline_path=MD.parent.parent/'03_analysis/runs/control/03_findings.json'
    baseline=json.loads(baseline_path.read_text());unchanged=deepcopy(baseline)
    payloads=[];snapshots=[];calls=[]
    async def sealed_response(request,phase):
        assert evidence_digest(request)==evidence_digest(second), 'Continuation differs from actual approved request'
        calls.append(phase)
        return deepcopy(second_result)
    def merge_step(step):
        payloads.append((f"object_operations_ogm5_revision_{step['revision']}",step['payload']))
        merged=combine_findings_with_targeted(baseline,payloads)
        attach_pending_source_reviews(merged['findings'],set(sources))
        snapshots.append(merged)
    with TemporaryDirectory(prefix='ogm5_completed_workflow_') as work:
        report=await run_object_operation_workflow(first,sources,images,MD.read_text(),work,
            executor_id='sealed-local-replay/no-network',expected_request_sha256=evidence_digest(first),
            expected_md_sha256=manifest['source_md_sha256'],initial_result=first_result,
            invoke=sealed_response,graph=graph,max_revisions=3,on_step=merge_step,
            # Stop before any third request: only two sealed responses exist.
            is_cancelled=lambda:len(calls)==1)
        assert len(report['steps'])==2
        assert calls==['object_operations_revision_1']
        final=snapshots[-1]
        assert baseline==unchanged
        assert final['findings'][:len(baseline['findings'])]==baseline['findings']
        assert all(f in final['findings'] for f in snapshots[0]['findings'])
        prepared=json.loads((Path(work)/'01/continuation.json').read_text())
        third=prepared.get('request')
        continuation_summary={'status':prepared['status'],'new_source_ids':prepared.get('new_source_ids',[]),
            'omitted':prepared.get('omitted',[]),'sent':False}
        if third:
            continuation_summary.update(message_chars=sum(len(m['content']) for m in third['messages']),
                images=len(third['image_paths']),request_sha256=evidence_digest(third))
            dest=Path('/tmp/auditmanager_km_ogm5_second_continuation_20261110')
            dest.mkdir(exist_ok=True)
            path=dest/'request.json'
            if path.exists():assert evidence_digest(json.loads(path.read_text()))==evidence_digest(third)
            path.write_text(json.dumps(third,ensure_ascii=False,indent=2)+'\n')
            continuation_summary['request_path']=str(path)
        ledger=[row for r in final['object_operation_reports'] for row in r['candidate_ledger']]
        pending=[row for r in final['object_operation_reports'] for row in r['pending']]
        result={'status':report['status'],'stop_reason':report.get('reason'),'steps':len(report['steps']),'next_preparation':continuation_summary,
            'baseline_findings_preserved':len(baseline['findings']),'final_findings':len(final['findings']),
            'first_step_findings_preserved':len(snapshots[0]['findings']),
            'candidate_ledger_records':len(ledger),'internal_pending_candidates':len(pending),
            'new_model_calls':0,'request_matches_approved_expanded_packet':True,
            'source_md_sha256':hashlib.sha256(MD.read_bytes()).hexdigest(),
            'baseline_sha256':hashlib.sha256(baseline_path.read_bytes()).hexdigest(),
            'sealed_response_hashes':[evidence_digest(first_result),evidence_digest(second_result)],
            'semantic_verified':False,'production_accepted':False,
            'limits':['Recorded provider answers replayed locally; no new independent audit.',
                'This verifies continuation and merge retention; final wording and technical scope still require review.',
                'No third request dispatched; remaining needs_data operations are unresolved.']}
        Path(__file__).with_name('KM_OGM5_completed_workflow_replay.json').write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n')
        print(json.dumps(result,ensure_ascii=False))


if __name__=='__main__':asyncio.run(main())
