"""Import sealed OGM5 result and prepare continuation; no model dispatch."""
import asyncio
import hashlib
import json
from pathlib import Path

from backend.app.pipeline.stages.findings_verify.source_review_evidence import evidence_digest
from backend.app.pipeline.stages.prepare.object_operation_workflow import run_object_operation_workflow
from .prepare_ogm5_continuation import ROOT, INVENTORY, MD


async def replay():
    original=json.loads((ROOT/'request.json').read_text())
    manifest=json.loads((ROOT/'manifest.json').read_text())
    result=json.loads((ROOT/'result.json').read_text())
    frozen=json.loads((INVENTORY/'manifest.json').read_text())
    sources,images={},{}
    for row,digest in zip(frozen['packets'],frozen['request_digests'],strict=True):
        request=json.loads((INVENTORY/(row['stage']+'.json')).read_text())
        if evidence_digest(request)!=digest:
            raise ValueError('Source packet changed')
        packet=json.loads(request['messages'][-1]['content'])
        sources.update({b['block_id']:b for b in packet['source_blocks']})
        for binding,path in zip(packet['images_in_attachment_order'],request['image_paths'],strict=True):
            images[binding['block_id']]={**binding,'path':path}
    graph=json.loads(Path('/tmp/auditmanager_six_candidate_inputs_20261110/KM/03_analysis/runs/candidate/document_graph.json').read_text())
    work=Path('/tmp/auditmanager_km_ogm5_workflow_replay_20261110')
    report=await run_object_operation_workflow(original,sources,images,MD.read_text(),work,
        executor_id='codex/gpt-6-astra/high',expected_request_sha256=manifest['request_sha256'],
        expected_md_sha256=frozen['source_md_sha256'],initial_result=result,graph=graph)
    if report['status']!='awaiting_dispatch':
        raise ValueError(report['status']+': '+report.get('reason',''))
    previous=json.loads(Path('/tmp/auditmanager_km_ogm5_continuation_20261110/manifest.json').read_text())
    same=report['next_request_sha256']==previous['request_sha256']
    assert same, 'Workflow changed the prepared continuation'
    assert not list(work.glob('*/attempt.json'))
    summary={'status':report['status'],'imported_result_sha256':hashlib.sha256((ROOT/'result.json').read_bytes()).hexdigest(),
        'workflow_dir':str(work),'next_request_sha256':report['next_request_sha256'],
        'matches_previously_prepared_continuation':same,'new_model_calls':0,
        'initial_result_reused':True,'semantic_verified':False,
        'scope':'Actual saved OGM5 response through reusable executor, stopped before pending expanded dispatch.'}
    Path(__file__).with_name('KM_OGM5_workflow_replay.json').write_text(json.dumps(summary,ensure_ascii=False,indent=2))
    print(json.dumps(summary,ensure_ascii=False))


if __name__=='__main__':
    asyncio.run(replay())
