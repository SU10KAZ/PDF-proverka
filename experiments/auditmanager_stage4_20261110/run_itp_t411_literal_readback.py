"""Independent literal graphic reading; no prior model answers or expected digits."""
import argparse
import asyncio
import hashlib
import json
from pathlib import Path
from .run_itp_pipe_operations import immutable
from backend.app.pipeline.stages.findings_verify.source_review_workflow import direct_cli_executor
from backend.app.pipeline.stages.findings_verify.source_review_evidence import evidence_digest
ROOT=Path('/tmp/auditmanager_itp_t411_literal_readback_20261111')
IMAGE=Path('/tmp/itp_t411_numeric_source_detail.png')
PROVENANCE=Path('/tmp/itp_t411_numeric_source_detail.json')


def prepare(root=ROOT, model="claude/claude-sonnet-5-5"):
    ROOT = Path(root)
    ROOT.mkdir(exist_ok=True)
    provenance=json.loads(PROVENANCE.read_text())
    assert hashlib.sha256(Path(provenance['source_pdf']).read_bytes()).hexdigest()==provenance['source_pdf_sha256']
    request={'messages':[{'role':'system','content':'Transcribe only the attached project drawing annotation. Do not use tools, outside sources, expected values or engineering inference. Return JSON with verbatim_annotation, object_mark, diameter_literal, service_literal, temperature_literal, flow_literal, pressure_literal and reading_uncertainties. Preserve printed digits and units exactly. If any character is unreadable, state that uncertainty rather than guessing. This is literal reading, not a defect verdict.'},{'role':'user','content':'Read the single annotation in the supplied image. The image was rendered directly from its PDF graphic and rotated for legibility; no OCR text is supplied.'}], 'image_paths':[str(IMAGE)]}
    binding={'request_sha256':evidence_digest(request),'model':model,'image_sha256':hashlib.sha256(IMAGE.read_bytes()).hexdigest(),'source_provenance':provenance,'expected_answer_or_prior_responses_supplied':False,'automatic_retries':0,'scope':'Known-source literal reading; not blind project discovery or full model comparison.'}
    immutable(ROOT/'request.json',request);immutable(ROOT/'binding.json',binding)
    print('PREPARED',sum(len(m['content']) for m in request['messages']),'chars',IMAGE.stat().st_size,'image bytes',flush=True)
    return request,binding


async def main(execute, root=ROOT, model="claude/claude-sonnet-5-5"):
    ROOT = Path(root)
    request,binding=prepare(ROOT, model)
    if not execute:return
    if (ROOT/'result.json').exists():return
    with (ROOT/'attempt.json').open('x') as f:json.dump({'status':'started'},f)
    result=await direct_cli_executor(model,project_id='ITP-T411-literal-readback')(request,'review')
    assert hashlib.sha256(IMAGE.read_bytes()).hexdigest()==binding['image_sha256']
    immutable(ROOT/'result.json',result)
    (ROOT/'attempt.json').write_text(json.dumps({'status':'finished','is_error':result.get('is_error')}))
    print('COMPLETE',not result.get('is_error'),flush=True)


if __name__=='__main__':
    p=argparse.ArgumentParser()
    p.add_argument('--execute',action='store_true')
    p.add_argument('--output-root',type=Path,default=ROOT)
    p.add_argument('--model',choices=['codex/gpt-6-astra','claude/claude-sonnet-5-5'],default='claude/claude-sonnet-5-5')
    args=p.parse_args()
    asyncio.run(main(args.execute,args.output_root,args.model))
