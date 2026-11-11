"""Independent same-input model check; no history, answer or automatic acceptance."""
import argparse
import asyncio
import json
from pathlib import Path
from . import run_km_f014_source_readjudication as original
from .run_itp_pipe_operations import immutable
from backend.app.pipeline.stages.findings_verify.source_review_workflow import direct_cli_executor
from backend.app.pipeline.stages.findings_verify.source_review_evidence import evidence_digest

ROOT=Path('/tmp/auditmanager_km_f014_claude_same_input_20261111')

async def main(execute, root=ROOT):
    # Rebuild and verify the old request against original source bytes. Does not
    # read the Astra response, the evaluator or any engineer answer.
    ROOT=Path(root)
    wire, source_binding=original.prepare()
    ROOT.mkdir(exist_ok=True)
    manifest={**source_binding, 'model':'claude/claude-sonnet-5-5',
              'comparison':'same supplied request and images as Astra; known-candidate control',
              'subscription_cost':'quota; monetary maximum unknown'}
    immutable(ROOT/'request.json',wire);immutable(ROOT/'binding.json',manifest)
    if not execute:return
    with (ROOT/'attempt.json').open('x') as f:
        json.dump({'status':'started','request_sha256':manifest['request_sha256']},f)
    async def save_event(event):
        immutable(ROOT/'cli_result_event.json',event)
    result=await direct_cli_executor(manifest['model'],project_id='KM-F014-independent-same-input',
        claude_result_observer=save_event)(wire,'readjudication')
    immutable(ROOT/'result.json',result)
    immutable(ROOT/'result_binding.json',{'request':manifest['request_sha256'],'result':evidence_digest(result)})
    original.prepare()
    usable=not result.get('is_error') and isinstance(result.get('json_data'),dict)
    immutable(ROOT/'completion.json',{'status':'response_requires_source_assessment' if usable else 'provider_error',
        'semantic_acceptance':False,'automatic_evaluator_change':False})
    if not usable:raise ValueError('Provider response unusable; inspect saved error before retry')
    print('COMPLETE: response saved; not accepted',flush=True)

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--execute',action='store_true')
    p.add_argument('--output-root',type=Path,default=ROOT)
    args=p.parse_args();asyncio.run(main(args.execute,args.output_root))
