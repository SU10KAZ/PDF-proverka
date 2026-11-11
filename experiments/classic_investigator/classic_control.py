"""Source-only classic Codex control, isolated from production and research answers."""
import argparse
import asyncio
from dataclasses import asdict, is_dataclass
import os
from pathlib import Path
import subprocess
import sys
import time
from investigate import ROOT, read, write, sha, configure as configure_transport
from subscription_retry import retry_subscription_call
from execution_receipts import summarize_methods, summarize_request_usage

MODELS = {'text_analysis':'codex/gpt-6.1-sol','block_batch':'ensemble/codex-only',
          'findings_merge':'codex/gpt-6-sol','findings_critic':'codex/gpt-6-sol',
          'findings_corrector':'codex/gpt-6.1-sol','norm_verify':'codex/gpt-6-astra',
          'norm_fix':'codex/gpt-6-astra','norm_requote':'codex/gpt-6-astra',
          'optimization':'ensemble/codex-codex-opt','optimization_critic':'codex/gpt-6-sol',
          'optimization_corrector':'codex/gpt-6-sol'}
ALLOWED = {'gpt-6-astra','gpt-6-sol','gpt-6.1-sol'}
REASONING_EFFORT = 'medium'
MAX_REQUEST_ATTEMPTS = None
METHOD_CONFIG = {}
ENV = {'STAGE01_CLAUDE_LEG_ENABLED':'false','STAGE01_THIRD_LEG_ENABLED':'false',
       'STAGE01_CODEX_ONLY_LEG_MODELS':'codex/gpt-6-astra,codex/gpt-6-sol',
       'STAGE01_CODEX_ONLY_LEG_EFFORT':'medium','STAGE01_CODEX_ONLY_JUDGE_MODEL':'codex/gpt-6.1-sol',
       'STAGE01_CODEX_ONLY_JUDGE_EFFORT':'medium','STAGE01_NEIGHBOR_COORD_TEXT_ENABLED':'false',
       'MD_MIRROR_RECONCILE_ENABLED':'false','FINDING_EVIDENCE_OCR_OBSERVER_ENABLED':'false'}


def validate_model(model):
    if str(model).removeprefix('codex/') not in ALLOWED:
        raise RuntimeError(f'Model outside frozen Codex control: {model}')


def method_context_options(project_id):
    """Explicit opt-in only; no callback means no extra calls in the baseline."""
    if not METHOD_CONFIG:
        return {}
    groups={
        'inventory': {'inventory_max_packets','inventory_max_source_chars','inventory_max_images'},
        'source_review': {'source_review_max_findings','source_review_auto_graphic_regions',
                          'source_final_check_max_findings','source_final_check_complete_citations'},
    }
    allowed=set().union(*groups.values())
    if set(METHOD_CONFIG)-allowed:
        raise ValueError('Unknown method configuration fields')
    enabled=[]
    for name, fields in groups.items():
        present=set(METHOD_CONFIG)&fields
        if present and present!=fields:
            raise ValueError('Incomplete method configuration: '+name)
        if present:enabled.append(name)
    booleans={'source_review_auto_graphic_regions','source_final_check_complete_citations'}
    for key,value in METHOD_CONFIG.items():
        if key in booleans:
            if type(value) is not bool:raise ValueError('Explicit method mode required: '+key)
        elif type(value) is not int or value<1:
            raise ValueError('Positive explicit method budget required: '+key)
    from backend.app.pipeline.stages.findings_verify.source_review_workflow import direct_cli_executor
    invoke=direct_cli_executor('codex/gpt-6-astra',project_id=project_id,reasoning_effort=REASONING_EFFORT)
    identity='codex/gpt-6-astra:'+REASONING_EFFORT+':paired-methods-v2'
    options=dict(METHOD_CONFIG)
    for name in enabled:
        options[name+'_invoke']=invoke
        options[name+'_executor_id']=identity
        if name=='source_review':
            options['source_final_check_invoke']=invoke
            options['source_final_check_executor_id']=identity
    return options


def inline_evidence_options(options):
    """No search/file tools in this source-only comparison scope."""
    if options.get('allowed_tools') not in (None, ''):
        raise RuntimeError('TOOLS_OUTSIDE_FROZEN_INLINE_COMPARISON_SCOPE')
    return {**options, 'allowed_tools': ''}


def preflight_inventory_inputs(version, output_dir, config):
    """Reject unexecutable inventory inputs before any graphic/text model call."""
    if 'inventory_max_packets' not in config:
        return {'status':'not_enabled','model_calls':0}
    from backend.app.pipeline.stages.prepare.object_inventory_passes import build_object_inventory_passes
    from backend.app.pipeline.stages.block_context.contract import resolve_blocks_dir
    requests, manifest = build_object_inventory_passes(
        Path(version)/'02_work/document.md', resolve_blocks_dir(Path(output_dir)),
        max_chars=config['inventory_max_source_chars'], max_images=config['inventory_max_images'])
    if not manifest['input_coverage_complete']:
        raise ValueError('inventory_preflight_incomplete_source_coverage')
    if len(requests) > config['inventory_max_packets']:
        raise ValueError('inventory_preflight_packet_budget_exceeded')
    return {'status':'passed','model_calls':0,'manifest':manifest,
            'semantic_inventory_complete':False}


def process_guard(event, args):
    if event == 'subprocess.Popen':
        executable = Path(os.fsdecode(args[0])).name
        if executable not in {'codex','codex.exe'}:
            raise RuntimeError(f'Non-Codex subprocess forbidden in classic control: {executable}')


def freeze(work, *, source_snapshot=None):
    root=work/'classic_control'
    if (root/'protocol.json').exists():raise ValueError('Protocol already frozen')
    if read(root/'input_checks.json')['status']!='passed':raise ValueError('Inputs not checked')
    files=[]
    for directory,pattern in [('backend/app','*.py'),('prompts','*'),('experiments/classic_investigator','*.py')]:
        files.extend(p for p in (ROOT/directory).rglob(pattern) if p.is_file())
    catalog=ROOT/'backend/app/pipeline/stages/block_context/reference_catalog'
    files.extend(p for p in catalog.rglob('*') if p.is_file() and p.suffix in {'.json','.md'})
    marker=ROOT/'backend/__init__.py'
    if marker.is_file():files.append(marker)
    source_files={str(p.relative_to(ROOT)):sha(p) for p in files}
    if source_snapshot is not None:
        if (source_snapshot.get('source_root')!=str(ROOT.resolve())
                or source_snapshot.get('source_files')!=source_files
                or not source_snapshot.get('provenance')):
            raise ValueError('Source snapshot provenance or bytes differ')
        # An exported tree has no Git HEAD. Its historical origin and overlays
        # are explicit provenance, never the coordinator checkout's commit.
        commit=None
    else:
        git_root=Path(subprocess.check_output(['git','rev-parse','--show-toplevel'],cwd=ROOT,text=True).strip())
        if git_root.resolve()!=ROOT.resolve():
            raise ValueError('Source root is not the Git checkout root; snapshot provenance required')
        commit=subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip()
    write(root/'protocol.json',{
        'name':'classic_fixed_codex_only','created':time.time(),
        'commit':commit,'source_snapshot':source_snapshot,
        'source_files':source_files,
        'input_manifest_sha256':sha(root/'input_manifest.json'),'stage_models':MODELS,'environment':ENV,
        'method_config':METHOD_CONFIG,'reasoning_effort':REASONING_EFFORT,'max_request_attempts':MAX_REQUEST_ATTEMPTS,'graphic_parallelism':2,
        'scope':'graphic detector ensemble + full authoritative MD + merge + structural/absence verification',
        'excluded':'optimization and external normative vault verification; experimental operation injection',
        'blindness':'Source-only inputs. Known development projects, not unseen validation.',
        'limits':'Different model topology and rerendered graphics from archived A/B; no causal comparison with archived counts.',
        'production_changed':False,'claude_allowed':False,'openrouter_allowed':False})


def configure(work,disc):
    root=work/'classic_control';runtime=root/'runtime'/disc;runtime.mkdir(parents=True,exist_ok=True)
    state=runtime/'appdata';state.mkdir(exist_ok=True)
    write(state/'stage_models.json',MODELS)
    os.environ.update(ENV)
    os.environ['AUDIT_PROMPTS_DIR']=str(ROOT/'prompts')
    os.environ.pop('AUDIT_WORKER_ATTEMPT_DIR',None);os.environ.pop('AUDIT_SECOND_LEG',None)
    configure_transport(runtime)
    from backend.app.core import config
    if config.STAGE_MODEL_CONFIG != MODELS:raise RuntimeError('Effective stage table differs')
    from backend.app.services.llm import codex_runner,claude_json_runner
    original=codex_runner.run_codex_json_messages
    async def deny(*a,**kw):raise RuntimeError('CLAUDE_DISABLED_IN_CLASSIC_CONTROL')
    claude_json_runner.run_claude_json_messages=deny
    async def guarded(messages,**kwargs):
        validate_model(kwargs.get('model'))
        kwargs=inline_evidence_options(kwargs)
        kwargs['reasoning_effort']=REASONING_EFFORT
        request=root/'requests'/disc/str(time.time_ns());request.mkdir(parents=True)
        write(request/'request.json',{'messages':messages,'options':{k:([str(p) for p in v] if k=='image_paths' and v else v) for k,v in kwargs.items() if k!='on_output'},
              'image_hashes':{str(p):sha(p) for p in kwargs.get('image_paths') or []}})
        attempts=[]
        async def attempt(*a,**kw):
            validate_model(kw.get('model'))
            result=await original(*a,**kw)
            write(request/f'response_{len(attempts)+1}.json',{'text':result.text,'json_data':result.json_data,
                  'error':result.error_message,'model':result.model})
            return result
        def record(item):attempts.append(item);write(request/'attempts.json',attempts)
        return await retry_subscription_call(attempt,messages,**kwargs,
            stop_requested=lambda:(work/'STOP_REQUESTED.json').exists() or (root/'STOP_REQUESTED.json').exists(),on_attempt=record,max_attempts=MAX_REQUEST_ATTEMPTS)
    codex_runner.run_codex_json_messages=guarded
    from backend.app.pipeline.stages.text_analysis import absence_guard
    def verify(md,candidates,*,timeout_sec=180):
        result=asyncio.run(guarded([{'role':'user','content':absence_guard.build_verification_prompt(md,candidates)}],
            model=MODELS['findings_corrector'],timeout=timeout_sec,stage='absence_verification',allowed_tools=''))
        if result.is_error or not isinstance(result.json_data,dict):raise RuntimeError('Absence check incomplete')
        return absence_guard.parse_verification_response(result.json_data)
    absence_guard.run_claude_verification=verify
    absence_guard.run_codex_verification=verify
    return runtime


async def run(work,disc):
    root=work/'classic_control';protocol=read(root/'protocol.json')
    if (protocol['stage_models']!=MODELS or protocol['environment']!=ENV
            or protocol['reasoning_effort']!=REASONING_EFFORT
            or protocol.get('max_request_attempts')!=MAX_REQUEST_ATTEMPTS
            or protocol.get('method_config',{})!=METHOD_CONFIG):
        raise ValueError('Execution settings differ from frozen protocol')
    for name,digest in protocol['source_files'].items():
        if sha(ROOT/name)!=digest:raise ValueError(f'Frozen implementation changed: {name}')
    if sha(root/'input_manifest.json')!=protocol['input_manifest_sha256']:raise ValueError('Input manifest changed')
    project=next(p for p in read(root/'input_manifest.json') if p['discipline']==disc)
    version=Path(project['version_dir']);out=Path(project['output_dir'])
    for name,digest in project['files'].items():
        if sha(version/name)!=digest:raise ValueError(f'Input changed: {name}')
    status_path=root/'status'/f'{disc}.json'
    if status_path.exists():raise ValueError('Run already exists; explicit recovery required')
    if (work/'STOP_REQUESTED.json').exists() or (root/'STOP_REQUESTED.json').exists():raise ValueError('Stop requested')
    configure(work,disc)
    from backend.app.pipeline.context import PipelineStageContext
    from backend.app.services.common import audit_scope
    from backend.app.pipeline.stages.block_analysis.gemma_findings_only import run_findings_only_for_project
    from backend.app.pipeline.stages.block_analysis.blocks_for_text import write_blocks_for_text_compact
    from backend.app.pipeline.stages.text_analysis.runner import run_text_analysis
    from backend.app.pipeline.stages.findings_merge.runner import run_findings_merge
    from backend.app.pipeline.stages.findings_verify.runner import run_findings_verify
    sys.addaudithook(process_guard)
    state={'status':'running','started':time.time(),'stages':{},'stage_results':{},'usage':[],'discipline':disc}
    def save():write(status_path,state)
    async def log(message,*a):print(f'[{disc}] {str(message)[:600]}',flush=True)
    async def yes():return not ((work/'STOP_REQUESTED.json').exists() or (root/'STOP_REQUESTED.json').exists())
    async def no(*a,**kw):return False
    async def deny(*a,**kw):raise RuntimeError('Unexpected subprocess stage')
    def usage(result,stage,*a,**kw):
        state['usage'].append({'stage':stage,**{k:getattr(result,k,None) for k in ('model','input_tokens','output_tokens','duration_ms','cost_usd')}});save()
    def update(stage,status,**kw):state['stages'][stage]={'status':status,**kw};save()
    def progress(event):
        if event.get('type')=='block_done':
            state['block_progress']={k:event.get(k) for k in ('completed','total','ok','detectors_failed')};save()
            print(f'[{disc}] {state["block_progress"]}',flush=True)
    pid=f'classic-control-{disc}'
    ctx=PipelineStageContext(project_dir=version,project_id=pid,output_dir=out,log=log,check_before_launch=yes,
        check_pause=yes,wait_for_rate_limit=no,record_cli_usage=usage,update_pipeline_log=update,
        run_subprocess=deny,project_info=read(version/'01_input/project_info.json'),version_id=version.name,
        is_cancelled=lambda:(work/'STOP_REQUESTED.json').exists() or (root/'STOP_REQUESTED.json').exists(),
        **method_context_options(pid))
    save()
    try:
        with audit_scope.bind_audit_scope(output_dir=out,version_dir=version,project_id=pid,version_id=version.name):
            preflight = preflight_inventory_inputs(version, out, METHOD_CONFIG)
            write(root/'method_input_preflight'/f'{disc}.json', preflight)
            state['method_input_preflight'] = {'status':preflight['status'],
                'sha256':sha(root/'method_input_preflight'/f'{disc}.json')}
            save()
            result=await run_findings_only_for_project(version,output_dir_override=out,model=MODELS['block_batch'],
                reasoning_effort=REASONING_EFFORT,parallelism=2,timeout_s=600,on_progress=progress,project_id=pid,version_id=version.name,job_id=pid)
            blocks=result['output_doc']['block_analyses']
            expected={b['block_id'] for b in read(out/'blocks_stage02_100/index.json')['blocks']}
            if {b['block_id'] for b in blocks}!=expected or len(blocks)!=len(expected):raise RuntimeError('Incomplete graphic coverage')
            if any(b.get('analysis_status')!='analyzed' or b.get('detectors_failed') for b in blocks):raise RuntimeError('Graphic detector failure')
            write_blocks_for_text_compact(out)
            for name,fn in [('text_analysis',run_text_analysis),('findings_merge',run_findings_merge),('findings_verify',run_findings_verify)]:
                if not await yes():raise RuntimeError('Stop requested')
                result=await fn(ctx,**({'with_rate_limit_retry':False} if name=='text_analysis' else {}))
                state['stage_results'][name]=asdict(result) if is_dataclass(result) else vars(result)
                save()
                if not getattr(result,'success',getattr(result,'ok',False)):raise RuntimeError(f'{name} incomplete: {result}')
                if (out/'03_findings.json').exists():write(root/'snapshots'/disc/f'{name}.json',read(out/'03_findings.json'))
            method_receipts=summarize_methods(out,METHOD_CONFIG,state['stage_results'])
            write(root/'method_execution'/f'{disc}.json',method_receipts)
            state.update(status='completed_unadjudicated' if method_receipts['declared_methods_execution_complete'] else 'completed_with_unresolved_methods',
                method_execution=method_receipts,final_sha256=sha(out/'03_findings.json'))
    except Exception as exc:
        state.update(status='failed',error=f'{type(exc).__name__}: {exc}');raise
    finally:
        state['finished']=time.time()
        state['request_usage']=summarize_request_usage(root/'requests'/disc)
        if state.get('status')=='completed_unadjudicated' and not state['request_usage']['accounting_complete']:
            state['status']='completed_with_incomplete_usage'
        save()

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('command',choices=['freeze','run']);p.add_argument('--work',type=Path,required=True);p.add_argument('--discipline',choices=['AK','ITP','AR','KM','VK','EOM'])
    a=p.parse_args()
    if a.command=='freeze':freeze(a.work)
    else:asyncio.run(run(a.work,a.discipline))
