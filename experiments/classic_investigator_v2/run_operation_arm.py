"""Run only merge/verify on identical classic candidates plus operation questions."""
import argparse
import asyncio
from pathlib import Path
import shutil
import sys
import time
from source_contract import read,write,sha
from investigate import ROOT
from classic_control import configure,process_guard


def prepare(work,disc):
    arm=work/'operation_arm'/disc;injection=read(arm/'injection.json')
    project=next(p for p in read(work/'classic_control/input_manifest.json') if p['discipline']==disc)
    source_version=Path(project['version_dir']);source_out=Path(project['output_dir'])
    if sha(source_out/'02_text_analysis.json')!=injection['baseline_text_sha256']:raise ValueError('Baseline text changed')
    root=arm/'classic_control';root.mkdir(exist_ok=False)
    version=root/'inputs'/disc/'versions'/source_version.name;out=version/'03_analysis/runs/control'
    # Copy source-only prepared files, then the two finished premerge stages.
    # Deliberately exclude baseline merged/verified answers and evaluation history.
    for name,digest in project['files'].items():
        src=source_version/name
        if sha(src)!=digest:raise ValueError(f'Prepared source changed: {name}')
        dst=version/name;dst.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(src,dst)
    for name in ['01_blocks_analysis.json','01_blocks_for_text.json']:
        shutil.copyfile(source_out/name,out/name)
    shutil.copyfile(arm/'02_text_analysis.json',out/'02_text_analysis.json')
    for path in version.rglob('*.json'):
        path.write_text(path.read_text().replace(str(source_version),str(version)))
    # Global user stop propagates to the isolated transport between attempts.
    (arm/'STOP_REQUESTED.json').symlink_to(work/'STOP_REQUESTED.json')
    files={str(p.relative_to(version)):sha(p) for p in version.rglob('*') if p.is_file()}
    baseline=read(work/'classic_control/protocol.json')
    write(root/'protocol.json',{'discipline':disc,'version_dir':str(version),'output_dir':str(out),'files':files,
        'code_sha256':sha(__file__),'baseline_code_files':baseline['source_files'],
        'injection_sha256':sha(arm/'injection.json'),'stage_models':baseline['stage_models'],
        'scope':'Downstream ablation: same finished graphic/text candidates, add only deterministic question receipts, rerun merge and verify with same Codex models/effort.',
        'limits':'Not a fresh independent whole-project audit. Stochastic merge effects require paired repetition if results are ambiguous. Questions are not scored as defects.',
        'source_history_supplied':False,'production_changed':False})
    print(disc,'operation arm prepared; source files',len(files),flush=True)


async def run(work,disc):
    arm=work/'operation_arm'/disc;root=arm/'classic_control';protocol=read(root/'protocol.json')
    if sha(__file__)!=protocol['code_sha256']:raise ValueError('Arm runner changed')
    for name,digest in protocol['baseline_code_files'].items():
        if sha(ROOT/name)!=digest:raise ValueError(f'Frozen classic implementation changed: {name}')
    if sha(arm/'injection.json')!=protocol['injection_sha256']:raise ValueError('Injection protocol changed')
    version=Path(protocol['version_dir']);out=Path(protocol['output_dir'])
    for name,digest in protocol['files'].items():
        if sha(version/name)!=digest:raise ValueError(f'Arm input changed: {name}')
    status=root/'status.json'
    if status.exists():raise ValueError('Arm exists; explicit recovery required')
    if (work/'STOP_REQUESTED.json').exists():raise ValueError('Stop requested')
    configure(arm,disc)
    from backend.app.pipeline.context import PipelineStageContext
    from backend.app.services.common import audit_scope
    from backend.app.pipeline.stages.findings_merge.runner import run_findings_merge
    from backend.app.pipeline.stages.findings_verify.runner import run_findings_verify
    sys.addaudithook(process_guard)
    state={'status':'running','discipline':disc,'started':time.time(),'usage':[],'stages':{}}
    def save():write(status,state)
    async def log(message,*a):print(f'[operations/{disc}] {str(message)[:700]}',flush=True)
    async def yes():return not (work/'STOP_REQUESTED.json').exists() and not (root/'STOP_REQUESTED.json').exists()
    async def no(*a,**kw):return False
    async def deny(*a,**kw):raise RuntimeError('Unexpected subprocess stage')
    def usage(result,stage,*a,**kw):
        state['usage'].append({'stage':stage,**{k:getattr(result,k,None) for k in ('model','input_tokens','output_tokens','duration_ms','cost_usd')}});save()
    def update(stage,status,**kw):state['stages'][stage]={'status':status,**kw};save()
    pid=f'operation-arm-{disc}'
    ctx=PipelineStageContext(project_dir=version,project_id=pid,output_dir=out,log=log,check_before_launch=yes,
        check_pause=yes,wait_for_rate_limit=no,record_cli_usage=usage,update_pipeline_log=update,
        run_subprocess=deny,project_info=read(version/'01_input/project_info.json'),version_id=version.name)
    save()
    try:
        with audit_scope.bind_audit_scope(output_dir=out,version_dir=version,project_id=pid,version_id=version.name):
            for name,fn in [('findings_merge',run_findings_merge),('findings_verify',run_findings_verify)]:
                if not await yes():raise RuntimeError('Stop requested')
                result=await fn(ctx)
                if not getattr(result,'success',getattr(result,'ok',False)):raise RuntimeError(f'{name} incomplete: {result}')
                write(root/'snapshots'/f'{name}.json',read(out/'03_findings.json'))
            state.update(status='completed_unadjudicated',final_sha256=sha(out/'03_findings.json'))
    except Exception as exc:state.update(status='failed',error=f'{type(exc).__name__}: {exc}');raise
    finally:state['finished']=time.time();save()

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('command',choices=['prepare','run']);p.add_argument('--work',type=Path,required=True);p.add_argument('--discipline',required=True);a=p.parse_args()
    if a.command=='prepare':prepare(a.work,a.discipline)
    else:asyncio.run(run(a.work,a.discipline))
