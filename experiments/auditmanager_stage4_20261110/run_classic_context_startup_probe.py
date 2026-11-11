import asyncio,json,shutil,sys
from pathlib import Path
sys.path.insert(0,'/home/coder/projects/PDF-proverka')
from experiments.auditmanager_stage4_20261110.paired_runner import load_harness
h=load_harness(Path('/tmp/auditmanager_paired_comparison_sources_20261111/classic_astra_with_catalog'),'classic')
work=Path('/tmp/am_classic_context_startup_probe');work.mkdir(exist_ok=False)
h.configure(work,'AK')
version=work/'version';shutil.copytree('/tmp/auditmanager_six_candidate_inputs_context_v3_20261111/AK',version)
from backend.app.pipeline.stages.block_analysis.gemma_findings_only import run_findings_only_for_project
class Ready(Exception):pass
def progress(event):
 if event.get('type')=='started':
  assert not event['uncovered_blocks'] and not event['stage02_crop_missing_blocks']
  (work/'result.json').write_text(json.dumps({'status':'reached_actual_detector_startup','model_calls':0,'blocks_total':event['blocks_total'],'model':event['model'],'source_counts':event['enrichment_sources']},indent=2))
  raise Ready()
def guard(event,args):
 if event in {'socket.connect','socket.getaddrinfo','subprocess.Popen','os.system'}:raise RuntimeError('No model/network allowed in startup probe')
sys.addaudithook(guard)
async def main():
 try:await run_findings_only_for_project(version,output_dir_override=version/'03_analysis/runs/candidate',model=h.MODELS['block_batch'],reasoning_effort='high',on_progress=progress)
 except Ready:print((work/'result.json').read_text())
 else:raise RuntimeError('Did not stop before dispatch')
asyncio.run(main())
