"""Read automatically selected regions from the first uncorrected source review."""
import argparse
import asyncio
import hashlib
import json
from pathlib import Path
from backend.app.pipeline.stages.findings_verify.graphic_region_reading import prepare_review_region_readings
from backend.app.pipeline.stages.findings_verify.graphic_literal_reading import bind_literal_reading
from backend.app.pipeline.stages.findings_verify.source_review_workflow import direct_cli_executor
from .run_itp_pipe_operations import immutable

SOURCE=Path('/tmp/auditmanager_itp_automatic_t411_source_review_20261111/workflow/00_review')
ROOT=Path('/tmp/auditmanager_itp_model_region_reading_20261111')


def prepare():
    request=json.loads((SOURCE/'request.json').read_text())['request']
    response=json.loads((SOURCE/'result.json').read_text())['json_data']
    manifest=prepare_review_region_readings(request,response,ROOT/'regions')
    immutable(ROOT/'manifest.json',manifest)
    return manifest


async def main(execute):
    manifest=prepare()
    print('PREPARED',len(manifest['regions']),'automatic regions',flush=True)
    if not execute:return
    invoke=direct_cli_executor('codex/gpt-6-astra',project_id='ITP-auto-region-reading')
    readings=[]
    for i,region in enumerate(manifest['regions']):
        request=region['request'];folder=ROOT/f'read_{i:02d}';folder.mkdir(exist_ok=True)
        immutable(folder/'request.json',request)
        parent=region['source'];assert hashlib.sha256(Path(parent['parent_path']).read_bytes()).hexdigest()==parent['parent_image_sha256']
        if (folder/'result.json').exists():result=json.loads((folder/'result.json').read_text())
        else:
            with (folder/'attempt.json').open('x') as f:json.dump({'status':'started'},f)
            result=await invoke(request,'literal_reading')
            immutable(folder/'result.json',result)
            (folder/'attempt.json').write_text(json.dumps({'status':'finished','is_error':result.get('is_error')}))
        if result.get('is_error'):raise ValueError('provider_error')
        reading=bind_literal_reading(request,result['json_data'])
        readings.append({'source':parent,'reading':reading})
    immutable(ROOT/'readings.json',readings)
    print('COMPLETE',len(readings),flush=True)


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--execute',action='store_true');asyncio.run(main(p.parse_args().execute))
