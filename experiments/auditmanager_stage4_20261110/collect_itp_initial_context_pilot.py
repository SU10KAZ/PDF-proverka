"""Validate a completed initial response and prepare its requested context locally."""
import json
from pathlib import Path
from backend.app.pipeline.stages.findings_verify.source_review_followup import prepare_review_followup
from experiments.auditmanager_stage4_20261110.run_itp_initial_context_pilot import ROOT
from experiments.auditmanager_stage4_20261110.run_itp_graphic_review_pilot import MD


def main():
    result=json.loads((ROOT/'result.json').read_text())
    attempt=json.loads((ROOT/'attempt.json').read_text())
    if result['is_error'] or attempt['status']!='finished':
        raise ValueError('Successful completed response required')
    request=json.loads((ROOT/'request.json').read_text())
    queue=json.loads((ROOT/'queue.json').read_text())
    finding=json.loads((ROOT/'finding.json').read_text())
    supplied={im['block_id']:im for im in request['image_bindings']}
    for receipt in queue['packets'][0]['graphic_attachments']:
        receipt['attached_to_model']=receipt['block_id'] in supplied and receipt['sha256']==supplied[receipt['block_id']]['sha256']
    followup=prepare_review_followup(result['json_data'],queue,[finding],MD.read_text(),MD.parents[1]/'03_analysis/runs/control/blocks_stage02_100')
    (ROOT/'followup.json').write_text(json.dumps(followup,ensure_ascii=False,indent=2)+'\n')
    if 'request' in followup:
        (ROOT/'followup_request.json').write_text(json.dumps(followup['request'],ensure_ascii=False,indent=2)+'\n')
    print(json.dumps({'status':followup['status'],'query_origins':followup.get('query_origins',[]),
        'search_receipt':followup.get('search_receipt'), 'automatic_dispatch':False},ensure_ascii=False,indent=2))


if __name__=='__main__':main()
