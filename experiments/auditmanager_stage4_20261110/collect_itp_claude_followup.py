"""Local provenance validation of the completed Claude follow-up; no dispatch."""
import json
from pathlib import Path
from backend.app.pipeline.stages.findings_verify.source_review_request import prepare_source_review_request
from backend.app.pipeline.stages.findings_verify.source_review_results import validate_source_review
from experiments.auditmanager_stage4_20261110.run_itp_graphic_review_pilot import MD
from experiments.auditmanager_stage4_20261110.run_itp_claude_followup import ROOT


def main():
    initial=Path('/tmp/auditmanager_itp_initial_context_astra_20261110')
    result=json.loads((ROOT/'result.json').read_text())
    if result['is_error']:
        raise ValueError('Provider failure; semantic validation unavailable')
    queue=json.loads((initial/'followup.json').read_text())['expanded_queue']
    finding=json.loads((initial/'finding.json').read_text())
    request=json.loads((ROOT/'request.json').read_text())
    if prepare_source_review_request(queue,finding,MD.read_text()) != request:
        raise ValueError('Current sources differ from executed packet')
    supplied={i['block_id']:i['sha256'] for i in request['image_bindings']}
    for receipt in queue['packets'][0]['graphic_attachments']:
        receipt['attached_to_model']=supplied.get(receipt['block_id'])==receipt['sha256']
    validation=validate_source_review(result['json_data'],queue,[finding],MD.read_text())
    (ROOT/'validation.json').write_text(json.dumps(validation,ensure_ascii=False,indent=2)+'\n')
    print(json.dumps({k:v for k,v in validation.items() if k not in ('claims','raw_response')},ensure_ascii=False,indent=2))
    print(json.dumps({k:v for k,v in result.items() if k not in ('text','json_data')},ensure_ascii=False,indent=2))
    print(json.dumps(result['json_data'],ensure_ascii=False,indent=2))


if __name__=='__main__':main()
