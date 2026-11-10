"""Check unchanged saved Claude decision against strengthened output contract."""
import json
from pathlib import Path
from backend.app.pipeline.stages.findings_verify.source_review_admission import apply_review_admissions
from experiments.auditmanager_stage4_20261110.run_itp_graphic_review_pilot import MD

HERE=Path(__file__).parent

def main():
    initial=Path('/tmp/auditmanager_itp_initial_context_astra_20261110')
    queue=json.loads((initial/'followup.json').read_text())['expanded_queue']
    finding=json.loads((initial/'finding.json').read_text())
    request=json.loads(Path('/tmp/auditmanager_itp_claude_followup_20261110/request.json').read_text())
    supplied={i['block_id']:i['sha256'] for i in request['image_bindings']}
    for receipt in queue['packets'][0]['graphic_attachments']:
        receipt['attached_to_model']=supplied.get(receipt['block_id'])==receipt['sha256']
    response=json.loads((HERE/'ITP_claude_followup_result.json').read_text())['json_data']
    decision=json.loads((HERE/'ITP_semantic_adjudication_result.json').read_text())['json_data']
    original={'findings':[finding]}
    final,receipts=apply_review_admissions(original,[{'queue':queue,'response':response,'decision':decision}],MD.read_text())
    assert final==original
    assert receipts==[{'finding_id':'F-042','status':'not_applied','reason':'incomplete_display_field_review'}]
    result={'new_model_calls':0,'original_finding_preserved':True,'receipts':receipts,
        'meaning':'Prior Claude candidate did not review risk, severity and normative display text. It is now rejected before mutation; raw decision preserved.',
        'supersedes':'Earlier successful admission replay applies only to the previous contract, not the current strengthened contract.',
        'remaining':'Produce and evaluate a complete decision with retrieved calculation and all report fields; no model output edited to fake compliance.'}
    (HERE/'ITP_display_field_guard_replay.json').write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n')
    print(json.dumps(result,ensure_ascii=False,indent=2))

if __name__=='__main__':main()
