"""Source and output-retention checks of the fresh ranked-context Astra response."""
from copy import deepcopy
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

from backend.app.pipeline.stages.prepare.object_operation_passes import validate_object_operation_payload
from backend.app.pipeline.stages.prepare.codex_targeted_findings import combine_findings_with_targeted
from backend.app.pipeline.stages.findings_verify.source_review_queue import attach_pending_source_reviews
from .run_ogm5_ranked_continuation import ROOT, EXPECTED
from backend.app.pipeline.stages.findings_verify.source_review_evidence import evidence_digest


def main():
    request=json.loads((ROOT/'request.json').read_text());result=json.loads((ROOT/'result.json').read_text())
    assert evidence_digest(request)==EXPECTED and not result['is_error']
    packet=json.loads(request['messages'][-1]['content']);sources={b['block_id']:b['content'] for b in packet['source_context']}
    payload=validate_object_operation_payload(SimpleNamespace(messages=request['messages']),result['json_data'])
    quotes=[]
    for i,f in enumerate(result['json_data']['findings']):
        for ev in f.get('evidence',[]):
            if 'quote' in ev:
                quotes.append({'finding_index':i,'block_id':ev['block_id'],'quote':ev['quote'],
                    'exact':ev['quote'] in sources.get(ev['block_id'],'')})
    assert all(q['exact'] for q in quotes)
    assert len(payload['findings'])==2 and len(payload['operation_pending'])==2
    baseline_path=Path('/home/coder/auditmanager/corpus-audits/20261106_classic_investigation/classic_control/inputs/KM/versions/v001/03_analysis/runs/control/03_findings.json')
    baseline=json.loads(baseline_path.read_text());original=deepcopy(baseline)
    combined=combine_findings_with_targeted(baseline,[('object_operations_ogm5_ranked',payload)])
    attach_pending_source_reviews(combined['findings'],set(sources))
    assert baseline==original and combined['findings'][:len(baseline['findings'])]==baseline['findings']
    assert len(combined['findings'])==28
    assert all(f.get('source_evidence_review_status')=='pending' for f in combined['findings'][26:])
    (ROOT/'validation.json').write_text(json.dumps(payload,ensure_ascii=False,indent=2)+'\n')
    (ROOT/'combined.json').write_text(json.dumps(combined,ensure_ascii=False,indent=2)+'\n')
    report={'request_sha256':EXPECTED,'result_sha256':hashlib.sha256((ROOT/'result.json').read_bytes()).hexdigest(),
        'model':result['model'],'duration_ms':result['duration_ms'],'input_tokens':result['input_tokens'],'output_tokens':result['output_tokens'],
        'new_model_calls':1,'raw_candidates':4,'retained_question_candidates':2,'internal_context_candidates':2,
        'baseline_findings_preserved':26,'combined_findings':28,'exact_quote_checks':quotes,
        'operation_states':{r['operation']:r['status'] for r in payload['object_operation_results']},
        'bounded_source_review':[
            {'claim':'Sheet reference 4 versus drawing register sheet 3','status':'previously_source_confirmed_and_retained','prior_assessment':'KM_OGM5_continuation_assessment.json'},
            {'claim':'ГОСТ 8732-78 on node В versus ГОСТ 8732-2025 in MD reference register','status':'source_confirmed_designation_conflict_only','graphic_block':'blk_cfd1c741a6fa4cd99c3eccd1d42afc7d','graphic_visually_inspected':True,'legal_applicability_verified':False},
            {'claim':'648 versus 650','status':'unresolved_measurement_basis','technical_defect_proven':False},
            {'claim':'Anchor applicability','status':'missing_calculation_and_base_context','whole_project_absence_proven':False}],
        'comparison_to_prior_response':{'prior_raw_questions':3,'current_raw_questions':4,'confirmed_question_categories_before':2,'confirmed_question_categories_after':2,'new_technical_defects':0,'prior_nonverbatim_quotes':3,'current_nonverbatim_quotes':0,
            'conclusion':'No demonstrated increase in confirmed findings in this known-case followup. Exact quotations improved in this sample; model variability is not controlled by one comparison.'},
        'production_accepted':False,'semantic_coverage_complete':False,
        'limits':['Fresh response to changed context; engineer answers not supplied, but known project and prior model response are used.',
            'This is a contextual followup, not blind independent discovery or a full candidate audit.',
            'Quantity statements inside dimension receipt were not separately accepted as a completed quantity operation.',
            'Final source-review gate stays pending; no automatic production admission.']}
    Path(__file__).with_name('KM_OGM5_ranked_continuation_assessment.json').write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n')
    print(json.dumps({k:report[k] for k in ('raw_candidates','retained_question_candidates','internal_context_candidates','baseline_findings_preserved','combined_findings')}))

if __name__=='__main__':main()
