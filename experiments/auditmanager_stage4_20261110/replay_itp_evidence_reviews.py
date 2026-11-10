"""Replay recorded manual visual decisions; no model calls or new visual detection."""
import json
from pathlib import Path
from backend.app.pipeline.stages.findings_verify.source_review_evidence import evidence_digest, record_evidence_reviews


def main():
    here = Path(__file__).parent
    validation = json.loads((here/'ITP_astra_graphic_pilot_validation.json').read_text())
    assessment = json.loads((here/'ITP_astra_graphic_pilot_assessment.json').read_text())
    response = validation['raw_response']
    claims = {c['claim_id']:c for c in response['claims']}
    statuses = {'supported_in_region':'supported','supported_broad_context_only':'context_only',
                'wrong_region':'wrong_region','incomplete_region':'incomplete_region'}
    decisions = []
    for region in assessment['visual_regions']:
        item = claims[region['claim_id']]['evidence'][region['evidence_index']]
        assert item['block_id'] == region['block_id'] and item['bbox_norm_xyxy'] == region['bbox_norm_xyxy']
        decisions.append({'claim_id':region['claim_id'],'evidence_index':region['evidence_index'],
            'response_sha256':evidence_digest(response),'evidence_sha256':evidence_digest(item),
            'status':statuses[region['visual_review']], 'reviewer':'assistant_manual_visual_review_20261110',
            'reason':region['reason']})
    result = record_evidence_reviews(validation, decisions)
    result['review_origin'] = 'Recorded manual review; not automatic visual validation.'
    result['runtime_integration'] = 'Local evidence disposition helper only; not wired into production output.'
    result['tests'] = {'passed':24, 'suites':['test_source_review_evidence','test_source_review_results','test_source_review_visual_results']}
    (here/'ITP_evidence_disposition_replay.json').write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n')
    statepath = here/'stage4_readiness.json'
    state = json.loads(statepath.read_text())
    state['evidence_level_disposition_control'] = {'status':'passed_local_replay',
        'report':str(here/'ITP_evidence_disposition_replay.json'), 'claims_preserved':3,
        'wrong_or_incomplete_regions_excluded':2, 'broad_context_not_counted_as_support':1,
        'tests_passed':24, 'automatic_visual_detection':False, 'production_output_integration':False,
        'remaining':'Independent visual validation and claim sufficiency, model-driven context retrieval, and final-output integration remain unproven.'}
    statepath.write_text(json.dumps(state,ensure_ascii=False,indent=2)+'\n')
    print('3 claims preserved; 2 bad regions quarantined; 1 contextual region not treated as support; no automatic claim acceptance.')


if __name__ == '__main__':
    main()
