from copy import deepcopy
from backend.app.pipeline.stages.findings_merge.recovery_target_binding import bind_report_targets
from backend.app.pipeline.stages.prepare.codex_targeted_findings import combine_findings_with_targeted


def payload(title, source):
    return {'findings': [{'id': 'F-036', 'problem': title, 'description': title, 'source_finding_ids': [source]}],
            'recovery_existing_target_ids': ['F-035'], 'candidate_decisions': [
                {'origin': 'graphic', 'pointer': '/' + source, 'target_finding_ids': ['F-036'],
                 'subclaims': [{'id': 'part', 'target_finding_ids': ['F-036']}]}]}


def test_two_requests_reusing_local_id_bind_to_distinct_global_findings():
    base = {'findings': [{'id': 'F-035', 'problem': 'Existing'}]}
    first, second = payload('Heater', 'G-013'), payload('Fan labels', 'G-018')
    original = deepcopy(second)
    combined = combine_findings_with_targeted(base, [('candidate_recovery_one', first), ('candidate_recovery_two', second)])
    assert second == original
    report = combined['candidate_recovery_reports'][1]
    decision = report['candidate_decisions'][0]
    assert decision['target_finding_ids'] == ['F-037']
    assert decision['subclaims'][0]['target_finding_ids'] == ['F-037']
    assert report['original_candidate_decisions'][0]['target_finding_ids'] == ['F-036']
    assert combined['findings'][-1]['problem'] == 'Fan labels'


def test_unknown_ambiguous_and_colliding_ids_do_not_fall_back_to_global_ids():
    for existing, local, reason in [([], {'F-036': ['F-038', 'F-039']}, 'unresolved_or_ambiguous_local_target'),
                                    ([], {}, 'unresolved_or_ambiguous_local_target'),
                                    (['F-036', 'F-036'], {}, 'ambiguous_existing_target'),
                                    (['F-036'], {'F-036': ['F-038']}, 'existing_and_local_id_collision')]:
        report = payload('ignored', 'G')['candidate_decisions']
        wrapper = {'candidate_decisions': report}
        result = bind_report_targets(wrapper, existing, local)
        assert result['unresolved'][0]['reason'] == reason
        assert report[0]['target_finding_ids'] == []
    wrapper = {'candidate_decisions': payload('ignored', 'G')['candidate_decisions']}
    assert not bind_report_targets(wrapper, ['F-036'], {})['unresolved']
    assert wrapper['candidate_decisions'][0]['target_finding_ids'] == ['F-036']
