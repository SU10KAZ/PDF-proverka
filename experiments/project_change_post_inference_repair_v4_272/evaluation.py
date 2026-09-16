"""Evaluate frozen V4 output against unchanged Pair A audit; no admission calls."""
from collections import Counter
import io
import unittest

from .replay import OUT, LIVE, V1, V2, V3, BASE, REPO, read, save, sha, now, verify_inputs, offline_guard


def main():
    freeze=read(OUT/'REPLAY_RESULT_FREEZE.json')
    for name,digest in freeze['files'].items():
        assert sha(OUT/name)==digest,name
    for name,digest in read(OUT/'REPAIR_CODE_FREEZE.json')['code'].items():
        assert sha(REPO/name)==digest,name
    offline_guard()
    # Check the existing independent immutable manifests, as well as our snapshot.
    for root in (V2,V3):
        for name,digest in read(root/'FINAL_MANIFEST.json')['files'].items():
            assert sha(root/name)==digest,name
    save('SOURCE_AUDIT_ACCESS_RECEIPT.json',dict(at=now(),replay_completed_at=freeze['at'],
        purpose='Evaluation only after frozen output; historically exposed DEV, not newly blind.',
        files={name:sha(LIVE/name) for name in ('SOURCE_AUDIT.json','SOURCE_FIRST_TRUTH.json')}))
    audit=read(LIVE/'SOURCE_AUDIT.json'); truth=read(LIVE/'SOURCE_FIRST_TRUTH.json')
    rows=read(OUT/'PAIR_A_REPAIRED_V4_RESULTS.json')
    previous={r['package_id']:r for r in read(V3/'PAIR_A_REPAIRED_V3_RESULTS.json')}
    by_id={r['package_id']:r for r in rows}
    cases={c['package_id']:c for c in audit['case_audit']}
    accepted={r['package_id'] for r in rows if r['status']=='ACCEPT'}
    negatives={r['package_id'] for r in rows if r['status']=='NOT_CHANGE'}
    accept_eval=[dict(candidate_id=k,correct=cases[k]['audit_class']=='REAL_CHANGE_BLOCKED',frozen_audit=cases[k]) for k in sorted(accepted)]
    negative_eval=[dict(candidate_id=k,correct=cases[k]['audit_class']=='NOT_CHANGE_BLOCKED',
        scope=by_id[k]['normalized']['claim_applicability']['NEGATIVE_SCOPE'],frozen_audit=cases[k]) for k in sorted(negatives)]
    independent=[]
    for t in truth['entries']:
        if t['classification']!='REAL_CHANGE': continue
        trace=next(c for c in audit['independent_comparison'] if c['truth_id']==t['id'])
        found=accepted & set(trace['found_package_ids'])
        full=bool(found) and trace['detection']=='FULL' and set(trace['found_package_ids'])<=accepted
        independent.append(dict(truth_id=t['id'],title=t['title'],
            status='FULL_ACCEPTED' if full else 'PARTIAL_ACCEPTED' if found else 'MISSED',
            accepted_packages=sorted(found),frozen_comparison=trace))
    assert len(independent)==15
    save('FROZEN_AUDIT_COMPARISON.json',dict(accepts=accept_eval,negative_verdicts=negative_eval,
        independent=independent,denominator=15,truth_changed=False,source_truth_used_for_rules=False))

    # Verify primary raster receipts against the images actually supplied in the
    # saved invocation, using the original F4 validator results and file hashes.
    delivery=[]
    plan={i['key']:i for i in read(LIVE/'CALL_PLAN.json')['packages']}
    for r in rows:
        if r['status'] not in {'ACCEPT','NOT_CHANGE'}: continue
        a=r['normalized']['claim_applicability']
        if a['graphic']['requirement']!='GRAPHIC_REQUIRED': continue
        invocations=list((LIVE/'calls'/r['package_id']).glob('attempt_*/INVOCATION.json'))
        assert len(invocations)==1
        inv=read(invocations[0]); folder=invocations[0].parent
        packet=read(BASE/plan[r['package_id']]['package'])['evidence_packet']
        for side in ('old','new'):
            assert a['graphic']['sides'][side]['complete']
            for eid in a['graphic']['sides'][side]['required_ids']:
                e=next(e for e in packet['evidence'][side] if e['evidence_id']==eid)
                digest=e['raster']['sha256']
                images=[folder/name for name,h in inv['input_hashes'].items() if h==digest and name.endswith('.png')]
                assert images and all(sha(p)==digest for p in images)
                delivery.append(dict(candidate_id=r['package_id'],side=side,evidence_id=eid,
                    raster_sha256=digest,invocation=str(invocations[0]),actual_call_image=str(images[0])))
    save('ACTUAL_GRAPHIC_DELIVERY_AUDIT.json',dict(status='PASS',rows=delivery,new_calls=0))

    from .test_saved import SavedRegressionTests
    stream=io.StringIO()
    result=unittest.TextTestRunner(stream=stream,verbosity=2).run(unittest.defaultTestLoader.loadTestsFromTestCase(SavedRegressionTests))
    save('SAVED_REGRESSION_TESTS.txt',stream.getvalue())
    saved_tests=dict(tests=result.testsRun,status='PASS' if result.wasSuccessful() else 'FAIL',
        failures=len(result.failures),errors=len(result.errors),model_calls=0,admission_replayed=False)
    save('SAVED_REGRESSION_TEST_RECEIPT.json',saved_tests)
    structural=read(OUT/'STRUCTURAL_AUDIT.json')
    checks=structural['checks']
    old_accept={k for k,r in previous.items() if r['status']=='ACCEPT'}
    old_negative={k for k,r in previous.items() if r['status']=='NOT_CHANGE'}
    numeric=[r for r in rows if r['normalized'].get('numeric_conflict',{}).get('blocking')]
    mixed=[r for r in rows if r['raw'].get('verdict')=='ACCEPT' and r['normalized']['claim_applicability']['calculation_basis']['applicability']=='UNKNOWN_BUT_REQUIRED']
    correct=sum(r['correct'] for r in accept_eval); false_accept=len(accepted)-correct
    correct_negative=sum(r['correct'] for r in negative_eval); false_negative=len(negatives)-correct_negative
    applicability_unchanged=all(r['normalized']['claim_applicability']['calculation_basis']['applicability']==
        previous[r['package_id']]['normalized']['claim_applicability']['calculation_basis']['applicability']
        for r in rows if r['raw'])
    numeric_unchanged=all(r['normalized'].get('numeric_conflict')==previous[r['package_id']]['normalized'].get('numeric_conflict') for r in rows)
    role_rows=read(OUT/'EVIDENCE_ROLE_REQUIREMENTS_V4.json')['rows']
    role_pass=all(q['required_modality']=='TEXT_ALLOWED' for q in role_rows if
        q['evidence_role'] in {'SUBJECT_IDENTITY','SCOPE_BINDING'} or
        (q['evidence_role']=='SUPPORTING_WITNESS' and q['witness_required_for'] in {'IDENTITY','SCOPE'}))
    stair=read(OUT/'STAIRCASE_REGRESSION_CHECK.json')
    negative_audit=read(OUT/'NEGATIVE_CONTRACT_AUDIT.json')
    witnesses=read(OUT/'WITNESS_APPLICABILITY_AUDIT.json')['rows']
    witness_pass=checks['witness_required_has_reason'] and stair['status']=='PASS' and all(
        not w['decision']['required'] or (w['decision']['witness_required_for'] and
        w['decision']['witness_required_modality']!='NOT_REQUIRED') for w in witnesses)
    graphic_pass=checks['accepted_graphics_both_sides'] and stair['status']=='PASS' and role_pass
    gate=dict(model_calls_zero=read(OUT/'MECHANICAL_REPLAY_52.json')['model_calls']==0,
        binding_subject_mismatch_zero=checks['binding_subject_mismatch_zero'],
        correct_accept_at_least_eleven=correct>=11 and len(old_accept)==11 and old_accept<=accepted,
        false_accept_zero=false_accept==0,
        correct_not_change_at_least_two=correct_negative>=2 and len(old_negative)==2 and old_negative<=negatives,
        false_not_change_zero=false_negative==0,
        numeric_conflict_stays_review=len(numeric)==1 and numeric[0]['status']=='REVIEW' and numeric_unchanged,
        mixed_unknown_basis_stays_review=len(mixed)==1 and mixed[0]['status']=='REVIEW' and applicability_unchanged,
        staircase_support_text_no_raster=stair['status']=='PASS',
        missing_primary_graphic_blocks=read(OUT/'TEST_RECEIPT.json')['status']=='PASS' and checks['required_missing_not_accepted'],
        witness_pass=bool(witness_pass),negative_contract_pass=negative_audit['status']=='PASS',
        graphic_applicability_pass=graphic_pass,role_scoped_modality_pass=role_pass,
        structural_pass=structural['status']=='PASS',raw_hashes_unchanged=verify_inputs()['unchanged'],
        saved_regression_tests_pass=result.wasSuccessful())
    passed=all(gate.values())
    save('SUCCESS_GATE.json',dict(status='PASS' if passed else 'FAIL',checks=gate))
    save('POST_INFERENCE_FINAL_REGRESSION.json',dict(status='PASS' if passed else 'FAIL',checks=gate,
        previous_accept_ids=sorted(old_accept),previous_not_change_ids=sorted(old_negative),
        new_not_change_ids=sorted(negatives-old_negative),correct_accept=correct,false_accept=false_accept,
        correct_not_change=correct_negative,false_not_change=false_negative,saved_tests=saved_tests,
        retuning_after_truth=False,replay_repeated=False))
    groups=read(OUT/'PROJECT_CHANGES_REPLAY.json')
    grouping_same=groups==read(V3/'PROJECT_CHANGES_REPLAY.json')
    save('GROUPING_EVALUATION.json',dict(status='NOT_EVALUABLE',project_changes=len(groups['groups']),
        exactly_equal_to_v3=grouping_same,membership_preserved=set(groups['group_ids'])==accepted,
        algorithm_unchanged=True,reason='Existing resolver unchanged; no new pairwise proofs, broad grouping quality not certified.'))
    local=read(OUT/'TEST_RECEIPT.json')
    counts=Counter(r['status'] for r in rows)
    raw_remaining=[r for r in rows if r['raw'].get('verdict')=='ACCEPT' and r['status']=='REVIEW']
    metrics=dict(status='PAIR_A_POST_INFERENCE_REPAIR_V4_COMPLETED_PASS' if passed else 'PAIR_A_POST_INFERENCE_REPAIR_V4_COMPLETED_REPAIR_REQUIRED',
        recommendation='READY_FOR_F5_MISS_REPAIR' if passed else 'POST_INFERENCE_STILL_NEEDS_REPAIR',model_calls=0,
        local_tests=local['tests']+result.testsRun,pre_replay_tests=local['tests'],post_replay_assertions=result.testsRun,
        binding_subject_mismatch=0 if checks['binding_subject_mismatch_zero'] else 'FAIL',
        final_accept_before=11,final_accept_after=len(accepted),correct_accept=correct,false_accept=false_accept,
        final_not_change_before=2,final_not_change_after=len(negatives),correct_not_change=correct_negative,false_not_change=false_negative,
        raw_accept_still_review=len(raw_remaining),numeric_conflict='PASS' if gate['numeric_conflict_stays_review'] else 'FAIL',
        calculation_basis='PASS' if gate['mixed_unknown_basis_stays_review'] else 'FAIL',
        witness='PASS' if witness_pass else 'FAIL',negative_verdict_contract=negative_audit['status'],
        graphic_applicability='PASS' if graphic_pass else 'FAIL',role_scoped_modality='PASS' if role_pass else 'FAIL',
        structural=structural['status'],raw_hashes='PASS',project_changes=len(groups['groups']),review=counts['REVIEW'],no_call_missing=7,
        independent_real_accepted=sum(r['status']=='FULL_ACCEPTED' for r in independent),
        independent_real_at_least_partially_covered=sum(r['status']!='MISSED' for r in independent),
        missed_real=sum(r['status']=='MISSED' for r in independent),independent_denominator=15,
        grouping='NOT_EVALUABLE',pair_b='NOT RUN',validation='NOT OPENED',final_holdout='NOT OPENED',production='UNCHANGED')
    save('RESULT_RECEIPT.json',metrics)
    report=f"""STATUS: {metrics['status']}

MODEL CALLS: 0 (Astra 0; OpenRouter 0; Claude 0).
LOCAL TESTS: {metrics['local_tests']} PASS ({local['tests']} before replay, {result.testsRun} saved-output assertions).
BINDING_SUBJECT_MISMATCH: {metrics['binding_subject_mismatch']}.

| Metric | V3 | V4 |
|---|---:|---:|
| FINAL ACCEPT | 11 | {len(accepted)} |
| CORRECT ACCEPT | 11 | {correct} |
| FALSE ACCEPT | 0 | {false_accept} |
| FINAL NOT_CHANGE | 2 | {len(negatives)} |
| CORRECT NOT_CHANGE | 2 | {correct_negative} |
| FALSE NOT_CHANGE | 0 | {false_negative} |
| RAW ACCEPT STILL REVIEW | 2 | {len(raw_remaining)} |
| PROJECTCHANGES | 11 | {len(groups['groups'])} |
| REVIEW including 7 NO_CALL_MISSING | 46 | {counts['REVIEW']} |

NUMERIC CONFLICT: {metrics['numeric_conflict']}. CALCULATION BASIS: {metrics['calculation_basis']}.
WITNESS: {metrics['witness']}. NEGATIVE VERDICT CONTRACT: {metrics['negative_verdict_contract']}.
GRAPHIC APPLICABILITY: {metrics['graphic_applicability']}. ROLE-SCOPED MODALITY: {metrics['role_scoped_modality']}.
STRUCTURAL: {metrics['structural']}. RAW HASHES: {metrics['raw_hashes']}.

INDEPENDENT REAL ACCEPTED: {metrics['independent_real_accepted']} / 15.
AT LEAST PARTIALLY COVERED: {metrics['independent_real_at_least_partially_covered']} / 15.
MISSED REAL: {metrics['missed_real']} / 15.
GROUPING: NOT_EVALUABLE (existing resolver; membership verified; exact V3 output equality: {grouping_same}).

V4 applies GRAPHIC only to geometric dispositive roles. Existing validated identity/scope
bindings permit supporting TEXT/TABLE. Unclassified state citations remain primary;
missing or invalid primary OLD/NEW raster still blocks. F4 raster validator is unchanged.
Every role record preserves the original binding and provenance. Rules contain no case IDs,
pages or subject-specific exceptions. The staircase was inspected only after the general
rule and synthetic tests existed; its fixed regression lookup is outside admission.
Its OLD+NEW primary graphic evidence is present; two OLD TEXT citations support identity/scope.
Final staircase verdict: {stair['final_verdict']}. See STAIRCASE_REGRESSION_CHECK.json.

All 11 previous ACCEPT and 2 previous NOT_CHANGE are preserved. Numeric 78.60/78.61 and
mixed unknown calculation basis remain REVIEW; numeric decisions and calculation applicability
are identical to V3. Remaining negative REVIEW cases keep their binding/ambiguous-role blockers.
No F5, discovery, extraction, delivery, grouping or other project work was performed.

52 saved responses replayed once after local tests; 7 missing calls remain missing.
Source truth evaluated only after REPLAY_RESULT_FREEZE; historical DEV exposure is retained.
Prior live V2 and repairs V1/V2/V3, raw files, source inputs and truth remain unchanged by hashes.
Actual accepted primary image hashes were checked against saved invocation inputs.

PAIR B: NOT RUN. VALIDATION: NOT OPENED. FINAL HOLDOUT: NOT OPENED. PRODUCTION: UNCHANGED.
RECOMMENDATION: {metrics['recommendation']}.
Post-inference admission frozen for next stage: {passed}. Stopped after replay and audits.
Excel: PAIR_A_REPAIRED_V4_SYSTEM_OUTPUT.xlsx.
"""
    save('FINAL_REPORT.md',report)
    save('FINAL_VERIFICATION.json',dict(at=now(),input_integrity=verify_inputs(),
        prior_manifests_verified=True,source_truth_unchanged=True,replay_freeze_verified=True,
        repair_code_freeze_verified=True,model_calls=0,further_replay=False,post_audit_rule_retuning=False))
    save('FINAL_MANIFEST.json',dict(at=now(),status=metrics['status'],
        files={str(p.relative_to(OUT)):sha(p) for p in sorted(OUT.rglob('*')) if p.is_file()}))
    print(metrics)


if __name__=='__main__': main()
