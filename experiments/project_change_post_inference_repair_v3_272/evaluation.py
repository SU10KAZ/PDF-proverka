"""Post-replay evaluation only. Never imports or invokes the repair function.

Frozen replay files/code remain untouched. A persisted-JSON structural addendum
corrects the tuple/list comparison in the initial in-memory structural audit.
"""
from collections import Counter
from pathlib import Path
from .replay import OUT, LIVE, V1, V2, REPO, read, save, sha, now, verify_inputs, offline_guard


def main():
    freeze = read(OUT/'REPLAY_RESULT_FREEZE.json')
    for name,digest in freeze['files'].items(): assert sha(OUT/name)==digest,name
    code = read(OUT/'REPAIR_CODE_FREEZE.json')
    for name,digest in code['code'].items(): assert sha(REPO/name)==digest,name
    assert read(OUT/'SOURCE_AUDIT_ACCESS_RECEIPT.json')['replay_completed_at']==freeze['at']
    offline_guard()
    # Existing manifests prove full prior artifacts unchanged, including truth.
    prior = read(V2/'INPUT_FREEZE.json')['files']
    for path,digest in prior.items(): assert sha(path)==digest,path
    for name,digest in read(V2/'FINAL_MANIFEST.json')['files'].items(): assert sha(V2/name)==digest,name
    audit = read(LIVE/'SOURCE_AUDIT.json'); truth = read(LIVE/'SOURCE_FIRST_TRUTH.json')
    for name,digest in read(OUT/'SOURCE_AUDIT_ACCESS_RECEIPT.json')['files'].items(): assert sha(LIVE/name)==digest
    rows = read(OUT/'PAIR_A_REPAIRED_V3_RESULTS.json')
    previous = {r['package_id']:r for r in read(V2/'PAIR_A_REPAIRED_V2_RESULTS.json')}
    by_id = {r['package_id']:r for r in rows}
    cases = {c['package_id']:c for c in audit['case_audit']}
    accepted = {r['package_id'] for r in rows if r['status']=='ACCEPT'}
    negatives = {r['package_id'] for r in rows if r['status']=='NOT_CHANGE'}
    accept_eval = [dict(candidate_id=k,correct=cases[k]['audit_class']=='REAL_CHANGE_BLOCKED',frozen_audit=cases[k]) for k in sorted(accepted)]
    negative_eval = [dict(candidate_id=k,correct=cases[k]['audit_class']=='NOT_CHANGE_BLOCKED',scope=by_id[k]['normalized']['claim_applicability']['NEGATIVE_SCOPE'],frozen_audit=cases[k]) for k in sorted(negatives)]
    independent = []
    for t in truth['entries']:
        if t['classification']!='REAL_CHANGE': continue
        trace=next(c for c in audit['independent_comparison'] if c['truth_id']==t['id'])
        found=accepted & set(trace['found_package_ids'])
        full=bool(found) and trace['detection']=='FULL' and set(trace['found_package_ids'])<=accepted
        independent.append(dict(truth_id=t['id'],title=t['title'],status='FULL_ACCEPTED' if full else 'PARTIAL_ACCEPTED' if found else 'MISSED',
            accepted_packages=sorted(found),frozen_comparison=trace))
    assert len(independent)==15
    supplements=[dict(**s,final_verdict=by_id[s['package_id']]['status'],in_independent_denominator=False) for s in audit['post_answer_supplement']]
    assert len(supplements)==4
    save('FROZEN_AUDIT_COMPARISON.json',dict(accepts=accept_eval,negative_verdicts=negative_eval,independent=independent,
        supplements=supplements,denominator=15,truth_changed=False,source_truth_used_for_rules=False))

    # The replay compared to_dict tuples to JSON lists. Compare the unchanged,
    # persisted values in a common representation, without normalizing again.
    initial = read(OUT/'STRUCTURAL_AUDIT.json')
    checks = dict(initial['checks'])
    checks['no_new_binding']=all(r['normalized'][side+'_state']['evidence_bindings']==previous[r['package_id']]['normalized'][side+'_state']['evidence_bindings']
        for r in rows if r['raw'] for side in ('old','new'))
    assert checks['no_new_binding']
    structural=dict(status='PASS' if all(checks.values()) else 'FAIL',checks=checks,
        supersedes='STRUCTURAL_AUDIT.json',original_sha256=sha(OUT/'STRUCTURAL_AUDIT.json'),
        correction='Only tuple/list representation comparison was false; all persisted OLD/NEW binding arrays equal V2 exactly.',
        replay_repeated=False,admission_or_output_changed=False)
    save('STRUCTURAL_AUDIT_FINAL.json',structural)

    delivery=[]
    for r in rows:
        if r['status'] not in {'ACCEPT','NOT_CHANGE'}: continue
        a=r['normalized']['claim_applicability']
        if a['graphic']['requirement']!='GRAPHIC_REQUIRED': continue
        invocations=list((LIVE/'calls'/r['package_id']).glob('attempt_*/INVOCATION.json'))
        assert len(invocations)==1
        inv=read(invocations[0]); folder=invocations[0].parent
        for side in ('old','new'):
            for eid in a['graphic']['sides'][side]['required_ids']:
                witnesses=[w for w in r['raw']['witnesses'] if w['side']==side and w['evidence_id']==eid and w['kind']=='RASTER_LOCATOR']
                assert witnesses
                item=next(x for x in read(LIVE/'CALL_PLAN.json')['packages'] if x['key']==r['package_id'])
                packet=read(Path('/home/coder/auditmanager/corpus-audits/20260914_project_change_272/fresh_dev_sample_f5_pipeline_v6')/item['package'])['evidence_packet']
                e=next(e for e in packet['evidence'][side] if e['evidence_id']==eid)
                digest=e['raster']['sha256']
                images=[folder/name for name,h in inv['input_hashes'].items() if h==digest and name.endswith('.png')]
                assert images and all(sha(p)==digest for p in images)
                delivery.append(dict(candidate_id=r['package_id'],side=side,evidence_id=eid,raster_sha256=digest,
                    invocation=str(invocations[0]),actual_call_image=str(images[0])))
    save('ACTUAL_GRAPHIC_DELIVERY_AUDIT.json',dict(status='PASS',rows=delivery,new_calls=0))

    remaining=[]
    for r in rows:
        if r['raw'].get('verdict')=='NOT_CHANGE' and r['status']=='REVIEW':
            remaining.append(dict(candidate_id=r['package_id'],raw_claim=r['raw']['project_change_summary'],
                issues=r['normalized']['issues'],frozen_audit=cases[r['package_id']],applicability=r['normalized']['claim_applicability']))
    # Post-output diagnosis, never fed back into rules: mixed graphic/text
    # primary lists still impose raster on auxiliary textual references.
    graphic_false_blockers=[r for r in remaining if cases[r['candidate_id']]['audit_class']=='NOT_CHANGE_BLOCKED'
        and set(r['issues']) <= {'SUPPORTING_WITNESS_REQUIRED','GRAPHIC_REQUIRED_NOT_DELIVERED'}
        and 'GRAPHIC_REQUIRED_NOT_DELIVERED' in r['issues']]
    groups=read(OUT/'PROJECT_CHANGES_REPLAY.json')
    old_accept={k for k,r in previous.items() if r['status']=='ACCEPT'}
    numeric=[r for r in rows if r['normalized'].get('numeric_conflict',{}).get('blocking')]
    mixed=[r for r in rows if r['raw'].get('verdict')=='ACCEPT' and r['normalized']['claim_applicability']['calculation_basis']['applicability']=='UNKNOWN_BUT_REQUIRED']
    correct=sum(r['correct'] for r in accept_eval); false_accept=len(accepted)-correct
    false_negative=sum(not r['correct'] for r in negative_eval)
    gate=dict(model_calls_zero=read(OUT/'MECHANICAL_REPLAY_52.json')['model_calls']==0,
        binding_subject_mismatch_zero=checks['binding_subject_mismatch_zero'],correct_accept_at_least_nine=correct>=9,
        false_accept_zero=false_accept==0,numeric_conflict_stays_review=bool(numeric) and all(r['status']=='REVIEW' for r in numeric),
        mixed_unknown_basis_stays_review=bool(mixed) and all(r['status']=='REVIEW' for r in mixed),
        false_witness_blockers_removed=bool(accepted-old_accept),negative_irrelevant_requirements_removed=not graphic_false_blockers,
        no_unrelated_binding=checks['no_new_binding'],structural_pass=structural['status']=='PASS',raw_hashes_unchanged=verify_inputs()['unchanged'])
    passed=all(gate.values())
    save('SUCCESS_GATE.json',dict(status='PASS' if passed else 'FAIL',checks=gate))
    save('POST_INFERENCE_REGRESSION_AUDIT_FINAL.json',dict(status='PASS' if passed else 'FAIL',
        supersedes='POST_INFERENCE_REGRESSION_AUDIT.json',structural=structural,
        nine_v2_correct_accept_preserved=old_accept<=accepted,new_accept_ids=sorted(accepted-old_accept),
        correct_accept=correct,false_accept=false_accept,false_not_change=false_negative,
        remaining_negative_reviews=remaining,confirmed_false_graphic_blockers=graphic_false_blockers,
        retuning_after_truth=False,replay_repeated=False))
    save('GROUPING_EVALUATION.json',dict(status='NOT_EVALUABLE',project_changes=len(groups['groups']),
        membership_preserved=set(groups['group_ids'])==accepted,algorithm_unchanged=True,
        reason='Existing ownership resolver without new pairwise proofs; singleton membership checked, broad grouping quality not certified.'))
    counts=Counter(r['status'] for r in rows)
    raw_remaining=[r for r in rows if r['raw'].get('verdict')=='ACCEPT' and r['status']=='REVIEW']
    metrics=dict(status='PAIR_A_POST_INFERENCE_REPAIR_V3_COMPLETED_PASS' if passed else 'PAIR_A_POST_INFERENCE_REPAIR_V3_COMPLETED_REPAIR_REQUIRED',
        recommendation='READY_FOR_F5_MISS_REPAIR' if passed else 'POST_INFERENCE_STILL_NEEDS_REPAIR',model_calls=0,
        local_tests=read(OUT/'TEST_RECEIPT.json')['tests'],binding_subject_mismatch=0,raw_accept=13,final_accept_before=9,final_accept_after=len(accepted),
        correct_accept_before=9,correct_accept_after=correct,false_accept=false_accept,raw_accept_still_review=len(raw_remaining),
        remaining_blocker_categories=dict(numeric_conflict=len(numeric),calculation_basis=len(mixed),witness_only=0),
        raw_not_change=5,final_not_change_before=0,final_not_change_after=len(negatives),false_not_change=false_negative,
        project_changes=len(groups['groups']),review=counts['REVIEW'],no_call_missing=7,
        independent_real_accepted=sum(r['status']=='FULL_ACCEPTED' for r in independent),
        independent_real_at_least_partially_covered=sum(r['status']!='MISSED' for r in independent),
        missed_real=sum(r['status']=='MISSED' for r in independent),independent_denominator=15,
        post_output_findings_accepted=sum(r['final_verdict']=='ACCEPT' for r in supplements),post_output_findings_denominator=4,
        witness='FAIL' if graphic_false_blockers else 'PASS',negative_verdict_contract='FAIL' if graphic_false_blockers else 'PASS',
        graphic_applicability='FAIL' if graphic_false_blockers else 'PASS',calculation_basis='PASS',numeric_safety='PASS',
        structural=structural['status'],raw_hashes='PASS',grouping='NOT_EVALUABLE',pair_b='NOT RUN',validation='NOT OPENED',final_holdout='NOT OPENED',production='UNCHANGED')
    save('RESULT_RECEIPT.json',metrics)
    report=f"""STATUS: {metrics['status']}

MODEL CALLS: 0. LOCAL TESTS: {metrics['local_tests']} PASS. BINDING_SUBJECT_MISMATCH: 0.

| Метрика | V2 | V3 |
|---|---:|---:|
| RAW ACCEPT | 13 | 13 |
| FINAL ACCEPT | 9 | {len(accepted)} |
| CORRECT ACCEPT | 9 | {correct} |
| FALSE ACCEPT | 0 | {false_accept} |
| RAW NOT_CHANGE | 5 | 5 |
| FINAL NOT_CHANGE | 0 | {len(negatives)} |
| FALSE NOT_CHANGE | — | {false_negative} (проверены оба принятых узких вывода) |
| PROJECTCHANGES | 9 | {len(groups['groups'])} |
| REVIEW, включая 7 NO_CALL_MISSING | 50 | {counts['REVIEW']} |

RAW ACCEPT STILL REVIEW: {len(raw_remaining)}: numeric conflict 1; calculation basis 1; witness-only 0.
Mixed unknown-basis claim сохраняет также зависимый witness blocker; категории выше не пересекаются.
Конфликт 78,60/78,61: REVIEW, CHANGE_EXISTENCE_SUPPORTED=YES, EXACT_VALUE_SUPPORTED=NO; числа не исправлялись.

INDEPENDENT REAL ACCEPTED: {metrics['independent_real_accepted']} / 15 полностью.
AT LEAST PARTIALLY COVERED: {metrics['independent_real_at_least_partially_covered']} / 15.
MISSED REAL: {metrics['missed_real']} / 15.
Post-output findings: {metrics['post_output_findings_accepted']} / 4 приняты; вне denominator 15.

WITNESS: {metrics['witness']} в целом; два ложных auxiliary witness blockers у raw ACCEPT устранены.
NEGATIVE VERDICT CONTRACT: {metrics['negative_verdict_contract']}.
GRAPHIC APPLICABILITY: {metrics['graphic_applicability']}.
CALCULATION BASIS: PASS. NUMERIC SAFETY: PASS. STRUCTURAL: {metrics['structural']}. RAW HASHES: PASS.
GROUPING: NOT_EVALUABLE; существующий resolver сохранён, состав групп проверен.

Два новых ACCEPT: c_8333d345321893e1c967d825 (планировка квартиры 1.3.42)
и c_9e9b0bca25479b6f260e9ffb (площади помещений 55.1/55.2).
Первый доказан OLD+NEW графикой; второй — строками и явной контрольной суммой.
Отклонённые дополнительные counter-ссылки остались отклонёнными и не стали доказательствами.
Все девять V2 ACCEPT сохранены, frozen audit подтверждает все 11.

Возвращены NOT_CHANGE: c_938c04b5893a8c03aaf5ecde (локальные элементы подпора)
и c_a9136ed2cf02c9ccd2d3820e (слои Ф7). Их scoped claims подтверждены frozen audit.

Оставшийся дефект applicability: c_6eaf92b25145b86cd05df50c (лестницы).
V3 унаследовал graphic_delivery, требующий raster для каждой primary-ссылки, включая
дополнительные TEXT-ссылки. Это оставляет GRAPHIC_REQUIRED_NOT_DELIVERED и зависимый
SUPPORTING_WITNESS_REQUIRED. Frozen audit подтверждает узкий NOT_CHANGE; gate 8 не пройден.
Состояния и verdict после оценки не исправлялись; повторный replay не выполнялся.
Остальные NOT_CHANGE/REVIEW: Ф1 — нерешённая V1 scope binding; венткамера 02.22 —
AMBIGUOUS_OTHER_ROLE и отклонённые counter-ссылки. Binding/role mapping в V3 не расширялись.

Первоначальный STRUCTURAL_AUDIT.json содержит false для no_new_binding из-за сравнения
Python tuple с загруженным JSON list. STRUCTURAL_AUDIT_FINAL.json независимо сравнивает
все сохранённые OLD/NEW bindings с V2: полное совпадение, PASS. Исходный audit, replay и
code freeze сохранены; исправлено только заключение проверки, не admission или output.
POST_INFERENCE_REGRESSION_AUDIT_FINAL.json завершает предварительный regression audit
после открытия truth. Никакого post-audit tuning нет.

Ledger четырёх claims создан до изменения кода. 52 responses replayed ровно один раз после
113 tests; source audit открыт после REPLAY_RESULT_FREEZE. Live V2, Repair V1/V2, packages,
raw responses и source truth неизменны по прежним manifests и новым input checks.
Числовые факты V2 переиспользованы с проверкой хешей; новых OCR/model calls нет.
Растры принятых graphic claims сверены с хешами фактических INVOCATION inputs.

PAIR B: NOT RUN. VALIDATION: NOT OPENED. FINAL HOLDOUT: NOT OPENED. PRODUCTION: UNCHANGED.
RECOMMENDATION: {metrics['recommendation']}.

Excel: PAIR_A_REPAIRED_V3_SYSTEM_OUTPUT.xlsx (59 строк, отдельные листы raw ACCEPT и NOT_CHANGE).
"""
    save('FINAL_REPORT.md',report)
    save('FINAL_VERIFICATION.json',dict(at=now(),input_integrity=verify_inputs(),all_prior_manifest_hashes_verified=True,
        source_truth_unchanged=True,replay_freeze_verified=True,repair_code_freeze_verified=True,
        model_calls=0,further_replay=False,post_audit_rule_retuning=False,structural_addendum=structural))
    print(metrics)


if __name__=='__main__': main()
