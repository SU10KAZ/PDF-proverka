"""Read-only post-replay assessment against the existing frozen source audit.

This module cannot normalize or replay. Case IDs below identify reported
regressions only; they are never used in admission/applicability code.
"""
from collections import Counter
from .replay import OUT, LIVE, V1, REPO, read, save, sha, now, verify_inputs, offline_guard, reasons


def main():
    freeze=read(OUT/'REPLAY_RESULT_FREEZE.json')
    for name,digest in freeze['files'].items(): assert sha(OUT/name)==digest,name
    for name,digest in read(OUT/'REPAIR_CODE_FREEZE.json')['code'].items(): assert sha(REPO/name)==digest,name
    offline_guard()
    af=read(LIVE/'PAIR_A_AUDIT_FREEZE.json')
    for name in ['SOURCE_AUDIT.json','SOURCE_FIRST_TRUTH.json']: assert sha(LIVE/name)==af['files'][name]
    audit=read(LIVE/'SOURCE_AUDIT.json'); truth=read(LIVE/'SOURCE_FIRST_TRUTH.json')
    rows=read(OUT/'PAIR_A_REPAIRED_V2_RESULTS.json'); by_id={r['package_id']:r for r in rows}
    prev={r['package_id']:r for r in read(V1/'PAIR_A_REPAIRED_RESULTS.json')}
    cases={a['package_id']:a for a in audit['case_audit']}
    accepted={r['package_id'] for r in rows if r['status']=='ACCEPT'}
    acceptance=[dict(candidate_id=k,correct_accept=cases[k]['audit_class']=='REAL_CHANGE_BLOCKED',
        audit_class=cases[k]['audit_class'],reason=cases[k]['audit_reason'],
        independent_matches=cases[k]['independent_truth_matches'],supplement_matches=cases[k]['supplement_matches'])
        for k in sorted(accepted)]
    old_correct={k for k,r in prev.items() if r['status']=='ACCEPT' and cases[k]['audit_class']=='REAL_CHANGE_BLOCKED'}
    independent=[]
    for t in truth['entries']:
        if t['classification']!='REAL_CHANGE': continue
        trace=next(a for a in audit['independent_comparison'] if a['truth_id']==t['id'])
        found=sorted(accepted & set(trace['found_package_ids']))
        full=bool(found) and trace['detection']=='FULL' and set(trace['found_package_ids'])<=accepted
        independent.append(dict(truth_id=t['id'],title=t['title'],accepted_packages=found,
            status='FULL_ACCEPTED' if full else 'PARTIAL_ACCEPTED' if found else 'MISSED',
            frozen_detection=trace['detection'],frozen_found_packages=trace['found_package_ids'],
            reason=trace['explanation'],old_pages=t['old_pages'],new_pages=t['new_pages']))
    assert len(independent)==15
    supplements=[dict(id=s['id'],candidate_id=s['package_id'],title=s['title'],
        frozen_classification=s['classification'],audit=s['audit'],
        final_verdict=by_id[s['package_id']]['status'],in_independent_denominator=False)
        for s in audit['post_answer_supplement']]
    assert len(supplements)==4
    correct=sum(a['correct_accept'] for a in acceptance)
    full=sum(a['status']=='FULL_ACCEPTED' for a in independent)
    partial=sum(a['status']!='MISSED' for a in independent)
    comparison=dict(correct_accept=correct,false_accept=len(accepted)-correct,acceptance=acceptance,
        previous_correct_accept_preserved=sorted(old_correct & accepted),
        previous_correct_accept_lost=sorted(old_correct-accepted),
        independent=independent,independent_full_accepted=full,
        independent_at_least_partially_covered=partial,missed_real=15-partial,supplements=supplements,
        new_truth_created=False,after_replay_only=True)
    save('FROZEN_AUDIT_COMPARISON.json',comparison)
    regressions=[]
    for k,p in prev.items():
        if p['status']!='NOT_CHANGE' or by_id[k]['status']=='NOT_CHANGE': continue
        r=by_id[k]; n=r['normalized']
        regressions.append(dict(candidate_id=k,raw_claim=r['raw']['project_change_summary'],
            before=p['status'],after=r['status'],reason=reasons(n),frozen_audit_class=cases[k]['audit_class'],
            frozen_audit_reason=cases[k]['audit_reason'],
            assessment='V2 introduced over-restrictive applicability. Do not label this a new source defect.',
            code_causes=[
                'primary_complete is additionally restricted by V1 architectural identity_proven, even for already bound other claim classes.',
                'basis_required defaults to not direct: failure to recognize direct multi-value prose becomes a calculation dependency.',
                'graphic_delivery requires every primary evidence ID to have a raster, including supplementary textual citations.'
            ]))
    save('POST_REPLAY_REGRESSION_AUDIT.json',dict(status='FAIL',rows=regressions,
        no_post_audit_retuning=True,stop_rule_observed=True,
        supporting_witness='FAIL: complete bound direct non-architectural/multi-value states can be misclassified as needing additional witness.',
        calculation_basis='FAIL: direct layered source-native parameters can fall through to UNKNOWN_BUT_REQUIRED.',
        graphic_applicability='FAIL: safety passes, but per-reference raster completeness introduces an over-restrictive no-change gate.'))
    delivery=read(OUT/'ACTUAL_GRAPHIC_DELIVERY_AUDIT.json')
    # Verify matching image was an actual invocation input, not merely a file.
    for d in delivery['rows']:
        from pathlib import Path
        image=Path(d['actual_call_image']); invocation=read(image.parent/'INVOCATION.json')
        assert invocation['input_hashes'][image.name]==d['raster_sha256']==sha(image)
    numeric=[r for r in rows if r['normalized'].get('numeric_conflict',{}).get('blocking')]
    conflict_cases=[r for r in numeric if any(c['printed_value']=='78.61' and c['claimed_value']=='78.60'
                   for c in r['normalized']['numeric_conflict']['conflicts'])]
    promoted=[r['candidate_id'] for r in read(OUT/'RAW_ACCEPT_13_BEFORE_AFTER.json')
              if r['before_final']=='REVIEW' and r['final_verdict']=='ACCEPT']
    checks={
        '1_model_calls_zero':read(OUT/'MECHANICAL_REPLAY_52.json')['model_calls']==0,
        '2_binding_subject_mismatch_zero':all('BINDING_SUBJECT_MISMATCH' not in r['normalized'].get('issues',[]) for r in rows),
        '3_false_exact_numeric_not_accepted':bool(conflict_cases) and all(r['status']=='REVIEW' for r in conflict_cases),
        '4_previous_correct_accept_preserved':old_correct<=accepted,
        '5_false_applicability_blocker_removed':bool(promoted),
        '6_graphic_required_accepted_have_actual_old_new_images':delivery['status']=='PASS',
        '7_unknown_required_basis_not_accepted':all(r['status']!='ACCEPT' for r in rows if
            (r['normalized'].get('claim_applicability') or {}).get('calculation_basis',{}).get('applicability')=='UNKNOWN_BUT_REQUIRED'),
        '8_ambiguous_other_not_accepted':all(r['status']!='ACCEPT' for r in rows if
            (r['normalized'].get('claim_applicability') or {}).get('other_role',{}).get('ambiguous')),
        '9_structural_audit_pass':read(OUT/'STRUCTURAL_AUDIT.json')['status']=='PASS',
        '10_frozen_hashes_unchanged':verify_inputs()['unchanged'],
    }
    save('SUCCESS_GATE.json',dict(necessary_gates=checks,necessary_gates_pass=all(checks.values()),
        overall_status='REPAIR_REQUIRED',additional_applicability_regressions=len(regressions),
        reason='Necessary safety gates pass, but newly introduced false applicability blockers violate the main principle. V2 is not an overall PASS.'))
    remaining=[dict(candidate_id=r['package_id'],summary=r['raw']['project_change_summary'],reasons=reasons(r['normalized']))
               for r in rows if r['raw'].get('verdict')=='ACCEPT' and r['status']=='REVIEW']
    grouping=read(OUT/'PROJECT_CHANGES_REPLAY.json')
    save('GROUPING_EVALUATION.json',dict(status='NOT_EVALUABLE',project_changes=len(grouping['groups']),
        accepted_inputs=len(accepted),lost_accepted_inputs=[],false_merges_observed=[],
        reason='Existing resolve_ownership kept singleton groups; no pairwise proofs or grouping rewrite. No basis for a broad grouping quality verdict.'))
    replay=read(OUT/'MECHANICAL_REPLAY_52.json'); counts=Counter(r['status'] for r in rows)
    metrics=dict(status='PAIR_A_POST_INFERENCE_REPAIR_V2_COMPLETED_REPAIR_REQUIRED',
        model_calls=0,local_tests=read(OUT/'TEST_RECEIPT.json')['tests'],binding_subject_mismatch=0,raw_accept=13,
        final_accept_before=4,final_accept_after=counts['ACCEPT'],project_changes=len(grouping['groups']),
        review=counts['REVIEW'],not_change=counts['NOT_CHANGE'],correct_accept_before=3,correct_accept_after=correct,
        false_accept_before=1,false_accept_after=len(accepted)-correct,numeric_conflict='PASS',
        witness_applicability='FAIL',graphic_applicability='FAIL',graphic_delivery_safety='PASS',
        calculation_basis='FAIL',other_role='PASS',structural_audit='PASS',
        independent_real_accepted_full=full,independent_real_at_least_partially_covered=partial,
        missed_real=15-partial,raw_accepts_still_review=len(remaining),remaining=remaining,
        grouping_problems='NOT_EVALUABLE',pair_b='NOT RUN',validation='NOT OPENED',
        final_holdout='NOT OPENED',production='UNCHANGED',recommendation='POST_INFERENCE_STILL_NEEDS_REPAIR')
    save('RESULT_RECEIPT.json',metrics)
    report=f"""STATUS: {metrics['status']}

MODEL CALLS: 0 (Astra/Codex 0; OpenRouter 0; Claude 0).
LOCAL TESTS: {metrics['local_tests']} PASS.
BINDING_SUBJECT_MISMATCH: 0.
STRUCTURAL AUDIT: PASS. Raw response hashes unchanged: YES.

| Метрика | V1 | V2 |
|---|---:|---:|
| Raw ACCEPT | 13 | 13 |
| F2 ACCEPT | 4 | {replay['f2_counts'].get('ACCEPT',0)} |
| FINAL ACCEPT | 4 | {counts['ACCEPT']} |
| PROJECTCHANGES | 4 | {len(grouping['groups'])} |
| REVIEW, включая 7 NO_CALL_MISSING | 53 | {counts['REVIEW']} |
| NOT_CHANGE | 2 | {counts['NOT_CHANGE']} |
| CORRECT ACCEPT | 3 | {correct} |
| FALSE ACCEPT, strict | 1 | {len(accepted)-correct} |

NUMERIC CONFLICT: PASS.
WITNESS APPLICABILITY: FAIL — обнаружен новый ложный blocker вне распознанного прямого архитектурного claim.
GRAPHIC APPLICABILITY: FAIL по полноте applicability; проверка фактической доставки и запрета недоказанного ACCEPT — PASS.
CALCULATION BASIS: FAIL — прямое многозначное сравнение может ошибочно требовать расчётную базу.
OTHER ROLE: PASS — OTHER для назначения 01.26 преобразован в ROOM_FUNCTION_CHANGE / TOPOLOGY с сохранением original_role; неоднозначные OTHER остаются REVIEW.

INDEPENDENT REAL ACCEPTED: {full} / 15 полностью.
INDEPENDENT REAL AT LEAST PARTIALLY COVERED: {partial} / 15, включая полностью покрытую группу.
MISSED REAL: {15-partial} / 15 без принятого покрытия.
Полностью покрыта A-REAL-07; частично — A-REAL-05, A-REAL-08, A-REAL-09, A-REAL-14.
Три из четырёх post-output supplements приняты; A-SUP-01 с числовым конфликтом остаётся REVIEW.
Эти четыре находки не включены в denominator 15; source truth не изменён.

Числовой claim 78,60/78,61 теперь REVIEW. Напечатанный итог 78,61 извлечён локальным Tesseract
из уже доставленного raster и уже указанного в raw response locator. Сумма сохранённых строк — 78,60;
delta 0,01 м²; модельное число оставлено без изменения. CHANGE_EXISTENCE_SUPPORTED=YES,
EXACT_VALUE_SUPPORTED=NO. Глобальный tolerance не используется. OCR не является новым
семантическим/API вызовом; его результаты, не прошедшие согласование table/digit passes, не приняты.
Guard покрывает распознаваемые явные row/total claims; он не сертифицирует всю произвольную числовую прозу.

Основные достигнутые исправления: {len(promoted)} прежних raw ACCEPT/REVIEW прошли admission.
Прямые строки МОП 4-го этажа больше не требуют calculation_basis/consumer_composition.
Для 01.26 дополнительная отклонённая counter-reference перестала блокировать полностью связанное
основное доказательство; сама ссылка осталась отклонённой. Четыре геометрических claims прошли
по фактически отправленным OLD+NEW изображениям: первичный route TABLE/TEXT не означает
отсутствия raster. Хеши изображений сверены с INVOCATION input_hashes. F4 и delivery не изменялись.

MAIN REMAINING BLOCKERS

RAW ACCEPTS STILL REVIEW: {len(remaining)}.

"""
    for r in remaining: report+=f"- {r['candidate_id']}: {r['summary']} Причины: {', '.join(r['reasons'])}.\n"
    report+="""
Две регрессии NOT_CHANGE → REVIEW сохранены в POST_REPLAY_REGRESSION_AUDIT.json.
Для Ф7 (c_a9136ed2cf02c9ccd2d3820e) прямое сравнение напечатанных толщин/слоёв получило
ложные CALCULATION_BASIS_REQUIRED_UNKNOWN и SUPPORTING_WITNESS_REQUIRED.
Причина в V2: отсутствие распознавания direct claim превращено в обязательность basis,
а primary completeness дополнительно зависит от архитектурного classifier V1.
Для лестниц (c_6eaf92b25145b86cd05df50c) правило требует raster для каждой primary-ссылки,
включая дополнительные текстовые; добавились также ложные basis/witness требования.
Frozen audit подтверждает оба no-change утверждения. Это дефекты V2, не новые дефекты источников.

Все десять необходимых safety gates формально пройдены, но общий V2 PASS не объявлен:
новые ложные applicability blockers нарушают основной принцип задания. Три корректных ACCEPT V1 сохранены.
85 тестов не покрывали указанные варианты многозначного no-change и смешанных primary reference roles.
После replay выполнена только оценка; код и frozen output по source audit не перенастраивались.
Нужен отдельный repair для этих регрессий и оставшихся вспомогательных witness-блокировок
до перехода к F5 miss repair. Поиск/доставка и claim splitting здесь не выполнялись.

GROUPING PROBLEMS: NOT_EVALUABLE.
Девять singleton-групп, потерянных принятых claims и наблюдаемых false merges нет;
pairwise ownership proofs не добавлялись, широкая оценка grouping не проводилась.

PAIR B: NOT RUN.
VALIDATION: NOT OPENED.
FINAL HOLDOUT: NOT OPENED.
PRODUCTION: UNCHANGED.
RECOMMENDATION: POST_INFERENCE_STILL_NEEDS_REPAIR.

Ledger 18 raw ACCEPT/NOT_CHANGE записан до изменения кода. Replay выполнен один раз после tests.
Исходные 52 responses, все live V2 / Repair V1 artifacts, packages, модельные входы и source truth
сохранены по хешам. Code/result freeze предшествуют открытию frozen source audit.
Полный Excel: PAIR_A_REPAIRED_V2_SYSTEM_OUTPUT.xlsx; в нём сохранён фактический frozen system output,
включая регрессии, без ручного исправления verdicts.
"""
    save('FINAL_REPORT.md',report)
    save('FINAL_VERIFICATION.json',dict(at=now(),input_integrity=verify_inputs(),replay_freeze_verified=True,
        repair_code_freeze_verified=True,actual_invocation_image_hashes_verified=True,
        source_truth_unchanged=True,model_calls=0,further_replay=False,post_audit_code_retuning=False,
        pair_b='NOT RUN',validation='NOT OPENED',final_holdout='NOT OPENED',production='UNCHANGED'))
    print({k:v for k,v in metrics.items() if k!='remaining'})


if __name__=='__main__': main()
