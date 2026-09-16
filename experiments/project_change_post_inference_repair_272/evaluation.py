"""Post-freeze evaluation against existing Pair A audit; never changes replay."""
from collections import Counter
from .replay import OUT, LIVE, REPO, read, save, sha, ref, now, verify_v2, offline_guard


def main():
    freeze = read(OUT / 'REPLAY_RESULT_FREEZE.json')
    for name, digest in freeze['files'].items():
        assert sha(OUT / name) == digest, name
    for path, digest in read(OUT / 'REPAIR_CODE_FREEZE.json')['code'].items():
        assert sha(REPO / path) == digest, path
    offline_guard()
    audit_freeze = read(LIVE / 'PAIR_A_AUDIT_FREEZE.json')
    for name in ('SOURCE_AUDIT.json', 'SOURCE_FIRST_TRUTH.json'):
        assert sha(LIVE / name) == audit_freeze['files'][name]
    save('SOURCE_AUDIT_EVALUATION_RECEIPT.json', dict(at=now(), replay_result_freeze=ref(OUT / 'REPLAY_RESULT_FREEZE.json'),
        source_audit=ref(LIVE / 'SOURCE_AUDIT.json'), independent_truth=ref(LIVE / 'SOURCE_FIRST_TRUTH.json'),
        evaluation_only=True, new_truth_created=False, normalization_retuned_after_audit=False,
        access_order='First semantic audit inspection occurred after the persisted replay result freeze; this receipt records evaluation assembly.'))
    audit = read(LIVE / 'SOURCE_AUDIT.json'); truth = read(LIVE / 'SOURCE_FIRST_TRUTH.json')
    rows = read(OUT / 'PAIR_A_REPAIRED_RESULTS.json'); by_id = {r['package_id']: r for r in rows}
    audits = {r['package_id']: r for r in audit['case_audit']}
    accepted = [r for r in rows if r['status'] == 'ACCEPT']
    accepted_ids = {r['package_id'] for r in accepted}
    acceptance = []
    for r in accepted:
        a = audits[r['package_id']]
        # Strict output correctness includes all reported numbers. Preserve the
        # original distinction: real change with a numeric defect is not a
        # wholly invented engineering event, but is not a clean ACCEPT either.
        correct = a['audit_class'] == 'REAL_CHANGE_BLOCKED'
        acceptance.append(dict(package_id=r['package_id'], correct_accept=correct,
            strict_false_accept=not correct, invented_event=not a['audit_class'].startswith('REAL_CHANGE'),
            audit_class=a['audit_class'], reason=a['audit_reason'],
            independent_matches=a['independent_truth_matches'], supplement_matches=a['supplement_matches']))
    independent = []
    for t in truth['entries']:
        if t['classification'] != 'REAL_CHANGE': continue
        trace = next(r for r in audit['independent_comparison'] if r['truth_id'] == t['id'])
        matches = sorted(accepted_ids & set(trace['found_package_ids']))
        independent.append(dict(truth_id=t['id'], title=t['title'], accepted_packages=matches,
            status='PARTIAL_ACCEPTED_COVERAGE' if matches else 'MISSED_AT_FINAL_ADMISSION',
            frozen_raw_detection=trace['detection'], raw_found_packages=trace['found_package_ids'],
            old_pages=t['old_pages'], new_pages=t['new_pages']))
    assert len(independent) == 15
    supplements = [dict(id=s['id'], title=s['title'], package_id=s['package_id'], frozen_classification=s['classification'],
        final_verdict=by_id[s['package_id']]['status'], audit=s['audit'], in_independent_denominator=False)
        for s in audit['post_answer_supplement']]
    assert len(supplements) == 4
    found = sum(bool(r['accepted_packages']) for r in independent)
    correct = sum(r['correct_accept'] for r in acceptance)
    strict_false = sum(r['strict_false_accept'] for r in acceptance)
    invented = sum(r['invented_event'] for r in acceptance)
    counters = Counter(r['status'] for r in rows)
    binding = read(OUT / 'BINDING_BEFORE_AFTER.json'); replay = read(OUT / 'MECHANICAL_REPLAY_52.json')
    issues = Counter(i for r in rows for i in set(r['normalized'].get('issues', [])))
    bound = sum(len(r['normalized'].get(side + '_state', {}).get('evidence_bindings', [])) for r in rows for side in ('old', 'new'))
    # Graph route remains exactly the existing downstream predicate. Inspect
    # its outcome without reclassifying evidence or changing F4.
    examples = []
    categories = {
        'Площади квартир / холлов': ['A-REAL-07', 'A-REAL-08', 'A-REAL-09'],
        'Помещение 01.26: ТБО → кладовая': ['A-REAL-05'],
        'Изменения отметок': ['A-REAL-11', 'A-REAL-12', 'A-REAL-13'],
        'МОП корпуса 4': ['A-REAL-14'],
    }
    for title, ids in categories.items():
        matched = [r for r in independent if r['truth_id'] in ids]
        packages = sorted({p for r in matched for p in r['raw_found_packages']})
        examples.append(dict(category=title, truth_ids=ids,
            packages=[dict(package_id=p, final_verdict=by_id[p]['status'],
                comparability=by_id[p]['normalized'].get('f2', {}).get('comparability'),
                issues=by_id[p]['normalized'].get('issues'), sufficiency=by_id[p]['normalized'].get('sufficiency')) for p in packages],
            reached_accept=any(r['accepted_packages'] for r in matched)))
    for s in supplements:
        examples.append(dict(category=s['title'], supplemental=True, package_id=s['package_id'], final_verdict=s['final_verdict']))
    terraces = [r for r in rows if r['raw'].get('verdict') == 'ACCEPT' and 'террас' in r['raw'].get('project_change_summary', '').lower()]
    examples.append(dict(category='МОП / террасы и расчётная база', packages=[dict(package_id=r['package_id'],
        final_verdict=r['status'], comparability=r['normalized']['f2']['comparability']) for r in terraces]))
    save('FROZEN_AUDIT_COMPARISON.json', dict(acceptance=acceptance, independent=independent, supplements=supplements,
        correct_accept=correct, strict_false_accept=strict_false, invented_event_accept=invented,
        independent_found_any=found, independent_missed=15-found, independent_full_coverage=0,
        descriptive_pair_a_precision=correct / len(accepted) if accepted else None,
        descriptive_coverage_label='FRESH DEV PAIR A descriptive coverage', descriptive_coverage=f'{found}/15',
        denominator_note='Existing non-exhaustive source-first groups. Partial coverage counts as found-any, never as full coverage. Four post-output supplements excluded.',
        precision_definition='Strict correctness of the accepted output including numbers. Separately disclose real changes with numeric defects.'))
    save('KNOWN_EXAMPLES_REGRESSION.json', examples)
    save('GROUPING_EVALUATION.json', dict(new_grouping_problems=False,
        project_changes=replay['project_changes'], accepted_inputs=len(accepted), lost_accepted_facts=[],
        false_merges=[], note='Four existing singleton groups. Two touch the broad A-REAL-14 audit group at different floors; this alone is not proof of duplicate engineering events. No pairwise ownership proofs supplied; cross-event grouping quality remains unevaluated.'))
    # Check the evidence gates from the persisted result, without another replay.
    violations = []
    for r in rows:
        raw, n = r['raw'], r['normalized']
        if not raw: continue
        for side in ('old', 'new'):
            for link in n[side + '_state']['evidence_bindings']:
                p = link['provenance']
                if link['side'] != side or not p['requirements'] or not p['source_receipt']['sha256']:
                    violations.append(r['package_id'])
                if link['role'] == 'STATE_VALUE' and link['evidence_id'] not in raw[side + '_state']['evidence_ids']:
                    violations.append(r['package_id'])
                if link['role'] == 'SUBJECT_IDENTITY' and link['evidence_id'] not in raw[side + '_state']['subject_identity']['evidence_ids']:
                    violations.append(r['package_id'])
        if r['status'] == 'ACCEPT' and raw['verdict'] != 'ACCEPT': violations.append(r['package_id'])
    assert not violations
    save('FINAL_VERIFICATION.json', dict(at=now(), v2=verify_v2(), bound_role_links=bound,
        provenance_violations=violations, model_calls=0, raw_review_or_not_change_promoted_to_accept=0,
        local_tests=read(OUT / 'LOCAL_TEST_RECEIPT.json'),
        unchanged_components=['F1 retrieval', 'F4 raster validation', 'F5 package building', 'evidence budget', 'prompts', 'raw responses', 'materiality', 'ProjectChange grouping'],
        replay_freeze_verified=True, repair_code_freeze_verified=True, pair_b='NOT RUN', validation='NOT OPENED',
        final_holdout='NOT OPENED', production='UNCHANGED', further_replay=False))
    report = f'''STATUS: PAIR_A_POST_INFERENCE_REPAIR_V1_COMPLETED_REPAIR_REQUIRED

MODEL CALLS: 0 (Codex/Astra 0; OpenRouter 0; Claude 0)
LOCAL TESTS: {read(OUT / 'LOCAL_TEST_RECEIPT.json')['tests']} PASS

| Метрика | До | После |
|---|---:|---:|
| BINDING_SUBJECT_MISMATCH, ответов | 52 | {binding['subject_mismatch_after']} |
| Raw ACCEPT | 13 | 13 |
| F2 ACCEPT | 0 | {replay['f2_counts'].get('ACCEPT', 0)} |
| Final ACCEPT | 0 | {counters['ACCEPT']} |
| ProjectChanges | 0 | {replay['project_changes']} |
| REVIEW, включая 7 NO_CALL_MISSING | 59 | {counters['REVIEW']} |
| NOT_CHANGE | 0 | {counters['NOT_CHANGE']} |

CORRECT ACCEPT: {correct}
FALSE ACCEPT: {strict_false} — строгая оценка всех утверждений и чисел; полностью выдуманных изменений: {invented}.
INDEPENDENT REAL FOUND: {found} / 15 (частичное покрытие A-REAL-14; полных групп: 0).
MISSED REAL AT FINAL ADMISSION: {15-found} / 15.
NEW GROUPING PROBLEMS: NO (на четырёх singleton-группах; широкая оценка grouping невозможна).

Descriptive Pair A precision по строгой точности вывода: {correct}/{len(accepted)} = {correct / len(accepted):.0%}.
FRESH DEV PAIR A descriptive coverage: {found}/15 = {found/15:.1%}; это не полнота всего проекта.
Из 4 post-output находок приняты 2; они исключены из независимого denominator. Одна из этих двух содержит числовой дефект.

Исправлен контракт subject binding: technical_candidate_id, стабильные discovery subject IDs, физическая identity и source/model wording разделены.
Сохранено {bound} явных role links с исходными OLD/NEW receipts, страницами, требованиями и raw paths.
У {binding['any_binding_failure_after']} из 52 ответов остаются отклонённые ссылки по иным основаниям; {52-binding['any_binding_failure_after']} ответов прошли binding без отклонённых ссылок.
Ни один raw REVIEW или NOT_CHANGE не превращён в ACCEPT. Синтетические отрицательные контроли проходят.

MAIN REMAINING ERROR CLASSES

- Числовой дефект в принятом c_3ad8c6113d1f4d4bdcb2b720: 78,60 указано как итог, хотя frozen audit фиксирует напечатанные 78,61 и сумму строк 78,60. Изменение реально, но ACCEPT нельзя считать полностью корректным. Этот дефект уже был в raw response; исправление binding раскрыло его в принятом выходе. Ответ и frozen replay не редактировались.
- Existing topology sufficiency требует evidence.route=GRAPHIC; несколько доставленных растров имеют основной route TABLE/TEXT. Планы квартир и размер 1550→1600 остаются REVIEW с CLAIM_GRAPHIC_WITNESS_REQUIRED. F4 и классификация маршрутов не менялись.
- Дополнительные явно указанные ссылки без валидированного witness блокируют отдельные claims; 10 ответов имеют BINDING_SUBJECT_WITNESS_REQUIRED. 6 ответов имеют BINDING_SCOPE_WITNESS_REQUIRED, 11 — BINDING_CANONICAL_IDENTITY_UNRESOLVED; категории пересекаются.
- Для 01.26 physical identity совпала, functional_role исключён из identity conditions, F2 comparability COMPARABLE. Final REVIEW сохраняется из-за дополнительных непроверенных ссылок и raw state_role=OTHER / UNKNOWN_SUFFICIENCY_PROFILE. Новый materiality/sufficiency profile не добавлялся.
- Для площадей статический operating_mode больше не блокирует при подтверждённой identity. МОП 4-го этажа остаются REVIEW из-за неизвестных calculation_basis/consumer_composition; объединённый claim МОП/террасы 16-го этажа — из-за различной calculation_basis.
- Отметки A-REAL-11/12/13 не были выделены в сохранённых ответах. Post-inference repair не создаёт новые claims.

| Известные примеры | Результат |
|---|---|
| Холлы/кухни квартир, A-REAL-07/08/09 | REVIEW: chiefly graphic-route sufficiency; часть ссылок без witness |
| 01.26 ТБО → кладовая | Same subject / changed role; final REVIEW |
| Локальный размер 1550→1600, A-SUP-04 | REVIEW: unchanged graphic-route gate |
| Отметки A-REAL-11/12/13 | Не обнаружены в raw; остаются пропущенными |
| МОП корпуса 4, A-REAL-14 | ACCEPT по 1-му и 2-му этажам; частичное покрытие группы |
| МОП 3-го и 6-го этажей, A-SUP-01/03 | ACCEPT; у A-SUP-01 числовой дефект |
| Террасы / 16-й этаж | REVIEW: calculation_basis DIFFERENT |

F2 ACCEPT здесь означает EXISTS_CHANGE=YES + COMPARABLE + sufficient; final admission дополнительно применяет прежнюю materiality и raw verdict.
В существующем normalizer role binding выполняется до F2 и передаёт ему типизированные состояния; отдельный V4-проход с повторным прикреплением evidence не создавался.
Source audit открыт только после REPLAY_RESULT_FREEZE. Использованы существующие audit classes и matches; новый truth не создавался, код после открытия audit не настраивался.
Все 1269 файлов V2 неизменны. 52 сохранённых ответа обработаны один раз; 7 MISSING перенесены как REVIEW без вызова.

PAIR B: NOT RUN
VALIDATION: NOT OPENED
FINAL HOLDOUT: NOT OPENED
PRODUCTION: UNCHANGED
RECOMMENDATION: DOWNSTREAM_STILL_NEEDS_REPAIR

Артефакты: PAIR_A_REPAIRED_SYSTEM_OUTPUT.xlsx, BINDING_BEFORE_AFTER.json, F2_BEFORE_AFTER.json,
PAIR_A_REPAIRED_RESULTS.json, PROJECT_CHANGES_REPLAY.json, FROZEN_AUDIT_COMPARISON.json,
KNOWN_EXAMPLES_REGRESSION.json, FINAL_VERIFICATION.json.
Остановлено после replay и обязательной оценки; повторного inference/replay и запуска Pair B нет.
'''
    save('FINAL_REPORT.md', report)
    print(dict(correct_accept=correct, strict_false_accept=strict_false, invented_event_accept=invented,
               independent_found=found, missed=15-found, recommendation='DOWNSTREAM_STILL_NEEDS_REPAIR'))


if __name__ == '__main__': main()
