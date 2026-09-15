"""Post-freeze audits only. This module is never imported by the builder."""
from collections import Counter, defaultdict
from pathlib import Path

from .common import fingerprint, write, code_hashes
from .repair_v2 import file_hashes, metrics, audit_packages
from .repair_v4 import read, V3, DIAGNOSTIC, PREVIOUS


def reasons(p):
    return sorted({r for b in p['boundary_completeness']['blockers'] for r in b['reasons']})


def requirement_key(q):
    return q['side'], q['page'], q['required_type'], q['evidence_role']


def diagnostic_audit(output):
    freeze = read(output / 'diagnostic_12/PACKAGES_FREEZE.json')
    if read(output / 'ALGORITHM_FREEZE.json')['code'] != code_hashes():
        raise ValueError('Code changed after diagnostic freeze')
    # First read of adjudications in this process, strictly after source rebuild.
    truth_path = DIAGNOSTIC / 'SOURCE_AUDIT_12_PACKAGES.json'
    truth = read(truth_path)
    roster = read(output / 'ROSTER.json')
    selected = {r['package_id']: r for r in roster['selected']}
    if set(selected) != {r['package_id'] for r in truth['packages']}:
        raise ValueError('Diagnostic selection drift')
    results, changes = [], []
    counts, judgments = Counter(), Counter()
    for t in truth['packages']:
        cid = t['package_id']
        rel = selected[cid]['package']
        before = read(V3 / rel)
        after = read(output / 'diagnostic_12' / rel)
        if after['package_hash'] != freeze['package_hashes'][rel]:
            raise ValueError('Unfrozen diagnostic package')
        oldq = before['f1_requirement_package']['requirements']
        newq = after['f1_requirement_package']['requirements']
        identity_only = (after['candidate_subject']['subject_confidence'] == 'SUBJECT_UNRESOLVED'
            and len(newq) == 2 and all(q['provenance']['purpose'] == 'SUBJECT_IDENTITY_ONLY' for q in newq))
        complete = after['completeness'] == 'COMPLETE'
        false_complete = complete and not t['could_be_honest_complete']
        wrong_removed = t['primary_verdict'] == 'WRONG_REQUIREMENT' and identity_only and not complete
        if t['primary_verdict'] == 'WRONG_REQUIREMENT':
            category = 'DEFERRED_SUBJECT_IDENTITY' if wrong_removed else 'WRONG_REQUIREMENT'
        elif t['primary_verdict'] == 'FALSE_PARTIAL':
            category = 'HONEST_COMPLETE' if complete else 'FALSE_PARTIAL'
        elif t['primary_verdict'] == 'CORRECT_PARTIAL':
            category = 'FALSE_COMPLETE' if complete else 'CORRECT_PARTIAL'
        else:
            # Source-known omissions are not reclassified as fixed by changing
            # mandatory flags or leaving an identity probe. A fresh resolution
            # would need an individually grounded source/payload check.
            category = 'FALSE_COMPLETE' if false_complete else t['primary_verdict']
        old_pages = {s: sorted({e['page'] for e in before['evidence_packet']['evidence'][s]}) for s in ('old', 'new')}
        new_pages = {s: sorted({e['page'] for e in after['evidence_packet']['evidence'][s]}) for s in ('old', 'new')}
        lost = {s: sorted(set(old_pages[s]) - set(new_pages[s])) for s in ('old', 'new')}
        added = {s: sorted(set(new_pages[s]) - set(old_pages[s])) for s in ('old', 'new')}
        # Dropping a valid context to a two-query identity probe cannot count as
        # an improvement. Route/subject provenance for each removal is retained.
        regression = false_complete or (identity_only and t['primary_verdict'] not in {'WRONG_REQUIREMENT', 'CORRECT_PARTIAL'} and any(lost.values()))
        local_repair = wrong_removed or (complete and t['could_be_honest_complete'])
        if not regression and not local_repair:
            # Only count a demonstrable narrow correction, not raw blocker-count
            # reduction across different requirements.
            exclusions = [r for v in after['canonical_subjects'].values() for r in v['evidence_support']
                          if not r['accepted'] and r['reason'] in {
                              'WRONG_DOCUMENT_OR_CIPHER', 'EQUIPMENT_TABLE_IS_NOT_A_FUNCTIONAL_SCHEME',
                              'NO_NATIVE_SUBJECT_SUPPORT', 'NATIVE_SUBJECT_OUTSIDE_GRAPHIC_REGION'}]
            local_repair = bool(exclusions) and not identity_only
        judgment = 'REGRESSION' if regression else 'IMPROVED_CORRECTLY' if local_repair else 'UNCHANGED_CORRECTLY'
        counts[category] += 1; judgments[judgment] += 1
        row = dict(audit_id=t['audit_id'], package_id=cid, package=rel,
            before_category=t['primary_verdict'], after_category=category, classification=judgment,
            before_completeness=before['completeness'], after_completeness=after['completeness'],
            before_subject=before['candidate_subject'], after_subject=after['candidate_subject'],
            canonical_subjects=after['canonical_subjects'], wrong_requirement_removed=wrong_removed,
            identity_only=identity_only, false_complete=false_complete,
            delivered_pages_before=old_pages, delivered_pages_after=new_pages,
            lost_delivered_pages=lost, added_delivered_pages=added,
            before_partial_reasons=reasons(before), after_partial_reasons=reasons(after),
            frozen_source_diagnosis={k: t[k] for k in ('source_reality', 'package_decision_reason',
                'source_locators', 'continuation_checks', 'note_status', 'note_reason', 'graphic_checks')},
            retrieval_gap_disposition='RETAINED_NOT_CLAIMED_RESOLVED' if t['primary_verdict'] == 'RETRIEVAL_GAP' else 'NOT_APPLICABLE',
            caution='Category counts are package-level. Identity-only deferrals are not repaired comparable systems; no new source change truth was generated.')
        results.append(row)
        changes.append(dict(package_id=cid, package=rel,
            before=dict(subject=before['candidate_subject'], requirements=oldq,
                continuation_requirements=[dict(requirement_id=c['requirement_id'], continuation=c['continuation']) for c in before['boundary_certificates']],
                partial_reasons=reasons(before)),
            after=dict(subject=after['candidate_subject'], canonical_subjects=after['canonical_subjects'],
                requirements=newq, continuation_requirements=after['continuation_analysis'], partial_reasons=reasons(after)),
            removed_requirement_scopes=[list(requirement_key(q)) for q in oldq if requirement_key(q) not in {requirement_key(n) for n in newq}],
            added_requirement_scopes=[list(requirement_key(q)) for q in newq if requirement_key(q) not in {requirement_key(n) for n in oldq}],
            classification=judgment))
    gates = dict(wrong_requirement_removed=counts['WRONG_REQUIREMENT'] == 0,
        false_partial_decreased=counts['FALSE_PARTIAL'] < 2,
        correct_partial_preserved=counts['CORRECT_PARTIAL'] == 1,
        retrieval_gaps_not_claimed_fixed=counts['RETRIEVAL_GAP'] == 6,
        false_complete_zero=not any(r['false_complete'] for r in results),
        no_regressions=judgments['REGRESSION'] == 0)
    artifact = dict(status='PASS' if all(gates.values()) else 'FAIL', packages=results,
        before=dict(truth['package_primary_verdict_counts']), after=dict(counts), classifications=dict(judgments), gates=gates,
        truth_access='POST_DIAGNOSTIC_REBUILD_FREEZE_ONLY', source_truth_path=str(truth_path),
        prior_exposure=roster['prior_exposure'], truth_for_other_67_opened=False,
        recommendation='READY_FOR_FRESH_INFERENCE' if all(gates.values()) else 'F5_STILL_NEEDS_REPAIR')
    write(output / 'REQUIREMENTS_BEFORE_AFTER.json', changes)
    write(output / 'DIAGNOSTIC_12_RESULTS.json', artifact)
    print({k: artifact[k] for k in ('status', 'after', 'classifications', 'gates')}, flush=True)


def final_audit(output):
    from .boundary import decide, package_completeness
    diagnostic = read(output / 'DIAGNOSTIC_12_RESULTS.json')
    freeze = read(output / 'PACKAGES_FREEZE.json')
    structural, delivery = audit_packages(output)
    # V2's mechanical audit equates overall status with its F1 receipt. V3/V4
    # explicitly give unchanged F5 certificates authority. Audit that difference
    # separately, while retaining all other legacy assertions.
    authority_differences = [x for x in structural['issues'] if x['issue'] == 'COMPLETENESS_PROMOTED']
    issues = [x for x in structural['issues'] if x['issue'] != 'COMPLETENESS_PROMOTED']
    reasons_index = defaultdict(list)
    wrongdoc = read(output / 'WRONG_DOCUMENT_AUDIT.json')
    excluded = {(g['document_version'], g['page']) for g in wrongdoc['mismatches']}
    for rel in freeze['package_hashes']:
        p = read(output / rel)
        if package_completeness(p['boundary_certificates']) != p['boundary_completeness']:
            issues.append(dict(package=rel, issue='BOUNDARY_COMPLETENESS_DRIFT'))
        for c in p['boundary_certificates']:
            if decide(c['evidence_type'], c['proof']) != (c['status'], c['reasons']):
                issues.append(dict(package=rel, issue='BOUNDARY_DECISION_DRIFT'))
            if c['provenance']['packet_hash'] != fingerprint(p['evidence_packet']):
                issues.append(dict(package=rel, issue='CERTIFICATE_PACKET_HASH_DRIFT'))
        for b in p['boundary_completeness']['blockers']:
            for reason in b['reasons']:
                reasons_index[reason].append(dict(package=rel, requirement_id=b['requirement_id']))
        for s in ('old', 'new'):
            for e in p['evidence_packet']['evidence'][s]:
                if (e['document_version'], e['page']) in excluded:
                    issues.append(dict(package=rel, issue='WRONG_DOCUMENT_DELIVERED'))
    preserved = {str(root): file_hashes(root) == read(output / 'PREVIOUS_FILE_HASHES.json')[str(root)] for root in PREVIOUS}
    stable = read(output / 'ALGORITHM_FREEZE.json')['code'] == code_hashes()
    if not all(preserved.values()):
        issues.append(dict(issue='PREVIOUS_ARTIFACT_DRIFT'))
    if not stable:
        issues.append(dict(issue='CODE_DRIFT_AFTER_DIAGNOSTIC_FREEZE'))
    selected_same = all(freeze['package_hashes'].get(rel) == value
        for rel, value in read(output / 'diagnostic_12/PACKAGES_FREEZE.json')['package_hashes'].items())
    if not selected_same:
        issues.append(dict(issue='DIAGNOSTIC_TO_ALL_PACKAGE_DRIFT'))
    before, after = metrics(V3), metrics(output)
    tests = read(output / 'TEST_RECEIPT.json')
    top = [dict(reason=k, requirements=len(v), packages=len({x['package'] for x in v}))
           for k, v in sorted(reasons_index.items(), key=lambda x: (-len(x[1]), x[0]))]
    guard_pass = bool(excluded) and not any(x['issue'] == 'WRONG_DOCUMENT_DELIVERED' for x in issues)
    layers = dict(subject_discovery='PASS' if diagnostic['gates']['wrong_requirement_removed'] and diagnostic['gates']['no_regressions'] else 'FAIL',
        continuation_logic='PASS' if diagnostic['gates']['false_partial_decreased'] and diagnostic['gates']['retrieval_gaps_not_claimed_fixed'] else 'FAIL',
        wrong_document_guard='PASS' if guard_pass else 'FAIL',
        note_applicability='FAIL' if any('NOTE_RELEVANCE_UNPROVEN' in r['after_partial_reasons'] for r in diagnostic['packages']) else 'PASS',
        structural_audit='FAIL' if issues else 'PASS')
    recommendation = 'READY_FOR_FRESH_INFERENCE' if diagnostic['status'] == 'PASS' and all(v == 'PASS' for v in layers.values()) and tests['status'] == 'PASS' and not issues else 'F5_STILL_NEEDS_REPAIR'
    result = dict(status='F5_V4_OFFLINE_REBUILD_COMPLETE', recommendation=recommendation,
        diagnostic_packages=12, diagnostic_before=diagnostic['before'], diagnostic_after=diagnostic['after'],
        classifications=diagnostic['classifications'], false_complete=sum(r['false_complete'] for r in diagnostic['packages']),
        false_complete_scope='12 source-audited packages; all-79 complete packages are also structurally certified. No truth of the other 67.',
        before={k: before[k] for k in ('packages', 'COMPLETE', 'PARTIAL', 'MISSING', 'STRONG')},
        after={k: after[k] for k in ('packages', 'COMPLETE', 'PARTIAL', 'MISSING', 'STRONG')},
        layers=layers, tests=tests, hash_stability='PASS' if stable and selected_same and all(preserved.values()) else 'FAIL',
        previous_artifacts_unchanged=preserved, top_partial_reasons=top,
        strong_downgrades=read(output / 'SUBJECT_AUDIT.json')['strong_downgrades'],
        model_calls=0, validation='NOT OPENED', final_holdout='NOT OPENED', other_projects='NO', production='UNCHANGED')
    write(output / 'STRUCTURAL_AUDIT.json', dict(status=layers['structural_audit'], issues=issues,
        unchanged_legacy_audit=structural, separately_verified_boundary_authority_differences=authority_differences,
        certificates_recomputed=True, wrong_document_payload_count=sum(x['issue'] == 'WRONG_DOCUMENT_DELIVERED' for x in issues),
        previous_artifacts_unchanged=preserved, selected_12_identical_in_all_79=selected_same))
    write(output / 'PARTIAL_REASON_INDEX.json', dict(summary=top, requirements=dict(reasons_index)))
    write(output / 'FINAL_HASH_STABILITY.json', dict(status=result['hash_stability'], prior_artifacts=preserved,
        selected_12_identical_in_all_79=selected_same, code_unchanged_after_diagnostic_freeze=stable))
    write(output / 'F5_V4_RESULT.json', result)
    (output / 'F5_V4_REPORT.md').write_text(report(result, diagnostic), encoding='utf-8')
    from experiments.project_change_272.inventory import sha
    write(output / 'ARTIFACT_HASHES.json', {str(p.relative_to(output)): sha(p)
        for p in sorted(output.rglob('*')) if p.is_file() and p.name != 'ARTIFACT_HASHES.json'})
    print({k: result[k] for k in ('status', 'recommendation', 'diagnostic_after', 'after', 'layers', 'hash_stability')}, flush=True)


def report(r, diagnostic):
    lines = ['# F5 V4 — subject discovery and continuations', '',
        f"STATUS: **{r['status']}**. RECOMMENDATION: **{r['recommendation']}**.", '',
        '## Diagnostic 12', '', '| Metric | Before | After |', '|---|---:|---:|']
    for k in ('WRONG_REQUIREMENT', 'FALSE_PARTIAL', 'CORRECT_PARTIAL', 'RETRIEVAL_GAP'):
        lines.append(f"| {k} | {r['diagnostic_before'].get(k, 0)} | {r['diagnostic_after'].get(k, 0)} |")
    lines += [f"| FALSE COMPLETE | 0 | {r['false_complete']} |", '',
        'Malformed subjects with only two identity probes are separately counted as DEFERRED_SUBJECT_IDENTITY '
        f"({r['diagnostic_after'].get('DEFERRED_SUBJECT_IDENTITY', 0)}). Removing a wrong full-system requirement does not establish a comparable system.", '',
        '| Case | Before | After | Evaluation |', '|---|---|---|---|']
    for p in diagnostic['packages']:
        lines.append(f"| {p['audit_id']} · {p['package_id']} | {p['before_category']} | {p['after_category']} / {p['after_completeness']} | {p['classification']} |")
    lines += ['', '## All 79: deterministic regression', '', '| Metric | Before | After |', '|---|---:|---:|']
    lines += [f"| {k} | {r['before'][k]} | {r['after'][k]} |" for k in r['before']]
    lines += ['', 'The same source-rebuilt tracking roster is refined without splitting or manual merging. '
        'A canonical context group is not a proven equipment/system identity. Location, consumers, equipment and subsystem '
        'remain unestablished where native sources do not ground them. No matching threshold was changed.', '',
        '## Layer gates', '']
    lines += [f'- {k}: **{v}**.' for k, v in r['layers'].items()]
    lines += ['', '## Remaining work', '',
        'The strict native table row-group and functional graphic connection predicates remain unchanged. '
        'The V4 extractor cannot yet supply those proofs for every locally sufficient fragment; UNKNOWN is retained. '
        'Note applicability remains UNKNOWN where a note target/effect lacks explicit source binding. '
        'No COMPLETE result is manufactured from a continuation waiver. The diagnostic gate requires fewer FALSE_PARTIAL packages; '
        'local rule tests alone do not satisfy that gate.', '',
        'Required text dependencies are enumerated before the eight-raster / 28,000-character allocation. '
        'Repeated table headers require same native table identity and scope; a different floor/building/system is not continuation. '
        'Graphic crop crossings refer to actual full-page delivery, with local connectivity still independently required. '
        'Wrong-cipher pages retain receipts and native hashes in WRONG_DOCUMENT_AUDIT; the loader suppresses all their payload.', '',
        '## Top mandatory partial reasons', '']
    lines += [f"- {x['reason']}: {x['requirements']} requirements / {x['packages']} packages." for x in r['top_partial_reasons'][:8]]
    lines += ['', '## STRONG correspondence', '']
    if r['strong_downgrades']:
        lines += [f"- {d['candidate_id']}: {d['reason']}. Per-endpoint accepted/rejected native source regions: SUBJECT_AUDIT.json." for d in r['strong_downgrades']]
    else:
        lines.append('All 26 STRONG retrieval correspondences preserved; no threshold change.')
    lines += ['', '## Verification and isolation', '',
        f"Local tests: {r['tests']['passed']} PASS. Hash stability: {r['hash_stability']}. Structural: {r['layers']['structural_audit']}.", '',
        'Each package was rebuilt twice from a fresh source inventory. The selected 12 were frozen and audited before the '
        '79-package run; their canonical hashes must match in both runs. This is two delivery/certificate passes, not '
        'two independent source extractions. Historical V1/V2/V3/diagnostic file sets and bytes are checked against the pre-task snapshot.', '',
        'The builder process reads only the four admitted DEV documents and its output, with network/subprocess operations denied. '
        'The post-freeze audit reads only the previous 12 source diagnoses; no truth for the other 67 is opened. '
        'The developer had prior exposure to these 12 diagnoses. No claim of a newly blind evaluation is made.', '',
        'Model calls: **0** (Codex inference endpoints / OpenRouter / Claude). VALIDATION: **NOT OPENED**. '
        'FINAL HOLDOUT: **NOT OPENED**. OTHER PROJECTS: **NO** (foreign-code leaf within an admitted PDF is quarantined). '
        'PRODUCTION: **UNCHANGED**. Offline rebuild ends here; no fresh inference launched.', '',
        'Detailed before/after requirements, source provenance, retained retrieval gaps, and all 12 classifications: '
        'REQUIREMENTS_BEFORE_AFTER.json and DIAGNOSTIC_12_RESULTS.json.']
    return '\n'.join(lines) + '\n'
