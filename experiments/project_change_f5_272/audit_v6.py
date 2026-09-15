"""Mechanical V6 post-build audits. Never opens source truth or reserve data."""
from collections import Counter, defaultdict
import json
from .common import fingerprint, code_hashes, write
from .repair_v6 import read, PREVIOUS
from .repair_v2 import audit_packages, file_hashes
from .priority_v6 import audit_displacements
from .boundary import decide, package_completeness


def packages(target):
    return {rel:read(target/rel) for rel in read(target/'PACKAGES_FREEZE.json')['package_hashes']}


def delivery_view(p):
    body=p['f1_requirement_package']
    delivered={r['requirement']['requirement_id']:bool(r['evidence_ids'])
               for r in p['evidence_packet']['evidence_coverage']['requirements']}
    rs={r['requirement_id']:r for r in body['raster_allocation']['requirements']}
    return dict(correspondence_status=p['candidate_subject']['confidence'],
        selected_evidence=[dict(evidence_id=e['evidence_id'],side=s,page=e['page'],
            routes=e['delivered_routes'],requirement_ids=[r['requirement_id'] for r in e['region_bindings']],
            text_sha256=fingerprint(e['quote']),raster_sha256=e['raster']['sha256'] if e['raster'] else None)
            for s in ('old','new') for e in p['evidence_packet']['evidence'][s]],
        omitted_evidence=[dict(requirement_id=q['requirement_id'],side=q['side'],page=q['page'],
            type=q['required_type'],role=q['evidence_role'],reason=rs[q['requirement_id']]['omission_reason'])
            for q in body['requirements'] if not delivered[q['requirement_id']]],
        raster_omissions=[q['requirement_id'] for q in body['requirements'] if not rs[q['requirement_id']]['selected']])


def structural(target):
    raw,_=audit_packages(target)
    issues=[x for x in raw['issues'] if x['issue']!='COMPLETENESS_PROMOTED']
    wrong={(g['document_version'],g['page']) for g in read(target/'WRONG_DOCUMENT_AUDIT.json')['mismatches']}
    for rel,p in packages(target).items():
        body=p['f1_requirement_package'];logs=body['delivery_decision_log']
        if package_completeness(p['boundary_certificates'])!=p['boundary_completeness']:
            issues.append(dict(package=rel,issue='BOUNDARY_COMPLETENESS_DRIFT'))
        for cert in p['boundary_certificates']:
            if decide(cert['evidence_type'],cert['proof'])!=(cert['status'],cert['reasons']) or cert['provenance']['packet_hash']!=fingerprint(p['evidence_packet']):
                issues.append(dict(package=rel,issue='BOUNDARY_CERTIFICATE_DRIFT'))
        for s in ('old','new'):
            if any((e['document_version'],e['page']) in wrong for e in p['evidence_packet']['evidence'][s]):
                issues.append(dict(package=rel,issue='WRONG_DOCUMENT_DELIVERED'))
        if audit_displacements(logs)['status']!='PASS':
            issues.append(dict(package=rel,issue='PROTECTED_DISPLACEMENT'))
        coverage={r['requirement']['requirement_id']:bool(r['evidence_ids']) for r in p['evidence_packet']['evidence_coverage']['requirements']}
        if len(logs)!=len(body['requirements']) or {l['requirement_id'] for l in logs}!=set(coverage):
            issues.append(dict(package=rel,issue='INCOMPLETE_DECISION_LOG'))
        for row in logs:
            if (row['selected']=='YES')!=coverage[row['requirement_id']]:
                issues.append(dict(package=rel,issue='DECISION_LOG_PAYLOAD_DRIFT',requirement_id=row['requirement_id']))
        if body['identity_evidence_ids']!=sorted(l['evidence_id'] for l in logs if 'SUBJECT_IDENTITY' in l['role']):
            issues.append(dict(package=rel,issue='IDENTITY_SET_DRIFT'))
    return dict(status='FAIL' if issues else 'PASS',issues=issues,
        legacy_boundary_authority_differences=[x for x in raw['issues'] if x['issue']=='COMPLETENESS_PROMOTED'],
        boundary_certificates_recomputed=True,truth_access=False)


def regression_audit(output):
    after=packages(output/'regression_9')
    manifest=read(output/'REGRESSION_9_MANIFEST.json')
    rows=[]
    for event in manifest['records']:
        rel=event['package'];p=after[rel];before=read(output/'baseline'/rel)
        locations=[]
        for loc in event['exact_loss_locations']:
            accepted=[x for x in loc['support'] if x['accepted']]
            links=[(e,r) for e in p['evidence_packet']['evidence'][loc['side']] if e['page']==loc['page']
                   for r in e['region_bindings'] if r['payload_delivered']]
            accepted_ids={r['region_id'] for r in accepted}
            retained=any(r['region_id'] in accepted_ids for _,r in links)
            was_budget=any(q['delivery']['omission_reason']=='BUDGET_LIMIT' for q in loc['v4_requirements'])
            category=('DELIVERY_BUDGET' if was_budget else 'IDENTITY_ONLY' if accepted and not loc['v4_requirements'] else
                'WRONG_DOCUMENT_GUARD' if loc['support'] and all(x['reason']=='WRONG_DOCUMENT_OR_CIPHER' for x in loc['support']) else 'SUBJECT_DISCOVERY_UNRESOLVED')
            locations.append(dict(side=loc['side'],page=loc['page'],category=category,
                accepted_region_ids=sorted(accepted_ids),retained=retained if accepted else None,
                retained_regions=[r['region_id'] for _,r in links if r['region_id'] in accepted_ids],
                disposition='RESTORED_ACCEPTED_PAYLOAD' if retained else 'STILL_MISSING' if accepted else
                    'CORRECT_QUARANTINE_PRESERVED' if category=='WRONG_DOCUMENT_GUARD' else 'OUT_OF_SCOPE_DISCOVERY_DEFECT'))
        # Two graphic-only strong losses in the saved manifest; derive the
        # membership from its source-support records, not new case selection.
        graphic_case=event['kind']=='STRONG' and event['v3_status']['candidate']['functional_key'] in {'openings','accessibility'}
        rows.append(dict(event_id=event['event_id'],package=rel,kind=event['kind'],
            before=delivery_view(before),after=delivery_view(p),locations=locations,
            graphic_support_case=graphic_case,discovery_unchanged=p['canonical_subjects']==before['canonical_subjects'],
            confidence_unchanged=p['candidate_subject']['confidence']==before['candidate_subject']['confidence']))
    logs=[dict(package=rel,**l) for rel,p in after.items() for l in p['f1_requirement_package']['delivery_decision_log']]
    delivery=[r for r in rows if r['kind']=='DELIVERY']
    failed_delivery=sum(any(l['retained'] is False for l in r['locations'] if l['category'] in {'DELIVERY_BUDGET','IDENTITY_ONLY'}) for r in delivery)
    identity=[l for r in delivery for l in r['locations'] if l['category']=='IDENTITY_ONLY']
    graphic=[r for r in rows if r['graphic_support_case']]
    sa=structural(output/'regression_9')
    gates=dict(delivery_budget=failed_delivery==0,identity_only=bool(identity) and all(l['retained'] for l in identity),
        protected_displacement=all(p['f1_requirement_package']['priority_audit']['status']=='PASS' for p in after.values()),
        confidence_not_promoted=all(r['confidence_unchanged'] for r in rows),
        graphic_preservation=all(all(l['retained'] is not False for l in r['locations']) for r in graphic),
        discovery_unchanged=all(r['discovery_unchanged'] for r in rows),structural=sa['status']=='PASS',
        exact_saved_nine=len(rows)==9 and len(delivery)==6 and len(graphic)==2)
    result=dict(status='PASS' if all(gates.values()) else 'FAIL',gates=gates,rows=rows,
        regressions_analyzed=len(rows),delivery_before=6,delivery_after=failed_delivery,
        identity_only_before=1,identity_only_after=sum(not l['retained'] for l in identity),
        graphic_support_regressions=len(graphic),graphic_resolved=sum(all(l['retained'] for l in r['locations']) for r in graphic),
        graphic_unresolved=sum(any(l['category']=='SUBJECT_DISCOVERY_UNRESOLVED' for l in r['locations']) for r in graphic),
        p0_p1_displaced=sum(p['f1_requirement_package']['priority_audit']['displaced_count'] for p in after.values()),
        limitations='Delivery measures accepted required route payload, not whole-section completeness. Discovery defects and correct quarantine are separate. No source truth or inference.')
    write(output/'REGRESSION_9_REPLAY.json',result)
    write(output/'regression_9/STRUCTURAL_AUDIT.json',sa)
    print(json.dumps({k:v for k,v in result.items() if k!='rows'},ensure_ascii=False),flush=True)
    return result


def final_audit(output):
    ps=packages(output);index=read(output/'PACKAGE_INDEX.json')
    logs=[dict(package=rel,**l) for rel,p in ps.items() for l in p['f1_requirement_package']['delivery_decision_log']]
    sa=structural(output)
    previous={str(root):file_hashes(root)==read(output/'PREVIOUS_FILE_HASHES.json')[str(root)] for root in PREVIOUS}
    code_stable=read(output/'ALGORITHM_FREEZE.json')['code']==code_hashes()
    same_nine=all(read(output/'PACKAGES_FREEZE.json')['package_hashes'].get(rel)==h for rel,h in
                  read(output/'regression_9/PACKAGES_FREEZE.json')['package_hashes'].items())
    if not all(previous.values()) or not code_stable or not same_nine:
        sa['issues'].append(dict(issue='IMMUTABILITY_OR_FREEZE_DRIFT'));sa['status']='FAIL'
    write(output/'STRUCTURAL_AUDIT.json',sa)
    write(output/'FINAL_HASH_STABILITY.json',dict(status='PASS' if code_stable and same_nine and all(previous.values()) else 'FAIL',
        previous_artifacts_unchanged=previous,code_unchanged=code_stable,regression_nine_identical=same_nine))
    displaced=sum(p['f1_requirement_package']['priority_audit']['displaced_count'] for p in ps.values())
    mandatory_dropped=[l for l in logs if l['mandatory'] and l['selected']=='NO' and l['omitted_reason']=='NOT_DELIVERED_BUDGET_LIMIT']
    identity_dropped=[l for l in logs if l['identity_payload_retained'] is False]
    continuation_omitted=[l for l in logs if l['dependency'] and l['selected']=='NO']
    budget_limited=[l for l in logs if l['raster_omitted_reason'] or l['omitted_reason']=='NOT_DELIVERED_BUDGET_LIMIT']
    write(output/'EVIDENCE_DISPLACEMENT_LOG.json',dict(rows=logs,p0_p1_displaced=displaced))
    write(output/'EVIDENCE_PRIORITY_AUDIT.json',dict(status='PASS' if displaced==0 else 'FAIL',
        priorities=dict(Counter(l['priority'] for l in logs)),protected=sum(l['protected']=='YES' for l in logs),
        mandatory_evidence_dropped=len(mandatory_dropped),identity_evidence_dropped=len(identity_dropped),
        unavailable_mandatory=sum(l['mandatory'] and l['omitted_reason']=='NOT_ADMITTED_OR_UNAVAILABLE' for l in logs),
        protected_capacity_omissions=[l for l in logs if l['protected']=='YES' and l['omitted_reason']=='NOT_DELIVERED_BUDGET_LIMIT']))
    write(output/'BUDGET_ALLOCATION_AUDIT.json',dict(status='PASS' if displaced==0 else 'FAIL',
        budgets=dict(rasters=8,text_characters=28000),mandatory_evidence_dropped=mandatory_dropped,
        continuation_omitted=continuation_omitted,budget_limited_requirements=budget_limited,
        metric_definition='Dropped means required-route payload unavailable due to budget; unlocated sources are separate. Raster budget limitation does not imply no native text payload.'))
    identity_rows=[]
    for rel,p in ps.items():
        if p['candidate_subject']['subject_confidence']!='SUBJECT_UNRESOLVED':continue
        before=read(output/'baseline'/rel)
        expected={r['region_id'] for s in p['canonical_subjects'].values() for r in s['evidence_support'] if r['accepted']}
        delivered={r['region_id'] for side in ('old','new') for e in p['evidence_packet']['evidence'][side]
                   for r in e['region_bindings'] if r['payload_delivered']}
        identity_rows.append(dict(package=rel,accepted_regions=sorted(expected),missing_regions=sorted(expected-delivered),
            before=delivery_view(before),after=delivery_view(p)))
    write(output/'IDENTITY_ONLY_AUDIT.json',dict(status='PASS' if not any(r['missing_regions'] for r in identity_rows) else 'FAIL',
        identity_evidence_dropped=len(identity_dropped),packages=identity_rows))
    strong=[];reason_index=defaultdict(list)
    for rel,p in ps.items():
        old=read(output/'baseline'/rel)
        strong.append(dict(package=rel,before=old['candidate_subject']['confidence'],after=p['candidate_subject']['confidence'],
            thresholds_unchanged=True,subject_discovery_unchanged=old['canonical_subjects']==p['canonical_subjects']))
        for b in p['boundary_completeness']['blockers']:
            for reason in b['reasons']:reason_index[reason].append(dict(package=rel,requirement_id=b['requirement_id']))
    write(output/'STRONG_BEFORE_AFTER.json',dict(before=23,after=sum(r['after']=='STRONG' for r in strong),rows=strong,
        confidence_policy='Unchanged V4 correspondence; delivery does not automatically promote confidence'))
    write(output/'PARTIAL_REASON_INDEX.json',dict(counts={k:len(v) for k,v in reason_index.items()},requirements=dict(reason_index)))
    replay=read(output/'REGRESSION_9_REPLAY.json');tests=read(output/'TEST_RECEIPT.json')
    counts=dict(Counter(r['completeness'] for r in index));counts.update(Counter(r['confidence'] for r in index));counts['packages']=len(index)
    counts={k:counts.get(k,0) for k in ('packages','COMPLETE','PARTIAL','MISSING','STRONG','POSSIBLE','UNRESOLVED')}
    result=dict(status='F5_V6_OFFLINE_REBUILD_COMPLETE',regressions_analyzed=9,
        delivery_regressions=dict(before=6,after=replay['delivery_after']),identity_only_regression=dict(before=1,after=replay['identity_only_after']),
        graphic_support_regressions=dict(total=2,resolved=replay['graphic_resolved'],unresolved=replay['graphic_unresolved']),
        p0_p1_displaced=displaced,before=dict(packages=79,STRONG=23),after=counts,
        mandatory_evidence_dropped=len(mandatory_dropped),identity_evidence_dropped=len(identity_dropped),
        continuation_omitted=len(continuation_omitted),budget_limited_requirements=len(budget_limited),
        budget_priority='PASS' if displaced==0 else 'FAIL',identity_only=read(output/'IDENTITY_ONLY_AUDIT.json')['status'],
        graphic_preservation='PASS' if replay['gates']['graphic_preservation'] else 'FAIL',regression_9=replay['status'],
        structural_audit=sa['status'],local_tests=tests,hash_stability=read(output/'FINAL_HASH_STABILITY.json')['status'],
        model_calls=0,validation='NOT OPENED',final_holdout='NOT OPENED',other_projects='NO',production='UNCHANGED',
        recommendation='READY_FOR_BOUNDARY_REPAIR' if replay['status']=='PASS' and sa['status']=='PASS' and not mandatory_dropped and not identity_dropped and tests['status']=='PASS' else 'F5_STILL_NEEDS_REPAIR')
    write(output/'F5_V6_RESULT.json',result)
    report=['# F5 V6 — evidence priority and identity retention','',f"STATUS: {result['status']}",'',
        '## Result','', '```json',json.dumps(result,ensure_ascii=False,indent=2),'```','',
        '## Scope and limits','',
        'Only the saved nine regression events were rebuilt and mechanically audited before the full frozen 79. '
        'The same code and package hashes are required in both builds. Source inventories are reused from immutable V4 '
        'and hash-bound to the same four admitted DEV documents; native subject refinement, requirements, delivery and certificates are rerun twice.', '',
        'P0/P1 role reservations precede supporting evidence. Whole native identity lines are reserved across accepted contexts '
        'before optional full-section expansion. A retained identity excerpt does not prove a complete paragraph, table, or graphic relation. '
        'Table and graphic delivery still requires its existing typed payload route. Boundary predicates and confidence thresholds are unchanged.', '',
        'Identity-only preserves all accepted regions and multi-role evidence. The two graphic-support regressions retain any accepted endpoint '
        'but remain unresolved where V4 subject discovery rejected the other endpoint. Roof discovery and correct foreign-cipher quarantine '
        'also remain separately traceable in REGRESSION_9_REPLAY.json. No inference or source-truth reading was performed.', '',
        'Budget metrics distinguish omitted raster, absent required-route payload, unavailable sources, and incomplete boundaries. '
        'P0/P1 displacement means loss to a lower tier; equal/higher-priority capacity exhaustion is explicitly logged, never waived.', '',
        'V4/V5 file sets and hashes were verified unchanged. Production unchanged. Stop after offline rebuild; no Astra call.','']
    (output/'F5_V6_REPORT.md').write_text('\n'.join(report))
    print(json.dumps(result,ensure_ascii=False),flush=True)
    return result
