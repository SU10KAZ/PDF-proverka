"""Diagnostic-only evaluation; missing model results never count as eliminated FP."""
from collections import Counter
from pathlib import Path

from experiments.project_change_272.inventory import read, sha
from .packages import BASE, save
from .contracts import resolve_ownership


def build():
    manifest=read(BASE/'DIAGNOSTIC_MANIFEST.json')
    rows=[];events=[];evidence={}
    for c in manifest['cases']:
        path=BASE/'results'/(c['case_id']+'.json')
        r=read(path) if path.exists() else None
        p=read(BASE/'packages'/(c['case_id']+'.json'))
        if r:
            o=r['output'];ids={w['evidence_id'] for a in o['claim_audits'] for s in ['old','new'] for w in a[s+'_witnesses']}
            # Prefix IDs so unrelated same-named page IDs cannot alias across packets.
            mapped={e['evidence_id']: c['case_id']+':'+e['evidence_id'] for e in p['sources']}
            evidence.update({mapped[e['evidence_id']]:dict(e) for e in p['sources']})
            events.append(dict(event_id=c['case_id'],object_id=272,status=r['gate']['status'],
                pair_index=c['pair_index'],evidence_ids=[mapped[i] for i in ids if i in mapped],
                output=o))
        rows.append(dict(case_id=c['case_id'],category=c['category'],subject=c['subject'],
            reference_verdict=c['v1_reference_verdict'],status=r['gate']['status'] if r else 'NOT_RUN',
            reasons=r['gate']['reasons'] if r else ['NO_INFERENCE_RESULT'],
            result_receipt=dict(path=str(path),sha256=sha(path)) if r else None,
            package_hash=p['package_hash'],high_value_ids=c['high_value_ids']))
    probes=read(BASE/'OWNERSHIP_PROBES.json')
    ownership=resolve_ownership(events,probes['links'],evidence)
    save(BASE/'CROSS_DOCUMENT_OWNERSHIP.json',ownership)
    counts={category:dict(Counter(r['status'] for r in rows if r['category']==category))
            for category in ['FP','UNCERTAIN','TP_CONTROL','HIGH_VALUE']}
    fp=sum(r['category']=='FP' and r['status']=='REVIEW' for r in rows)
    tp=sum(r['category']=='TP_CONTROL' and r['status']=='ACCEPT' for r in rows)
    uncertain_accept=sum(r['category']=='UNCERTAIN' and r['status']=='ACCEPT' for r in rows)
    high=[]
    for row in rows:
        for hid in row['high_value_ids']:
            p=read(BASE/'packages'/(row['case_id']+'.json'))
            coverage=next(c for c in read(BASE/'PACKAGE_COVERAGE.json') if c['case_id']==row['case_id'])
            high.append(dict(high_value_id=hid,case_id=row['case_id'],
                evidence_present_v2=all(e['present_v2_text'] or e['present_v2_raster'] for e in coverage['pages']),
                v1_missing_pages=[{k:e[k] for k in ['pair_index','side','page']} for e in coverage['pages'] if not e['present_v1_primary']],
                retrieval='BOUNDED_MANUAL_REGISTRY_NOT_AUTONOMOUS_DISCOVERY',
                state_comparison='PASS' if row['status']=='ACCEPT' else 'REVIEW_OR_NOT_RUN',
                grouping='LOCAL_ONLY_CROSS_DOCUMENT_NOT_PROVEN',final_projectchange=row['status']))
    successes=[read(p) for p in (BASE/'inference'/'calls').glob('*/SUCCESS.json')]
    validations=[read(p) for p in (BASE/'inference'/'calls').glob('*/attempt_*/VALIDATION.json')]
    attempts=list((BASE/'inference'/'calls').glob('*/attempt_*/INVOCATION.json'))
    usage=dict(invocations=len(attempts),successful_calls=len(successes),
        failed_or_interrupted=len(attempts)-len(successes),retries=0,
        input_tokens=sum(u.get('input_tokens',0) for v in validations for u in v['usage']),
        output_tokens=sum(u.get('output_tokens',0) for v in validations for u in v['usage']),
        cached_input_tokens=sum(u.get('cached_input_tokens',0) for v in validations for u in v['usage']),
        cost_usd=None,openrouter_requests=0,orchestration_usage_included=False)
    reasons=[]
    if fp!=6:reasons.append('KNOWN_FALSE_POSITIVES_NOT_ALL_ELIMINATED')
    if tp<11:reasons.append('TRUE_POSITIVE_REGRESSION')
    if uncertain_accept:reasons.append('UNCERTAIN_PROMOTIONS_REQUIRE_SOURCE_AUDIT')
    if len(successes)!=28:reasons.append('INCOMPLETE_DIAGNOSTIC_INFERENCE')
    # These explicit experimental limitations must not be hidden by good local scores.
    reasons.append('AUTONOMOUS_ENGINEERING_SCOPE_RETRIEVAL_NOT_PROVEN')
    if any(not any(set(probe) <= set(g['member_ids']) for g in ownership['groups'])
           for probe in probes['required_fusions']):
        reasons.append('KNOWN_CROSS_DOCUMENT_UNDER_GROUPING_NOT_RECOVERED')
    blocked=any(read(p).get('status')=='AUTHORIZATION_BLOCKED' for p in (BASE/'stops').glob('*.json'))
    result=dict(status='REJECTED_DIAGNOSTIC_V2',execution_status='AUTHORIZATION_BLOCKED' if blocked else 'COMPLETE' if len(successes)==28 else 'INCOMPLETE',
        go_to_full_codex_v2_dev=False,rejection_reasons=reasons,case_count=len(rows),
        fp_eliminated_from_accept=fp,fp_total=6,tp_controls_retained=tp,tp_controls_total=12,
        uncertain_promoted_to_accept=uncertain_accept,counts=counts,cases=rows,high_value=high,
        usage=usage,diagnostic_precision=None,dev_precision=None,coverage=None,
        validation_opened=False,final_holdout_opened=False,other_projects_used=False,
        v1_v2_outputs_mixed=False,source_audit='V1 reference retained separately; V2 model certificates not independent expertise')
    save(BASE/'DIAGNOSTIC_RESULTS.json',result);save(BASE/'REJECTION.json',dict(status=result['status'],go=False,reasons=reasons))
    save(BASE/'CODEX_USAGE.json',usage)
    print(result['status'],'FP',fp,'/6, TP',tp,'/12, uncertain ACCEPT',uncertain_accept,'GO=NO',flush=True)


if __name__=='__main__':build()
