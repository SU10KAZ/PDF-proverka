"""Precision-first gates with explicit source truth and unresolved denominators."""
import argparse
from pathlib import Path

from experiments.project_change_272.inventory import read,immutable,sha,now
from experiments.project_change_272.candidate import CONFIG as PREDECESSOR_CONFIG

READINESS=dict(PREDECESSOR_CONFIG['readiness'])
PROTOCOL=dict(
    unit='One scoped engineering ProjectChange; facts and repeated source witnesses are not extra changes',
    precision='Source-confirmed correct accepted ProjectChanges / all accepted ProjectChanges; unresolved accepted cases remain in denominator',
    coverage='Confirmed high-value source truth changes whose required core state change is recovered / all confirmed high-value source truth changes',
    truth='Source-first reserve inventory locked before candidate inference; explicit coverage gaps and unknowns; no inferred absence',
    load='Accepted changes plus unresolved unique engineering review cases, divided by correct accepted changes',
    grouping='Source-adjudicated duplicate, over-group, under-group and parameter-explosion cases; do not infer quality from model confidence',
    independence='Same-model double checks are pipeline outputs. Implementing-agent source adjudication is explicitly labeled, not independent human evaluation',
    blind='Historical exposure must be disclosed separately from prospective cipher isolation')


def evaluate(prediction,truth,judgements,review_cases):
    accepted={c['project_change_id']:c for c in prediction['project_changes'] if c['status']=='ACCEPTED_CANDIDATE'}
    assessed={j['project_change_id']:j for j in judgements}
    if len(assessed)!=len(judgements) or set(assessed)!=set(accepted):
        raise ValueError('Every accepted change needs exactly one source judgement, including unresolved cases')
    if len({r['review_case_id'] for r in review_cases})!=len(review_cases):
        raise ValueError('Review cases must be deduplicated with retained source lineage')
    required=['verdict','truth_ids','source_checks','false_addition','false_removal','duplicate',
              'over_grouped','under_grouped','parameter_explosion','traceable']
    gold={g['truth_id']:g for g in truth['changes']}
    if len(gold)!=len(truth['changes']):raise ValueError('Duplicate source truth ID')
    for j in judgements:
        if any(k not in j for k in required) or j['verdict'] not in {'CORRECT','INCORRECT','UNRESOLVED'}:
            raise ValueError('Incomplete source judgement')
        if not isinstance(j['source_checks'],list) or not j['source_checks']:raise ValueError('Source checks required')
        if not set(j['truth_ids'])<=set(gold):raise ValueError('Unknown truth mapping')
        if any(type(j[k]) is not bool for k in required[3:]):raise ValueError('Error categories must be explicit booleans')
        if j['verdict']=='CORRECT' and any(j[k] for k in ['false_addition','false_removal','over_grouped']):
            raise ValueError('Contradictory correctness judgement')
    correct=[j for j in judgements if j['verdict']=='CORRECT']
    high={k for k,g in gold.items() if g['status']=='CONFIRMED' and g['importance']=='HIGH'}
    recovered={k for j in correct for k in j['truth_ids'] if k in high}
    n=len(accepted);tp=len(correct)
    metrics=dict(accepted=n,correct=tp,incorrect=sum(j['verdict']=='INCORRECT' for j in judgements),
        unresolved_accepted=sum(j['verdict']=='UNRESOLVED' for j in judgements),
        project_change_precision=tp/n if n else None,high_value_truth=len(high),high_value_recovered=len(recovered),
        high_value_coverage=len(recovered)/len(high) if high else None,
        represented_correct_ciphers=len({accepted[j['project_change_id']]['pair_index'] for j in correct}),
        review_cases=len(review_cases),review_to_correct_accepted=(n+len(review_cases))/tp if tp else None,
        traceability=sum(j['traceable'] for j in judgements)/n if n else None,
        duplicate_fraction=sum(j['duplicate'] for j in judgements)/n if n else None,
        **{k:sum(j[k] for j in judgements) for k in ['false_addition','false_removal','duplicate','over_grouped','under_grouped','parameter_explosion']})
    failures=[]
    for field,limit in [('project_change_precision','project_change_precision_min'),('high_value_coverage','high_value_coverage_min'),
                        ('accepted','accepted_events_min'),('represented_correct_ciphers','represented_validation_ciphers_min'),
                        ('traceability','traceability_min')]:
        if metrics[field] is None or metrics[field]<READINESS[limit]:failures.append(field)
    for field,limit in [('false_addition','false_additions_max'),('false_removal','false_removals_max'),
                        ('duplicate_fraction','duplicate_fraction_max'),('over_grouped','over_grouping_max'),
                        ('review_to_correct_accepted','review_to_correct_accepted_max')]:
        if metrics[field] is None or metrics[field]>READINESS[limit]:failures.append(field)
    if not truth.get('coverage_inventory_complete'):failures.append('incomplete_source_truth_inventory')
    if truth.get('partition')!='DEV' and not truth.get('locked_before_inference'):failures.append('reserve_truth_not_locked_before_inference')
    return dict(metrics=metrics,failed_gates=failures,decision='PASS' if not failures else 'REJECT_CANDIDATE',
                protocol=PROTOCOL,thresholds=READINESS,truth_provenance={k:truth.get(k) for k in ['partition','adjudicator','independent','historical_blind','coverage_inventory_complete']})


if __name__=='__main__':
    p=argparse.ArgumentParser()
    for name in ['prediction','truth','judgements','reviews','output']:p.add_argument('--'+name,type=Path,required=True)
    a=p.parse_args();result=evaluate(read(a.prediction),read(a.truth),read(a.judgements),read(a.reviews))
    immutable(a.output,dict(created_at=now(),**result,input_receipts={k:dict(path=str(getattr(a,k)),sha256=sha(getattr(a,k))) for k in ['prediction','truth','judgements','reviews']}))
