"""Account for saved final checks, including held and contract-invalid answers."""
import argparse
import json
from pathlib import Path
from backend.app.pipeline.stages.findings_verify.source_review_evidence import evidence_digest
from .measure_publication_trial import FIELDS
from .run_primary_text_runtime_probe import sha, write


def measure(roots):
    records=[];seen=set()
    for root in roots:
        result_path=(root/'workflow/result.json').resolve()
        if result_path in seen:
            raise ValueError('Duplicate final response')
        seen.add(result_path)
        result=json.loads(result_path.read_text())
        request=json.loads((root/'workflow/request.json').read_text())
        bound=json.loads((root/'workflow/result_binding.json').read_text())
        if bound!={'request':evidence_digest(request),'result':evidence_digest(result)}:
            raise ValueError('Unbound or changed final response')
        receipt_path=root/'receipt.json'
        receipt=json.loads(receipt_path.read_text()) if receipt_path.exists() else None
        if receipt and (receipt['request_sha256']!=bound['request'] or receipt['result_sha256']!=bound['result']):
            raise ValueError('Receipt belongs to another response')
        records.append({'response_path':str(result_path),'response_sha256':sha(result_path),
            'request_sha256':sha(root/'workflow/request.json'),
            'model':result.get('model'),'admission_status':receipt['status'] if receipt else 'no_validated_receipt',
            'transport_success':not result.get('is_error') and isinstance(result.get('json_data'),dict),
            **{k:result.get(k) for k in FIELDS},'reported_cost_usd':result.get('cost_usd'),
            'cost_source':result.get('cost_source')})
    usage={}
    for key in FIELDS:
        values=[r[key] for r in records if type(r[key]) in (int,float) and r[key]>=0]
        usage[key]={'reported_total':sum(values),'records_with_value':len(values),
                    'complete_for_saved_attempts':len(values)==len(records)}
    return {'status':'final_check_resource_snapshot_not_quality_acceptance','saved_responses':len(records),
        'usage':usage,'records':records,'monetary_cost_estimate':None,'subscription_quota_fraction':None,
        'limits':['Held and contract-invalid model results consume resources and are included.',
                  'Distinct from standard publication runner accounting; do not count these files again.',
                  'Cached/reasoning tokens are reported separately, not added again to input/output totals.',
                  'Subscription USD=0 does not establish free inference; no quota percentage inferred.',
                  'Unreported internal retries and unfinished requests are outside saved-response totals.'],
        'production_acceptance':False}


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('roots',nargs='+',type=Path);p.add_argument('--output',required=True,type=Path);a=p.parse_args()
    result=measure(a.roots);write(a.output,result);print(json.dumps({'saved_responses':result['saved_responses'],'usage':result['usage']}))
