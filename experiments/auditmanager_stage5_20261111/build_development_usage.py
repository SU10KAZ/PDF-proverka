"""Read-only provider-result accounting for development, not comparative acceptance."""
from collections import defaultdict
import hashlib
import json
from pathlib import Path
SOURCE=Path('experiments/auditmanager_stage4_20261110')
OUT=Path(__file__).parent
FIELDS=('model','response_id','input_tokens','output_tokens','cached_tokens','cache_write_tokens',
        'reasoning_tokens','duration_ms','cost_usd','cost_source','is_error')


def main():
    indexed=defaultdict(list);missing=[];files=0
    for path in sorted(SOURCE.rglob('*.json')):
        try:
            raw=path.read_bytes();data=json.loads(raw)
        except (OSError,ValueError):continue
        if not isinstance(data,dict) or not isinstance(data.get('model'),str) or 'is_error' not in data or 'input_tokens' not in data:
            continue
        files+=1
        record={'path':str(path),'sha256':hashlib.sha256(raw).hexdigest(),
                'usage':{k:data.get(k) for k in FIELDS}}
        response=data.get('response_id')
        if not isinstance(response,str) or not response.strip():missing.append(record);continue
        indexed[(data['model'],response)].append(record)
    unique=[];conflicts=[]
    for (model,response),copies in indexed.items():
        values={json.dumps(r['usage'],sort_keys=True) for r in copies}
        entry={'model':model,'response_id':response,'source_copies':copies}
        if len(values)!=1:conflicts.append(entry);continue
        entry['usage']=copies[0]['usage'];unique.append(entry)
    totals={}
    for entry in unique:
        u=entry['usage'];model=entry['model'];t=totals.setdefault(model,{'unique_responses':0,'errors':0,
            'input_tokens':0,'output_tokens':0,'cached_tokens':0,'reasoning_tokens':0,
            'summed_provider_duration_ms':0,'subscription_responses':0,'non_subscription_reported_cost_usd':0,
            'missing_measurements':defaultdict(int)})
        t['unique_responses']+=1;t['errors']+=u['is_error'] is True
        for k in ('input_tokens','output_tokens','cached_tokens','reasoning_tokens','duration_ms'):
            target='summed_provider_duration_ms' if k=='duration_ms' else k
            v=u[k]
            if isinstance(v,(int,float)) and not isinstance(v,bool) and v>=0:t[target]+=v
            else:t['missing_measurements'][k]+=1
        if u['cost_source']=='subscription':t['subscription_responses']+=1
        elif isinstance(u['cost_usd'],(int,float)) and u['cost_source']:
            t['non_subscription_reported_cost_usd']+=u['cost_usd']
        else:t['missing_measurements']['monetary_cost']+=1
    report={'scope':'Archived stage4 development results only; not a full-project A/B measurement.',
        'source_directory':str(SOURCE),'provider_result_files':files,'unique_responses':len(unique),
        'duplicate_copies_removed':sum(len(e['source_copies'])-1 for e in unique),
        'conflicting_response_ids':conflicts,'files_without_response_id':missing,
        'totals_by_model':totals,'responses':unique,
        'limits':['Subscription zero cost is not free usage; quota percentage and monetary equivalent are unknown.',
            'Summed response durations are not elapsed wall time because requests can overlap.',
            'Cached and reasoning tokens are reported separately and are not added again to input/output totals.',
            'Files without response IDs and conflicting measurements are excluded from aggregates, not treated as zero-cost calls.',
            'Current temp-only runs and missing archived results are outside this snapshot; completeness is not claimed.',
            'No model-quality conclusion or stage5 completion follows from development usage alone.'],
        'stage5_complete':False}
    (OUT/'development_usage.json').write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n')
    print(json.dumps({k:report[k] for k in ('provider_result_files','unique_responses','duplicate_copies_removed','totals_by_model')},ensure_ascii=False))

if __name__=='__main__':main()
