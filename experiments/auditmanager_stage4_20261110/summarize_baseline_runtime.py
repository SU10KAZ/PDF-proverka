"""Summarize verified transport usage; subscription USD zero is not free usage."""
import json
import math
from collections import Counter
from pathlib import Path
from statistics import median
from .verify_completed_baseline_seals import verify, sha
from .verify_output_review_history import encode

ROOT = Path('/tmp/auditmanager_comparative_trial_context_v3_20261111')
HERE = Path(__file__).resolve().parent


def build():
    seals = verify(ROOT)
    assert len(seals['verified_runs']) == 18
    rows = []; totals = Counter()
    for seal in seals['verified_runs']:
        p = Path(seal['status_path'])
        state = json.loads(p.read_text())
        assert sha(p) == seal['status_sha256']
        usage = state['request_usage']
        row = dict(seal)
        row.update(tokens=usage['totals'], by_model_and_cost_source=usage['by_model_and_cost_source'], actual_monetary_cost_usd=usage['actual_monetary_cost_usd'], monetary_cost_fully_measured=usage['monetary_cost_fully_measured'])
        rows.append(row)
        totals.update({k: usage['totals'][k] for k in ['input_tokens', 'output_tokens', 'cached_tokens', 'reasoning_tokens']})
        totals.update({k: seal[k] for k in ['requests', 'attempts', 'failed_attempts']})
    times = sorted(r['wall_seconds'] for r in rows)
    grouped = {}
    for disc in sorted({r['discipline'] for r in rows}):
        values = [r['wall_seconds'] for r in rows if r['discipline'] == disc]
        grouped[disc] = {'runs':len(values), 'min_seconds':min(values), 'median_seconds':median(values), 'max_seconds':max(values)}
    return {'status':'verified_baseline_runtime_not_cost_acceptance','trial_sha256':seals['trial_sha256'],'runs':rows,'request_totals':dict(totals),'wall_time':{'sum_run_seconds':sum(times),'median_seconds':median(times),'p95_nearest_rank_seconds':times[math.ceil(.95*len(times))-1],'min_seconds':min(times),'max_seconds':max(times)},'by_discipline':grouped,'production_acceptance':False,'limits':['Subscription cost_usd=0 is not actual free usage; monetary cost and quota percentage are not measured.','Input tokens include repeated and cached context; do not treat as unique project text or billable dollars.','Summed request duration differs from wall time because operations overlap. Sum of run wall times is not total calendar elapsed time.','P95 uses nearest-rank with only18 heterogeneous runs; compare matched disciplines and repeats before acceptance.','No paired experimental runtime ratio, engineer time or transferability acceptance yet.']}


if __name__ == '__main__':
    result=build()
    with (HERE/'baseline_runtime_summary.json').open('xb') as stream:stream.write(encode(result))
    print(json.dumps({k:result[k] for k in ['request_totals','wall_time','by_discipline']},ensure_ascii=False))
