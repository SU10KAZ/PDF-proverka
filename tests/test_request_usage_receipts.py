import importlib.util
import json
from pathlib import Path
import pytest

spec=importlib.util.spec_from_file_location('receipts',Path(__file__).resolve().parents[1]/'experiments/classic_investigator/execution_receipts.py')
module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)


def request(root,name,attempts):
    p=root/name;p.mkdir()
    (p/'request.json').write_text(json.dumps({'options':{'model':'codex/gpt-6-astra'}}))
    rows=[]
    for i,tokens in enumerate(attempts,1):
        rows.append({'attempt':i,'model':'codex/gpt-6-astra','reported_model':'gpt-6-astra','cost_source':'subscription','is_error':i<len(attempts),
            'usage':{'input_tokens':tokens,'output_tokens':3,'cached_tokens':4,'cache_write_tokens':0,'reasoning_tokens':1,'cost_usd':0,'duration_ms':20}})
        (p/f'response_{i}.json').write_text(json.dumps({'model':'gpt-6-astra'}))
    (p/'attempts.json').write_text(json.dumps(rows))
    return p,rows


def test_nested_callbacks_and_retry_attempts_counted_once(tmp_path):
    request(tmp_path,'detector',[10,20]);request(tmp_path,'source_review',[30])
    result=module.summarize_request_usage(tmp_path)
    assert result['accounting_complete']
    assert result['request_count']==2 and result['attempt_count']==3
    assert result['failed_attempt_count']==1
    assert result['totals']['input_tokens']==60
    assert result['totals']['cached_tokens']==12
    assert result['totals']['duration_ms']==60
    assert result['by_model_and_cost_source'][0]['cost_source']=='subscription'


@pytest.mark.parametrize('damage',['missing_attempts','extra_response','missing_usage','unknown_cost','duplicate_attempt','missing_response'])
def test_incomplete_accounting_is_not_silently_complete(tmp_path,damage):
    p,rows=request(tmp_path,'request',[10,20])
    if damage=='missing_attempts':(p/'attempts.json').unlink()
    elif damage=='extra_response':(p/'response_3.json').write_text('{}')
    elif damage=='missing_response':(p/'response_1.json').unlink()
    else:
        if damage=='missing_usage':rows[0]['usage'].pop('input_tokens')
        if damage=='unknown_cost':rows[0].pop('cost_source')
        if damage=='duplicate_attempt':rows.append(rows[0])
        (p/'attempts.json').write_text(json.dumps(rows))
    result=module.summarize_request_usage(tmp_path)
    assert not result['accounting_complete'] and result['issues']
    if damage=='duplicate_attempt':assert result['totals']['input_tokens']==30


def test_no_requests_is_unknown_not_free(tmp_path):
    result=module.summarize_request_usage(tmp_path)
    assert not result['accounting_complete']


@pytest.mark.parametrize('cost_source,measured', [('actual',True),('estimated',False),('subscription',False)])
def test_monetary_metric_never_promotes_subscription_zero_to_measured_cost(tmp_path,cost_source,measured):
    p,rows=request(tmp_path,'request',[10])
    rows[0]['cost_source']=cost_source
    (p/'attempts.json').write_text(json.dumps(rows))
    summary=module.summarize_request_usage(tmp_path)
    assert summary['accounting_complete']
    assert summary['monetary_cost_fully_measured'] is measured
    assert summary['actual_monetary_cost_usd']==(0 if measured else None)
