import asyncio
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace
import pytest

ROOT=Path(__file__).resolve().parents[1]

def load(name,file):
    spec=importlib.util.spec_from_file_location(name,ROOT/'experiments/classic_investigator'/file)
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module);return module

retry=load('trial_retry_usage','subscription_retry.py')
receipts=load('trial_usage_receipts','execution_receipts.py')

@pytest.mark.parametrize('bad', [None, 'missing', -1, True])
def test_unknown_attempt_usage_survives_until_final_accounting(tmp_path,bad):
    results=[]
    for i in range(2):
        r=SimpleNamespace(**{k:0 for k in retry._USAGE_FIELDS},is_error=i==0,
            error_message='503 service unavailable' if i==0 else '',text='',model='gpt-6-astra',cost_source='subscription')
        r.input_tokens=bad if i==0 else 20
        if bad=='missing' and i==0:del r.input_tokens
        results.append(r)
    folder=tmp_path/'request';folder.mkdir();(folder/'request.json').write_text('{}')
    rows=[];delays=[];calls=[]
    async def call(*args,**kwargs):
        calls.append(kwargs);i=len(calls)
        (folder/f'response_{i}.json').write_text('{"model":"gpt-6-astra"}')
        return results[i-1]
    async def sleep(seconds):delays.append(seconds)
    result=asyncio.run(retry.retry_subscription_call(call,stop_requested=lambda:False,
        on_attempt=rows.append,sleep=sleep,max_attempts=3,model='codex/gpt-6-astra',stage='test'))
    assert len(calls)==2 and delays==[10]
    assert rows[0]['usage']['input_tokens']==(None if bad=='missing' else bad)
    assert result.input_tokens==20 # available count, not a complete total
    (folder/'attempts.json').write_text(json.dumps(rows))
    summary=receipts.summarize_request_usage(tmp_path)
    assert not summary['accounting_complete']
    assert summary['totals']['input_tokens']==20
    assert any(i.get('field')=='input_tokens' for i in summary['issues'])
