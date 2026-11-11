import asyncio
import importlib.util
from pathlib import Path
from types import SimpleNamespace
import unittest

spec=importlib.util.spec_from_file_location('retry',Path(__file__).with_name('subscription_retry.py'))
m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)

class RetryTest(unittest.TestCase):
    def test_format_error_repeats_identical_request_after_ten_seconds(self):
        calls=[];waits=[];receipts=[]
        async def call(*args,**kwargs):
            calls.append((args,kwargs))
            failed=len(calls)==1
            return SimpleNamespace(is_error=failed,error_message='claude_json_not_found' if failed else '',text='',model='sonnet')
        async def sleep(delay):waits.append(delay)
        result=asyncio.run(m.retry_subscription_call(call,[{'role':'user','content':'same'}],model='sonnet',stop_requested=lambda:False,on_attempt=receipts.append,sleep=sleep))
        self.assertFalse(result.is_error);self.assertEqual(waits,[10]);self.assertEqual(calls[0],calls[1]);self.assertEqual(len(receipts),2)
    def test_auth_failure_is_terminal(self):
        r=SimpleNamespace(is_error=True,error_message='unauthorized 429',text='')
        self.assertEqual(m.transient_reason(r),'')
    def test_stop_prevents_request(self):
        async def call():raise AssertionError('request sent')
        with self.assertRaises(m.TrialStopped):asyncio.run(m.retry_subscription_call(call,stop_requested=lambda:True,on_attempt=lambda _:None))


class BoundedRetryTest(unittest.TestCase):
    def test_terminal_retry_budget_preserves_error_and_total_usage(self):
        calls=[];waits=[];receipts=[]
        async def call():
            calls.append(1)
            return SimpleNamespace(is_error=True,error_message='503 service unavailable',text='',input_tokens=10,output_tokens=2)
        async def sleep(delay):waits.append(delay)
        result=asyncio.run(m.retry_subscription_call(call,stop_requested=lambda:False,on_attempt=receipts.append,sleep=sleep,max_attempts=3))
        self.assertTrue(result.is_error)
        self.assertEqual(len(calls),3);self.assertEqual(waits,[10,10])
        self.assertEqual(result.input_tokens,30)
        self.assertTrue(receipts[-1]['retry_budget_exhausted'])
        self.assertIsNone(receipts[-1]['next_delay_seconds'])
    def test_bad_limit_fails_before_dispatch(self):
        async def call():raise AssertionError('request sent')
        for limit in (True,0,-1,1.5):
            with self.assertRaises(ValueError):
                asyncio.run(m.retry_subscription_call(call,stop_requested=lambda:False,on_attempt=lambda _:None,max_attempts=limit))

if __name__=='__main__':unittest.main()
