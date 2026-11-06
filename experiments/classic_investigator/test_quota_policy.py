import json
from pathlib import Path
import tempfile
import unittest
from quota_policy import assert_allowed, SubscriptionCeiling

class QuotaTest(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup);self.w=Path(self.tmp.name)
        self.policy={'claude_stop_percent':30,'max_cache_age_seconds':60,'claude_mode':'check_cache'}
        self.cache=self.w/'cache.json'
    def check(self,pct=24,age=0,model='claude-sonnet-5-5'):
        (self.w/'provider_policy.json').write_text(json.dumps(self.policy))
        self.cache.write_text(json.dumps({'cachedUsageUtilization':{'fetchedAtMs':(1000-age)*1000,'utilization':{'seven_day':{'utilization':pct}}}}))
        assert_allowed(self.w,model,cache_file=self.cache,now=1000)
    def test_fresh_below_ceiling(self):self.check(24)
    def test_exact_ceiling_blocks(self):
        with self.assertRaises(SubscriptionCeiling):self.check(30)
    def test_above_ceiling_blocks(self):
        with self.assertRaises(SubscriptionCeiling):self.check(31)
    def test_stale_cache_blocks(self):
        with self.assertRaises(SubscriptionCeiling):self.check(24,age=61)
    def test_disabled_blocks_below_ceiling(self):
        self.policy['claude_mode']='disabled'
        with self.assertRaises(SubscriptionCeiling):self.check(24)
    def test_codex_not_blocked(self):
        self.policy['claude_mode']='disabled';self.check(99,model='gpt-6-astra')

if __name__=='__main__':unittest.main()
