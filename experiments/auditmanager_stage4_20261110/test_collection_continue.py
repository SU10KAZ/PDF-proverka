import unittest
from unittest.mock import patch
from .run_experimental_collection_continue import pending_runs


class ContinuationTests(unittest.TestCase):
    def setUp(self):
        self.slots = [{'work': str(i), 'variant': 'inventory', 'discipline': 'AK', 'repeat': i} for i in range(3)]
        self.state = {'pid': 0, 'status': 'failed_requires_inspection', 'runs': [
            dict(self.slots[0], status='terminal', exit_code=0, pid=1),
            dict(self.slots[1], status='terminal', exit_code=1, pid=2)]}

    @patch('experiments.auditmanager_stage4_20261110.run_experimental_collection_continue.process', return_value={'live': False})
    def test_keeps_success_and_failure_without_retry(self, _):
        self.assertEqual(pending_runs({'runs': self.slots}, self.state), [self.slots[2]])
        self.assertEqual(self.state['runs'][1]['exit_code'], 1)

    @patch('experiments.auditmanager_stage4_20261110.run_experimental_collection_continue.process', return_value={'live': True})
    def test_refuses_live_dispatcher(self, _):
        with self.assertRaises(ValueError): pending_runs({'runs': self.slots}, self.state)

    @patch('experiments.auditmanager_stage4_20261110.run_experimental_collection_continue.process', return_value={'live': False})
    def test_refuses_nonterminal_or_reordered(self, _):
        self.state['runs'][1]['status'] = 'running'
        with self.assertRaises(ValueError): pending_runs({'runs': self.slots}, self.state)
        self.state['runs'][1]['status'] = 'terminal'
        self.state['runs'][1]['repeat'] = 99
        with self.assertRaises(ValueError): pending_runs({'runs': self.slots}, self.state)
