import copy
import unittest
from unittest.mock import patch
from experiments.auditmanager_stage4_20261110.run_experimental_collection_interrupted_continue import pending_runs


class InterruptedContinuationTests(unittest.TestCase):
    def setUp(self):
        self.slots = [dict(work=str(i), variant='source_review', discipline='AK', repeat=i) for i in range(4)]
        self.state = dict(pid=10, status='running', runs=[
            dict(self.slots[0], status='terminal', exit_code=0, pid=11),
            dict(self.slots[1], status='terminal', exit_code=1, pid=12),
            dict(self.slots[2], status='running', pid=13)])

    @patch('experiments.auditmanager_stage4_20261110.run_experimental_collection_interrupted_continue.process', return_value={'live': False})
    def test_preserves_interrupted_slot_and_only_returns_untouched(self, _):
        original = copy.deepcopy(self.state)
        self.assertEqual(pending_runs({'runs': self.slots}, self.state), [self.slots[3]])
        self.assertEqual(self.state, original)

    @patch('experiments.auditmanager_stage4_20261110.run_experimental_collection_interrupted_continue.process')
    def test_refuses_live_child_even_when_dispatcher_dead(self, process):
        process.side_effect = lambda pid, _: {'live': pid == 13}
        with self.assertRaises(ValueError): pending_runs({'runs': self.slots}, self.state)

    @patch('experiments.auditmanager_stage4_20261110.run_experimental_collection_interrupted_continue.process', return_value={'live': True})
    def test_refuses_live_dispatcher(self, _):
        with self.assertRaises(ValueError): pending_runs({'runs': self.slots}, self.state)

    @patch('experiments.auditmanager_stage4_20261110.run_experimental_collection_interrupted_continue.process', return_value={'live': False})
    def test_refuses_reordered_slots_and_invented_exit_code(self, _):
        self.state['runs'][-1]['repeat'] = 99
        with self.assertRaises(ValueError): pending_runs({'runs': self.slots}, self.state)
        self.state['runs'][-1]['repeat'] = 2
        self.state['runs'][-1]['exit_code'] = -1
        with self.assertRaises(ValueError): pending_runs({'runs': self.slots}, self.state)

    @patch('experiments.auditmanager_stage4_20261110.run_experimental_collection_interrupted_continue.process', return_value={'live': False})
    def test_refuses_multiple_unfinished_slots(self, _):
        self.state['runs'][1]['status'] = 'running'
        with self.assertRaises(ValueError): pending_runs({'runs': self.slots}, self.state)
