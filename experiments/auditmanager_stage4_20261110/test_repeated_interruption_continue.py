import copy
import json
import tempfile
from pathlib import Path
import unittest
from unittest.mock import patch
from experiments.auditmanager_stage4_20261110.run_experimental_collection_repeated_interruption_continue import pending_runs, verify_prior_interruption, sha


class InterruptedContinuationTests(unittest.TestCase):
    def setUp(self):
        self.slots = [dict(work=str(i), variant='source_review', discipline='AK', repeat=i) for i in range(4)]
        self.state = dict(pid=10, status='running', runs=[
            dict(self.slots[0], status='terminal', exit_code=0, pid=11),
            dict(self.slots[1], status='terminal', exit_code=1, pid=12),
            dict(self.slots[2], status='running', pid=13)])

    @patch('experiments.auditmanager_stage4_20261110.run_experimental_collection_repeated_interruption_continue.process', return_value={'live': False})
    def test_preserves_interrupted_slot_and_only_returns_untouched(self, _):
        original = copy.deepcopy(self.state)
        self.assertEqual(pending_runs({'runs': self.slots}, self.state), [self.slots[3]])
        self.assertEqual(self.state, original)

    @patch('experiments.auditmanager_stage4_20261110.run_experimental_collection_repeated_interruption_continue.process')
    def test_refuses_live_child_even_when_dispatcher_dead(self, process):
        process.side_effect = lambda pid, _: {'live': pid == 13}
        with self.assertRaises(ValueError): pending_runs({'runs': self.slots}, self.state)

    @patch('experiments.auditmanager_stage4_20261110.run_experimental_collection_repeated_interruption_continue.process', return_value={'live': True})
    def test_refuses_live_dispatcher(self, _):
        with self.assertRaises(ValueError): pending_runs({'runs': self.slots}, self.state)

    @patch('experiments.auditmanager_stage4_20261110.run_experimental_collection_repeated_interruption_continue.process', return_value={'live': False})
    def test_refuses_reordered_slots_and_invented_exit_code(self, _):
        self.state['runs'][-1]['repeat'] = 99
        with self.assertRaises(ValueError): pending_runs({'runs': self.slots}, self.state)
        self.state['runs'][-1]['repeat'] = 2
        self.state['runs'][-1]['exit_code'] = -1
        with self.assertRaises(ValueError): pending_runs({'runs': self.slots}, self.state)

    @patch('experiments.auditmanager_stage4_20261110.run_experimental_collection_repeated_interruption_continue.process', return_value={'live': False})
    def test_refuses_multiple_unfinished_slots(self, _):
        self.state['runs'][1]['status'] = 'running'
        with self.assertRaises(ValueError): pending_runs({'runs': self.slots}, self.state)

    @patch('experiments.auditmanager_stage4_20261110.run_experimental_collection_repeated_interruption_continue.process', return_value={'live': False})
    def test_accepts_prior_recorded_interruption_without_replacing_it(self, _):
        self.state['runs'][1].update(status='interrupted', exit_code=None,
            execution_status='interrupted_without_terminal_receipt',
            interruption_receipt={'path': 'incident.json', 'sha256': 'bound'})
        original = copy.deepcopy(self.state)
        self.assertEqual(pending_runs({'runs': self.slots}, self.state), [self.slots[3]])
        self.assertEqual(self.state, original)

    @patch('experiments.auditmanager_stage4_20261110.run_experimental_collection_repeated_interruption_continue.process', return_value={'live': False})
    def test_rejects_unbound_prior_interruption(self, _):
        self.state['runs'][1].update(status='interrupted', exit_code=None,
            execution_status='interrupted_without_terminal_receipt')
        with self.assertRaises(ValueError): pending_runs({'runs': self.slots}, self.state)


class PriorReceiptIntegrityTests(unittest.TestCase):
    def test_rejects_changed_receipt_status_or_request(self):
        for changed in ('receipt', 'status', 'request'):
            with self.subTest(changed=changed), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                status = root/'status/ITP.json'
                request = root/'requests/ITP/one/request.json'
                status.parent.mkdir(parents=True)
                request.parent.mkdir(parents=True)
                status.write_text('{}')
                request.write_text('{}')
                receipt = root/'incident.json'
                receipt.write_text(json.dumps({
                    'classification': 'interrupted_without_terminal_receipt',
                    'run_state': {'path': str(status), 'sha256': sha(status)},
                    'last_request': {'path': str(request), 'sha256': sha(request)}}))
                record = {'discipline': 'ITP', 'interruption_receipt': {
                    'path': str(receipt), 'sha256': sha(receipt)}}
                verify_prior_interruption(record, status)
                {'receipt': receipt, 'status': status, 'request': request}[changed].write_text('{"changed": true}')
                with self.assertRaises(ValueError):
                    verify_prior_interruption(record, status)
