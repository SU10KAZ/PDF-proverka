import unittest
from experiments.auditmanager_stage4_20261110.prepare_current_method_evaluation import execution_classification as classify


class ExecutionClassificationTests(unittest.TestCase):
    def test_failed_is_not_empty_success(self):
        self.assertEqual(classify({'status': 'terminal', 'exit_code': 1}, {'status': 'failed'}, False), 'terminal_failed')

    def test_live_failure_receipt_is_not_terminal(self):
        self.assertEqual(classify({'status': 'terminal', 'exit_code': 1}, {'status': 'failed'}, True), 'running_or_unverified')

    def test_unstarted_is_not_failed(self):
        self.assertEqual(classify({}, {}, False), 'no_verified_terminal_completion')

    def test_success_requires_completed_status(self):
        self.assertEqual(classify({'exit_code': 0}, {'status': 'failed'}, False), 'no_verified_terminal_completion')
        self.assertEqual(classify({'exit_code': 0}, {'status': 'completed_unadjudicated'}, False), 'terminal_completed')

    def test_inconsistent_failure_is_not_accepted(self):
        self.assertEqual(classify({'status': 'running', 'exit_code': 1}, {'status': 'failed'}, False), 'no_verified_terminal_completion')
