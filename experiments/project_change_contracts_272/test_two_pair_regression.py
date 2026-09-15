import json
from pathlib import Path
import unittest

from .state_regression import evaluate_fixture


class TwoPairStateTests(unittest.TestCase):
    def test_source_audit_typed_states(self):
        cases=json.loads(Path(__file__).with_name('two_pair_cases.json').read_text())['cases']
        for case in cases:
            with self.subTest(pair=case['pair_index'],case=case['case_id']):
                result=evaluate_fixture(case)
                self.assertTrue(result['f2_regression_pass'],result)

    def test_required_cases_are_present(self):
        cases=json.loads(Path(__file__).with_name('two_pair_cases.json').read_text())['cases']
        actual={(c['pair_index'],c['case_id']) for c in cases if c['required_f2']}
        expected={(5,c) for c in ['C01','C05','C07','C08','C17','C22']} | {
            (7,c) for c in ['R01','R05','R11','R19','Q04','R25']}
        self.assertEqual(actual,expected)


if __name__ == '__main__':
    unittest.main()
