import unittest
from unittest.mock import patch

from . import report, run


class HarnessTests(unittest.TestCase):
    def test_truth_closed_before_twelve_successes_and_after_extra_call(self):
        for completed, calls in ((11, 12), (12, 13)):
            with self.subTest(completed=completed, calls=calls), patch.object(report, 'verify', return_value=({}, [])), \
                    patch.object(report, 'collect', return_value={'completed_count': completed, 'model_calls': calls}), \
                    patch.object(report, 'read') as read:
                with self.assertRaises(PermissionError):
                    report.report()
                read.assert_not_called()

    def test_failed_a2_or_d2_cannot_freeze(self):
        for failure in ('A2_PACKAGE_REGRESSION.json', 'D2_MECHANICAL_REPLAY.json'):
            with self.subTest(failure=failure), patch.object(run, 'OUT') as out, patch.object(run, 'provider_module') as provider:
                out.__truediv__.return_value.exists.return_value = False
                out.__truediv__.side_effect = lambda name: __import__('pathlib').Path('/tmp/projectchange_v3_unit/' + name)
                with patch.object(run, 'read', side_effect=lambda p: {'status': 'FAIL' if p.name == failure else 'PASS'}):
                    with self.assertRaises(AssertionError):
                        run.freeze()
                provider.assert_not_called()


if __name__ == '__main__':
    unittest.main()
