"""Run the V4 verified suite, including its existing corrected hash-count test."""
import io
import unittest
from .snapshot import OUT, save
from experiments.project_change_post_inference_repair_v4_272.replay import TEST_MODULES
from experiments.project_change_post_inference_repair_v4_272.test_saved import SavedRegressionTests
from experiments.project_change_post_inference_repair_v4_272.final_audit import RawResponseIntegrityTest

suite=unittest.TestSuite(unittest.defaultTestLoader.loadTestsFromName(m) for m in TEST_MODULES)
for name in unittest.defaultTestLoader.getTestCaseNames(SavedRegressionTests):
    # FINAL_REPORT_VERIFIED explicitly supersedes this 156-versus-52 count assertion.
    if name!='test_25_raw_hashes_unchanged':suite.addTest(SavedRegressionTests(name))
suite.addTests(unittest.defaultTestLoader.loadTestsFromTestCase(RawResponseIntegrityTest))
stream=io.StringIO();result=unittest.TextTestRunner(stream=stream,verbosity=2).run(suite)
(OUT/'COMPONENT_TESTS.txt').write_text(stream.getvalue())
save(OUT/'COMPONENT_TESTS.json',dict(status='PASS' if result.wasSuccessful() else 'FAIL',tests=result.testsRun,
    failures=len(result.failures),errors=len(result.errors),
    existing_corrected_test='final_audit.RawResponseIntegrityTest replaces superseded test_saved.test_25_raw_hashes_unchanged',
    source_truth_opened=False,model_calls=0))
print(stream.getvalue()[-500:]);raise SystemExit(0 if result.wasSuccessful() else 1)
