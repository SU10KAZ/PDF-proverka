import json
from pathlib import Path
import tempfile
import unittest
from .pipeline import lock_truth


class ReserveSourceFirst(unittest.TestCase):
    def test_no_reserve_prediction_before_truth_lock(self):
        with self.assertRaises(PermissionError):lock_truth(None,'FINAL_HOLDOUT',Path('/unused'))

    def test_post_prediction_labels_cannot_be_called_source_first(self):
        with tempfile.TemporaryDirectory() as tmp:
            path=Path(tmp)/'truth.json'
            path.write_text(json.dumps(dict(partition='FINAL_HOLDOUT',predictions_seen=True,changes=[])))
            with self.assertRaises(PermissionError):lock_truth(path,'FINAL_HOLDOUT',Path(tmp)/'output')

    def test_empty_scan_metadata_cannot_claim_coverage(self):
        with tempfile.TemporaryDirectory() as tmp:
            path=Path(tmp)/'truth.json'
            path.write_text(json.dumps(dict(partition='VALIDATION',predictions_seen=False,source_scan_coverage=[],changes=[])))
            with self.assertRaises(PermissionError):lock_truth(path,'VALIDATION',Path(tmp)/'output')
