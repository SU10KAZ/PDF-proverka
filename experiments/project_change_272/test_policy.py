import copy
import json
from pathlib import Path
import tempfile
import unittest

from .inventory import ROOT, OBJECT, sha, write
from .policy import admitted_pairs, verify_split
from .split import fingerprints


class IsolationTest(unittest.TestCase):
    def test_frozen_allocation_excludes_foreign_and_fragment_pair(self):
        split = verify_split()
        all_indices = [i for group in split['allocation'].values() for i in group]
        self.assertEqual(len(all_indices), len(set(all_indices)))
        self.assertEqual(set(all_indices), set(range(2, 23)))
        self.assertEqual([len(x) for x in split['allocation'].values()], [13, 4, 4])
        self.assertFalse(split['historical_blind'])
        self.assertTrue(all(not r['historical_blind'] for r in split['pairs']))

    def test_reserve_denied_before_source_access(self):
        for partition in ['VALIDATION', 'FINAL_HOLDOUT']:
            with self.assertRaises(PermissionError):
                admitted_pairs(partition)

    def test_split_tampering_rejected(self):
        with tempfile.TemporaryDirectory() as name:
            root = Path(name)
            for file in ['INVENTORY.json', 'OVERLAP_AUDIT.json', 'SPLIT.json', 'SPLIT.sha256']:
                (root / file).write_bytes((ROOT / file).read_bytes())
            split = json.loads((root / 'SPLIT.json').read_text())
            split['allocation']['DEV'].append(11)
            write(root / 'SPLIT.json', split)
            with self.assertRaisesRegex(ValueError, 'split drift'):
                verify_split(root)

    def test_overlap_detects_same_evidence_after_page_move(self):
        content = 'A substantive engineering paragraph with fixed observations. ' * 10
        a = fingerprints('## Page 3\n' + content)
        b = fingerprints('## Page 19\n' + content)
        self.assertEqual(set(a), set(b))
        self.assertEqual(next(iter(a.values())), {3})
        self.assertEqual(next(iter(b.values())), {19})


if __name__ == '__main__':
    unittest.main()
