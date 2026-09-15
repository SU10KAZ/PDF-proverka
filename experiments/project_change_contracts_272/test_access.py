from pathlib import Path
import unittest
from unittest.mock import patch

from experiments.project_change_272 import policy


class NarrowAccessTests(unittest.TestCase):
    def test_requested_indices_cannot_enlarge_dev(self):
        with self.assertRaises(PermissionError):
            policy.admitted_pairs(indices={999999})

    def test_selected_pair_hashes_only_selected_sources(self):
        from experiments.project_change_272.inventory import sha
        seen = []
        def recorded(path):
            seen.append(str(path))
            return sha(path)
        with patch.object(policy, 'sha', side_effect=recorded):
            rows = policy.admitted_pairs(indices={5, 7}, source_repo=Path('/home/coder/projects/PDF-proverka'))
        allowed_paths = {a['path'] for r in rows for s in ('old','new') for a in r[s]['artifacts'].values()}
        source_reads = {p for p in seen if '/projects_v2/' in p}
        self.assertEqual(source_reads, allowed_paths)
        self.assertEqual({r['index'] for r in rows}, {5, 7})


if __name__ == '__main__':
    unittest.main()
