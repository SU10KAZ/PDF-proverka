import json
import os
from pathlib import Path
import tempfile
import unittest

from .preflight import ROOT, SourceGuard, budget_status, bundle


class PreflightSafetyTests(unittest.TestCase):
    def test_budget_boundary(self):
        self.assertEqual(budget_status(12), 'INVENTORY_WITHIN_LIMIT')
        self.assertEqual(budget_status(13), 'STOP_BROAD_CONTEXT_LIMIT')

    def test_truth_network_and_baseline_writes_rejected(self):
        source = ROOT / 'allowed_source.pdf'
        guard = SourceGuard([source], ROOT / 'new_experiment')
        guard.check('open', (str(source), 'r', os.O_RDONLY))
        for event, args in [
            ('open', (str(ROOT / 'SOURCE_VERIFICATION.json'), 'r', os.O_RDONLY)),
            ('open', (str(source), 'w', os.O_WRONLY)),
            ('socket.connect', ()),
            ('subprocess.Popen', ()),
        ]:
            with self.assertRaises(PermissionError):
                guard.check(event, args)
        self.assertEqual(guard.reads, {str(source)})

    def test_full_page_expansion_retains_cross_modal_regions(self):
        inv = {
            'old': dict(document_version='old', pages=[dict(page=2, status='INDEXED')],
                        regions=[dict(region_id='old_text', page=2, source_type='TEXT', bbox_norm=[0, 0, 1, 1]),
                                 dict(region_id='old_table', page=2, source_type='TABLE', bbox_norm=[0, 0, 1, 1])]),
            'new': dict(document_version='new', pages=[dict(page=3, status='INDEXED')],
                        regions=[dict(region_id='new_graphic', page=3, source_type='GRAPHIC', bbox_norm=[0, 0, 1, 1])]),
        }
        package = dict(candidate_subject=dict(candidate_id='c_seed', subject='Coarse heading'),
                       evidence_packet=dict(evidence={
                           'old': [dict(evidence_id='ot', page=2, route='TEXT')],
                           'new': [dict(evidence_id='ng', page=3, route='GRAPHIC')],
                       }))
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'package.json'
            path.write_text(json.dumps(package))
            result = bundle(package, inv, path)
            self.assertEqual({r['source_type'] for r in result['old']['region_refs']}, {'TEXT', 'TABLE'})
            self.assertEqual(result['new']['region_refs'][0]['source_type'], 'GRAPHIC')
            self.assertFalse(result['subject_identity_confirmed'])
            self.assertFalse(result['new']['raw_pdf_delivered_to_model'])
            inv['new']['pages'][0]['status'] = 'QUARANTINED'
            with self.assertRaises(ValueError):
                bundle(package, inv, path)


if __name__ == '__main__':
    unittest.main()
