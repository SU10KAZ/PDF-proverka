from copy import deepcopy
from pathlib import Path
import tempfile
import unittest

import fitz

from experiments.project_change_272.inventory import sha
from experiments.project_change_semantic_272.run import check_event
from experiments.project_change_semantic_272.test_gates import SourceGates
from .witnesses import raster_locator_errors


class RasterTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        path = Path(self.tmp.name) / 'source.png'
        with fitz.open() as pdf:
            pdf.new_page(width=100, height=100).get_pixmap().save(path)
        self.e = dict(evidence_id='e', raster=dict(path=str(path), sha256=sha(path)))
        self.w = dict(evidence_id='e', kind='RASTER_LOCATOR', literal_quote='',
                      visual_locator='7', bbox_norm=[.1,.2,.3,.4], binding_reason='Tag at branch node')

    def test_short_labels_have_no_text_length_threshold(self):
        for label in ['7', 'В1', 'ДУ4', 'П1кл', 'П1сс']:
            with self.subTest(label=label):
                self.assertEqual(raster_locator_errors(dict(self.w, visual_locator=label), self.e), [])

    def test_bad_coordinates(self):
        for box in [None, [], [0,0,1,2], [-.1,0,1,1], [0,0,float('nan'),1],
                    [0,0,float('inf'),1], [0,0,True,1], [.5,0,.5,1]]:
            self.assertIn('INVALID_RASTER_BBOX', raster_locator_errors(dict(self.w,bbox_norm=box),self.e))

    def test_missing_or_changed_raster(self):
        self.assertIn('MISSING_OR_INVALID_RASTER', raster_locator_errors(self.w, dict(self.e,raster=None)))
        Path(self.e['raster']['path']).write_bytes(b'invalid')
        self.assertIn('MISSING_OR_INVALID_RASTER', raster_locator_errors(self.w, self.e))

    def test_unbound_empty_or_mixed_witness(self):
        for change in [{'visual_locator':''}, {'binding_reason':''}, {'evidence_id':'foreign'}, {'literal_quote':'tag'}]:
            self.assertTrue(raster_locator_errors(self.w | change, self.e))

    def test_v1_short_graphic_reaches_semantic_route(self):
        fixture = SourceGates()
        fixture.setUp()
        p, event = fixture.packet, fixture.event
        e = p['evidence']['old'][0]
        e.update(source_kind='PDF_RASTER_CROP', visual_audit_required=True, raster=self.e['raster'])
        event['facts'][0]['old_witnesses'] = [dict(self.w, evidence_id=e['evidence_id'], route='GRAPHIC')]
        self.assertEqual(check_event(p,event), [])
        # Same short characters on the textual route remain invalid.
        event['facts'][0]['old_witnesses'] = [dict(evidence_id=e['evidence_id'], route='TEXT', literal_quote='7')]
        self.assertIn('QUOTE_NOT_IN_SOURCE', check_event(p,event))

    def test_legacy_short_graphic_does_not_become_truth_by_length_fix(self):
        fixture = SourceGates(); fixture.setUp()
        e = fixture.packet['evidence']['old'][0]
        e.update(source_kind='PDF_RASTER_CROP', visual_audit_required=True, raster=self.e['raster'])
        fixture.event['facts'][0]['old_witnesses'] = [dict(evidence_id=e['evidence_id'], route='GRAPHIC', quote='П1сс')]
        errors = check_event(fixture.packet, fixture.event)
        self.assertNotIn('QUOTE_NOT_IN_SOURCE', errors)
        self.assertIn('INVALID_RASTER_BBOX', errors)


if __name__ == '__main__':
    unittest.main()
