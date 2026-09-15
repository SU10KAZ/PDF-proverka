import json
from pathlib import Path
import tempfile
import unittest
import fitz

from experiments.project_change_272.inventory import sha

from .evidence import coverage_receipt, comparison_readiness
from .packages import package, select_sections, same_subject_counter, write_package, AdmittedPageLoader
from .test_evidence import requirement, unit


class PackageTests(unittest.TestCase):
    def loader(self, r):
        return dict(native='Heading\nA complete engineering paragraph.', full_page=True,
                    scope_binding=r.scope_binding, raster={'sha256': r.requirement_id},
                    provenance={'pdf': 'fixture'})

    def test_section_budget_never_slices_paragraph(self):
        text = '# Stamp\nUnrelated metadata.\n# System\nRequired complete paragraph.\nNext paragraph.'
        selected = select_sections(text, ['Required'], 65)
        self.assertIn('Next paragraph.', selected['text'])
        self.assertNotIn('Stamp', selected['text'])
        self.assertFalse(selected['boundary_complete'])

    def test_table_and_note_not_split(self):
        text = '# Demand\n| Consumer | Flow |\n| A | 2 |\n\nNote: standby included.'
        self.assertEqual(select_sections(text, ['Demand'], len(text))['text'], text)
        self.assertEqual(select_sections(text, ['Demand'], len(text)-1)['text'], '')

    def test_receipt_is_written_and_visible_in_model_package(self):
        body = package([requirement()], self.loader)
        self.assertTrue(body['coverage_complete'])
        with tempfile.TemporaryDirectory() as root:
            write_package(root, body)
            self.assertEqual(json.loads((Path(root)/'EVIDENCE_COVERAGE.json').read_text()), body['evidence_coverage'])

    def test_raster_budget_reports_missing_graphic_counterpart(self):
        reqs = [requirement(evidence_type='GRAPHIC_REGION'),
                requirement(requirement_id='n', side='NEW', evidence_type='GRAPHIC_REGION')]
        body = package(reqs, self.loader, raster_budget=1)
        self.assertFalse(body['coverage_complete'])
        self.assertFalse(comparison_readiness(body['evidence_coverage'], reqs[0].subject)['ready'])

    def test_zero_budget_never_claims_complete(self):
        body = package([requirement()], self.loader, text_budget=0, raster_budget=0)
        self.assertFalse(body['coverage_complete'])
        self.assertEqual(body['evidence'][0]['text'], '')

    def test_identical_pixels_on_two_sides_still_consume_two_source_inputs(self):
        def loader(r):
            return self.loader(r) | {'raster':{'sha256':'same pixels'}}
        body=package([requirement(), requirement(requirement_id='n',side='NEW')],loader,raster_budget=1)
        self.assertEqual(sum(bool(e['raster']) for e in body['evidence']),1)
        self.assertFalse(body['coverage_complete'])

    def test_subject_scoped_counter_rejects_foreign_version(self):
        r = requirement(evidence_type='COUNTER_EVIDENCE')
        result = same_subject_counter([r], [unit(), unit(evidence_id='x', document_version='v2'),
                                           unit(evidence_id='y', subject='Other')])
        self.assertEqual(result['evidence_ids'], ['e'])
        self.assertFalse(result['absence_proven'])

    def test_excluded_page_is_not_opened(self):
        r = requirement()
        key = (r.document, r.document_version, r.side)
        loader = AdmittedPageLoader({key: {'artifacts': {'pdf': {'path': '/must/not/open'}}}},
                                    {key: {r.page}}, '/unused')
        self.assertIsNone(loader(r))
        self.assertEqual(loader.opened, [])

    def test_raster_dimensions_are_actual_pixels_not_rounded_pdf_points(self):
        with tempfile.TemporaryDirectory() as name:
            path=Path(name)/'source.pdf'
            with fitz.open() as pdf:
                pdf.new_page(width=842.1,height=595.2)
                pdf.save(path)
            r=requirement(page=1)
            key=(r.document,r.document_version,r.side)
            loader=AdmittedPageLoader({key:{'artifacts':{'pdf':{'path':str(path),'sha256':sha(path)}}}}, {}, Path(name)/'images')
            try:
                raster=loader(r)['raster']
                pixmap=fitz.Pixmap(raster['path'])
                self.assertEqual((raster['width'],raster['height']),(pixmap.width,pixmap.height))
                self.assertEqual(loader(r)['raster'],raster)
            finally:
                loader.close()


if __name__ == '__main__':
    unittest.main()
