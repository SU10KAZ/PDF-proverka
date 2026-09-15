from dataclasses import replace
import unittest

from .evidence import EvidenceRequirement, assess, coverage_receipt, comparison_readiness


def requirement(**changes):
    fields = dict(requirement_id='r', subject='Water branch', side='OLD',
                  evidence_role='STATE', document='D', document_version='v1', page=2,
                  expected_semantic_content='Branch demand table including its basis',
                  evidence_type='TABLE_COMPLETE', scope_binding='audited table region',
                  provenance={'source_audit': 'fixture'})
    return EvidenceRequirement(**(fields | changes))


def unit(**changes):
    return dict(evidence_id='e', subject='Water branch', side='OLD', document='D',
                document_version='v1', page=2, content_kind='TABLE',
                text='Complete demand table: consumer A, flow 2; note: design mode.',
                delivered_parts=['heading', 'rows', 'columns', 'footnotes'],
                scope_binding='audited table region', boundary_complete=True,
                provenance={'source': 'fixture'}, **{}) | changes


class EvidenceTests(unittest.TestCase):
    def test_page_presence_is_not_content(self):
        self.assertEqual(assess(requirement(), [unit(delivered_parts=[])])['completeness'], 'PARTIAL')

    def test_header_stamp_truncation(self):
        for kind, changes, expected in [
            ('HEADER', {'delivered_parts': ['heading']}, 'HEADER_ONLY'),
            ('STAMP', {'delivered_parts': []}, 'STAMP_ONLY'),
            ('TEXT', {'truncated': True}, 'TRUNCATED')]:
            with self.subTest(expected=expected):
                self.assertEqual(assess(requirement(), [unit(content_kind=kind, **changes)])['completeness'], expected)

    def test_complete_table_requires_rows_columns_footnotes(self):
        self.assertEqual(assess(requirement(), [unit()])['completeness'], 'COMPLETE')
        for missing in ['rows', 'columns', 'footnotes']:
            parts = [p for p in unit()['delivered_parts'] if p != missing]
            self.assertEqual(assess(requirement(), [unit(delivered_parts=parts)])['completeness'], 'PARTIAL')

    def test_wrong_subject_or_version(self):
        for change in [{'document_version': 'v2'}, {'subject': 'Other branch'}, {'document': 'X'}]:
            self.assertEqual(assess(requirement(), [unit(**change)])['completeness'], 'WRONG_SCOPE')

    def test_unverified_boundary_cannot_be_complete(self):
        for change in [{'boundary_complete': False}, {'scope_binding': ''}, {'provenance': {}}, {'text':''}]:
                self.assertNotEqual(assess(requirement(), [unit(**change)])['completeness'], 'COMPLETE')

    def test_graphic_parts_metadata_without_raster_is_not_complete(self):
        r = requirement(evidence_type='GRAPHIC_REGION')
        self.assertNotEqual(assess(r, [unit(delivered_parts=list(r.parts))])['completeness'], 'COMPLETE')

    def test_graphic_requires_old_counterpart(self):
        new = requirement(side='NEW', requirement_id='n', evidence_type='GRAPHIC_REGION')
        receipt = coverage_receipt([new], [unit(side='NEW', delivered_parts=list(new.parts))])
        self.assertIn('OLD_EVIDENCE_INCOMPLETE', comparison_readiness(receipt, new.subject)['reasons'])

    def test_novelty_requires_same_version_counter(self):
        old = requirement(evidence_type='COUNTER_EVIDENCE')
        new = replace(old, side='NEW', evidence_type='TEXT_SECTION', requirement_id='n')
        receipt = coverage_receipt([old, new], [unit(delivered_parts=['counter_context']),
            unit(evidence_id='n', side='NEW', delivered_parts=['section'])])
        self.assertTrue(comparison_readiness(receipt, old.subject, novelty=True)['ready'])
        wrong = coverage_receipt([old, new], [unit(document_version='v2')])
        self.assertFalse(comparison_readiness(wrong, old.subject, novelty=True)['ready'])


if __name__ == '__main__':
    unittest.main()
