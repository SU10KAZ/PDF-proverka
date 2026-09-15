from dataclasses import replace
import unittest

from .allocation import package
from .evidence import EvidenceRequirement
from .sources import source_types


def req(name, side, page, kind='GRAPHIC_SCHEME'):
    return EvidenceRequirement(name, 'function', side, 'STATE', 'doc', side, page,
        'function', kind, {'scope': 'fixture'}, scope_binding='bound-' + name)


def source(r, kind=None):
    return dict(native='Functional state', evidence_types=[kind or r.required_type.value],
        full_page=True, scope_binding=r.scope_binding, raster={'sha256': str(r.page)},
        bbox=[0, 0, 100, 100], provenance={'pdf': {'sha256': 'fixture'}})


class DeliveryTests(unittest.TestCase):
    def test_table_cannot_replace_mandatory_scheme(self):
        result = package([req('new', 'NEW', 1)], lambda r: source(r, 'TABLE_COMPLETE'))
        row = result['raster_allocation']['requirements'][0]
        self.assertEqual(row['required_type'], 'GRAPHIC_SCHEME')
        self.assertEqual(row['selected_evidence_type'], 'TABLE_COMPLETE')
        self.assertEqual(row['type_match'], 'NO')
        self.assertFalse(row['delivered'])
        self.assertFalse(result['coverage_complete'])

    def test_new_scheme_before_supporting_old(self):
        reqs = [req('old', 'OLD', 1, 'TEXT_SECTION'), req('new', 'NEW', 2, 'TEXT_SECTION'),
                req('old_support', 'OLD', 3, 'TABLE_COMPLETE'), req('scheme', 'NEW', 9)]
        result = package(reqs, source, raster_budget=3)
        delivered = {r['requirement_id'] for r in result['raster_allocation']['requirements'] if r['delivered']}
        self.assertEqual(delivered, {'old', 'new', 'scheme'})

    def test_other_region_cannot_satisfy_scheme(self):
        result = package([req('new', 'NEW', 1)], lambda r: source(r, 'GRAPHIC_REGION'))
        self.assertFalse(result['coverage_complete'])

    def test_wrong_functional_region_cannot_satisfy(self):
        def wrong(r):
            return source(r) | {'scope_binding': 'other-function'}
        self.assertFalse(package([req('n', 'NEW', 1)], wrong)['coverage_complete'])

    def test_budget_exhaustion_is_partial(self):
        result = package([req('o', 'OLD', 1), req('n', 'NEW', 2)], source, raster_budget=1)
        self.assertEqual(result['evidence_coverage']['status'], 'PARTIAL_BUDGET_LIMIT')
        self.assertFalse(result['coverage_complete'])

    def test_stale_region_does_not_displace_actual_scheme(self):
        reqs = [req('table', 'NEW', 1, 'GRAPHIC_REGION'), req('scheme', 'NEW', 2, 'GRAPHIC_REGION')]
        result = package(reqs, lambda r: source(r, 'TABLE_COMPLETE' if r.page == 1 else 'GRAPHIC_SCHEME'), raster_budget=1)
        rows = {r['requirement_id']: r for r in result['raster_allocation']['requirements']}
        self.assertTrue(rows['scheme']['delivered'])
        self.assertFalse(rows['table']['delivered'])

    def test_source_form_does_not_follow_requirement_label(self):
        self.assertNotIn('GRAPHIC_SCHEME', source_types('Таблица воздухообменов\nСхема исп.', [0, 0, 595, 842]))
        self.assertIn('GRAPHIC_SCHEME', source_types('Принципиальная схема вентиляции', [0, 0, 2384, 1684]))


if __name__ == '__main__':
    unittest.main()
