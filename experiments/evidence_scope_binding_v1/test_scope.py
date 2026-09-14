"""Ownership safety and frozen-interface regression tests; no provider calls."""
from copy import deepcopy
import unittest

from .core import bind, gate, local_package, node, SOURCE_TYPES
from .source import group_label, floor_label, NUMBERED_EQUIPMENT


def evidence(version='v1'):
    return dict(document_version=version,page=1,quote='source',source_receipts={})


def fragment(**overrides):
    return dict(fragment_id='f',document_version='v1',source_type='TABLE',route='TABLE',text='row',
                interval=[12,12],pages=[1],container_id='t',source_verified=True,
                source_evidence=[evidence()],**overrides)


def scope(dimension='apartment_or_unit',value='Unit A',start=10,end=19,**kwargs):
    return node(version='v1',dimension=dimension,value=value,start=start,end=end,pages=[1],
                evidence=[evidence()],container_id='t',**kwargs)


class ScopeTests(unittest.TestCase):
    def test_source_gate_matrix(self):
        for source in SOURCE_TYPES:
            for route in ('TEXT','TABLE'):
                self.assertEqual(gate(source,route)=='PROVEN',
                                 source=={'TEXT':'NARRATIVE_TEXT','TABLE':'TABLE'}[route])

    def test_wrong_apartment_next_heading_never_owns_preceding_row(self):
        nodes=[scope(start=0,end=12),scope(value='Unit B',start=13,end=30)]
        self.assertEqual(bind(fragment(),nodes)['dimensions']['apartment_or_unit'],['Unit A'])
        self.assertEqual(bind(fragment(),nodes,'nearest_heading')['dimensions']['apartment_or_unit'],['Unit B'])

    def test_repeated_equipment_model_is_not_scope(self):
        f=fragment();f['text']='Model ZZ200 same in all apartments'
        self.assertEqual(bind(f,[])['status'],'REVIEW')

    def test_no_cross_version_parent(self):
        n=scope();n['document_version']='v2'
        self.assertEqual(bind(fragment(),[n])['status'],'REVIEW')

    def test_no_cross_table_inheritance(self):
        n=scope();n['container_id']='other'
        self.assertEqual(bind(fragment(),[n])['status'],'REVIEW')

    def test_multiple_dimensions_and_unknowns(self):
        nodes=[scope(),scope('floor','5'),scope('room','501'),scope('system','П12')]
        result=bind(fragment(),nodes)
        self.assertEqual(result['status'],'PROVEN')
        self.assertIsNone(result['dimensions']['building_section'])
        self.assertEqual(result['dimensions']['room'],['501'])

    def test_conflicting_parents_review(self):
        self.assertEqual(bind(fragment(),[scope(),scope(value='Unit B')])['status'],'REVIEW')

    def test_explicit_multiple_ownership(self):
        f=fragment(multiple_dimensions=['apartment_or_unit'])
        self.assertEqual(bind(f,[scope(),scope(value='Unit B')])['status'],'PROVEN')

    def test_missing_ancestor_review(self):
        self.assertEqual(bind(fragment(),[scope(parent_ids=['absent'])])['status'],'REVIEW')

    def test_cyclic_ancestry_review(self):
        n=scope();n['parent_scope_ids']=[n['scope_id']]
        self.assertEqual(bind(fragment(),[n])['status'],'REVIEW')

    def test_unverified_source_review(self):
        f=fragment();f['source_verified']=False
        self.assertEqual(bind(f,[scope()])['status'],'REVIEW')

    def test_fragment_crosses_sibling_boundary(self):
        f=fragment();f['interval']=[12,20]
        self.assertEqual(bind(f,[scope(),scope(value='B',start=20,end=25)])['status'],'REVIEW')

    def test_long_distance_list_scope(self):
        f=fragment();f.update(source_type='NARRATIVE_TEXT',route='TEXT',interval=[190,190],pages=[4])
        n=scope('engineering_function','Calculation list',start=5,end=200);n['pages']=[1,2,3,4]
        self.assertEqual(bind(f,[n])['status'],'PROVEN')
        self.assertEqual(bind(f,[n],'structural_intervals')['status'],'REVIEW')

    def test_uncertain_ocr_parent_not_promoted(self):
        self.assertEqual(bind(fragment(),[scope(status='REVIEW')])['status'],'REVIEW')

    def test_model_does_not_enter_numbered_group(self):
        m=NUMBERED_EQUIPMENT.match('Наружный блок №47.9 MODEL500-A')
        self.assertEqual(m[2].strip(),'47.9')
        self.assertIsNone(NUMBERED_EQUIPMENT.match('Внутренний блок MODEL500-A'))

    def test_general_group_and_floor_boundaries(self):
        self.assertEqual(group_label(['Квартира 102','','']),('apartment_or_unit','Квартира 102'))
        self.assertEqual(group_label(['ABC42.7','Итого на квартиру:','500']),('apartment_or_unit','ABC42.7'))
        self.assertIsNone(group_label(['насос MODEL1','1','500']))
        self.assertEqual(floor_label(['12-15 этаж','','']),'12-15 этаж')

    def test_parent_evidence_required(self):
        with self.assertRaises(ValueError):
            node(version='v1',dimension='room',value='1',start=0,end=1,pages=[1],evidence=[])

    def test_local_package_budget(self):
        f=fragment();f['text']='x'*30000
        with self.assertRaises(ValueError):
            local_package(f,[])

    def test_schema_and_read_only_inputs(self):
        import jsonschema
        from .schema import SCHEMA
        f=fragment();n=[scope()];before=deepcopy([f,n]);result=bind(f,n)
        jsonschema.Draft202012Validator(SCHEMA).validate(result)
        self.assertEqual([f,n],before)


if __name__ == '__main__':
    unittest.main()
