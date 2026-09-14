from unittest.mock import patch
import unittest
from .packets import packet
from .sheet_scopes import purpose_key,code_key


class SheetPurposeBinding(unittest.TestCase):
    def test_printed_code_glyphs_do_not_break_title_binding(self):
        self.assertEqual(code_key('АА_БЭ-03-ДС3-АР1'),code_key('АА/БЭ-03-ДC3 - АР1'))

    def test_scale_and_generic_plan_word_are_not_subject_keys(self):
        self.assertEqual(purpose_key('План дорожных покрытий. М 1:500'),purpose_key('Схема дорожных покрытий М 1:500'))
        self.assertNotEqual(purpose_key('Ситуационный план'),purpose_key('Схема организации рельефа'))

    def test_changed_elevation_is_not_floor_identity(self):
        self.assertEqual(purpose_key('План второго этажа на отм. +3.300'),purpose_key('План второго этажа на отм. +4.100'))
        self.assertNotEqual(purpose_key('План 2 этажа'),purpose_key('План 3 этажа'))

    def test_shared_table_caption_cannot_override_drawing_purpose(self):
        pair=dict(index=16,pair_key='same-cipher',partition='DEV',embargo_pages={'old':[],'new':[]})
        pools={}
        for side in ['old','new']:
            pair[side]=dict(document_version=side,artifacts={'pdf':dict(path='/fixture/'+side+'.pdf',sha256=side)})
            def row(page,quote):return dict(page=page,quote=quote,bbox=[1,2,30,40],document_version=side,
                source_kind='PDF_NATIVE_DRAWING_LABEL',source_receipt=pair[side]['artifacts']['pdf'])
            pools[side]=[row(1,'site amenity table children recreation sports complete table'),row(2,'site amenity scope')] if side=='old' else [row(3,'site amenity table children recreation sports')]
        frames={'old':{1:dict(purpose_key='situational',status='EXPLICIT_STAMP_TITLE_CANDIDATE',title='Situational plan'),
                       2:dict(purpose_key='relief',status='EXPLICIT_STAMP_TITLE_CANDIDATE',title='Relief plan')},
                'new':{3:dict(purpose_key='relief',status='EXPLICIT_STAMP_TITLE_CANDIDATE',title='Relief plan')}}
        with patch('experiments.project_change_semantic_272.sheet_scopes.document_scopes',side_effect=lambda doc,emb:frames[doc['document_version']]):
            p=packet(pair,'site amenity table children recreation sports',pools,'CHANGED_NATIVE_SCOPE',dict(page=3))
        self.assertEqual({e['page'] for e in p['evidence']['old']},{2})
        self.assertEqual(p['sheet_scopes']['old'][0]['purpose_key'],'relief')
