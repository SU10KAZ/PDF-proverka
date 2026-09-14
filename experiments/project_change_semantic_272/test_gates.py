import unittest
from copy import deepcopy
from .run import check_event


class SourceGates(unittest.TestCase):
    def setUp(self):
        self.packet=dict(source_versions={'old':'vold','new':'vnew'},evidence={s:[dict(
            evidence_id=s+'_1',document_version='v'+s,quote='Heating circuit flow '+v+' cubic metres per hour.')]
            for s,v in [('old','10'),('new','20')]})
        self.event=dict(event_id='x',engineering_subject='Heating circuit',identity_basis='Same explicit function',
            old_state='10',new_state='20',summary_ru='Расход изменен',change_type='CAPACITY_CHANGED',
            confidence='HIGH',importance='HIGH',facts=[dict(property='flow',old_value='10',new_value='20',
                old_witnesses=[dict(evidence_id='old_1',quote='Heating circuit flow 10')],
                new_witnesses=[dict(evidence_id='new_1',quote='Heating circuit flow 20')])])

    def test_supported_witnesses(self):
        self.assertEqual(check_event(self.packet,self.event),[])

    def test_new_before_column_cannot_be_old(self):
        self.event['facts'][0]['old_witnesses'][0]['evidence_id']='new_1'
        self.assertIn('WRONG_VERSION_OR_SOURCE',check_event(self.packet,self.event))

    def test_invented_quote_rejected(self):
        self.event['facts'][0]['new_witnesses'][0]['quote']='Heating circuit flow 1000'
        self.assertIn('QUOTE_NOT_IN_SOURCE',check_event(self.packet,self.event))

    def test_absence_not_promoted(self):
        self.event['change_type']='EQUIPMENT_REMOVED'
        self.assertIn('UNSUPPORTED_CHANGE_TYPE',check_event(self.packet,self.event))

    def test_wrong_document_rejected(self):
        self.packet['evidence']['old'][0]['document_version']='foreign'
        self.assertIn('WRONG_DOCUMENT_VERSION',check_event(self.packet,self.event))

    def test_visual_transcription_requires_route_and_receipt(self):
        self.packet['evidence']['old'][0].update(source_kind='PDF_RASTER_CROP',quote=None)
        errors=check_event(self.packet,self.event)
        self.assertIn('MISSING_VISUAL_SOURCE_RECEIPT',errors)
        self.assertIn('MISSING_VISUAL_WITNESS_ROUTE',errors)


if __name__=='__main__':unittest.main()
