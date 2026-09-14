import unittest
from copy import deepcopy
from unittest.mock import patch

from .run import check_packet_scope
from .access import authorize


class PacketPartition(unittest.TestCase):
    def setUp(self):
        self.allowed={5:dict(pair_key='pair5',embargo_pages={'old':[2],'new':[]},
            old=dict(document_version='oldv',artifacts={'pdf':{'path':'/tmp/old.pdf','sha256':'oldhash'}}),
            new=dict(document_version='newv',artifacts={'pdf':{'path':'/tmp/new.pdf','sha256':'newhash'}}))}
        self.packet=dict(pair_index=5,pair_key='pair5',partition='DEV',source_versions={'old':'oldv','new':'newv'},
            evidence={s:[dict(side=s,document_version=s+'v',page=1,
                        source_receipt={'path':'/tmp/'+s+'.pdf','sha256':s+'hash'})] for s in ['old','new']})

    def test_pinned_same_partition(self):
        check_packet_scope(self.packet,self.allowed,'DEV')

    def test_wrong_partition(self):
        self.packet['partition']='FINAL_HOLDOUT'
        with self.assertRaises(PermissionError):check_packet_scope(self.packet,self.allowed,'DEV')

    def test_embargo_not_bypassed_by_raster(self):
        self.packet['evidence']['old'][0].update(page=2,source_kind='PDF_RASTER_CROP')
        with self.assertRaises(PermissionError):check_packet_scope(self.packet,self.allowed,'DEV')

    def test_foreign_source(self):
        self.packet['evidence']['new'][0]['source_receipt']['path']='/tmp/foreign.pdf'
        with self.assertRaises(PermissionError):check_packet_scope(self.packet,self.allowed,'DEV')

    def test_another_algorithms_freeze_is_insufficient(self):
        with patch('experiments.project_change_semantic_272.access.admitted_pairs',return_value=[]),patch('experiments.project_change_semantic_272.access.verify_candidate',return_value={'code':{}}):
            with self.assertRaises(PermissionError):authorize('VALIDATION','other_candidate.json')


if __name__=='__main__':unittest.main()
