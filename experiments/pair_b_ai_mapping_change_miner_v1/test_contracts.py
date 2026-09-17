"""Checks that incomplete/embargoed mappings cannot acquire a freeze."""
import copy
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from . import run


class MappingContract(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.out = Path(self.temp.name)
        run.write(self.out/'DOCUMENT_STRUCTURE.json',[
            {'side':'old','page':1}, {'side':'old','page':2},
            {'side':'new','page':1}, {'side':'new','page':3}])
        self.valid = dict(groups=[dict(map_group_id='G001',old_pages=[1,2],new_pages=[1])],
                          unmatched_old=[],unmatched_new=[3])

    def check(self, value):
        with patch.object(run,'OUT',self.out):
            return run.mapping_checks(value)

    def test_many_to_one_and_unmatched(self):
        self.assertEqual(self.check(self.valid)['many_to_one'],['G001'])

    def test_embargo_page_rejected(self):
        data=copy.deepcopy(self.valid)
        data['groups'][0]['new_pages'].append(2)
        with self.assertRaises(AssertionError):
            self.check(data)

    def test_dropped_page_rejected(self):
        data=copy.deepcopy(self.valid)
        data['unmatched_new']=[]
        with self.assertRaises(AssertionError):
            self.check(data)

    def test_matched_unmatched_overlap_rejected(self):
        data=copy.deepcopy(self.valid)
        data['unmatched_new'].append(1)
        with self.assertRaises(AssertionError):
            self.check(data)

    def test_freeze_detects_mutation(self):
        run.write(self.out/'data.json',{'v':1})
        run.write(self.out/'FREEZE.json',{'hashes':{'data.json':run.sha(self.out/'data.json')}})
        with patch.object(run,'OUT',self.out):
            run.verify_freeze('FREEZE.json')
            (self.out/'data.json').write_text('{"v":2}')
            with self.assertRaises(AssertionError):
                run.verify_freeze('FREEZE.json')


if __name__=='__main__':
    unittest.main()
