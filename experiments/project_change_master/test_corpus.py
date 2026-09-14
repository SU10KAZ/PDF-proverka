from copy import deepcopy
import unittest
from .corpus import convert_result


class CorpusTests(unittest.TestCase):
    def source(self):
        return {'pages':[{'page_number':1,'width':100,'height':200,'blocks':[
            {'id':'b1','page_index':1,'coords_norm':[0,.5,.9,1],'block_type':'image','category_code':'stamp'}]}]}

    def test_enclosing_page_and_explicit_stamp(self):
        d=convert_result(self.source(),1)
        self.assertEqual(d['blocks'][0]['page_index'],0)
        self.assertEqual(d['blocks'][0]['block_type'],'stamp')

    def test_ambiguous_or_invalid_source_refused(self):
        for change in ['missing_page','outside_page','bad_geometry','duplicate_id']:
            d=self.source();p=d['pages'][0]
            if change=='missing_page':del p['page_number']
            elif change=='outside_page':p['page_number']=2
            elif change=='bad_geometry':p['blocks'][0]['coords_norm']=[0,0,2,1]
            else:p['blocks'].append(deepcopy(p['blocks'][0]))
            with self.assertRaises(ValueError):convert_result(d,1)


if __name__=='__main__':unittest.main()
