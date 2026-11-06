import importlib.util
from pathlib import Path
import json
import tempfile
import unittest

spec=importlib.util.spec_from_file_location('investigate',Path(__file__).with_name('investigate.py'))
m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)

class CorpusTest(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.root=Path(self.tmp.name)
        (self.root/'document.md').write_text('## Page 1\n### BLOCK #1 [TEXT]: text1\n\n> **Crop:** secret-url\n\nрасход 12.13\n## Page 2\n### BLOCK #2 [IMAGE]: image1\n\nрасход 13.34\n')
        (self.root/'blocks.json').write_text(json.dumps({'blocks':[{'block_id':'text1','block_type':'text','page_label':1},{'block_id':'image1','block_type':'image','page_label':2}]}))
        self.c=m.Corpus(self.root,self.root/'images')
    def test_no_metadata_links_or_page_bleed(self):
        text=self.c.execute({'tool':'read','id':'text1'})[0]['text']
        self.assertEqual(text,'расход 12.13');self.assertNotIn('secret',text)
    def test_text_raster_forbidden(self):
        with self.assertRaisesRegex(ValueError,'TEXT'):self.c.execute({'tool':'image','id':'text1'})
    def test_graphic_description_not_evidence(self):
        self.c.execute({'tool':'read','id':'image1'})
        with self.assertRaisesRegex(ValueError,'unread'):self.c.validate_findings([{'id':'f1','claim':'x','evidence':'13.34','status':'candidate','source_ids':['image1']}])
    def test_path_not_accessible(self):
        with self.assertRaises(KeyError):self.c.execute({'tool':'read','id':'../../history.json'})
    def test_decimal_calculation_requires_read_source(self):
        action={'tool':'calculate','operator':'sum','values':['0.1','0.2'],'source_ids':['text1'],'unit':'m3/h','meaning':'sum'}
        with self.assertRaises(ValueError):self.c.execute(action)
        self.c.execute({'tool':'read','id':'text1'})
        self.assertEqual(self.c.execute(action)[0]['value'],'0.3')
    def test_unknown_evidence_not_accepted(self):
        with self.assertRaises(ValueError):self.c.validate_findings([{'id':'f','claim':'x','evidence':'y','status':'candidate','source_ids':['unknown']}])
    def test_search_graphic_hint_not_grounding(self):
        result,_=self.c.execute({'tool':'search','query':'расход'})
        self.assertEqual(result['total'],2);self.assertEqual(self.c.seen,{'text1'})
    def test_unsupported_tool(self):
        with self.assertRaises(ValueError):self.c.execute({'tool':'shell','command':'cat history'})

if __name__=='__main__':unittest.main()
