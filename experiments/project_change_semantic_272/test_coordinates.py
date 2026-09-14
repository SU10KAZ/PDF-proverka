import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import fitz
from .packets import sources
from experiments.project_change_272.inventory import sha


class RotatedTableSource(unittest.TestCase):
    def test_native_text_binds_to_displayed_table_on_rotated_page(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);path=root/'rotated.pdf'
            with fitz.open() as pdf:
                page=pdf.new_page(width=400,height=300)
                for x in [80,180,280]:page.draw_line((x,40),(x,160))
                for y in [40,100,160]:page.draw_line((80,y),(280,y))
                page.insert_text((90,70),'Heating branch supply calculation',fontsize=5)
                page.insert_text((190,130),'Heating branch return calculation',fontsize=5)
                page.set_rotation(90);pdf.save(path)
            (root/'blocks.json').write_text(json.dumps(dict(blocks=[dict(block_type='table',page_index=0,
                coords_norm=[140/300,80/400,260/300,280/400])])))
            (root/'document.md').write_text('## Page 1\nRotated engineering table fixture.\n')
            doc=dict(document_version='synthetic-coordinate-regression',artifacts={k:dict(path=str(p),sha256=sha(p)) for k,p in
                [('pdf',path),('blocks',root/'blocks.json'),('work_md',root/'document.md')]})
            with patch('experiments.project_change_semantic_272.packets.BASE',root/'cache'):
                rows=sources(doc,[])
            self.assertTrue(rows)
            self.assertTrue(all(r['source_kind']=='PDF_NATIVE_TABLE' for r in rows))
            self.assertTrue(all(r['pdf_rotation']==90 for r in rows))
            self.assertTrue(all(r['bbox']!=r['bbox_native'] for r in rows))
            with fitz.open(path) as pdf:
                table=fitz.Rect(pdf[0].find_tables(strategy='lines_strict').tables[0].bbox)
                for row in rows:
                    box=fitz.Rect(row['bbox'])
                    self.assertAlmostEqual((box & table).get_area(),box.get_area(),places=3)
