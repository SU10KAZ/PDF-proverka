from pathlib import Path
import tempfile
import unittest

import fitz

from experiments.project_change_272.inventory import sha
from experiments.project_change_semantic_272.run import view
from experiments.project_change_semantic_272.vision import image_messages
from .delivery import semantic_packet
from .packages import package
from .test_evidence import requirement


class DeliveryTests(unittest.TestCase):
    def test_image_delivery_deduplicates_requirement_references_and_carries_coverage(self):
        with tempfile.TemporaryDirectory() as name:
            path=Path(name)/'page.png'
            with fitz.open() as pdf:
                pdf.new_page(width=20,height=20).get_pixmap().save(path)
            raster=dict(path=str(path),sha256=sha(path))
            def loader(r):
                return dict(native='Whole source table',bbox=[0,0,20,20],full_page=True,
                    scope_binding=r.scope_binding,raster=raster,provenance={'pdf':{'sha256':'fixture'}})
            pair=dict(index=0,pair_key='synthetic',partition='DEV',
                      old=dict(document_version='v1',document_code='D'),new=dict(document_version='v1',document_code='D'))
            reqs=[requirement(),requirement(requirement_id='counter',evidence_type='COUNTER_EVIDENCE')]
            body=package(reqs,loader)
            packet=semantic_packet(body,pair)
            images,receipts=image_messages(packet)
            self.assertEqual(len(receipts),1)
            self.assertEqual(len(images),2)
            rendered=view(packet)
            self.assertTrue(rendered['coverage_complete'])
            ids={e['evidence_id'] for e in rendered['evidence']['old']}
            for row in rendered['evidence_coverage']['requirements']:
                self.assertTrue(set(row['evidence_ids']) <= ids)

    def test_legacy_page_coverage_cannot_claim_requirement_completeness(self):
        packet=dict(packet_id='legacy',proposal_kind='page',proposal_query='subject',
                    coverage_complete=True,pair_key='synthetic',evidence={'old':[],'new':[]})
        self.assertFalse(view(packet)['coverage_complete'])
        self.assertEqual(view(packet)['evidence_coverage']['missing_reason'],'SUBJECT_REQUIREMENTS_NOT_DECLARED')


if __name__ == '__main__':
    unittest.main()
