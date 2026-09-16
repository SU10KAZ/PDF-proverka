"""Meaningful no-network checks before the single-use Pair B run."""
import ast
import copy
import json
from pathlib import Path
import subprocess
import unittest
from .snapshot import OUT, LIVE_A, BASE, read
from .capacity import AUDIT, measured_request
from experiments.project_change_semantic_codex_272.serialization import prepare_request_bytes, MAX_TEXT_CHARACTERS

class CapacityTests(unittest.TestCase):
    def test_guard_boundary(self):
        self.assertEqual(MAX_TEXT_CHARACTERS,83000)
        prepare_request_bytes('x'*(83000-2),{})
        with self.assertRaisesRegex(ValueError,'83000'):prepare_request_bytes('x'*(83000-1),{})

    def test_serializer_semantics_unchanged_after_removing_guard(self):
        path='experiments/project_change_semantic_codex_272/serialization.py'
        old=ast.parse(subprocess.check_output(['git','show','52d106dc:'+path],text=True))
        new=ast.parse(Path(path).read_text())
        def clean(tree):
            tree.body=[n for n in tree.body if not(isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id=='MAX_TEXT_CHARACTERS' for t in n.targets))]
            for n in tree.body:
                if isinstance(n,ast.FunctionDef) and n.name=='prepare_request_bytes':n.body=n.body[1:]
            return ast.dump(tree,include_attributes=False)
        self.assertEqual(clean(old),clean(new))

    def test_all_requests_exact_bytes_and_content_unchanged(self):
        audit=read(AUDIT/'PAIR_B_INPUT_CAPACITY_AUDIT.json')
        expected={r['package_id']:r for r in audit['rows']}
        totals={}
        for label,source in [('A',LIVE_A),('B',OUT/'pair_b_live')]:
            count=0
            for item in read(source/'CALL_PLAN.json')['packages']:
                if item['action']!='MODEL_CALL':continue
                key=item['key'];p=read(BASE/item['package']);data=read(source/f'inputs/{key}/MODEL_INPUT.json');packet=copy.deepcopy(p['evidence_packet'])
                measured,_,_=measured_request((LIVE_A/'PROMPT.txt').read_text(),data,packet)
                for es in packet['evidence'].values():
                    for e in es:
                        if e.get('raster'):e['raster']['path']=str(BASE/e['raster']['path'])
                payload,_=prepare_request_bytes((LIVE_A/'PROMPT.txt').read_text(),data,packet)
                self.assertEqual(payload,measured)
                if label=='A':self.assertEqual(payload,(source/f'inputs/{key}/EXACT_PROMPT.txt').read_bytes())
                else:
                    self.assertGreater(expected[key]['safety_headroom_tokens'],15000)
                    self.assertEqual(__import__('hashlib').sha256(payload).hexdigest(),expected[key]['canonical_request_sha256'])
                count+=1
            totals[label]=count
        self.assertEqual(totals,{'A':52,'B':18})

if __name__=='__main__':unittest.main()
