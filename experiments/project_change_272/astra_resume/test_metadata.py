import unittest
from copy import deepcopy
from experiments.project_change_272.astra_resume.metadata import normalize_checkpoint, region_id


class MetadataTest(unittest.TestCase):
    def test_region_uses_frozen_region(self):
        self.assertEqual(region_id({'frozen_region': {'region_id': 'A-R012'}}), 'A-R012')
        self.assertIsNone(region_id({'pair': 'p'}))

    def test_normalization_preserves_answer_payload_and_raw(self):
        raw = {'model': 'opus', 'reasoning': 'old', 'regions': [{
            'accepted_call_id': 'call', 'result': {'projectchanges': [{'id': 'one'}]},
            'model_visible_input': {'model': 'opus', 'reasoning': 'old',
                                    'model_visible_payload_sha256': 'unchanged'},
        }]}
        before = deepcopy(raw)
        receipt = {'call': {'model': 'gpt-6-astra', 'reasoning': {'effort': 'xhigh'},
                            'lineage': {'path': 'raw.json', 'sha256': 'abc'}}}
        fixed = normalize_checkpoint(raw, receipt)
        self.assertEqual(raw, before)
        self.assertEqual(fixed['regions'][0]['result'], raw['regions'][0]['result'])
        self.assertEqual(fixed['regions'][0]['model_visible_input']['model_visible_payload_sha256'], 'unchanged')
        self.assertEqual(fixed['model'], 'gpt-6-astra')
        self.assertEqual(fixed['reasoning'], 'xhigh')
        self.assertEqual(fixed['regions'][0]['metadata_lineage']['sha256'], 'abc')
        receipt['call']['model'] = 'wrong'
        with self.assertRaises(AssertionError):
            normalize_checkpoint(raw, receipt)


if __name__ == '__main__':
    unittest.main()
