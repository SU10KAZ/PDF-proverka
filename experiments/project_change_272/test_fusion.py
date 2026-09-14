from copy import deepcopy
import unittest

from .candidate import fuse
from .inventory import ROOT, read


class FusionContractTest(unittest.TestCase):
    def source(self):
        r = read(ROOT/'runs/dev_union_v1/pairs/9.json')
        return r['graphic']['project_changes'] + r['compound']['project_changes']

    def test_same_temperature_pair_is_owned_by_graphic_circuit(self):
        events, receipts = fuse(self.source())
        self.assertEqual(len(events), 2)
        self.assertEqual(len(receipts), 1)

    def test_other_temperature_state_cannot_be_absorbed(self):
        source = deepcopy(self.source())
        compound = source[-1]
        compound['supporting_fact_changes'][0]['old']['value'] = '85/60'
        events, receipts = fuse(source)
        self.assertEqual(len(events), 3)
        self.assertFalse(receipts)


if __name__ == '__main__':
    unittest.main()
