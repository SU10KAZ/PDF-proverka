from copy import deepcopy
import unittest

from .graphic_ports import parse_port, parent_function, compare
from .inventory import ROOT, read


class GraphicPortContractTest(unittest.TestCase):
    def test_values_do_not_establish_port_identity(self):
        a = parse_port('Подающий трубопровод в систему отопления, Т17\nG=24,1 м³/ч\nТрасч=80°C')
        b = parse_port('Подающий трубопровод в систему отопления, Т17\nG=56,3 м³/ч\nТрасч=95°C')
        self.assertEqual(tuple(a[k] for k in ['mark', 'role', 'function']), tuple(b[k] for k in ['mark', 'role', 'function']))
        self.assertNotEqual(a['values'], b['values'])

    def test_parent_normalization_retains_scope_conflicts(self):
        self.assertEqual(parent_function('в систему отопления, '), parent_function('из системы отопления, '))
        self.assertNotEqual(parent_function('в систему отопления корпуса 1'), parent_function('из системы отопления корпуса 2'))

    def test_duplicate_port_cannot_be_silently_matched(self):
        source = read(ROOT / 'cycles/07b_graphic_with_text_legend/pairs/9.json')['sources']
        original = compare('test-scope', source['old'], source['new'])
        self.assertEqual(len(original['project_changes']), 2)
        old = deepcopy(source['old'])
        old['ports'] += deepcopy(old['ports'])
        result = compare('test-scope', old, source['new'])
        self.assertFalse(result['project_changes'])

    def test_missing_pair_declaration_prevents_circuit_claim(self):
        source = read(ROOT / 'cycles/07b_graphic_with_text_legend/pairs/9.json')['sources']
        source['old']['pair_declarations'] = []
        self.assertFalse(compare('test-scope', source['old'], source['new'])['project_changes'])


if __name__ == '__main__':
    unittest.main()
