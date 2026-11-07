import copy
import unittest
from inventory_partition import partition, validate_partition


class PartitionTests(unittest.TestCase):
    def test_unicode_table_and_existing_offset_preserved(self):
        source = [dict(id='t', kind='TEXT', offset=12000, text='| насос | 25 |\n' * 20,
                       navigation_only=False),
                  dict(id='g', kind='GRAPHIC', offset=0, text='Схема' * 50,
                       navigation_only=True),
                  dict(id='empty', kind='TEXT', offset=0, text='', navigation_only=False)]
        before = copy.deepcopy(source)
        children = partition(source, 39)
        self.assertEqual(source, before)
        self.assertTrue(all(len(c['packet'][0]['text']) <= 39 for c in children
                            if not c['packet'][0]['navigation_only']))
        self.assertEqual(sum(c['parent_source_index'] == 1 for c in children), 1)
        self.assertEqual(children[1]['packet'][0]['offset'], 12000 + len(children[0]['packet'][0]['text']))
        for mutation in ('delete', 'reorder', 'offset', 'text', 'identity'):
            bad = copy.deepcopy(children)
            if mutation == 'delete':
                del bad[0]
            elif mutation == 'reorder':
                bad[0], bad[1] = bad[1], bad[0]
            elif mutation == 'offset':
                bad[0]['packet'][0]['offset'] += 1
            elif mutation == 'text':
                bad[0]['packet'][0]['text'] = 'invented'
            else:
                bad[0]['packet'][0]['id'] = 'other'
            with self.subTest(mutation=mutation), self.assertRaises(ValueError):
                validate_partition(source, bad)

    def test_oversized_single_line_is_not_dropped(self):
        source = [dict(id='t', kind='TEXT', offset=0, text='Ж' * 101, navigation_only=False)]
        parts = partition(source, 40)
        self.assertEqual([len(c['packet'][0]['text']) for c in parts], [40, 40, 21])
        with self.assertRaises(ValueError):
            partition(source, 0)


if __name__ == '__main__':
    unittest.main()
