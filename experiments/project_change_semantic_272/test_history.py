import unittest
from .history import history_pages,same_history_grid


class HistoryBoundaries(unittest.TestCase):
    def test_unheaded_continuation_not_current_state(self):
        text='''## Page 1
| Ранее разработанные решения | Суть изменения | Лист |
| Three old pumps used in this room | Two new pumps used in this room | ТЧ лист 4 |
## Page 2
| Old rated capacity for this system 30 | New rated capacity for this system 50 | ТЧ лист 5 |
## Page 3
Current technical narrative without a revision table.
## Page 4
| Equipment designation | Flow capacity | Unit |
| The water supply pump equipment | 50 | m3/h |
'''
        self.assertEqual(set(history_pages(text)),{1,2})

    def test_orphan_continuation_after_hidden_header(self):
        text='''## Page 7

## Page 8
| Old pump supply cable size and length | New pump cable size and length | ГЧ лист 3 |
| Old system interface and control device | New system interface and control device | ГЧ лист 4 |
'''
        self.assertEqual(set(history_pages(text,{7})),{8})

    def test_empty_reference_column_still_preserves_grid(self):
        self.assertTrue(same_history_grid([.155,.457,.692],[.155,.457,.692]))
        self.assertFalse(same_history_grid([.155,.457,.692],[]))
        self.assertFalse(same_history_grid([.155,.457,.692],[.21,.53,.84]))


if __name__=='__main__':unittest.main()
