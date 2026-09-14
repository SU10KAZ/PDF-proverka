import unittest
from .packets import scope_context


def source(page,text):
    return dict(page=page,quote=text,source_kind='PDF_NATIVE_TEXT')


class PhysicalContinuation(unittest.TestCase):
    def test_owner_on_previous_page_preserved_after_anchor(self):
        rows=scope_context([source(9,'The explicit owner of the following load'),source(10,'continued load state'),source(40,'unrelated ranked page')],[10,40],'new')
        self.assertEqual(rows[0]['page'],10)
        self.assertIn(9,{r['page'] for r in rows})

    def test_no_jump_across_missing_embargoed_page(self):
        rows=scope_context([source(8,'content before an excluded page'),source(10,'admitted anchor')],[10],'new')
        self.assertEqual({r['page'] for r in rows},{10})

    def test_second_anchor_not_duplicated_as_continuation(self):
        rows=scope_context([source(10,'anchor'),source(11,'next page')],[10,11],'new')
        self.assertEqual(len(rows),2)
