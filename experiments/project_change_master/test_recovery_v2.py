import unittest
from .recovery_v2 import canonical_envelope


class EnvelopeTests(unittest.TestCase):
    def test_content_and_line_positions_preserved(self):
        source='## СТРАНИЦА 2\n\n### BLOCK [TEXT]: ABC\n| Модель | X-12 |\nРасход 31,4\n'
        text,changes,ids=canonical_envelope(source)
        self.assertEqual(ids,{(2,'ABC')})
        self.assertEqual(text.splitlines()[3:],source.splitlines()[3:])
        self.assertEqual([c['line'] for c in changes],[1,3])
        self.assertEqual(canonical_envelope(text)[0],text)

    def test_block_without_page_refused(self):
        with self.assertRaises(ValueError):canonical_envelope('### BLOCK [TEXT]: ABC\ntext')


if __name__=='__main__':unittest.main()
