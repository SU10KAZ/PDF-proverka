import copy
import json
from pathlib import Path
import tempfile
import unittest
from related_context import packet, expand, sha


class RelatedContextTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        (self.root/'document.md').write_text(
            '### BLOCK #1 [TEXT]: t\n\nТочный TEXT: 1,5; НФ.\n\n'
            '### BLOCK #2 [IMAGE]: a\n\nUnverified graphic prose.\n\n'
            '### BLOCK #3 [IMAGE]: b\n\nRelated view.\n\n'
            '## Page 2\n### BLOCK #4 [IMAGE]: c\n\nRemote view.\n')
        self.meta = [{'block_id': bid, 'page_index': page, 'block_type': kind}
                     for bid, page, kind in [('t', 0, 'text'), ('a', 0, 'image'),
                                             ('b', 0, 'image'), ('s', 0, 'stamp'), ('c', 1, 'image')]]
        self.save_meta()
        for bid in ('a', 'b', 'c'):
            (self.root/f'block_{bid}.png').write_bytes(bid.encode())
        image = str(self.root/'block_c.png')
        self.request = {'messages': [{'role': 'system', 'content': 'Original instruction'}],
                        'options': {'model': 'codex/gpt-6-astra', 'image_paths': [image]},
                        'image_hashes': {image: sha(image)}}

    def save_meta(self):
        (self.root/'blocks.json').write_text(json.dumps({'blocks': self.meta}))

    def test_context_preserves_text_and_existing_remote_attachment(self):
        context = packet(self.root, self.root, 'a')
        self.assertEqual([r['block_id'] for r in context['graphics']], ['a', 'b'])
        self.assertEqual(len(context['TEXT']), 1)
        self.assertIn('Точный TEXT: 1,5; НФ.', context['TEXT'][0]['authoritative_md'])
        self.assertNotIn('Unverified', context['TEXT'][0]['authoritative_md'])
        before = copy.deepcopy(self.request)
        expanded = expand(self.request, context, max_added_chars=10000)
        self.assertEqual(self.request, before)
        self.assertEqual(len(expanded['options']['image_paths']), 3)
        self.assertEqual(expanded['messages'][0], before['messages'][0])
        self.assertFalse(context['engineering_review_complete'])

    def test_missing_content_is_not_silently_omitted(self):
        self.meta.append({'block_id': 'lost', 'page_index': 0, 'block_type': 'image'})
        self.save_meta()
        with self.assertRaisesRegex(ValueError, 'Unexported'):
            packet(self.root, self.root, 'a')
        self.meta.pop(); self.save_meta()
        (self.root/'block_b.png').unlink()
        with self.assertRaisesRegex(ValueError, 'Missing source graphic'):
            packet(self.root, self.root, 'a')

    def test_budget_and_source_mutation_fail_closed(self):
        context = packet(self.root, self.root, 'a')
        with self.assertRaisesRegex(ValueError, 'budget exceeded'):
            expand(self.request, context, max_added_chars=10)
        (self.root/'document.md').write_text('Changed authoritative text')
        with self.assertRaisesRegex(ValueError, 'source changed'):
            expand(self.request, context, max_added_chars=10000)


if __name__ == '__main__':
    unittest.main()
