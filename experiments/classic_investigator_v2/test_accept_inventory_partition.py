import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from source_contract import write, read, sha
from inventory import PROMPT, recover
from inventory_partition import prepare
from accept_inventory_partition import accept, child_results


class AcceptTests(unittest.TestCase):
    def fixture(self, root):
        packet = [dict(id='t', kind='TEXT', offset=10, text='abcdef', navigation_only=False)]
        write(root / 'plan.json', [packet, packet])
        write(root / 'status.json', {'status': 'running', 'completed_packets': 0,
              'source_hashes': {}, 'prompt_sha256': hashlib.sha256(PROMPT.encode()).hexdigest()})
        prepared, output = root / 'partition.json', root / 'children'
        plan = prepare(root, 0, prepared, 3)
        state = {'status': 'completed_unverified', 'prepared_sha256': sha(prepared), 'accepted': {}}
        for i, child in enumerate(plan['children']):
            prefix = str(i)
            state['accepted'][str(i)] = prefix
            write(output / f'{prefix}_request.json', {'messages': [
                {'role': 'system', 'content': PROMPT}, {'role': 'user', 'content': json.dumps(
                    {'packet': child['packet'], 'image_receipts': []})}], 'images': [], 'image_hashes': {}})
            write(output / f'{prefix}_response.json', {'error': None, 'json': {
                'reviewed_source_ids': ['t'], 'limitations': [], 'objects': [
                    {'label': 'same', 'kind': 'equipment', 'facts': [{'block_id': 't',
                      'quote': child['packet'][0]['text']}]}]}})
        write(output / 'status.json', state)
        return prepared, output, packet

    def test_acceptance_and_resume_preserve_children_not_synthetic_parent(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            prepared, output, packet = self.fixture(root)
            state = accept(root, prepared, output)
            self.assertEqual(state['completed_packets'], 1)
            self.assertNotIn('0', state.get('accepted_responses', {}))
            _, results = recover(root, [packet, packet], {})
            self.assertEqual([i for i, _ in results], ['P000:S000', 'P000:S001'])
            self.assertEqual(len(read(root / 'registry.json')['observations']), 2)
            response = output / '0_response.json'
            changed = read(response)
            changed['json']['objects'] = []
            write(response, changed)
            with self.assertRaises(ValueError):
                recover(root, [packet, packet], {})

    def test_partial_recovery_cannot_advance_parent(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            prepared, output, packet = self.fixture(root)
            state = read(output / 'status.json')
            del state['accepted']['1']
            write(output / 'status.json', state)
            before = (root / 'status.json').read_bytes()
            with self.assertRaises(ValueError):
                accept(root, prepared, output)
            self.assertEqual((root / 'status.json').read_bytes(), before)


if __name__ == '__main__':
    unittest.main()
