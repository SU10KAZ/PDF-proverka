import asyncio
import fcntl
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import patch, AsyncMock
from inventory import PROMPT
from inventory_partition import prepare
from run_inventory_partition import check_plan, run


class RecoveryTests(unittest.TestCase):
    def test_success_keeps_parent_state_and_child_provenance(self):
        with tempfile.TemporaryDirectory() as directory:
            work = Path(directory)
            root = work / 'inventory_v2' / 'ITP'
            root.mkdir(parents=True)
            packet = [dict(id='t', kind='TEXT', offset=0, text='насос 25', navigation_only=False)]
            (root / 'plan.json').write_text(json.dumps([packet]))
            state = {'completed_packets': 0, 'source_hashes': {},
                     'prompt_sha256': hashlib.sha256(PROMPT.encode()).hexdigest()}
            (root / 'status.json').write_text(json.dumps(state))
            original = (root / 'status.json').read_bytes()
            prepared = root / 'recovery.json'
            prepare(root, 0, prepared)
            result = SimpleNamespace(json_data={'reviewed_source_ids': ['t'], 'objects': [],
                                               'limitations': []}, text='', error_message=None,
                                     is_error=False)
            call = AsyncMock(return_value=result)
            with patch('run_inventory_partition.configure'), patch('run_inventory_partition.Corpus'), \
                    patch('backend.app.services.llm.codex_runner.run_codex_json_messages', call):
                asyncio.run(run(work, 'ITP', prepared, work / 'output'))
            self.assertEqual(call.await_count, 1)
            self.assertEqual(call.call_args.kwargs['model'], 'gpt-6-astra')
            self.assertEqual((root / 'status.json').read_bytes(), original)
            saved = json.loads((work / 'output/status.json').read_text())
            self.assertEqual(saved['status'], 'completed_unverified')
            self.assertEqual(set(saved['accepted']), {'0'})

    def test_frozen_plan_and_accepted_parent_cannot_change(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            packet = [dict(id='t', kind='TEXT', offset=0, text='насос 25', navigation_only=False)]
            (root / 'plan.json').write_text(json.dumps([packet]))
            state = {'completed_packets': 0, 'source_hashes': {},
                     'prompt_sha256': hashlib.sha256(PROMPT.encode()).hexdigest()}
            (root / 'status.json').write_text(json.dumps(state))
            prepared = root / 'recovery.json'
            prepare(root, 0, prepared)
            self.assertEqual(check_plan(root, prepared)['parent_packet_index'], 0)
            state['completed_packets'] = 1
            (root / 'status.json').write_text(json.dumps(state))
            with self.assertRaises(ValueError):
                check_plan(root, prepared)

    def test_live_original_worker_prevents_requests_and_output_creation(self):
        with tempfile.TemporaryDirectory() as directory:
            work = Path(directory)
            inventory = work / 'inventory_v2' / 'ITP'
            inventory.mkdir(parents=True)
            output = work / 'output'
            with (inventory / '.lock').open('a') as lock:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                with self.assertRaises(BlockingIOError):
                    asyncio.run(run(work, 'ITP', work / 'does_not_exist.json', output))
            self.assertFalse(output.exists())


if __name__ == '__main__':
    unittest.main()
