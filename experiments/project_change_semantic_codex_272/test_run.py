"""Offline queue and resumption tests with synthetic non-corpus packets."""
import asyncio
from contextlib import ExitStack
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from experiments.project_change_272.inventory import immutable, read, sha
from experiments.project_change_semantic_codex_272 import run as runner
from experiments.project_change_semantic_codex_272.provider import AuthorizationBlocked


class Queue(unittest.TestCase):
    def test_concurrency_stop_and_resume_preserve_completed_work(self):
        with tempfile.TemporaryDirectory() as tmp, ExitStack() as stack:
            root = Path(tmp)
            (root / 'SPLIT.json').write_text('{}')
            rows = [dict(packet_id=str(i), route=route, path=str(root / (str(i)+'.json')), sha256='synthetic')
                    for i, route in enumerate(['dev_sheet_scopes_v1', 'dev_typed_semantic_v2'])]
            audit = root / 'audit.json'
            immutable(audit, dict(status='PASS', split_sha256=sha(root / 'SPLIT.json'), packets=rows))
            for name, value in [('ROOT', root), ('BASE', root), ('AUDIT', audit)]:
                stack.enter_context(patch.object(runner, name, value))
            stack.enter_context(patch.object(runner, 'prepared_pairs', return_value=[]))
            stack.enter_context(patch.object(runner, 'runtime_identity', return_value={'provider': 'offline'}))
            stack.enter_context(patch.object(runner, 'code_identity', return_value={}))
            stack.enter_context(patch.object(runner, 'verify_packet', side_effect=lambda path, *args: {'packet_id': Path(path).stem}))
            active = 0
            maximum = 0
            completed = []
            first_run = True

            async def infer(packet, call, prompt):
                nonlocal active, maximum
                active += 1
                maximum = max(maximum, active)
                await asyncio.sleep(.01)
                active -= 1
                if first_run and packet['packet_id'] == '0':
                    raise AuthorizationBlocked('synthetic failure')
                completed.append(packet['packet_id'])
                return dict(packet_id=packet['packet_id'], pair_index=2, events=[], unknowns=[])

            stack.enter_context(patch.object(runner, 'infer_packet', side_effect=infer))
            report = asyncio.run(runner.execute('codex_fixture', smoke=True))
            self.assertEqual(maximum, 2)
            self.assertEqual(report['completed_packets'], 1)
            first_run = False
            report = asyncio.run(runner.execute('codex_fixture', smoke=True))
            self.assertEqual(report['status'], 'COMPLETE')
            self.assertEqual(report['completed_packets'], 2)
            self.assertEqual(completed.count('1'), 1)
            before = list(completed)
            report = asyncio.run(runner.execute('codex_fixture', smoke=True))
            self.assertEqual(before, completed)

    def test_propose_verify_repair_algorithm_matches_existing_executor(self):
        import inspect
        from experiments.project_change_semantic_272 import run as legacy
        from experiments.project_change_semantic_codex_272.inference import infer_packet
        source = inspect.getsource(legacy.run)
        start = source.index("        proposed=await call(p,'propose'")
        end = source.index("        immutable(out/'results'/", start)
        expected = '\n'.join(line.strip() for line in source[start:end].splitlines())
        current = inspect.getsource(infer_packet)
        current = current[current.index("    proposed=await call(p,'propose'"):current.rindex('    return result')]
        self.assertEqual(expected, '\n'.join(line.strip() for line in current.splitlines()))


if __name__ == '__main__':
    unittest.main()
