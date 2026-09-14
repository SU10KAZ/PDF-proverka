"""Offline transport and boundary tests; fixtures are never quality evidence."""
import asyncio
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from experiments.project_change_semantic_272.packets import digest
from experiments.project_change_semantic_codex_272.provider import (
    CodexProvider, AuthorizationBlocked, InferenceFailed, classify_error, safe_env, sandbox_command)
from experiments.project_change_semantic_codex_272.schema import PROPOSAL, validate
from experiments.project_change_semantic_codex_272.run import verify_packet

EMPTY = dict(events=[], unknowns=[], unchanged=[])


class Transport(unittest.IsolatedAsyncioTestCase):
    async def exercise(self, output=EMPTY, exit_code=0, error='', tool=False):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        out = Path(temporary.name)
        provider = CodexProvider(out, dict(model='synthetic-test', provider='offline'))
        count = []

        async def create(*args, **kwargs):
            work = Path(args[0])
            count.append(work)
            class Process:
                returncode = exit_code
                async def communicate(self, data):
                    (work / 'final.txt').write_text(json.dumps(output))
                    records = [dict(type='turn.completed', usage=dict(input_tokens=10, output_tokens=2))]
                    if tool:
                        records.append(dict(type='item.completed', item=dict(type='command_execution')))
                    if error:
                        records.append(dict(type='error', message=error))
                    kwargs['stdout'].write(('\n'.join(json.dumps(r) for r in records)).encode())
                    return None, None
            return Process()
        with patch('experiments.project_change_semantic_codex_272.provider.sandbox_command',
                   side_effect=lambda work, cmd: [str(work)]), patch('asyncio.create_subprocess_exec', side_effect=create):
            try:
                result = await provider.call('p_propose', 'test', {}, PROPOSAL)
                yield_result = (provider, count, result, out)
                # A successful call must be reused, not invoked twice.
                result2 = await provider.call('p_propose', 'test', {}, PROPOSAL)
                self.assertEqual(result, result2)
                self.assertEqual(len(count), 1)
                with self.assertRaises(ValueError):
                    await provider.call('p_propose', 'changed prompt', {}, PROPOSAL)
                return yield_result
            except Exception:
                self.assertTrue(list(out.glob('calls/*/attempt_*/VALIDATION.json')))
                raise

    async def test_success_resume_and_prompt_drift(self):
        provider, count, result, out = await self.exercise()
        self.assertEqual(result, EMPTY)
        self.assertEqual(provider.cache_hits, 1)

    async def test_malformed_output_is_not_success(self):
        with self.assertRaises(Exception) as caught:
            await self.exercise(dict(events='wrong type'))
        self.assertEqual(type(caught.exception).__name__, 'ValidationError')

    async def test_authorization_denial_has_receipt(self):
        with self.assertRaises(AuthorizationBlocked):
            await self.exercise(exit_code=1, error='You have hit your usage limit')

    async def test_tool_use_is_rejected(self):
        with self.assertRaises(InferenceFailed):
            await self.exercise(tool=True)

    async def test_stopped_queue_does_not_invoke(self):
        with tempfile.TemporaryDirectory() as tmp:
            provider = CodexProvider(tmp, {})
            provider.stopped = True
            with patch('asyncio.create_subprocess_exec') as create:
                with self.assertRaises(AuthorizationBlocked):
                    await provider.call('p', 'test', {}, PROPOSAL)
                create.assert_not_called()


class Boundaries(unittest.TestCase):
    def test_environment_has_no_provider_or_parent_session(self):
        with patch.dict(os.environ, {'OPENAI_API_KEY': 'test', 'OPENROUTER_API_KEY': 'test',
                                     'OPENAI_BASE_URL': 'test', 'CODEX_THREAD_ID': 'test'}):
            self.assertFalse(set(safe_env()) & {'OPENAI_API_KEY', 'OPENROUTER_API_KEY', 'OPENAI_BASE_URL', 'CODEX_THREAD_ID'})

    def test_schema_rejects_extra_fields_and_missing_witness_fields(self):
        with self.assertRaises(Exception):
            validate(dict(**EMPTY, arbitrary='bad'), PROPOSAL)
        with self.assertRaises(Exception):
            validate(dict(events=[{'event_id': 'unwitnessed'}], unknowns=[], unchanged=[]), PROPOSAL)

    def test_wrong_partition_and_hash_fail_before_call(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'packet.json'
            packet = dict(pair_index=11, partition='FINAL_HOLDOUT', pair_key='reserved')
            packet['packet_id'] = digest(packet)[:24]
            path.write_text(json.dumps(packet))
            from experiments.project_change_272.inventory import sha
            with self.assertRaises(PermissionError):
                verify_packet(path, sha(path), {}, {})
            with self.assertRaises(ValueError):
                verify_packet(path, 'wrong-hash', {}, {})

    def test_classify_transport_separately_from_quota(self):
        self.assertEqual(classify_error('connection reset'), 'INFERENCE_FAILED')
        self.assertEqual(classify_error('usage limit exceeded'), 'AUTHORIZATION_BLOCKED')

    def test_no_corpus_mounts(self):
        with tempfile.TemporaryDirectory() as tmp:
            args = sandbox_command(tmp, ['/bin/true'])
            self.assertNotIn('/home/coder/auditmanager', args)
            self.assertNotIn('/home/coder/projects', args)
            self.assertIn('--unshare-all', args)
            self.assertIn('--new-session', args)


if __name__ == '__main__':
    unittest.main()
