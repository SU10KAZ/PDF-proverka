"""Offline exact-byte guards, including the 52 frozen Pair A inputs."""
import copy
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from .provider import CodexProvider
from .serialization import canonical_json_bytes, prepare_request_bytes, request_sha256


def reversed_keys(value):
    if isinstance(value, dict):
        return {key: reversed_keys(value[key]) for key in reversed(value)}
    if isinstance(value, list):
        return [reversed_keys(item) for item in value]
    return value


class CanonicalSerialization(unittest.TestCase):
    def setUp(self):
        self.data = {'z': [None, 3, 1.25, True, {'б': 'АР1', 'a': 2}],
                     'evidence': {'quote': 'Состояние OLD → NEW', 'page': 12}}

    def test_nested_key_order_has_identical_utf8_bytes(self):
        self.assertEqual(canonical_json_bytes(self.data), canonical_json_bytes(reversed_keys(self.data)))
        self.assertIn('АР1'.encode('utf-8'), canonical_json_bytes(self.data))

    def test_disk_roundtrip_freeze_equals_runtime(self):
        disk = json.loads(json.dumps(self.data, sort_keys=True, ensure_ascii=False, indent=2))
        self.assertEqual(prepare_request_bytes('unchanged prompt', self.data),
                         prepare_request_bytes('unchanged prompt', disk))

    def test_request_hash_is_stable(self):
        payload, _ = prepare_request_bytes('prompt', self.data)
        self.assertEqual(request_sha256(payload), request_sha256(prepare_request_bytes('prompt', self.data)[0]))

    def test_prompt_change_changes_hash(self):
        self.assertNotEqual(request_sha256(prepare_request_bytes('prompt', self.data)[0]),
                            request_sha256(prepare_request_bytes('prompt!', self.data)[0]))

    def test_evidence_change_changes_hash(self):
        changed = copy.deepcopy(self.data)
        changed['evidence']['quote'] += ' changed'
        self.assertNotEqual(request_sha256(prepare_request_bytes('prompt', self.data)[0]),
                            request_sha256(prepare_request_bytes('prompt', changed)[0]))

    def test_only_key_order_does_not_change_request_hash(self):
        self.assertEqual(request_sha256(prepare_request_bytes('prompt', self.data)[0]),
                         request_sha256(prepare_request_bytes('prompt', reversed_keys(self.data))[0]))

    def test_array_order_and_scalar_types_survive(self):
        self.assertEqual(json.loads(canonical_json_bytes(self.data)), self.data)
        changed = copy.deepcopy(self.data)
        changed['z'].reverse()
        self.assertNotEqual(canonical_json_bytes(changed), canonical_json_bytes(self.data))

    def test_nonfinite_numbers_are_rejected(self):
        with self.assertRaises(ValueError):
            canonical_json_bytes({'value': float('nan')})


class BeforeInvocation(unittest.IsolatedAsyncioTestCase):
    async def test_mismatched_hash_never_invokes_process(self):
        with tempfile.TemporaryDirectory() as tmp:
            provider = CodexProvider(tmp, {'provider': 'offline'})
            with patch('asyncio.create_subprocess_exec') as create:
                with self.assertRaisesRegex(ValueError, 'before invocation'):
                    await provider.call('test', 'prompt', {}, {}, expected_request_sha256='0' * 64)
                create.assert_not_called()
            self.assertEqual(provider.invocations, 0)
            self.assertFalse((Path(tmp) / 'calls').exists())

    @unittest.skipUnless(os.environ.get('PAIR_A_SERIALIZATION_INPUT_ROOT'),
                         'Requires explicitly selected frozen Pair A input artifacts')
    async def test_all_52_frozen_pair_a_requests_match_actual_stdin(self):
        root = Path(os.environ['PAIR_A_SERIALIZATION_INPUT_ROOT'])
        plan = json.loads((root / 'CALL_PLAN.json').read_text())['packages']
        eligible = [r for r in plan if r['action'] == 'MODEL_CALL']
        self.assertEqual(len(eligible), 52)
        corpus = root.parent
        base = corpus / 'fresh_dev_sample_f5_pipeline_v6'
        system = (root / 'PROMPT.txt').read_text()
        seen = []
        expected = None

        async def create(*args, **kwargs):
            work = Path(args[0])

            class Process:
                returncode = 0

                async def communicate(self, payload):
                    self_test.assertEqual(payload, expected)
                    self_test.assertEqual((work / 'prompt.txt').read_bytes(), payload)
                    seen.append(request_sha256(payload))
                    (work / 'final.txt').write_text('{}')
                    kwargs['stdout'].write(b'{"type":"turn.completed","usage":{"input_tokens":0,"output_tokens":0}}\n')
                    return None, None

            return Process()

        self_test = self
        with tempfile.TemporaryDirectory() as tmp:
            provider = CodexProvider(tmp, {'provider': 'offline_mock_no_network'})
            with patch('experiments.project_change_semantic_codex_272.provider.sandbox_command',
                       side_effect=lambda work, cmd: [str(work)]), \
                    patch('asyncio.create_subprocess_exec', side_effect=create), \
                    patch('socket.socket', side_effect=AssertionError('Network forbidden in serialization tests')):
                for row in eligible:
                    data = json.loads((root / 'inputs' / row['key'] / 'MODEL_INPUT.json').read_text())
                    packet = json.loads((base / row['package']).read_text())['evidence_packet']
                    for evidence in (e for es in packet['evidence'].values() for e in es):
                        if evidence.get('raster'):
                            evidence['raster']['path'] = str(base / evidence['raster']['path'])
                    expected, _ = prepare_request_bytes(system, reversed_keys(data), packet)
                    digest = request_sha256(expected)
                    await provider.call(row['key'], system, data,
                                        {'type': 'object', 'properties': {}, 'additionalProperties': False},
                                        packet, expected_request_sha256=digest)
                    receipt = json.loads((Path(tmp) / 'calls' / row['key'] / 'SUCCESS.json').read_text())
                    self.assertEqual(receipt['exact_request_sha256'], digest)
                    self.assertEqual(receipt['expected_request_sha256'], digest)
            self.assertEqual(len(seen), 52)


if __name__ == '__main__':
    unittest.main()
