import tempfile
import unittest
from pathlib import Path

from .recovery import command_for
from experiments.project_change_semantic_codex_272.provider import DISABLED


class RecoveryIsolationTests(unittest.TestCase):
    def test_only_runtime_companion_is_added(self):
        with tempfile.TemporaryDirectory() as directory:
            command = command_for(directory, ['image_00.png'])
        invocation = command.index('/opt/codex', command.index('/opt/codex') + 1)
        self.assertIn('/opt/codex-code-mode-host', command[:invocation])
        enabled = [command[i+1] for i, v in enumerate(command) if v == '--enable']
        disabled = [command[i+1] for i, v in enumerate(command) if v == '--disable']
        self.assertEqual(enabled, ['code_mode_host'])
        self.assertEqual(set(disabled), set(DISABLED) - {'code_mode_host'})
        self.assertNotIn('code_mode_host', disabled)
        self.assertNotIn(str(Path('/home/coder/projects/PDF-proverka')), command)
        self.assertIn('read-only', command)
        self.assertIn('gpt-6-astra', command)
        self.assertIn('model_reasoning_effort="xhigh"', command)


if __name__ == '__main__':
    unittest.main()
