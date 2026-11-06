import asyncio
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
sys.path.insert(0,str(Path(__file__).parent))
from classic_control import validate_model,process_guard

class ControlTests(unittest.TestCase):
    def test_only_explicit_models(self):
        for model in ['codex/gpt-6-astra','gpt-6-sol','codex/gpt-6.1-sol']:
            validate_model(model)
        for model in [None,'claude-sonnet-5-5','openrouter/gpt-6-astra','ensemble/gpt-codex']:
            with self.assertRaises(RuntimeError):validate_model(model)

    def test_direct_claude_and_shell_denied(self):
        for exe in ['/usr/bin/claude','/bin/sh','/bin/bash','python','curl']:
            with self.assertRaises(RuntimeError):process_guard('subprocess.Popen',(exe,[],None,None))
        process_guard('subprocess.Popen',('/usr/bin/codex',[],None,None))

    def test_fresh_configuration_without_model_calls(self):
        # Fresh process: import-time settings must not inherit another audit's config.
        script='''
import asyncio,sys
from pathlib import Path
from classic_control import configure,MODELS
configure(Path(sys.argv[1]),'TEST')
from backend.app.core import config
assert config.STAGE_MODEL_CONFIG == MODELS
assert config.codex_only_mode_active()
from backend.app.services.llm import claude_json_runner, codex_runner
for fn in [claude_json_runner.run_claude_json_messages,codex_runner.run_codex_exec]:
    try: asyncio.run(fn([]))
    except RuntimeError: pass
    else: raise AssertionError('Forbidden transport allowed')
try: asyncio.run(codex_runner.run_codex_json_messages([],model='claude-sonnet-5-5',timeout=1))
except RuntimeError: pass
else: raise AssertionError('Forbidden model allowed')
from backend.app.pipeline.stages.block_analysis.gemma_findings_only import STAGE01_CLAUDE_LEG_ENABLED
assert not STAGE01_CLAUDE_LEG_ENABLED
'''
        with tempfile.TemporaryDirectory() as d:
            result=subprocess.run([sys.executable,'-c',script,d],cwd=Path(__file__).parent,capture_output=True,text=True)
            self.assertEqual(result.returncode,0,result.stderr+result.stdout)

if __name__=='__main__':unittest.main()
