"""Startup provider selection and actual Astra pipeline wiring; zero model calls."""
import os
from pathlib import Path
import subprocess
import sys
import textwrap

import pytest

ROOT = Path(__file__).resolve().parents[3]


def run_python(code, selection=None):
    env = dict(os.environ, PYTHONPATH=str(ROOT))
    env.pop("PROJECT_COMPARISON_V3_PROVIDER", None)
    if selection is not None:
        env["PROJECT_COMPARISON_V3_PROVIDER"] = selection
    return subprocess.run([sys.executable, "-c", textwrap.dedent(code)], cwd=ROOT,
                          env=env, capture_output=True, text=True, timeout=60)


@pytest.mark.parametrize("selection", [None, "claude", "codex"])
def test_configuration_and_factory_agree_and_stay_frozen(selection):
    result = run_python('''
        import os
        from backend.app.services.project_change_v3 import contracts, provider, provenance, transport
        p = provider.get_provider()
        prov = provenance.build_provenance()
        codex = os.environ.get('PROJECT_COMPARISON_V3_PROVIDER') == 'codex'
        assert isinstance(p, provider.CodexProvider if codex else provider.ClaudeOpusProvider)
        assert prov['model'] == p.model == ('gpt-6-astra' if codex else 'claude-opus-5')
        assert prov['provider'] == p.provider
        assert prov['reasoning'] == p.reasoning == 'xhigh'
        assert prov['provider_transport_version'] == (transport.CODEX_TRANSPORT_VERSION if codex
                                                       else transport.CLAUDE_TRANSPORT_VERSION)
        os.environ['PROJECT_COMPARISON_V3_PROVIDER'] = 'claude' if codex else 'codex'
        assert type(provider.get_provider()) is type(p)
        assert provenance.build_provenance() == prov
    ''', selection)
    assert result.returncode == 0, result.stdout + result.stderr


def test_unknown_provider_is_rejected_instead_of_falling_back():
    result = run_python('from backend.app.services.project_change_v3 import contracts', 'typo')
    assert result.returncode != 0
    assert 'PROJECT_COMPARISON_V3_PROVIDER must be claude or codex' in result.stderr


def test_astra_pipeline_gate_receipts_and_persistence(tmp_path):
    result = run_python('''
        import json, os
        from pathlib import Path
        from types import SimpleNamespace
        from backend.tests.project_change_v3 import generic_fixture as gf
        from backend.app.services.project_change_v3 import contracts, scope, provider, provider_gate
        from backend.app.services.stage_comparison import production_orchestrator as orch, production_store
        from backend.app.services.stage_comparison.ai import gateway
        root = Path(ROOT_FIXTURE)
        os.environ['COMPARISON_ROOT'] = str(root / 'comparison')
        os.environ['PROJECT_COMPARISON_ENGINE'] = 'v3'
        os.environ['PROJECT_COMPARISON_V3_ALLOW_INFERENCE'] = '1'
        os.environ['PROJECT_COMPARISON_V3_FORCE_UNAVAILABLE'] = '0'
        os.environ.pop('PROJECT_COMPARISON_V3_PROVIDER_READY', None)
        built = gf.build_comparison(root)
        scope._object_stage_paths = lambda: {gf.OBJECT_ID: (built['stage_1'], built['stage_2'])}
        def forbidden(*args, **kw):
            raise AssertionError('Unexpected transport or fallback')
        gateway.call_claude_multimodal = gateway.validate_claude_runtime = forbidden
        gateway.call_codex_app_server = forbidden  # the synthetic fixture fits an ordinary turn
        orch._run_production_comparison_impl = orch._run_production_comparison_locked = forbidden
        checks, calls = [], []
        def readiness(**kw):
            checks.append(kw)
            return {'ok': False, 'problems': ['offline test: unavailable']}
        gateway.validate_runtime = readiness
        assert provider_gate.check_provider_readiness()['available'] is False
        assert checks == [{'require_vision': True, 'deep': False, 'require_json_events': True}]
        gateway.validate_runtime = lambda **kw: {'ok': True}
        handlers = gf.fake_handlers()
        def codex(payload, **kw):
            calls.append(kw)
            assert kw['model'] == 'gpt-6-astra' and kw['reasoning_level'] == 'xhigh'
            assert kw['json_events'] is True and kw['retries'] == 0
            assert kw['cancel'] is not None
            data = json.loads(payload.split('\\nSOURCE DATA:\\n', 1)[1])
            stage = ('MAPPING' if kw['run_id'].endswith('_SEMANTIC_MAPPING') else
                     'DEDUPE' if kw['run_id'].endswith('_DEDUPE') else 'MINING')
            assert payload.startswith(getattr(contracts, {'MAPPING':'MAPPER_PROMPT',
                        'MINING':'MINER_PROMPT', 'DEDUPE':'DEDUPE_PROMPT'}[stage]))
            answer = handlers[stage](call_id=kw['run_id'], pair_id=gf.PAIR_ID, data=data,
                                     schema=kw['schema'], images=kw['images'])
            return SimpleNamespace(ok=True, parsed=answer, usage={'input_tokens':100,
                 'cached_input_tokens':30, 'output_tokens':20, 'reasoning_output_tokens':5})
        gateway.call_codex = codex
        assert isinstance(provider.get_provider(), provider.CodexProvider)
        state = orch.run_production_comparison(built['session_id'], gf.PAIR_ID, input_mode='DOCUMENT')
        assert state['status'] in {'COMPLETED', 'REVIEW'}, state
        prov = state['provenance']
        receipts = prov['transport_calls']
        assert [c['stage'] for c in receipts] == ['MAPPING','MINING','MINING','DEDUPE']
        assert len(calls) == state['model_calls'] == 4
        assert {(c['provider'],c['model'],c['reasoning']) for c in receipts} == {
            ('codex_cli_subscription','gpt-6-astra','xhigh')}
        assert prov['model'] == 'gpt-6-astra' and prov['engine_variant'] == 'ProjectChange V3 / Astra'
        assert prov['usage_total'] == {'input_tokens':400,'cached_input_tokens':120,'output_tokens':80,
            'reasoning_output_tokens':20,'calls':4,'calls_with_usage':4,'calls_without_usage':0}
        stored = production_store.load_artifact(built['session_id'], gf.PAIR_ID,
                       'project_change_v3_result', include_domain_keys=True)
        assert stored['provenance']['model'] == 'gpt-6-astra'
        # Schema rejection still retains the completed answer for the attempt store.
        p = provider.get_provider()
        gateway.call_codex = lambda *a, **kw: SimpleNamespace(ok=True, parsed={'wrong':True}, usage={})
        try:
            p.complete(stage='MINING',call_id='rejected',pair_id='P',prompt='test',data={},
                       schema={'type':'object','required':['pair']},images=[])
        except provider.ProviderError as exc:
            assert exc.code == 'schema_invalid'
        else:
            raise AssertionError('invalid output accepted')
        assert p.last_response == {'wrong':True}
    '''.replace('ROOT_FIXTURE', repr(str(tmp_path))), 'codex')
    assert result.returncode == 0, result.stdout + result.stderr
