import asyncio
import importlib.util
import json
from pathlib import Path
import sys
import pytest

@pytest.fixture
def harness(monkeypatch):
    folder=Path(__file__).parent
    monkeypatch.syspath_prepend(str(folder))
    spec=importlib.util.spec_from_file_location('test_classic_method_harness',folder/'classic_control.py')
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    return module


def config():
    return {'inventory_max_packets':128,'inventory_max_source_chars':60000,'inventory_max_images':4,
            'source_review_max_findings':256,'source_review_auto_graphic_regions':True}


def test_classic_never_builds_extra_callbacks(harness,monkeypatch):
    from backend.app.pipeline.stages.findings_verify import source_review_workflow
    def forbidden(*args,**kwargs):raise AssertionError('Unexpected provider callback')
    monkeypatch.setattr(source_review_workflow,'direct_cli_executor',forbidden)
    assert harness.method_context_options('classic')=={}


def test_candidate_budgets_and_executor_are_forwarded(harness,monkeypatch):
    from backend.app.pipeline.stages.findings_verify import source_review_workflow
    sentinel=object();calls=[]
    def factory(*args,**kwargs):calls.append((args,kwargs));return sentinel
    monkeypatch.setattr(source_review_workflow,'direct_cli_executor',factory)
    harness.METHOD_CONFIG=config();harness.REASONING_EFFORT='high'
    options=harness.method_context_options('trial')
    assert all(options[k]==v for k,v in config().items())
    assert options['inventory_invoke'] is sentinel and options['source_review_invoke'] is sentinel
    assert calls==[(('codex/gpt-6-astra',),{'project_id':'trial','reasoning_effort':'high'})]

@pytest.mark.parametrize('change',[{'inventory_max_packets':0},{'inventory_max_packets':True},{'source_review_auto_graphic_regions':'true'},{'extra':'unexpected'}])
def test_invalid_method_config_rejected(harness,change):
    harness.METHOD_CONFIG={**config(),**change}
    with pytest.raises(ValueError):harness.method_context_options('trial')


def test_frozen_method_mismatch_stops_before_input_or_provider_access(harness,tmp_path):
    root=tmp_path/'classic_control';root.mkdir()
    (root/'protocol.json').write_text(json.dumps({'stage_models':harness.MODELS,'environment':harness.ENV,
        'reasoning_effort':harness.REASONING_EFFORT,'max_request_attempts':harness.MAX_REQUEST_ATTEMPTS,
        'method_config':config()}))
    with pytest.raises(ValueError,match='Execution settings differ'):
        asyncio.run(harness.run(tmp_path,'KM'))
