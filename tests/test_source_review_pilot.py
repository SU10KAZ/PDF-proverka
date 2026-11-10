import asyncio
from dataclasses import dataclass
import json
import os
import importlib.util
from pathlib import Path

import pytest

from backend.app.pipeline.stages.findings_verify.source_review_queue import build_source_review_queue
spec = importlib.util.spec_from_file_location(
    'source_review_pilot', Path(__file__).resolve().parents[1] /
    'experiments/auditmanager_stage4_20261110/run_source_review_pilot.py')
pilot = importlib.util.module_from_spec(spec)
spec.loader.exec_module(pilot)


@dataclass
class FakeResult:
    json_data: object
    is_error: bool = False


@pytest.fixture
def prepared(tmp_path, monkeypatch):
    # Only synthetic TEXT is used, and the provider is always replaced below.
    source = tmp_path/'document.md'
    source.write_text('### BLOCK #1 [TEXT]: t\n12 pieces\n')
    findings = [{'id': fid, 'problem': 'Quantity mismatch', 'source_evidence_review_status': 'pending',
                 'source_review_requests': [{'block_ids': ['t']}]} for fid in pilot.IDS]
    queue = build_source_review_queue(findings, source.read_text())
    (tmp_path/'EOM_source_review_queue.json').write_text(json.dumps(queue))
    monkeypatch.setattr(pilot, 'SOURCE', source)
    monkeypatch.setattr(pilot, 'HERE', tmp_path)
    monkeypatch.setattr(pilot, 'ROOT', tmp_path/'run')
    from backend.app.services.llm import codex_runner, codex_model_substitution
    monkeypatch.setattr(codex_model_substitution, 'substitute_codex_model', lambda name: name)
    async def forbid(*args, **kwargs):
        raise AssertionError('Unexpected provider invocation')
    monkeypatch.setattr(codex_runner, 'run_codex_json_messages', forbid)
    pilot.prepare()
    digest = json.loads((pilot.ROOT/'manifest.json').read_text())['request_sha256']
    reviews = [{'finding_id': p['finding_id'], 'finding_sha256': p['finding_sha256'],
                'source_md_sha256': queue['source_md_sha256'],
                'claims': [{'claim_id': 'q', 'claim': 'Quantity is 12', 'status': 'supported',
                            'reason': 'Explicit source row', 'evidence': [{'block_id': 't', 'quote': '12 pieces'}]}]}
               for p in queue['packets']]
    return digest, reviews, codex_runner


def test_single_attempt_persists_and_does_not_repeat(prepared, monkeypatch):
    digest, reviews, runner = prepared
    calls = []
    async def fake(messages, **kwargs):
        calls.append(kwargs)
        assert os.environ['CODEX_TRANSIENT_RETRIES'] == '0'
        return FakeResult({'reviews': reviews})
    monkeypatch.setattr(runner, 'run_codex_json_messages', fake)
    monkeypatch.setenv('CODEX_TRANSIENT_RETRIES', '3')
    asyncio.run(pilot.execute(digest))
    validation = json.loads((pilot.ROOT/'validation.json').read_text())
    assert validation['response_finding_set_matches']
    assert all(r['status'] == 'recorded_pending_semantic_acceptance' for r in validation['receipts'])
    assert not validation['semantic_verified']
    assert os.environ['CODEX_TRANSIENT_RETRIES'] == '3'
    with pytest.raises(FileExistsError):
        asyncio.run(pilot.execute(digest))
    assert len(calls) == 1
    assert calls[0]['model'] == 'codex/gpt-6-astra'


def test_exception_is_terminal_and_does_not_repeat(prepared, monkeypatch):
    digest, _, runner = prepared
    calls = []
    async def fail(*args, **kwargs):
        calls.append(1)
        raise TimeoutError('synthetic timeout')
    monkeypatch.setattr(runner, 'run_codex_json_messages', fail)
    with pytest.raises(TimeoutError):
        asyncio.run(pilot.execute(digest))
    assert json.loads((pilot.ROOT/'attempt.json').read_text())['status'] == 'failed'
    with pytest.raises(FileExistsError):
        asyncio.run(pilot.execute(digest))
    assert len(calls) == 1


@pytest.mark.parametrize('mode', ['duplicate', 'invalid_id', 'provider_error'])
def test_incomplete_or_failed_response_not_accepted(prepared, monkeypatch, mode):
    digest, reviews, runner = prepared
    if mode == 'duplicate':
        reviews[1] = reviews[0]
    elif mode == 'invalid_id':
        reviews[0]['finding_id'] = []
    async def fake(*args, **kwargs):
        return FakeResult({'reviews': reviews}, is_error=mode == 'provider_error')
    monkeypatch.setattr(runner, 'run_codex_json_messages', fake)
    asyncio.run(pilot.execute(digest))
    validation = json.loads((pilot.ROOT/'validation.json').read_text())
    assert not validation['response_finding_set_matches']
    assert validation['receipts'] == []
    assert (pilot.ROOT/'result.json').exists()


def test_changed_payload_rejected_before_provider(prepared):
    digest, _, _ = prepared
    with (pilot.ROOT/'request.json').open('a') as f:
        f.write(' ')
    with pytest.raises(RuntimeError, match='differs'):
        asyncio.run(pilot.execute(digest))
    assert not (pilot.ROOT/'attempt.json').exists()
