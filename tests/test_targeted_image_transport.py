import pytest
from backend.app.models.usage import LLMResult


@pytest.mark.asyncio
async def test_json_stage_passes_actual_images_to_codex_and_writes_result(monkeypatch, tmp_path):
    from backend.app.services.llm import claude_runner, codex_runner
    calls = []
    image_path = tmp_path / 'graphic.png'
    image_path.write_bytes(b'fixture image: transport is stubbed')

    async def fake_run(messages, **kwargs):
        calls.append(kwargs)
        return LLMResult(text='{"findings":[]}', json_data={'findings': []}, model='codex/gpt-6-astra')

    async def fake_status(*args, **kwargs):
        pass

    monkeypatch.setattr(codex_runner, 'run_codex_json_messages', fake_run)
    monkeypatch.setattr(claude_runner, '_send_status_llm', fake_status)
    monkeypatch.setattr(claude_runner, '_save_audit_trail', lambda *a, **k: None)
    code, _, _ = await claude_runner._run_codex_json_stage(
        stage='candidate_recovery', messages=[{'role': 'user', 'content': 'Inspect the attached graphic'}],
        model='codex/gpt-6-astra', timeout=60, project_id='test', on_output=None,
        output_filename='recovery.json', audit_stage='test', output_dir=tmp_path,
        image_paths=(image_path,))
    assert code == 0
    assert calls[0]['image_paths'] == (image_path,)
    assert (tmp_path / 'recovery.json').exists()
