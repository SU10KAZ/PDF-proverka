import asyncio
import pytest
from backend.app.services.llm import astra_parallelism as gate

@pytest.fixture(autouse=True)
def settings(tmp_path, monkeypatch):
    monkeypatch.setattr(gate, 'SETTINGS_FILE', tmp_path / 'settings.json')
    monkeypatch.setattr(gate, '_active', 0)

@pytest.mark.parametrize('mode,model,expected', [('limited_2','gpt-6-astra',2), ('classic','gpt-6-astra',6), ('limited_2','gpt-5.6-sol',6)])
def test_shared_limit(mode, model, expected):
    gate.set_mode(mode)
    async def scenario():
        active = peak = 0
        @gate.limit_astra
        async def call(**kwargs):
            nonlocal active, peak
            active += 1
            peak = max(peak, active)
            await asyncio.sleep(.02)
            active -= 1
        await asyncio.gather(*(call(model=model, project_id=str(i)) for i in range(6)))
        assert peak == expected
        assert gate._active == 0
    asyncio.run(scenario())

def test_cancel_releases_slot():
    async def scenario():
        @gate.limit_astra
        async def call(**kwargs):
            await asyncio.sleep(100)
        task = asyncio.create_task(call(model='gpt-6-astra'))
        await asyncio.sleep(.01)
        assert gate._active == 1
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert gate._active == 0
    asyncio.run(scenario())

def test_default_and_validation():
    assert gate.get_mode() == 'limited_2'
    gate.set_mode('classic')
    assert gate.get_mode() == 'classic'
    with pytest.raises(ValueError):
        gate.set_mode('bad')
    assert gate.get_mode() == 'classic'
