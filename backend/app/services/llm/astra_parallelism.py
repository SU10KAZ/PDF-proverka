"""Shared Astra admission limit for audit Codex runners in this backend process."""
import asyncio
import json
import os
import threading
from functools import wraps

from backend.app.core.config import APP_DATA_DIR, resolve_codex_model

SETTINGS_FILE = APP_DATA_DIR / 'astra_parallelism.json'
_lock = threading.Lock()
_active = 0


def get_mode():
    try:
        mode = json.loads(SETTINGS_FILE.read_text()).get('mode')
        return mode if mode in ('limited_2', 'classic') else 'limited_2'
    except (OSError, ValueError, AttributeError):
        return 'limited_2'


def set_mode(mode):
    if mode not in ('limited_2', 'classic'):
        raise ValueError('Unknown Astra parallelism mode')
    SETTINGS_FILE.parent.mkdir(parents=True, exist_ok=True)
    with _lock:
        temp = SETTINGS_FILE.with_suffix('.tmp')
        temp.write_text(json.dumps({'mode': mode}))
        os.replace(temp, SETTINGS_FILE)


def limit_astra(fn):
    @wraps(fn)
    async def wrapped(*args, **kwargs):
        global _active
        if resolve_codex_model(kwargs.get('model')) != 'gpt-6-astra':
            return await fn(*args, **kwargs)
        # Count classic calls too: switching to limited mode waits for them.
        while True:
            with _lock:
                if get_mode() == 'classic' or _active < 2:
                    _active += 1
                    break
            await asyncio.sleep(0.1)
        try:
            return await fn(*args, **kwargs)
        finally:
            with _lock:
                _active -= 1
    return wrapped
