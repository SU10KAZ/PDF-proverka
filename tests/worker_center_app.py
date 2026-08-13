"""ASGI-приложение центра для запуска настоящим uvicorn.

Нужно ровно для одного: тестам разделения agent/executor и smoke-прогону,
где агент — ОТДЕЛЬНЫЙ процесс и ходит по настоящему HTTP, а не через
ASGITransport. Собирается теми же роутерами, что и в make_center_app.
"""
from __future__ import annotations

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from tests.distributed_workers_helpers import make_center_app  # noqa: E402

app = make_center_app()
