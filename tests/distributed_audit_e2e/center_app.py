"""ASGI-приложение стенда: НАСТОЯЩИЙ `backend.app.main:app`, но под другим argv.

Здесь нет ни одной собственной строки приложения — объект импортируется как
есть, вместе с lifespan, роутерами, middleware, портальной аутентификацией и
врезкой `PipelineManager`. Модуль существует ровно ради командной строки.

**Зачем.** Продовый вотчдог этой машины (`~/bin/webapp-watchdog.sh` раз в
минуту) при подозрении на падение портала выполняет `stop_server.sh`, а тот —
`pgrep -f "uvicorn.*backend.app.main"` и `kill -9` по ВСЕМ совпадениям. Стенд,
запущенный как `uvicorn backend.app.main:app`, попадает под этот шаблон и
умирает молча посреди прогона — что и случилось: центр стенда был убит между
приёмом результата и центральным хвостом, а в логе осталось только успешное
завершение старта.

Симметрия важна не меньше: под тем же шаблоном стенд не должен принимать
продовый процесс за свой. Разные argv разводят их окончательно.

Запуск: `python -m uvicorn tests.distributed_audit_e2e.center_app:app --port N`
"""
from __future__ import annotations

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from backend.app.main import app  # noqa: E402,F401 — тот же объект, что в проде

def app_is_production_object() -> bool:
    """Тот ли это объект, что отдаёт прод. Проверяемое утверждение, не намерение."""
    import backend.app.main as production_main

    return app is production_main.app
