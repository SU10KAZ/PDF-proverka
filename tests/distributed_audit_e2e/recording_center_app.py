"""Изолированный тестовый центр, ЗАПИСЫВАЮЩИЙ фактические HTTP-запросы воркера.

Зачем это нужно и почему именно так.

Этап 11C обязан предъявить не «примерно такой payload», а тот самый, который
уходит на центр: метод, путь, заголовки, тело, ключ идемпотентности,
идентичность воркера и задания. Собрать его в тесте вызовом сериализатора
нельзя — это доказывало бы работу сериализатора, а не транспорта.

Три способа получить настоящие байты, и почему выбран третий:

  1. врезать запись в `audit_worker/client.py` — правка ПРОДОВОГО транспорта
     ради теста. Нет;
  2. поставить записывающий прокси между воркером и центром — лишний узел на
     пути, который сам может исказить загрузку архива по чанкам;
  3. записать на приёмной стороне, в ТЕСТОВОМ приложении центра. Приложение
     остаётся тем же объектом `backend.app.main:app` (это проверяется), а
     запись живёт во внешней ASGI-обёртке, которой в проде не существует.

Обёртка намеренно не парсит и не изменяет поток: она копирует байты тела,
пропускает событие дальше и складывает запись в JSONL. Заголовки пишутся с
ЗАМАСКИРОВАННЫМИ значениями чувствительных имён: цель — доказать, что заголовок
есть и имеет ожидаемую форму, а не сохранить токен на диск.

Запуск:
    E2E_REQUEST_LOG=/путь/requests.jsonl \\
    python -m uvicorn tests.distributed_audit_e2e.recording_center_app:app --port N
"""
from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from backend.app.main import app as _production_app  # noqa: E402

#: Имена заголовков, значения которых не сохраняются НИКОГДА. Записывается
#: только длина: «заголовок был и он непустой» — проверяемое утверждение,
#: которое не требует знать значение.
_MASKED_HEADERS = frozenset({
    "authorization", "x-worker-token", "x-execution-token", "cookie",
    "x-bootstrap-secret", "set-cookie",
})

#: Сколько байт тела сохранять. Архив результата уезжает чанками по 32 МиБ, и
#: складывать их на диск второй раз незачем: для доказательства транспорта
#: нужны форма и начало, а целостность архива доказывает его sha256.
_MAX_BODY_BYTES = 64 * 1024


def _log_path() -> Path | None:
    raw = os.environ.get("E2E_REQUEST_LOG", "").strip()
    return Path(raw) if raw else None


def _safe_headers(raw_headers) -> dict[str, object]:
    out: dict[str, object] = {}
    for key, value in raw_headers:
        name = key.decode("latin-1").lower()
        if name in _MASKED_HEADERS:
            out[name] = {"present": True, "length": len(value)}
        else:
            out[name] = value.decode("latin-1")[:200]
    return out


class RequestRecorder:
    """ASGI-обёртка: копирует тело запроса, ничего не меняя в потоке."""

    def __init__(self, app, log_path: Path | None) -> None:
        self.app = app
        self.log_path = log_path
        if log_path is not None:
            log_path.parent.mkdir(parents=True, exist_ok=True)

    async def __call__(self, scope, receive, send):
        if self.log_path is None or scope.get("type") != "http":
            await self.app(scope, receive, send)
            return
        chunks: list[bytes] = []
        size = 0

        async def _receive():
            nonlocal size
            message = await receive()
            if message.get("type") == "http.request":
                body = message.get("body") or b""
                size += len(body)
                if sum(len(c) for c in chunks) < _MAX_BODY_BYTES:
                    chunks.append(body)
            return message

        status = {"code": None}

        async def _send(message):
            if message.get("type") == "http.response.start":
                status["code"] = message.get("status")
            await send(message)

        try:
            await self.app(scope, _receive, _send)
        finally:
            self._write(scope, chunks, size, status["code"])

    def _write(self, scope, chunks, size, status_code) -> None:
        raw = b"".join(chunks)[:_MAX_BODY_BYTES]
        try:
            parsed = json.loads(raw.decode("utf-8"))
            body_json, body_text = parsed, None
        except Exception:                              # noqa: BLE001 — не JSON, и это нормально
            body_json = None
            body_text = raw[:2048].decode("utf-8", "replace")
        # Тела ЗАПИСЫВАЮТСЯ НА ДИСК, поэтому проходят тот же редактор секретов,
        # что и всё остальное в подсистеме. Дело не в гипотезе: в теле
        # `POST /api/v1/worker/claim` claim-secret лежит ОТКРЫТО — так устроен
        # обмен «одноразовый секрет → постоянный токен», и иначе он невозможен.
        # Записать его в файл доказательств значило бы создать новый носитель
        # секрета там, где его раньше не было.
        from audit_worker import redaction

        if body_json is not None:
            body_json = (
                redaction.redact_mapping(body_json)
                if isinstance(body_json, dict) else body_json
            )
        if body_text:
            body_text = redaction.redact(body_text)
        record = {
            "at": time.time(),
            "method": scope.get("method"),
            "path": scope.get("path"),
            "query": (scope.get("query_string") or b"").decode("latin-1"),
            "client": (scope.get("client") or ("", 0))[0],
            "status": status_code,
            "headers": _safe_headers(scope.get("headers") or []),
            "body_bytes": size,
            "body_json": body_json,
            "body_text": body_text,
        }
        try:
            with self.log_path.open("a", encoding="utf-8") as stream:
                stream.write(json.dumps(record, ensure_ascii=False) + "\n")
        except OSError:
            pass


app = RequestRecorder(_production_app, _log_path())


def wrapped_app_is_production_object() -> bool:
    """Обёрнут ли ИМЕННО продовый объект. Проверяемое утверждение."""
    import backend.app.main as production_main

    return app.app is production_main.app
