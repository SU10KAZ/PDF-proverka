"""Поддельный шлюз OpenRouter для стенда (этап 11J).

Зачем он нужен и почему именно такой.

Подделка двух CLI (`backend/app/pipeline/execution/fake_providers.py`) закрывает
ровно то, что запускается подпроцессом. Третий провайдер 11J подпроцессом не
запускается вовсе: это HTTPS-запрос из самого процесса конвейера. Подменить его
теми же средствами нельзя — подменять нечего.

Поэтому здесь ровно то же решение, что и с CLI, только на своём уровне: не
подменяется ОРКЕСТРАЦИЯ, подменяется последний метр. Запрос настоящий: httpx,
настоящий сокет, настоящий заголовок `Authorization`, настоящий разбор ответа
адаптером. Не настоящий — только собеседник.

Что это даёт и чего не даёт:

  * ДАЁТ проверку всего пути «действие плана → маршрут → адаптер → сеть →
    разбор → журнал → провенанс», включая кодирование картинки в data-URL и
    нормализацию `usage`. Реальных обращений к OpenRouter при этом ноль;
  * НЕ ДАЁТ проверки того, что настоящий шлюз ответит тем же. Это предмет
    следующего этапа, и подделка на него не претендует.

Заглушка ЖУРНАЛИРУЕТ каждый запрос — и это её вторая работа, не менее важная
первой. Без журнала «настоящий OpenRouter не вызывался» неотличимо от «нога
вообще не исполнилась»: оба случая выглядят как отсутствие сетевой активности.
В журнал пишутся факты вызова, но НЕ содержимое: промпт — это замечания по
документу заказчика, а заголовок `Authorization` — секрет. Оба заменяются
отпечатками sha256.

Запуск:
    python -m tests.distributed_audit_e2e.openrouter_stub --port 8099 \
        --log /path/to/openrouter_calls.jsonl
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Optional

#: Поведение заглушки. Задаётся стендом, не заданием — ровно как у поддельных
#: CLI (`fake_providers.BEHAVIOUR_ENV`).
BEHAVIOUR_OK = "ok"
BEHAVIOUR_AUTH_ERROR = "auth_error"
BEHAVIOUR_RATE_LIMIT = "rate_limit"
BEHAVIOUR_TIMEOUT = "timeout"
BEHAVIOUR_SERVER_ERROR = "server_error"
BEHAVIOUR_BROKEN_JSON = "broken_json"
BEHAVIOUR_WRONG_MODEL = "wrong_model"
BEHAVIOURS: tuple[str, ...] = (
    BEHAVIOUR_OK, BEHAVIOUR_AUTH_ERROR, BEHAVIOUR_RATE_LIMIT, BEHAVIOUR_TIMEOUT,
    BEHAVIOUR_SERVER_ERROR, BEHAVIOUR_BROKEN_JSON, BEHAVIOUR_WRONG_MODEL,
)

#: Фиксированный расход. Детерминизм принципиален: бюджет прогона сверяется с
#: числом вызовов, а «примерно столько-то токенов» сверить нельзя.
INPUT_TOKENS_PER_CALL = 1000
OUTPUT_TOKENS_PER_CALL = 24
COST_PER_CALL_USD = 0.0123


def _sha256(text: str) -> str:
    return hashlib.sha256((text or "").encode("utf-8")).hexdigest()


class _Handler(BaseHTTPRequestHandler):
    behaviour: str = BEHAVIOUR_OK
    log_path: Optional[Path] = None
    lock = threading.Lock()

    # Тишина в stdout: сервер живёт внутри стенда, и его собственный access-log
    # засоряет вывод сценария, ничего не добавляя — всё нужное в JSONL.
    def log_message(self, fmt: str, *args: Any) -> None:  # noqa: A003
        return

    def do_POST(self) -> None:                            # noqa: N802 — контракт BaseHTTPRequestHandler
        length = int(self.headers.get("Content-Length") or 0)
        raw = self.rfile.read(length) if length else b""
        try:
            payload = json.loads(raw.decode("utf-8"))
        except Exception:                                 # noqa: BLE001
            payload = {}
        self._record(payload)

        if self.behaviour == BEHAVIOUR_AUTH_ERROR:
            return self._json(401, {"error": {"code": 401, "message": "No auth credentials found"}})
        if self.behaviour == BEHAVIOUR_RATE_LIMIT:
            return self._json(429, {"error": {"code": 429, "message": "Rate limit exceeded"}})
        if self.behaviour == BEHAVIOUR_SERVER_ERROR:
            return self._json(503, {"error": {"code": 503, "message": "Service unavailable"}})
        if self.behaviour == BEHAVIOUR_TIMEOUT:
            # Не спим и не держим сокет: тест таймаута не должен стоить стенду
            # реального ожидания. Возвращается тот же класс ответа, что даёт
            # шлюз при истёкшем апстриме.
            return self._json(504, {"error": {"code": 504, "message": "Upstream timeout"}})
        if self.behaviour == BEHAVIOUR_BROKEN_JSON:
            return self._raw(200, b"{ not json at all")

        requested = str(payload.get("model") or "")
        answered = "someone/other-model" if self.behaviour == BEHAVIOUR_WRONG_MODEL else requested
        content = json.dumps(_findings_payload(payload), ensure_ascii=False)
        return self._json(200, {
            "id": "stub-" + _sha256(requested)[:12],
            "model": answered,
            "choices": [{
                "index": 0,
                "finish_reason": "stop",
                "message": {"role": "assistant", "content": content},
            }],
            "usage": {
                "prompt_tokens": INPUT_TOKENS_PER_CALL,
                "completion_tokens": OUTPUT_TOKENS_PER_CALL,
                "total_tokens": INPUT_TOKENS_PER_CALL + OUTPUT_TOKENS_PER_CALL,
                "cost": COST_PER_CALL_USD,
            },
        })

    def do_GET(self) -> None:                             # noqa: N802
        """Признак живости для стенда. Модель здесь не участвует."""
        self._json(200, {"stub": "openrouter", "behaviour": self.behaviour})

    # ─── Внутреннее ──────────────────────────────────────────────────────────
    def _record(self, payload: dict[str, Any]) -> None:
        if self.log_path is None:
            return
        messages = payload.get("messages") or []
        images = 0
        text_chars = 0
        for message in messages:
            content = message.get("content")
            if isinstance(content, list):
                for part in content:
                    if part.get("type") == "image_url":
                        images += 1
                    elif part.get("type") == "text":
                        text_chars += len(str(part.get("text") or ""))
            elif isinstance(content, str):
                text_chars += len(content)
        auth = str(self.headers.get("Authorization") or "")
        row = {
            "path": self.path,
            "model": payload.get("model"),
            "reasoning_effort": (payload.get("reasoning") or {}).get("effort"),
            "images": images,
            "text_chars": text_chars,
            # Ни промпта, ни ключа. Отпечатки отвечают на все вопросы, ради
            # которых журнал заведён: «те же ли входы у трёх ног ансамбля» и
            # «дошёл ли до шлюза ключ вообще» — и ни на один сверх того.
            "prompt_sha256": _sha256(json.dumps(messages, ensure_ascii=False, sort_keys=True)),
            "authorization_present": auth.startswith("Bearer ") and len(auth) > 10,
            "authorization_sha256": _sha256(auth) if auth else "",
            "behaviour": self.behaviour,
        }
        with self.lock:
            with open(self.log_path, "a", encoding="utf-8") as fh:
                fh.write(json.dumps(row, ensure_ascii=False) + "\n")

    def _json(self, status: int, body: dict[str, Any]) -> None:
        self._raw(status, json.dumps(body, ensure_ascii=False).encode("utf-8"))

    def _raw(self, status: int, body: bytes) -> None:
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def _findings_payload(payload: dict[str, Any]) -> dict[str, Any]:
    """ДЕТЕРМИНИРОВАННЫЙ ответ детектора блока.

    Одно замечание с полями, которых ждёт `provider_transport`. Значение
    выводится из отпечатка запроса, чтобы два разных блока дали разные ответы,
    а один и тот же блок — побайтово одинаковый: иначе сравнить два прогона
    было бы нечем.
    """
    digest = _sha256(json.dumps(payload.get("messages") or [], ensure_ascii=False, sort_keys=True))
    return {
        "findings": [{
            "severity": "medium",
            "category": "документация",
            "problem": f"Заглушка шлюза: контрольное замечание {digest[:8]}",
            "evidence_quote": "поддельный ответ стенда",
            "confidence": 0.9,
        }]
    }


def serve(*, port: int, behaviour: str, log_path: Optional[Path]) -> None:
    _Handler.behaviour = behaviour if behaviour in BEHAVIOURS else BEHAVIOUR_OK
    _Handler.log_path = Path(log_path) if log_path else None
    if _Handler.log_path is not None:
        _Handler.log_path.parent.mkdir(parents=True, exist_ok=True)
    server = ThreadingHTTPServer(("127.0.0.1", port), _Handler)
    print(f"openrouter-stub слушает 127.0.0.1:{server.server_address[1]} "
          f"behaviour={_Handler.behaviour}", flush=True)
    server.serve_forever()


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Поддельный шлюз OpenRouter для стенда")
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--behaviour", default=BEHAVIOUR_OK, choices=list(BEHAVIOURS))
    parser.add_argument("--log", default="")
    args = parser.parse_args(argv)
    serve(
        port=int(args.port),
        behaviour=str(args.behaviour),
        log_path=Path(args.log) if args.log else None,
    )
    return 0


if __name__ == "__main__":                                # pragma: no cover
    sys.exit(main())
