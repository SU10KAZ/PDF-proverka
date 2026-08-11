"""HTTP-клиент к центру. Только исходящие соединения, только HTTPS в проде.

Постоянного WebSocket центр↔воркер нет (ADR-005 и ограничение C-05): heartbeat,
long-poll задания, пакеты событий и чанки архива — обычные HTTPS-запросы.
Это выживает там, где долгоживущий WSS рвут прокси и таймауты.
"""
from __future__ import annotations

import random
import time
from pathlib import Path
from typing import Any, Iterator, Optional

import httpx

from audit_worker import PROTOCOL_VERSION


class CenterError(RuntimeError):
    """Ответ центра с ошибкой. Несёт статус и разобранное тело."""

    def __init__(self, status: int, detail: Any, body: Any = None):
        super().__init__(f"HTTP {status}: {detail}")
        self.status = status
        self.detail = detail
        self.body = body


class SequenceGapError(CenterError):
    """409 sequence_gap: центр сообщил, с какого seq повторять."""

    def __init__(self, expected_seq: int, body: Any):
        super().__init__(409, "sequence_gap", body)
        self.expected_seq = expected_seq


class AttemptSupersededError(CenterError):
    """409 attempt_superseded: попытка отозвана, воркер обязан остановиться."""


class CenterClient:
    def __init__(
        self,
        base_url: str,
        *,
        token: Optional[str] = None,
        worker_id: Optional[str] = None,
        instance_id: Optional[str] = None,
        timeout: float = 60.0,
        verify: bool = True,
        transport: Optional[httpx.BaseTransport] = None,
    ):
        """`transport` подменяет сетевой слой.

        Нужен ровно для одного: end-to-end тесты гоняют настоящего агента
        против настоящего FastAPI-приложения через ASGITransport, без сокетов
        и портов. В проде остаётся None.
        """
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.worker_id = worker_id
        self.instance_id = instance_id
        self._client = httpx.Client(
            timeout=timeout, verify=verify, follow_redirects=False, transport=transport
        )
        self._control_context_headers: dict[str, str] = {}

    def set_control_context(self, *, connection_id: str | None = None) -> None:
        """Bind HTTPS package requests to the current authenticated gRPC session."""
        self._control_context_headers = (
            {"X-Agent-Stream-Connection-Id": connection_id} if connection_id else {}
        )

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> "CenterClient":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    # ─── Низкий уровень ──────────────────────────────────────────────────────
    def _headers(self, extra: Optional[dict[str, str]] = None) -> dict[str, str]:
        headers = {"X-Protocol-Version": str(PROTOCOL_VERSION)}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        if self.worker_id:
            headers["X-Worker-Id"] = self.worker_id
        if self.instance_id:
            headers["X-Instance-Id"] = self.instance_id
        headers.update(self._control_context_headers)
        if extra:
            headers.update({k: v for k, v in extra.items() if v is not None})
        return headers

    def request(
        self,
        method: str,
        path: str,
        *,
        json_body: Any = None,
        content: Optional[bytes] = None,
        headers: Optional[dict[str, str]] = None,
        expect_204: bool = False,
    ) -> Any:
        response = self._client.request(
            method,
            f"{self.base_url}{path}",
            json=json_body,
            content=content,
            headers=self._headers(headers),
        )
        if response.status_code == 204:
            return None if expect_204 else {}
        if response.status_code >= 400:
            self._raise(response)
        if not response.content:
            return {}
        try:
            return response.json()
        except ValueError:
            return {"raw": response.text}

    @staticmethod
    def _raise(response: httpx.Response) -> None:
        try:
            body = response.json()
        except ValueError:
            body = {"detail": response.text[:500]}
        detail = body.get("detail", body) if isinstance(body, dict) else body

        if response.status_code == 409:
            payload = body if isinstance(body, dict) else {}
            nested = detail if isinstance(detail, dict) else payload
            if payload.get("error") == "sequence_gap":
                expected = int((payload.get("detail") or {}).get("expected_seq", 1))
                raise SequenceGapError(expected, body)
            if isinstance(nested, dict) and nested.get("error") == "attempt_superseded":
                raise AttemptSupersededError(409, nested, body)
        raise CenterError(response.status_code, detail, body)

    # ─── Операции протокола ──────────────────────────────────────────────────
    def register(self, payload: dict[str, Any], bootstrap_secret: str) -> dict[str, Any]:
        response = self._client.post(
            f"{self.base_url}/api/v1/worker/register",
            json=payload,
            headers={
                "Authorization": f"Bearer {bootstrap_secret}",
                "X-Protocol-Version": str(PROTOCOL_VERSION),
            },
        )
        if response.status_code >= 400:
            self._raise(response)
        return response.json()

    def claim(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Обменять одноразовый claim-secret на токен (после одобрения)."""
        response = self._client.post(
            f"{self.base_url}/api/v1/worker/claim",
            json=payload,
            headers={"X-Protocol-Version": str(PROTOCOL_VERSION)},
        )
        if response.status_code >= 400:
            self._raise(response)
        return response.json()

    def update_registration(self, payload: dict[str, Any]) -> dict[str, Any]:
        return self.request("PUT", "/api/v1/worker/registration", json_body=payload)

    def heartbeat(self, payload: dict[str, Any]) -> dict[str, Any]:
        return self.request("POST", "/api/v1/worker/heartbeat", json_body=payload)

    def next_job(
        self, payload: dict[str, Any], *, idempotency_key: Optional[str] = None
    ) -> Optional[dict[str, Any]]:
        headers = {"Idempotency-Key": idempotency_key} if idempotency_key else None
        return self.request(
            "POST", "/api/v1/worker/jobs/next", json_body=payload,
            headers=headers, expect_204=True,
        )

    def accept_job(
        self, job_id: str, payload: dict[str, Any], execution_token: str
    ) -> dict[str, Any]:
        return self.request(
            "POST",
            f"/api/v1/worker/jobs/{job_id}/accept",
            json_body=payload,
            headers={
                "X-Execution-Token": execution_token,
                # Ключ детерминирован по попытке: повтор из-за обрыва вернёт
                # тот же ответ, а не выполнит операцию второй раз. Без него
                # серверная защита от повтора вообще не включалась.
                "Idempotency-Key": f"accept:{job_id}:{payload.get('attempt_id', '')}",
            },
        )

    def reject_job(
        self, job_id: str, payload: dict[str, Any], execution_token: str
    ) -> dict[str, Any]:
        return self.request(
            "POST",
            f"/api/v1/worker/jobs/{job_id}/reject",
            json_body=payload,
            headers={"X-Execution-Token": execution_token},
        )

    def download_source(
        self, job_id: str, dest: Path, execution_token: str, *, resume: bool = True,
        transfer_id: str = "",
    ) -> int:
        """Скачать пакет с докачкой через Range. Возвращает итоговый размер."""
        part = dest.with_suffix(dest.suffix + ".part")
        part.parent.mkdir(parents=True, exist_ok=True)
        offset = part.stat().st_size if (resume and part.exists()) else 0
        headers = {}
        if execution_token:
            headers["X-Execution-Token"] = execution_token
        if transfer_id:
            headers["X-Package-Transfer-Id"] = transfer_id
        if offset:
            headers["Range"] = f"bytes={offset}-"

        with self._client.stream(
            "GET",
            f"{self.base_url}/api/v1/worker/jobs/{job_id}/source",
            headers=self._headers(headers),
        ) as response:
            if response.status_code == 416:      # диапазон уже весь получен
                offset = 0
                part.unlink(missing_ok=True)
                raise CenterError(416, "Range вне диапазона — начните скачивание заново")
            if response.status_code >= 400:
                response.read()
                self._raise(response)
            mode = "ab" if (offset and response.status_code == 206) else "wb"
            with part.open(mode) as fh:
                for chunk in response.iter_bytes(1024 * 1024):
                    fh.write(chunk)
        part.replace(dest)
        return dest.stat().st_size

    def post_events(
        self, job_id: str, attempt_id: str, first_seq: int,
        events: list[dict[str, Any]], execution_token: str
    ) -> dict[str, Any]:
        return self.request(
            "POST",
            "/api/v1/worker/events",
            json_body={
                "job_id": job_id,
                "attempt_id": attempt_id,
                "first_seq": first_seq,
                "count": len(events),
                "events": events,
            },
            headers={"X-Execution-Token": execution_token},
        )

    def create_upload(self, payload: dict[str, Any], execution_token: str) -> dict[str, Any]:
        return self.request(
            "POST",
            "/api/v1/worker/uploads",
            json_body=payload,
            headers={
                "X-Execution-Token": execution_token,
                # По хэшу архива: тот же архив — та же сессия загрузки.
                # Поле именно `expected_hash` — так оно называется в теле
                # запроса. Раньше здесь стояло `sha256`, которого в теле нет,
                # и ключ вырождался в один и тот же для любого архива попытки.
                "Idempotency-Key": (
                    f"upload:{payload.get('job_id', '')}:"
                    f"{payload.get('attempt_id', '')}:"
                    f"{payload.get('expected_hash', '')}"
                ),
            },
        )

    def get_upload(self, upload_id: str) -> dict[str, Any]:
        return self.request("GET", f"/api/v1/worker/uploads/{upload_id}")

    def put_chunk(
        self, upload_id: str, idx: int, data: bytes, sha256_hex: str,
        execution_token: str = "",
    ) -> dict[str, Any]:
        headers = {
            "X-Chunk-SHA256": sha256_hex,
            "Content-Type": "application/octet-stream",
        }
        # Токен попытки — как и на остальных ручках по заданию: отозванная
        # попытка не должна дописывать чанки в свою сессию.
        if execution_token:
            headers["X-Execution-Token"] = execution_token
        return self.request(
            "PUT",
            f"/api/v1/worker/uploads/{upload_id}/chunks/{idx}",
            content=data,
            headers=headers,
        )

    def complete_upload(
        self, upload_id: str, payload: dict[str, Any], execution_token: str
    ) -> dict[str, Any]:
        return self.request(
            "POST",
            f"/api/v1/worker/uploads/{upload_id}/complete",
            json_body=payload,
            headers={"X-Execution-Token": execution_token},
        )

    def get_commands(self) -> dict[str, Any]:
        return self.request("GET", "/api/v1/worker/commands")

    def ack_command(self, command_id: str, result: dict[str, Any]) -> dict[str, Any]:
        return self.request(
            "POST",
            f"/api/v1/worker/commands/{command_id}/ack",
            json_body={"result": result, "acknowledged_at": time.time()},
        )

    def reconcile(self, payload: dict[str, Any]) -> dict[str, Any]:
        return self.request("POST", "/api/v1/worker/reconcile", json_body=payload)


def backoff_delays(
    start: float = 1.0, cap: float = 30.0, jitter: float = 0.2
) -> Iterator[float]:
    """1→2→4→8→16→30 c джиттером ±20 %. Бесконечно: связь может не вернуться долго."""
    delay = start
    while True:
        spread = delay * jitter
        yield max(0.5, delay + random.uniform(-spread, spread))
        delay = min(cap, delay * 2)
