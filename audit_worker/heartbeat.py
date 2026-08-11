"""Фоновый heartbeat: живость, ресурсы, активные задания.

Отдельный поток — принципиально: heartbeat обязан идти, пока задание считается,
иначе центр увидит молчание работающего воркера. При этом провал heartbeat
НИКОГДА не останавливает задание (I-01): ошибки логируются и уходят в backoff.
"""
from __future__ import annotations

import threading
import time
from typing import Any, Callable, Optional

from audit_worker.client import CenterClient, backoff_delays


class HeartbeatClient:
    def __init__(
        self,
        client: CenterClient,
        *,
        interval_sec: float,
        build_payload: Callable[[], dict[str, Any]],
        on_response: Optional[Callable[[dict[str, Any]], None]] = None,
        on_error: Optional[Callable[[Exception], None]] = None,
    ):
        self._client = client
        self._interval = max(5.0, interval_sec)
        self._build_payload = build_payload
        self._on_response = on_response
        self._on_error = on_error
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self.last_ok_at: Optional[float] = None
        self.last_error: Optional[str] = None
        self.consecutive_failures = 0

    def start(self) -> None:
        if self._thread is not None:
            return
        self._thread = threading.Thread(target=self._loop, name="heartbeat", daemon=True)
        self._thread.start()

    def stop(self, timeout: float = 5.0) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=timeout)
            self._thread = None

    def beat_once(self) -> Optional[dict[str, Any]]:
        payload = self._build_payload()
        payload.setdefault("sent_at", time.time())
        response = self._client.heartbeat(payload)
        self.last_ok_at = time.time()
        self.last_error = None
        self.consecutive_failures = 0
        if self._on_response:
            self._on_response(response)
        return response

    def _loop(self) -> None:
        delays = backoff_delays()
        while not self._stop.is_set():
            try:
                self.beat_once()
                # A streaming transport can negotiate the CenterHello policy.
                # Polling clients do not expose this property and retain the
                # configured legacy interval.
                wait = max(
                    5.0,
                    float(getattr(self._client, "heartbeat_interval_sec", self._interval)),
                )
                delays = backoff_delays()
            except Exception as exc:  # noqa: BLE001 — сеть не повод падать
                self.consecutive_failures += 1
                self.last_error = str(exc)
                if self._on_error:
                    self._on_error(exc)
                wait = next(delays)
            self._stop.wait(wait)
