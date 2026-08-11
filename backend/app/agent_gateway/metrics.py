"""Low-cardinality in-process metrics for gateway tests and future export."""
from __future__ import annotations

import threading
from collections import Counter
from typing import Mapping


METRIC_NAMES = frozenset(
    {
        "connections_total", "connection_rejects", "heartbeats_total",
        "job_offers_total", "job_accepts_total", "job_declines_total",
        "event_batches_total", "event_duplicates_total", "event_gap_count",
        "cancel_commands_total", "result_ready_total", "result_ack_total",
        "result_reject_total", "stream_disconnects", "protocol_errors",
        "queue_rejections", "active_connections",
    }
)
ALLOWED_LABELS = frozenset({"protocol_version", "reason", "result"})


class GatewayMetrics:
    def __init__(self) -> None:
        self._values: Counter[tuple[str, tuple[tuple[str, str], ...]]] = Counter()
        self._lock = threading.Lock()

    def inc(self, name: str, *, labels: Mapping[str, str | int] | None = None, value: int = 1) -> None:
        if name not in METRIC_NAMES:
            raise ValueError(f"unknown gateway metric {name}")
        normalized = tuple(sorted((str(key), str(item)[:32]) for key, item in (labels or {}).items()))
        if any(key not in ALLOWED_LABELS for key, _ in normalized):
            raise ValueError("high-cardinality gateway metric label rejected")
        with self._lock:
            self._values[(name, normalized)] += int(value)

    def set_active(self, count: int) -> None:
        with self._lock:
            for key in list(self._values):
                if key[0] == "active_connections":
                    del self._values[key]
            self._values[("active_connections", ())] = max(0, int(count))

    def snapshot(self) -> dict[str, int]:
        with self._lock:
            return {
                name + ("{" + ",".join(f"{k}={v}" for k, v in labels) + "}" if labels else ""): value
                for (name, labels), value in self._values.items()
            }
