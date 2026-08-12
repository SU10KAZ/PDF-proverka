"""Ограничение частоты заявок на регистрацию воркера.

Почему это отдельный модуль, а не пара счётчиков в памяти. Эндпоинт
`POST /api/v1/worker/register` публичный по необходимости: воркер приходит сам,
и до появления у него токена другого способа нет. Единственная защита до сих
пор — bootstrap-секрет, а его перебор ничем не ограничивался (§32.6 отчёта 05,
§13 отчёта 03).

Готового механизма в проекте нет — проверено: в `backend/app` есть четыре
middleware (`CurrentObjectMiddleware`, gzip, `PortalAuthMiddleware`,
`ActionLogMiddleware`), и ни один из них не ограничивает частоту; внешних
библиотек вроде `slowapi` в зависимостях тоже нет. Счётчик в памяти процесса
здесь не годится по двум причинам, и обе не гипотетические: он обнуляется
рестартом backend (а рестарт делает вотчдог), и он не общий для нескольких
воркеров uvicorn. Поэтому состояние живёт в той же `workers.db`, что и всё
остальное состояние подсистемы, — переживает рестарт и общий для процессов.

Что здесь НЕ делается намеренно:

* **лимит не подсказывает, существует ли instance_id.** Ответ 429 одинаков для
  известного и неизвестного значения, ключи хранятся хэшами, в тексте ответа
  нет ни IP, ни instance_id. Иначе ограничитель сам стал бы оракулом
  существования (§2.4 задания);
* **лимит не заменяет bootstrap-секрет.** Он проверяется всегда и ПОСЛЕ
  ограничителя: иначе перебор секрета оставался бы неограниченным;
* **окно фиксированное, а не скользящее.** Скользящее требует хранить каждую
  попытку; фиксированное окно даёт худший случай «двойной лимит на стыке
  окон», и для защиты от перебора этого достаточно.
"""
from __future__ import annotations

import hashlib
import math
import time
from dataclasses import dataclass
from typing import Any, Optional

from backend.app.services.distributed_workers import database
from backend.app.services.distributed_workers.settings import DistributedWorkersSettings

#: Записи старше этого числа окон удаляются на любой записи счётчика.
_RETENTION_WINDOWS = 4

SCOPE_IP = "ip"
SCOPE_IP_INSTANCE = "ip_instance"


@dataclass(frozen=True)
class RateDecision:
    """Решение ограничителя. `scope` нужен для журнала, но не для ответа клиенту."""

    allowed: bool
    scope: Optional[str] = None
    retry_after_sec: int = 0
    limit: int = 0
    window_sec: int = 0

    @property
    def message(self) -> str:
        return (
            "Слишком много заявок на регистрацию. Повторите позже."
            if not self.allowed
            else "ok"
        )


def _key(*parts: str) -> str:
    """Ключ окна — хэш. В базе не должно лежать ни IP, ни instance_id открытым."""
    raw = "\x00".join(str(p or "") for p in parts)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _normalize_ip(value: Optional[str]) -> str:
    text = (value or "").strip()
    return text or "unknown"


def check_and_consume(
    *,
    source_ip: Optional[str],
    instance_id: str,
    settings: DistributedWorkersSettings,
    now: Optional[float] = None,
) -> RateDecision:
    """Списать одну попытку регистрации. Возвращает решение.

    Считается КАЖДОЕ обращение, включая те, что потом отобьются неверным
    секретом: иначе перебор секрета не ограничивался бы вовсе. Обратная сторона
    — поток неверных заявок с одного адреса временно закрывает регистрацию и
    легальному воркеру с того же адреса. Сторона ошибки выбрана осознанно:
    отложенная регистрация стоит ожидания, подобранный секрет — чужого доступа.
    """
    window = max(1, int(settings.registration_rate_window_sec))
    per_ip = int(settings.registration_rate_max_per_ip)
    per_pair = int(settings.registration_rate_max_per_instance)
    if per_ip <= 0 and per_pair <= 0:
        # Оба лимита выключены явной настройкой — ограничителя нет.
        return RateDecision(allowed=True)

    moment = time.time() if now is None else now
    window_start = math.floor(moment / window) * window
    ip = _normalize_ip(source_ip)
    buckets: list[tuple[str, str, int]] = []
    if per_pair > 0:
        buckets.append((SCOPE_IP_INSTANCE, _key(ip, instance_id), per_pair))
    if per_ip > 0:
        buckets.append((SCOPE_IP, _key(ip), per_ip))

    with database.write_txn(settings) as conn:
        conn.execute(
            "DELETE FROM registration_rate_limit WHERE window_start < ?",
            (window_start - _RETENTION_WINDOWS * window,),
        )
        # Сначала ПРОВЕРЯЕМ все корзины, потом инкрементируем: иначе отказ по
        # второй корзине оставлял бы первую уже списанной, и один запрос
        # съедал бы квоту дважды.
        for scope, key, limit in buckets:
            row = conn.execute(
                "SELECT count FROM registration_rate_limit "
                "WHERE scope = ? AND key = ? AND window_start = ?",
                (scope, key, window_start),
            ).fetchone()
            used = int(row["count"]) if row is not None else 0
            if used >= limit:
                return RateDecision(
                    allowed=False,
                    scope=scope,
                    retry_after_sec=max(1, int(window_start + window - moment)),
                    limit=limit,
                    window_sec=window,
                )
        for scope, key, _limit in buckets:
            conn.execute(
                "INSERT INTO registration_rate_limit "
                "(scope, key, window_start, count, updated_at) VALUES (?,?,?,1,?) "
                "ON CONFLICT(scope, key, window_start) DO UPDATE SET "
                "count = count + 1, updated_at = excluded.updated_at",
                (scope, key, window_start, moment),
            )
    return RateDecision(allowed=True, window_sec=window)


def snapshot(*, settings: DistributedWorkersSettings) -> list[dict[str, Any]]:
    """Содержимое счётчиков — для тестов и диагностики. Ключи остаются хэшами."""
    with database.read_conn(settings) as conn:
        rows = conn.execute(
            "SELECT scope, key, window_start, count, updated_at "
            "FROM registration_rate_limit ORDER BY window_start DESC, scope"
        ).fetchall()
    return [dict(row) for row in rows]
