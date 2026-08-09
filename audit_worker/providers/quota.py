"""Нормализованный снимок лимита провайдера.

Главное правило модуля выражено в коде, а не в комментарии: сконструировать
`QuotaWindow` с процентом, не указав источник и достоверность, невозможно —
конструктор это отвергнет. Причина прозаична: «62 %» без ответа на вопрос
«откуда» ничем не отличается от выдуманного числа, а выдуманное число на этом
экране опаснее пустого места, потому что по нему принимают решения.

Три отдельные оси, которые легко перепутать:

  * `quota_state` — что сейчас с лимитом (готов / мало / упёрлись / неизвестно);
  * `source` — откуда сведения (официальный structured API … ручной ввод);
  * `confidence` — насколько источнику можно верить.

Их независимость важна: `limited` с `confidence="high"` (провайдер сам ответил
«rate limited») и `limited` с `confidence="low"` (мы так поняли текст) — разные
новости, и оператор обязан видеть разницу.
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Optional

# ─── Состояния (§12 задания) ─────────────────────────────────────────────────
QUOTA_READY = "ready"
QUOTA_LOW = "low"
QUOTA_LIMITED = "limited"
QUOTA_COOLDOWN = "cooldown"
QUOTA_AUTH_REQUIRED = "auth_required"
QUOTA_UNKNOWN = "unknown"
QUOTA_STALE = "stale"
QUOTA_ERROR = "error"
QUOTA_POLICY_BLOCKED = "policy_blocked"

QUOTA_STATES: tuple[str, ...] = (
    QUOTA_READY,
    QUOTA_LOW,
    QUOTA_LIMITED,
    QUOTA_COOLDOWN,
    QUOTA_AUTH_REQUIRED,
    QUOTA_UNKNOWN,
    QUOTA_STALE,
    QUOTA_ERROR,
    QUOTA_POLICY_BLOCKED,
)

# ─── Источники (§11 задания), от самого надёжного к самому слабому ───────────
SOURCE_OFFICIAL_STRUCTURED_API = "official_structured_api"
SOURCE_OFFICIAL_APP_SERVER_RPC = "official_app_server_rpc"
SOURCE_OFFICIAL_MACHINE_READABLE = "official_machine_readable"
SOURCE_OFFICIAL_DOCUMENTED_TEXT = "official_documented_text"
SOURCE_OBSERVED_RATE_LIMIT = "observed_rate_limit_response"
SOURCE_LOCAL_USAGE_STATS = "local_usage_statistics"
SOURCE_OPERATOR_MANUAL = "operator_manual"
#: Отдельное значение: официального способа НЕ существует. Это не «мы не
#: смогли» — это «спрашивать нечего», и оно не должно выглядеть как сбой.
SOURCE_UNAVAILABLE = "unavailable"

#: Порядок — он же приоритет. Меньше индекс = надёжнее источник.
QUOTA_SOURCE_PRIORITY: tuple[str, ...] = (
    SOURCE_OFFICIAL_STRUCTURED_API,
    SOURCE_OFFICIAL_APP_SERVER_RPC,
    SOURCE_OFFICIAL_MACHINE_READABLE,
    SOURCE_OFFICIAL_DOCUMENTED_TEXT,
    SOURCE_OBSERVED_RATE_LIMIT,
    SOURCE_LOCAL_USAGE_STATS,
    SOURCE_OPERATOR_MANUAL,
    SOURCE_UNAVAILABLE,
)


def source_rank(source: str) -> int:
    """Индекс приоритета. Неизвестный источник — хуже любого известного."""
    try:
        return QUOTA_SOURCE_PRIORITY.index(str(source))
    except ValueError:
        return len(QUOTA_SOURCE_PRIORITY)


# ─── Достоверность ───────────────────────────────────────────────────────────
CONFIDENCE_HIGH = "high"
CONFIDENCE_MEDIUM = "medium"
CONFIDENCE_LOW = "low"
CONFIDENCE_NONE = "none"

CONFIDENCE_LEVELS: tuple[str, ...] = (
    CONFIDENCE_HIGH,
    CONFIDENCE_MEDIUM,
    CONFIDENCE_LOW,
    CONFIDENCE_NONE,
)

# ─── Стабильность контракта источника ────────────────────────────────────────
#: Отдельная ось от `confidence`, и путать их нельзя. `confidence` отвечает
#: «можно ли верить ЭТОМУ числу», `stability` — «переживёт ли этот интерфейс
#: следующее обновление CLI». Пример, ради которого ось и заведена: Codex
#: `app-server` в собственном `--help` помечен `[experimental]`, а числа он
#: отдаёт первой стороной и структурой. Число достоверно; контракт — нет.
STABILITY_STABLE = "stable"
STABILITY_EXPERIMENTAL = "experimental"
STABILITY_UNDOCUMENTED = "undocumented"
STABILITY_NOT_APPLICABLE = "not_applicable"


class QuotaContractError(ValueError):
    """Попытка собрать снимок, нарушающий правила модуля."""


def _clamp_pct(value: Optional[float], *, field_name: str) -> Optional[float]:
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise QuotaContractError(f"{field_name}: не число ({value!r})") from exc
    if number != number:                                    # NaN
        raise QuotaContractError(f"{field_name}: NaN")
    # Провайдер может вернуть 100.4 при округлении на своей стороне. Зажимаем,
    # но НЕ превращаем мусор вроде -500 в ноль молча — такой ввод отвергается.
    if number < -1.0 or number > 101.0:
        raise QuotaContractError(f"{field_name}: {number} вне диапазона 0..100")
    return max(0.0, min(100.0, number))


@dataclass(frozen=True)
class QuotaWindow:
    """Одно окно лимита (§10 задания).

    `used_pct` и `remaining_pct` связаны: если известен один, второй
    вычисляется. Если неизвестны оба — окно всё равно имеет смысл, когда
    известен `reset_at`: «когда сбросится» — самостоятельная новость.
    """

    window_id: str
    source: str
    confidence: str
    used_pct: Optional[float] = None
    remaining_pct: Optional[float] = None
    reset_at: Optional[float] = None
    duration_sec: Optional[int] = None

    def __post_init__(self) -> None:
        if not str(self.window_id or "").strip():
            raise QuotaContractError("window_id обязателен")
        if self.source not in QUOTA_SOURCE_PRIORITY:
            raise QuotaContractError(f"неизвестный source={self.source!r}")
        if self.confidence not in CONFIDENCE_LEVELS:
            raise QuotaContractError(f"неизвестный confidence={self.confidence!r}")
        used = _clamp_pct(self.used_pct, field_name="used_pct")
        remaining = _clamp_pct(self.remaining_pct, field_name="remaining_pct")
        if used is not None and remaining is None:
            remaining = round(100.0 - used, 4)
        elif remaining is not None and used is None:
            used = round(100.0 - remaining, 4)
        # Процент без источника — ровно то, что запрещено §10. Ловим здесь, а
        # не «на ревью»: класс должен быть невозможно использовать неправильно.
        if (used is not None or remaining is not None) and self.source in (
            SOURCE_UNAVAILABLE,
        ):
            raise QuotaContractError(
                "процент указан при source=unavailable — источник обязателен"
            )
        if (used is not None or remaining is not None) and self.confidence == (
            CONFIDENCE_NONE
        ):
            raise QuotaContractError(
                "процент указан при confidence=none — так число неотличимо от выдуманного"
            )
        object.__setattr__(self, "used_pct", used)
        object.__setattr__(self, "remaining_pct", remaining)
        if self.reset_at is not None:
            try:
                object.__setattr__(self, "reset_at", float(self.reset_at))
            except (TypeError, ValueError) as exc:
                raise QuotaContractError(f"reset_at: не число ({self.reset_at!r})") from exc
        if self.duration_sec is not None:
            try:
                object.__setattr__(self, "duration_sec", int(self.duration_sec))
            except (TypeError, ValueError) as exc:
                raise QuotaContractError(
                    f"duration_sec: не число ({self.duration_sec!r})"
                ) from exc

    def as_dict(self) -> dict[str, Any]:
        return {
            "window_id": self.window_id,
            "used_pct": self.used_pct,
            "remaining_pct": self.remaining_pct,
            "reset_at": self.reset_at,
            "duration_sec": self.duration_sec,
            "source": self.source,
            "confidence": self.confidence,
        }


@dataclass(frozen=True)
class ProviderQuotaSnapshot:
    """Нормализованный снимок лимита одного провайдера на одном воркере."""

    provider: str
    quota_state: str
    observed_at: float
    source: str
    confidence: str
    auth_state: str
    account_group_id: Optional[str] = None
    stale_after: Optional[float] = None
    primary_window: Optional[QuotaWindow] = None
    secondary_windows: tuple[QuotaWindow, ...] = ()
    next_reset_at: Optional[float] = None
    estimated_remaining_pct: Optional[float] = None
    raw_remaining_supported: bool = False
    probe_error_code: Optional[str] = None
    #: Стабильность контракта источника, отдельно от достоверности значения.
    source_stability: str = STABILITY_NOT_APPLICABLE
    #: Версия разборщика. Обязательна для текстовых источников (§11): без неё
    #: невозможно понять, каким кодом получено значение в истории.
    parser_version: Optional[str] = None
    cli_version: Optional[str] = None
    #: Человекочитаемое пояснение «почему так». Показывается оператору.
    detail: Optional[str] = None

    def __post_init__(self) -> None:
        if self.quota_state not in QUOTA_STATES:
            raise QuotaContractError(f"неизвестный quota_state={self.quota_state!r}")
        if self.source not in QUOTA_SOURCE_PRIORITY:
            raise QuotaContractError(f"неизвестный source={self.source!r}")
        if self.confidence not in CONFIDENCE_LEVELS:
            raise QuotaContractError(f"неизвестный confidence={self.confidence!r}")
        remaining = _clamp_pct(
            self.estimated_remaining_pct, field_name="estimated_remaining_pct"
        )
        # Ключевой запрет §10: процент существует только вместе с источником,
        # который его выдал, и с признанием, что «сырой остаток» поддержан.
        if remaining is not None and not self.raw_remaining_supported:
            raise QuotaContractError(
                "estimated_remaining_pct задан при raw_remaining_supported=False: "
                "процент, не подтверждённый источником, показывать нельзя"
            )
        if remaining is not None and self.source == SOURCE_UNAVAILABLE:
            raise QuotaContractError(
                "estimated_remaining_pct при source=unavailable — откуда число?"
            )
        if self.raw_remaining_supported and remaining is None and self.quota_state in (
            QUOTA_READY,
            QUOTA_LOW,
        ):
            raise QuotaContractError(
                f"quota_state={self.quota_state} обещает известный остаток, "
                "но estimated_remaining_pct пуст"
            )
        object.__setattr__(self, "estimated_remaining_pct", remaining)
        object.__setattr__(self, "secondary_windows", tuple(self.secondary_windows or ()))

    # ─── Производные ────────────────────────────────────────────────────────
    def is_stale(self, *, now: Optional[float] = None) -> bool:
        if self.stale_after is None:
            return False
        return (now if now is not None else time.time()) > float(self.stale_after)

    def with_state(self, state: str, *, detail: Optional[str] = None) -> "ProviderQuotaSnapshot":
        data = self.as_dataclass_kwargs()
        data["quota_state"] = state
        if detail is not None:
            data["detail"] = detail
        return ProviderQuotaSnapshot(**data)

    def as_dataclass_kwargs(self) -> dict[str, Any]:
        return {
            "provider": self.provider,
            "quota_state": self.quota_state,
            "observed_at": self.observed_at,
            "source": self.source,
            "confidence": self.confidence,
            "auth_state": self.auth_state,
            "account_group_id": self.account_group_id,
            "stale_after": self.stale_after,
            "primary_window": self.primary_window,
            "secondary_windows": self.secondary_windows,
            "next_reset_at": self.next_reset_at,
            "estimated_remaining_pct": self.estimated_remaining_pct,
            "raw_remaining_supported": self.raw_remaining_supported,
            "probe_error_code": self.probe_error_code,
            "source_stability": self.source_stability,
            "parser_version": self.parser_version,
            "cli_version": self.cli_version,
            "detail": self.detail,
        }

    def as_dict(self) -> dict[str, Any]:
        return {
            "provider": self.provider,
            "account_group_id": self.account_group_id,
            "quota_state": self.quota_state,
            "observed_at": self.observed_at,
            "source": self.source,
            "confidence": self.confidence,
            "source_stability": self.source_stability,
            "parser_version": self.parser_version,
            "cli_version": self.cli_version,
            "stale_after": self.stale_after,
            "primary_window": self.primary_window.as_dict() if self.primary_window else None,
            "secondary_windows": [w.as_dict() for w in self.secondary_windows],
            "next_reset_at": self.next_reset_at,
            "estimated_remaining_pct": self.estimated_remaining_pct,
            "raw_remaining_supported": self.raw_remaining_supported,
            "auth_state": self.auth_state,
            "probe_error_code": self.probe_error_code,
            "detail": self.detail,
        }


def unknown_snapshot(
    provider: str,
    *,
    auth_state: str,
    reason: str,
    observed_at: Optional[float] = None,
    probe_error_code: Optional[str] = None,
    cli_version: Optional[str] = None,
    source: str = SOURCE_UNAVAILABLE,
    quota_state: str = QUOTA_UNKNOWN,
) -> ProviderQuotaSnapshot:
    """Честный «ничего не знаем».

    Отдельная фабрика нужна, чтобы такой снимок нельзя было случайно собрать
    «почти правильно»: у него по построению нет процента, нет окна и нет
    достоверности выше `none`.
    """
    return ProviderQuotaSnapshot(
        provider=provider,
        quota_state=quota_state,
        observed_at=observed_at if observed_at is not None else time.time(),
        source=source,
        confidence=CONFIDENCE_NONE,
        auth_state=auth_state,
        raw_remaining_supported=False,
        estimated_remaining_pct=None,
        probe_error_code=probe_error_code,
        cli_version=cli_version,
        detail=reason,
    )


def limited_snapshot(
    provider: str,
    *,
    auth_state: str,
    source: str,
    reason: str,
    reset_at: Optional[float] = None,
    observed_at: Optional[float] = None,
    confidence: str = CONFIDENCE_HIGH,
    cli_version: Optional[str] = None,
    source_stability: str = STABILITY_STABLE,
) -> ProviderQuotaSnapshot:
    """«Упёрлись в лимит», но процент неизвестен (§10).

    Отдельная фабрика, а не `unknown_snapshot(quota_state="limited")`: факт
    отказа по лимиту сообщён самим провайдером и достоверен, даже когда числа
    нет. Схлопнуть это в `confidence="none"` значило бы приравнять твёрдый факт
    к незнанию — и «лимит исчерпан» перестало бы отличаться от «мы не смотрели».
    """
    return ProviderQuotaSnapshot(
        provider=provider,
        quota_state=QUOTA_LIMITED,
        observed_at=observed_at if observed_at is not None else time.time(),
        source=source,
        confidence=confidence,
        auth_state=auth_state,
        # Процента нет и не будет: раз провайдер его не сообщил, выводить
        # «0 % осталось» нельзя — окно могло почти сброситься.
        raw_remaining_supported=False,
        estimated_remaining_pct=None,
        next_reset_at=reset_at,
        cli_version=cli_version,
        source_stability=source_stability,
        detail=reason,
    )


def apply_low_threshold(
    snapshot: ProviderQuotaSnapshot, *, low_threshold_pct: Optional[float]
) -> ProviderQuotaSnapshot:
    """Перевести `ready` в `low`, если остаток ниже НАСТРОЕННОГО порога.

    Порог обязателен: без него `low` не вычисляется вовсе (§12). Значение по
    умолчанию задаётся конфигурацией и документируется — здесь его нет
    намеренно, чтобы «порог по умолчанию» не появился в двух местах сразу.
    """
    if low_threshold_pct is None:
        return snapshot
    if snapshot.quota_state != QUOTA_READY:
        return snapshot
    if snapshot.estimated_remaining_pct is None:
        return snapshot
    if snapshot.estimated_remaining_pct > float(low_threshold_pct):
        return snapshot
    return snapshot.with_state(
        QUOTA_LOW,
        detail=(
            f"остаток {snapshot.estimated_remaining_pct:.1f}% ≤ порога "
            f"{float(low_threshold_pct):.1f}%"
        ),
    )
