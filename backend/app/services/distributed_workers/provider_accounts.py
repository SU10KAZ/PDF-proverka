"""Учётные записи подписок и наблюдение за лимитами на ЦЕНТРЕ.

Четыре правила модуля, каждое — ответ на конкретный способ соврать оператору.

1. НИКАКИХ ВЫДУМАННЫХ ПРОЦЕНТОВ. Снимок от воркера — полу-доверенный ввод; он
   проходит `sanitize_provider_snapshot`, где процент принимается ТОЛЬКО
   вместе с признанием источника (`raw_remaining_supported`) и допустимым
   `source`/`confidence`. Всё остальное превращается в `None`. Ноль и сто —
   не «значения по умолчанию», их здесь взять неоткуда.

2. ОДИН АККАУНТ — ОДИН РЕСУРС. Два VPS, вошедшие в одну подписку, дают ДВА
   снимка одного и того же лимита. Складывать их нельзя: 40 % + 40 % ≠ 80 %,
   это одни и те же 40 %, увиденные дважды. `reconcile_group` выбирает ОДИН
   снимок по объявленной политике, а не агрегирует (§15).

3. ДВЕ ДАТЫ СБРОСА ЖИВУТ ОТДЕЛЬНО. Наблюдаемая приходит от провайдера, ручную
   ставит оператор. Ни одна не перетирает другую, и при расхождении обе видны
   (§13). Автоматика не имеет права «поправить» человека.

4. ПРЕДУПРЕЖДЕНИЕ О СГОРАЮЩЕМ ЛИМИТЕ — только на фактах. `reset_soon_unused`
   зажигается либо когда остаток ДЕЙСТВИТЕЛЬНО известен и велик, либо когда
   оператор сам пометил аккаунт как неиспользованный. Предупреждать «у вас
   пропадает лимит» по неизвестному остатку значит звать человека впустую
   (§23).
"""
from __future__ import annotations

import json
import math
import re
import time
import uuid
from typing import Any, Iterable, Optional

from backend.app.services.distributed_workers import database
from backend.app.services.distributed_workers.settings import DistributedWorkersSettings

# ─── Закрытые словари. Всё, чего здесь нет, приходит от воркера как «unknown» ─
#: OpenRouter добавлен на 11J. До него `sanitize_provider_snapshot` ВЫБРАСЫВАЛА
#: его состояние молча: воркер сообщал «ключ есть / ключа нет», а карточка VPS
#: этого не показывала — то есть единственный способ узнать о нехватке ключа
#: оставался отказ в назначении задания, уже после сборки пакета.
#:
#: `account_group_id` (§21 задания) для него означает то же, что и для
#: подписок: непрозрачный идентификатор, которым ОПЕРАТОР помечает воркеры,
#: делящие один платёжный счёт. Ключа он не содержит и вывести его из ключа
#: нельзя — соответствие задаёт человек.
PROVIDERS: tuple[str, ...] = ("claude", "codex", "openrouter")

QUOTA_STATES: tuple[str, ...] = (
    "ready", "low", "limited", "cooldown", "auth_required",
    "unknown", "stale", "error", "policy_blocked",
)
QUOTA_SOURCES: tuple[str, ...] = (
    "official_structured_api", "official_app_server_rpc",
    "official_machine_readable", "official_documented_text",
    "observed_rate_limit_response", "local_usage_statistics",
    "operator_manual", "unavailable",
)
CONFIDENCE_LEVELS: tuple[str, ...] = ("high", "medium", "low", "none")
#: Коды причины «почему остаток такой или почему его нет». Закрытый список —
#: чтобы объяснение оператору собиралось ЗДЕСЬ, из словаря, а не приезжало
#: свободным текстом с полу-доверенного воркера прямо в браузер.
QUOTA_REASON_CODES: tuple[str, ...] = (
    "local_cache_available",
    "local_cache_stale",
    "local_cache_missing",
    "local_cache_schema_unsupported",
    "no_safe_supported_source",
)
SOURCE_STABILITY: tuple[str, ...] = (
    "stable", "experimental", "undocumented", "not_applicable",
)
#: Состояния установки, принимаемые ОТ ВОРКЕРА. Кроме трёх исходов настоящего
#: наблюдения сюда входит `not_observed` — «опроса ещё не было».
#:
#: Сегодня оно не приходит: воркер шлёт совместимую с работающим шлюзом
#: заглушку `missing` (принятый остаточный дефект 12I.3). Список расширен
#: заранее и намеренно — иначе к моменту выкатки шлюза честное значение
#: молча схлопнулось бы обратно в `missing` уже ЗДЕСЬ, и правка на проводе
#: оказалась бы бесполезной, а обнаружилось бы это только в бою.
INSTALLATION_STATES: tuple[str, ...] = (
    "installed", "missing", "broken", "not_observed",
)
AUTH_STATES: tuple[str, ...] = ("logged_in", "logged_out", "expired", "unknown", "error")
POLICY_STATES: tuple[str, ...] = ("allowed", "review_required", "policy_blocked")
ACCOUNT_KINDS: tuple[str, ...] = (
    "subscription_personal", "subscription_team", "subscription_enterprise",
    "commercial_api", "unknown",
)

#: Состояния, в которых остаток В ПРИНЦИПЕ не может быть известен. Если воркер
#: прислал процент вместе с одним из них — это рассогласование, и процент
#: отбрасывается: доверять «62 % при auth_required» нельзя.
STATES_WITHOUT_REMAINING: frozenset[str] = frozenset(
    {"auth_required", "unknown", "error", "policy_blocked"}
)

DEFAULT_WARNING_DAYS: tuple[int, ...] = (7, 3, 1)

#: Потолок числа АВТОМАТИЧЕСКИ заводимых учётных записей. У
#: `subscription_accounts` нет своего пруннинга, а группа приходит от
#: полу-доверенного воркера: без потолка агент, меняющий `account_group_id`
#: в каждом heartbeat, за сутки создал бы тысячи строк, каждая из которых
#: требует решения человека. Записи, заведённые ОПЕРАТОРОМ через API, под
#: потолок не попадают — там источник доверенный.
MAX_AUTO_ACCOUNTS = 64

#: Предел размера `capability` от воркера. Это было единственное поле
#: снимка, принимавшееся целиком любого размера: 2 МБ от воркера ложились
#: в базу и возвращались браузеру оператора дословно.
_MAX_CAPABILITY_BYTES = 8192
_MAX_CAPABILITY_KEYS = 40

#: Ограничители длины операторских строк. Не про XSS (экранирование —
#: обязанность отображения), а про размер строки в базе и в ответе API.
_MAX_DISPLAY_NAME = 120
_MAX_NOTES = 4000
_MAX_GROUP_ID = 64
_MAX_LABEL = 120
_MAX_TZ = 64

#: `account_group_id` попадает в ключ уникальности и в URL. Закрытый алфавит —
#: не косметика: он же исключает подстановку разделителей и управляющих
#: символов в идентификатор, по которому потом сходятся два VPS.
_GROUP_ID_RE = re.compile(r"^[a-z0-9][a-z0-9._-]{0,63}$")


class ProviderAccountError(ValueError):
    """Некорректный ввод оператора. Наверх уходит как 400, а не 500."""


# ─── Санитизация ввода от воркера ────────────────────────────────────────────
def _enum(value: Any, allowed: tuple[str, ...], default: str) -> str:
    text = str(value or "").strip()
    return text if text in allowed else default


def _opt_str(value: Any, limit: int = 200) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    return text[:limit]


def _opt_float(value: Any) -> Optional[float]:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(number) or math.isinf(number):
        return None
    return number


def _pct(value: Any) -> Optional[float]:
    number = _opt_float(value)
    if number is None:
        return None
    if number < 0.0 or number > 100.0:
        # Не зажимаем: значение вне диапазона означает, что разошёлся контракт,
        # и «зажатое» число выглядело бы достоверным, не будучи им.
        return None
    return round(number, 2)


def _epoch(value: Any) -> Optional[float]:
    number = _opt_float(value)
    if number is None:
        return None
    # Отсекаем заведомо бессмысленные метки: до 2020 и после 2100. Такое
    # значение на экране («сброс 12.03.1970») хуже отсутствия.
    if number < 1_577_836_800 or number > 4_102_444_800:
        return None
    return number


def _bounded_capability(raw: Any) -> dict[str, Any]:
    """`capability` от воркера — с ограничением размера и глубины.

    Единственное поле снимка, которое раньше принималось целиком: воркер мог
    прислать мегабайты, они ложились в `capability_json` и возвращались
    браузеру оператора дословно. Ограничитель числа ЭЛЕМЕНТОВ в модели
    (`max_length=8`) от этого не спасал — он считает провайдеров, а не байты.
    """
    if not isinstance(raw, dict):
        return {}
    out: dict[str, Any] = {}
    for key in sorted(raw)[:_MAX_CAPABILITY_KEYS]:
        value = raw[key]
        if isinstance(value, (str, int, float, bool)) or value is None:
            out[str(key)[:64]] = value if not isinstance(value, str) else value[:200]
        elif isinstance(value, dict):
            # Один уровень вложенности: этого хватает на `provider_home`,
            # а произвольную глубину принимать незачем.
            out[str(key)[:64]] = {
                str(k)[:64]: (v[:200] if isinstance(v, str) else v)
                for k, v in list(value.items())[:_MAX_CAPABILITY_KEYS]
                if isinstance(v, (str, int, float, bool)) or v is None
            }
    try:
        if len(json.dumps(out, ensure_ascii=False).encode("utf-8")) > _MAX_CAPABILITY_BYTES:
            return {"truncated": True}
    except (TypeError, ValueError):
        return {}
    return out


def sanitize_quota_window(raw: Any) -> Optional[dict[str, Any]]:
    if not isinstance(raw, dict):
        return None
    window_id = _opt_str(raw.get("window_id"), 64)
    if not window_id:
        return None
    source = _enum(raw.get("source"), QUOTA_SOURCES, "unavailable")
    confidence = _enum(raw.get("confidence"), CONFIDENCE_LEVELS, "none")
    used = _pct(raw.get("used_pct"))
    remaining = _pct(raw.get("remaining_pct"))
    if source == "unavailable" or confidence == "none":
        # Процент без источника или без достоверности — ровно то, что запрещено.
        used = remaining = None
    duration = _opt_float(raw.get("duration_sec"))
    return {
        "window_id": window_id,
        "used_pct": used,
        "remaining_pct": remaining,
        "reset_at": _epoch(raw.get("reset_at")),
        "duration_sec": int(duration) if duration is not None and duration >= 0 else None,
        "source": source,
        "confidence": confidence,
    }


def sanitize_quota(raw: Any, *, provider: str) -> dict[str, Any]:
    """Привести снимок квоты от воркера к безопасному виду.

    Полу-доверенный ввод: воркер может ошибиться, отстать версией или быть
    скомпрометированным. Поэтому здесь не «проверка на всякий случай», а
    пересборка объекта из разрешённых значений.
    """
    data = raw if isinstance(raw, dict) else {}
    state = _enum(data.get("quota_state"), QUOTA_STATES, "unknown")
    source = _enum(data.get("source"), QUOTA_SOURCES, "unavailable")
    confidence = _enum(data.get("confidence"), CONFIDENCE_LEVELS, "none")
    raw_supported = bool(data.get("raw_remaining_supported"))
    remaining = _pct(data.get("estimated_remaining_pct"))

    # Три независимых условия, при любом из которых процент не показывается.
    if not raw_supported or source == "unavailable" or confidence == "none":
        remaining = None
    if state in STATES_WITHOUT_REMAINING:
        remaining = None
        raw_supported = False
    if remaining is None:
        raw_supported = False
        # `ready`/`low` обещают известный остаток. Без остатка это `unknown`:
        # иначе экран сказал бы «готов», не имея на то оснований.
        if state in ("ready", "low"):
            state = "unknown"

    primary = sanitize_quota_window(data.get("primary_window"))
    secondaries = [
        w for w in (
            sanitize_quota_window(item)
            for item in (data.get("secondary_windows") or [])[:12]
        ) if w is not None
    ]
    return {
        "provider": provider,
        "quota_state": state,
        # Через `_epoch`, а не `_opt_float`: именно это значение решает, протух
        # ли снимок. Метка из будущего (сбитые часы VPS или подставленное
        # `1e18`) сделала бы снимок вечно свежим, и «остаток 88 %» показывался
        # бы как текущий бессрочно.
        "observed_at": _epoch(data.get("observed_at")),
        "source": source,
        "confidence": confidence,
        "source_stability": _enum(
            data.get("source_stability"), SOURCE_STABILITY, "not_applicable"
        ),
        "parser_version": _opt_str(data.get("parser_version"), 64),
        "cli_version": _opt_str(data.get("cli_version"), 64),
        "stale_after": _opt_float(data.get("stale_after")),
        "primary_window": primary,
        "secondary_windows": secondaries,
        "next_reset_at": _epoch(data.get("next_reset_at")),
        "estimated_remaining_pct": remaining,
        "raw_remaining_supported": raw_supported,
        "auth_state": _enum(data.get("auth_state"), AUTH_STATES, "unknown"),
        "probe_error_code": _opt_str(data.get("probe_error_code"), 64),
        "detail": _opt_str(data.get("detail"), 600),
        # Необязательное поле: старые воркеры его не шлют, и это не ошибка —
        # тогда интерфейс выводит причину из состояния снимка сам.
        "reason_code": (
            _enum(data.get("reason_code"), QUOTA_REASON_CODES, "")
            if data.get("reason_code") else None
        ) or None,
    }


def sanitize_provider_snapshot(raw: Any) -> Optional[dict[str, Any]]:
    """Один элемент `heartbeat.providers`. `None`, если провайдер неизвестен."""
    if not isinstance(raw, dict):
        return None
    provider = str(raw.get("provider") or "").strip().lower()
    if provider not in PROVIDERS:
        return None
    capability = _bounded_capability(raw.get("capability"))
    quota = sanitize_quota(raw.get("quota"), provider=provider)
    group = normalize_group_id(raw.get("account_group_id"), allow_empty=True)
    return {
        "provider": provider,
        "account_group_id": group,
        "installation_status": _enum(
            raw.get("installation_status"), INSTALLATION_STATES, "missing"
        ),
        "cli_version": _opt_str(raw.get("cli_version"), 64),
        "auth_state": _enum(raw.get("auth_state"), AUTH_STATES, "unknown"),
        "auth_method": _opt_str(raw.get("auth_method"), 64) or "none",
        "plan_type": _opt_str(raw.get("plan_type"), 64),
        "policy_state": _enum(raw.get("policy_state"), POLICY_STATES, "allowed"),
        "inference_allowed": bool(raw.get("inference_allowed")),
        "account_fingerprint": _opt_str(raw.get("account_fingerprint"), 64),
        "credential_present": bool(raw.get("credential_present")),
        "credential_mode": _opt_str(raw.get("credential_mode"), 8),
        "credential_insecure": bool(
            raw.get("credential_world_readable") or raw.get("credential_group_readable")
        ),
        "capability": capability,
        "error_code": _opt_str(raw.get("error_code"), 64),
        "detail": _opt_str(raw.get("detail"), 600),
        # Границы те же, что у квоты: метка времени решает вопрос свежести.
        "observed_at": _epoch(raw.get("observed_at")),
        "last_auth_check_at": _epoch(raw.get("last_auth_check_at")),
        "last_quota_check_at": _epoch(raw.get("last_quota_check_at")),
        "quota": quota,
    }


def normalize_group_id(value: Any, *, allow_empty: bool = False) -> Optional[str]:
    """Привести `account_group_id` к каноническому виду или отвергнуть."""
    if value is None:
        if allow_empty:
            return None
        raise ProviderAccountError("account_group_id обязателен")
    text = str(value).strip().lower()
    if not text:
        if allow_empty:
            return None
        raise ProviderAccountError("account_group_id пуст")
    if len(text) > _MAX_GROUP_ID or not _GROUP_ID_RE.match(text):
        if allow_empty:
            # Мусор от воркера просто игнорируется: это не повод отвергать
            # весь heartbeat.
            return None
        raise ProviderAccountError(
            "account_group_id: допустимы латиница, цифры, точка, дефис и "
            "подчёркивание; 1..64 символа (например claude-account-01)"
        )
    return text


# ─── Запись состояния провайдеров ────────────────────────────────────────────
def record_worker_providers(
    *,
    worker_id: str,
    snapshots: Iterable[Any],
    settings: DistributedWorkersSettings,
    now: Optional[float] = None,
) -> list[dict[str, Any]]:
    """Сохранить состояние провайдеров воркера. Вызывается из heartbeat.

    Никогда не бросает наружу из-за содержимого снимка: провайдерская новость
    не имеет права уронить heartbeat (§27). Некорректный элемент отбрасывается
    молча, корректные сохраняются.
    """
    moment = float(now if now is not None else time.time())
    clean = [s for s in (sanitize_provider_snapshot(x) for x in (snapshots or [])) if s]
    if not clean:
        return []
    with database.write_txn(settings) as conn:
        for snap in clean:
            quota = snap["quota"]
            conn.execute(
                """
                INSERT INTO worker_provider_states (
                    worker_id, provider, account_group_id, installation_status,
                    cli_version, auth_state, auth_method, plan_type, policy_state,
                    inference_allowed, account_fingerprint, credential_present,
                    credential_mode, credential_insecure, capability_json,
                    quota_json, quota_state, quota_source, quota_confidence,
                    remaining_pct, observed_next_reset_at, error_code, detail,
                    observed_at, reported_at, account_group_source
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,'worker')
                ON CONFLICT(worker_id, provider) DO UPDATE SET
                    -- Привязка к учётной записи — РЕШЕНИЕ ОПЕРАТОРА, и heartbeat
                    -- его не отменяет. Раньше здесь стояло
                    -- `excluded.account_group_id`, и всё работало ровно один
                    -- такт: воркер, у которого переменная группы не задана (а
                    -- это умолчание), присылал NULL и стирал то, что человек
                    -- только что назначил через API.
                    --
                    -- Заодно это закрывает вторую дыру: воркер больше не может
                    -- объявить СЕБЯ участником чужой учётной записи и стать по
                    -- ней источником истины на экране. Значение из снимка
                    -- принимается только когда центр ещё ничего не знает.
                    account_group_id=COALESCE(
                        worker_provider_states.account_group_id,
                        excluded.account_group_id
                    ),
                    -- Происхождение не понижается: раз привязку сделал
                    -- оператор, heartbeat не превращает её в самопривязку.
                    account_group_source=CASE
                        WHEN worker_provider_states.account_group_source='operator'
                            THEN 'operator' ELSE excluded.account_group_source
                    END,
                    installation_status=excluded.installation_status,
                    cli_version=excluded.cli_version,
                    auth_state=excluded.auth_state,
                    auth_method=excluded.auth_method,
                    plan_type=excluded.plan_type,
                    policy_state=excluded.policy_state,
                    inference_allowed=excluded.inference_allowed,
                    account_fingerprint=excluded.account_fingerprint,
                    credential_present=excluded.credential_present,
                    credential_mode=excluded.credential_mode,
                    credential_insecure=excluded.credential_insecure,
                    capability_json=excluded.capability_json,
                    quota_json=excluded.quota_json,
                    quota_state=excluded.quota_state,
                    quota_source=excluded.quota_source,
                    quota_confidence=excluded.quota_confidence,
                    remaining_pct=excluded.remaining_pct,
                    observed_next_reset_at=excluded.observed_next_reset_at,
                    error_code=excluded.error_code,
                    detail=excluded.detail,
                    observed_at=excluded.observed_at,
                    reported_at=excluded.reported_at
                """,
                (
                    worker_id, snap["provider"], snap["account_group_id"],
                    snap["installation_status"], snap["cli_version"],
                    snap["auth_state"], snap["auth_method"], snap["plan_type"],
                    snap["policy_state"], 1 if snap["inference_allowed"] else 0,
                    snap["account_fingerprint"], 1 if snap["credential_present"] else 0,
                    snap["credential_mode"], 1 if snap["credential_insecure"] else 0,
                    json.dumps(snap["capability"], ensure_ascii=False),
                    json.dumps(quota, ensure_ascii=False),
                    quota["quota_state"], quota["source"], quota["confidence"],
                    quota["estimated_remaining_pct"], quota["next_reset_at"],
                    snap["error_code"], snap["detail"],
                    quota["observed_at"] or snap["observed_at"] or moment,
                    moment,
                ),
            )
        # Аккаунт заводится автоматически ТОЛЬКО под ту группу, которая уже
        # закреплена за этой парой воркер+провайдер, — то есть под назначенную
        # оператором либо принятую при первом знакомстве. Читаем из таблицы,
        # а не из снимка: иначе воркер, меняющий группу в каждом heartbeat,
        # заводил бы по новой учётной записи каждые тридцать секунд, и каждая
        # требовала бы решения человека.
        #
        # Предел числа автозаводимых записей — отдельный рубеж на тот же
        # случай: у `subscription_accounts` нет своего пруннинга, и раздуть её
        # некому, кроме этого места.
        stored = {
            (row["provider"], row["account_group_id"])
            for row in conn.execute(
                "SELECT provider, account_group_id FROM worker_provider_states "
                "WHERE worker_id=? AND account_group_id IS NOT NULL",
                (worker_id,),
            )
        }
        existing_total = int(
            conn.execute("SELECT COUNT(*) FROM subscription_accounts").fetchone()[0]
        )
        for provider, group in sorted(stored):
            if existing_total >= MAX_AUTO_ACCOUNTS:
                break
            if _ensure_account_row(
                conn, provider=provider, account_group_id=group, now=moment
            ):
                existing_total += 1
    _append_history(worker_id=worker_id, snapshots=clean, settings=settings, now=moment)
    return clean


def _ensure_account_row(
    conn, *, provider: str, account_group_id: str, now: float
) -> bool:
    """Завести запись, если её ещё нет. `True` — если действительно завели."""
    row = conn.execute(
        "SELECT account_id FROM subscription_accounts WHERE provider=? AND account_group_id=?",
        (provider, account_group_id),
    ).fetchone()
    if row is not None:
        return False
    conn.execute(
        """
        INSERT INTO subscription_accounts (
            account_id, provider, display_name, account_group_id, warning_days,
            policy_state, account_kind, created_at, updated_at
        ) VALUES (?,?,?,?,?,?,?,?,?)
        """,
        (
            str(uuid.uuid4()), provider, account_group_id, account_group_id,
            json.dumps(list(DEFAULT_WARNING_DAYS)),
            # Заводится как «требует решения»: комплаенс по учётной записи —
            # решение человека, и молчаливое `allowed` было бы этим решением
            # вместо него.
            "review_required", "unknown", now, now,
        ),
    )
    return True


def _append_history(
    *,
    worker_id: str,
    snapshots: list[dict[str, Any]],
    settings: DistributedWorkersSettings,
    now: float,
) -> None:
    """Записать историю — только при СМЕНЕ значимых полей либо по интервалу.

    Прямая реализация §24: каждая 30-секундная запись heartbeat в историю не
    попадает. Значимое — состояние, остаток, дата сброса и источник; всё
    остальное (например, `detail`) историю не двигает.
    """
    min_interval = max(0, int(settings.quota_history_min_interval_sec))
    with database.write_txn(settings) as conn:
        for snap in snapshots:
            quota = snap["quota"]
            group = snap["account_group_id"]
            account_id = None
            if group:
                row = conn.execute(
                    "SELECT account_id FROM subscription_accounts "
                    "WHERE provider=? AND account_group_id=?",
                    (snap["provider"], group),
                ).fetchone()
                account_id = row["account_id"] if row else None
            last = conn.execute(
                "SELECT observed_at, state, remaining_pct, reset_at, source "
                "FROM provider_quota_snapshots "
                "WHERE worker_id=? AND provider=? ORDER BY observed_at DESC LIMIT 1",
                (worker_id, snap["provider"]),
            ).fetchone()
            changed = True
            if last is not None:
                changed = not (
                    last["state"] == quota["quota_state"]
                    and _same_number(last["remaining_pct"], quota["estimated_remaining_pct"])
                    and _same_number(last["reset_at"], quota["next_reset_at"])
                    and last["source"] == quota["source"]
                )
                if not changed:
                    age = now - float(last["observed_at"] or 0.0)
                    if age < min_interval:
                        continue
            conn.execute(
                """
                INSERT INTO provider_quota_snapshots (
                    account_id, worker_id, provider, account_group_id, observed_at,
                    state, remaining_pct, reset_at, source, confidence, snapshot_json
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    account_id, worker_id, snap["provider"], group,
                    quota["observed_at"] or now, quota["quota_state"],
                    quota["estimated_remaining_pct"], quota["next_reset_at"],
                    quota["source"], quota["confidence"],
                    json.dumps(quota, ensure_ascii=False),
                ),
            )
        _prune_history(conn, settings=settings, now=now)


def _same_number(left: Any, right: Any) -> bool:
    if left is None and right is None:
        return True
    if left is None or right is None:
        return False
    try:
        return abs(float(left) - float(right)) < 0.01
    except (TypeError, ValueError):
        return False


def _prune_history(conn, *, settings: DistributedWorkersSettings, now: float) -> None:
    """Два независимых предела: по времени и по числу строк.

    Один без другого дырявый: только по времени — сбойный воркер, меняющий
    значение каждые полминуты, раздувает таблицу за сутки; только по числу —
    редкий воркер держит записи вечно.
    """
    days = max(1, int(settings.quota_history_retention_days))
    conn.execute(
        "DELETE FROM provider_quota_snapshots WHERE observed_at < ?",
        (now - days * 86400.0,),
    )
    cap = max(100, int(settings.quota_history_max_rows_per_account))
    rows = conn.execute(
        "SELECT worker_id, provider, COUNT(*) AS n FROM provider_quota_snapshots "
        "GROUP BY worker_id, provider HAVING n > ?",
        (cap,),
    ).fetchall()
    for row in rows:
        conn.execute(
            """
            DELETE FROM provider_quota_snapshots
            WHERE id IN (
                SELECT id FROM provider_quota_snapshots
                WHERE worker_id=? AND provider=?
                ORDER BY observed_at DESC LIMIT -1 OFFSET ?
            )
            """,
            (row["worker_id"], row["provider"], cap),
        )


# ─── Чтение ──────────────────────────────────────────────────────────────────
def list_worker_provider_states(
    *, settings: DistributedWorkersSettings, worker_id: Optional[str] = None
) -> list[dict[str, Any]]:
    with database.read_conn(settings) as conn:
        if worker_id:
            rows = conn.execute(
                "SELECT * FROM worker_provider_states WHERE worker_id=? ORDER BY provider",
                (worker_id,),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM worker_provider_states ORDER BY worker_id, provider"
            ).fetchall()
    out = []
    for row in rows:
        item = dict(row)
        item["capability"] = _loads(item.pop("capability_json", None), {})
        item["quota"] = _loads(item.pop("quota_json", None), None)
        item["inference_allowed"] = bool(item.get("inference_allowed"))
        item["credential_present"] = bool(item.get("credential_present"))
        item["credential_insecure"] = bool(item.get("credential_insecure"))
        out.append(item)
    return out


def list_accounts(*, settings: DistributedWorkersSettings) -> list[dict[str, Any]]:
    with database.read_conn(settings) as conn:
        rows = conn.execute(
            "SELECT * FROM subscription_accounts ORDER BY provider, account_group_id"
        ).fetchall()
    return [_account_row(row) for row in rows]


def get_account(
    account_id: str, *, settings: DistributedWorkersSettings
) -> Optional[dict[str, Any]]:
    with database.read_conn(settings) as conn:
        row = conn.execute(
            "SELECT * FROM subscription_accounts WHERE account_id=?", (account_id,)
        ).fetchone()
    return _account_row(row) if row is not None else None


def account_history(
    account_id: str, *, limit: int = 200, settings: DistributedWorkersSettings
) -> list[dict[str, Any]]:
    """История снимков квоты учётной записи, новые сверху."""
    capped = max(1, min(2000, int(limit)))
    with database.read_conn(settings) as conn:
        rows = conn.execute(
            "SELECT id, worker_id, provider, observed_at, state, remaining_pct, "
            "reset_at, source, confidence FROM provider_quota_snapshots "
            "WHERE account_id=? ORDER BY observed_at DESC LIMIT ?",
            (account_id, capped),
        ).fetchall()
    return [dict(row) for row in rows]


def _account_row(row) -> dict[str, Any]:
    item = dict(row)
    item["warning_days"] = _warning_days(item.get("warning_days"))
    item["operator_marked_unused"] = bool(item.get("operator_marked_unused"))
    return item


def _loads(raw: Any, default: Any) -> Any:
    if not raw:
        return default
    try:
        return json.loads(raw)
    except (TypeError, ValueError):
        return default


def _warning_days(raw: Any) -> list[int]:
    data = _loads(raw, list(DEFAULT_WARNING_DAYS))
    if not isinstance(data, list):
        return list(DEFAULT_WARNING_DAYS)
    out = sorted({
        int(x) for x in data
        if isinstance(x, (int, float)) and not isinstance(x, bool) and 0 < int(x) <= 365
    }, reverse=True)
    return out or list(DEFAULT_WARNING_DAYS)


# ─── Изменение (операторские действия) ───────────────────────────────────────
def upsert_account(
    *,
    provider: str,
    account_group_id: str,
    settings: DistributedWorkersSettings,
    display_name: Optional[str] = None,
    notes: Optional[str] = None,
    manual_reset_label: Optional[str] = None,
    manual_next_reset_at: Optional[float] = None,
    manual_reset_recurrence: Optional[str] = None,
    reset_timezone: Optional[str] = None,
    warning_days: Optional[list[int]] = None,
    operator_marked_unused: Optional[bool] = None,
    policy_state: Optional[str] = None,
    account_kind: Optional[str] = None,
    now: Optional[float] = None,
) -> dict[str, Any]:
    """Создать или обновить учётную запись. Только перечисленные поля.

    Переданное `None` означает «не трогать», а не «стереть»: частичное
    обновление из формы не должно молча обнулять соседние поля. Для явного
    стирания даты сброса есть отдельное значение — см. `clear_manual_reset`.
    """
    if provider not in PROVIDERS:
        raise ProviderAccountError(f"неизвестный провайдер {provider!r}")
    group = normalize_group_id(account_group_id)
    moment = float(now if now is not None else time.time())
    if policy_state is not None and policy_state not in POLICY_STATES:
        raise ProviderAccountError(f"policy_state={policy_state!r}")
    if account_kind is not None and account_kind not in ACCOUNT_KINDS:
        raise ProviderAccountError(f"account_kind={account_kind!r}")
    reset_at = _validate_manual_reset(manual_next_reset_at)

    with database.write_txn(settings) as conn:
        _ensure_account_row(conn, provider=provider, account_group_id=group, now=moment)
        row = conn.execute(
            "SELECT * FROM subscription_accounts WHERE provider=? AND account_group_id=?",
            (provider, group),
        ).fetchone()
        updates: dict[str, Any] = {"updated_at": moment}
        if display_name is not None:
            text = str(display_name).strip()[:_MAX_DISPLAY_NAME]
            if not text:
                raise ProviderAccountError("display_name не может быть пустым")
            updates["display_name"] = text
        if notes is not None:
            updates["notes"] = str(notes)[:_MAX_NOTES]
        if manual_reset_label is not None:
            updates["manual_reset_label"] = str(manual_reset_label).strip()[:_MAX_LABEL] or None
        if manual_next_reset_at is not None:
            updates["manual_next_reset_at"] = reset_at
        if manual_reset_recurrence is not None:
            updates["manual_reset_recurrence"] = _validate_recurrence(manual_reset_recurrence)
        if reset_timezone is not None:
            updates["reset_timezone"] = _validate_timezone(reset_timezone)
        if warning_days is not None:
            updates["warning_days"] = json.dumps(_validate_warning_days(warning_days))
        if operator_marked_unused is not None:
            updates["operator_marked_unused"] = 1 if operator_marked_unused else 0
        if policy_state is not None:
            updates["policy_state"] = policy_state
        if account_kind is not None:
            updates["account_kind"] = account_kind
        assignments = ", ".join(f"{k}=?" for k in updates)
        conn.execute(
            f"UPDATE subscription_accounts SET {assignments} WHERE account_id=?",
            (*updates.values(), row["account_id"]),
        )
        fresh = conn.execute(
            "SELECT * FROM subscription_accounts WHERE account_id=?", (row["account_id"],)
        ).fetchone()
    return _account_row(fresh)


def clear_manual_reset(
    account_id: str, *, settings: DistributedWorkersSettings, now: Optional[float] = None
) -> Optional[dict[str, Any]]:
    """Стереть ручную дату. Отдельная операция, а не `manual_next_reset_at=None`.

    Разница существенна: в `upsert_account` `None` значит «не трогать». Если бы
    он значил «стереть», любая форма, не заполнившая поле, молча удаляла бы
    дату, которую оператор ставил руками.
    """
    moment = float(now if now is not None else time.time())
    with database.write_txn(settings) as conn:
        conn.execute(
            "UPDATE subscription_accounts SET manual_next_reset_at=NULL, "
            "manual_reset_recurrence=NULL, updated_at=? WHERE account_id=?",
            (moment, account_id),
        )
    return get_account(account_id, settings=settings)


def set_worker_provider_group(
    *,
    worker_id: str,
    provider: str,
    account_group_id: Optional[str],
    settings: DistributedWorkersSettings,
    now: Optional[float] = None,
) -> dict[str, Any]:
    """Привязать провайдера воркера к учётной записи (§15). Решение оператора."""
    if provider not in PROVIDERS:
        raise ProviderAccountError(f"неизвестный провайдер {provider!r}")
    group = normalize_group_id(account_group_id, allow_empty=True)
    moment = float(now if now is not None else time.time())
    with database.write_txn(settings) as conn:
        exists = conn.execute(
            "SELECT 1 FROM worker_provider_states WHERE worker_id=? AND provider=?",
            (worker_id, provider),
        ).fetchone()
        if exists is None:
            # Воркер ещё не рассказал о провайдере — заводим заготовку, чтобы
            # привязку можно было сделать заранее, до первого heartbeat.
            conn.execute(
                "INSERT INTO worker_provider_states "
                "(worker_id, provider, account_group_id, account_group_source, "
                " reported_at) VALUES (?,?,?,'operator',?)",
                (worker_id, provider, group, moment),
            )
        else:
            conn.execute(
                "UPDATE worker_provider_states SET account_group_id=?, "
                "account_group_source='operator', reported_at=? "
                "WHERE worker_id=? AND provider=?",
                (group, moment, worker_id, provider),
            )
        if group:
            _ensure_account_row(conn, provider=provider, account_group_id=group, now=moment)
    return {"worker_id": worker_id, "provider": provider, "account_group_id": group}


def _validate_manual_reset(value: Any) -> Optional[float]:
    if value is None:
        return None
    number = _opt_float(value)
    if number is None:
        raise ProviderAccountError("manual_next_reset_at: ожидается unix-время в секундах")
    if number < 1_577_836_800 or number > 4_102_444_800:
        raise ProviderAccountError(
            "manual_next_reset_at вне разумного диапазона (2020..2100)"
        )
    return number


_RECURRENCE_ALLOWED = ("none", "daily", "weekly", "monthly", "every_5_hours")


def _validate_recurrence(value: Any) -> Optional[str]:
    text = str(value or "").strip().lower()
    if not text or text == "none":
        return None
    if text not in _RECURRENCE_ALLOWED:
        raise ProviderAccountError(
            f"recurrence: допустимы {_RECURRENCE_ALLOWED}"
        )
    return text


def _validate_timezone(value: Any) -> Optional[str]:
    text = str(value or "").strip()[:_MAX_TZ]
    if not text:
        return None
    try:
        from zoneinfo import ZoneInfo

        ZoneInfo(text)
    except Exception as exc:                            # noqa: BLE001
        raise ProviderAccountError(f"неизвестная таймзона {text!r}") from exc
    return text


def _validate_warning_days(value: Any) -> list[int]:
    if not isinstance(value, list):
        raise ProviderAccountError("warning_days: ожидается список целых чисел")
    out: set[int] = set()
    for item in value[:10]:
        if isinstance(item, bool) or not isinstance(item, (int, float)):
            raise ProviderAccountError("warning_days: только целые числа")
        day = int(item)
        if day <= 0 or day > 365:
            raise ProviderAccountError("warning_days: значения в диапазоне 1..365")
        out.add(day)
    if not out:
        raise ProviderAccountError("warning_days: список пуст")
    return sorted(out, reverse=True)
